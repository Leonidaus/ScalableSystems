package tick

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import java.util.Collections
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.TopicExistsException
import scala.jdk.CollectionConverters._

/**
 * Streams the DEBS CSVs line by line into a Kafka topic, then loops.
 * Only trade lines are published (~22% of the real data), cut after the last
 * column the job reads; they stay raw CSV, so parsing stays in the Flink job.
 */
object TickPublisher {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic     = sys.env.getOrElse("TOPIC", "ticks")
    val inputDir  = Paths.get(sys.env.getOrElse("INPUT", "/mock_data"))
    val delayMs   = sys.env.getOrElse("DELAY_MS", "2").toLong
    // LOOP=false publishes the input once and exits (full-dataset runs).
    val loop      = sys.env.getOrElse("LOOP", "true").toBoolean
    // Caps the job's source parallelism: one Kafka partition per subtask at most.
    val partitions = sys.env.getOrElse("PARTITIONS", "12").toInt

    val csvs = listCsvs(inputDir)
    if (csvs.isEmpty) {
      println(s"no CSVs under $inputDir")
      sys.exit(1)
    }

    createTopic(bootstrap, topic, partitions)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // Bigger, compressed batches: the full dataset is millions of lines.
    props.put("linger.ms", "20")
    props.put("batch.size", "262144")
    props.put("compression.type", "lz4")

    val producer = new KafkaProducer[String, String](props)
    var pass = 0
    // The mock data is a single week, so the producer replays it forever to
    // keep the pipeline streaming. A real deployment would consume a live
    // feed instead of looping over files.
    do {
      pass += 1
      var sent = 0L
      // Streamed line by line: the full-day files are several GB.
      for (csv <- csvs) {
        val reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)
        try {
          var line = reader.readLine()
          while (line != null) {
            if (TickParser.parse(line).isDefined) {
              producer.send(new ProducerRecord[String, String](topic, trim(line)))
              sent += 1
            }
            if (delayMs > 0) Thread.sleep(delayMs)
            line = reader.readLine()
          }
        } finally reader.close()
      }
      producer.flush()
      println(s"[pass $pass] $sent trades published to '$topic'${if (loop) ", replaying" else ""}")
    } while (loop)
    producer.close()
  }

  /** The line up to the job's last column (Trading date, index 26); later columns are never read. */
  private[tick] def trim(line: String): String = {
    var end = -1
    var col = 0
    while (col <= 26) {
      end = line.indexOf(',', end + 1)
      if (end < 0) return line
      col += 1
    }
    line.substring(0, end)
  }

  /** All .csv files in a directory, sorted by name; or the file itself. */
  private def listCsvs(dir: Path): Seq[Path] =
    if (Files.isDirectory(dir))
      Files.list(dir).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".csv"))
        .toSeq
        .sorted
    else if (Files.exists(dir)) Seq(dir)
    else Seq.empty

  /** Creates the topic, retrying until the broker is up. */
  private def createTopic(bootstrap: String, topic: String, partitions: Int): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    var attempt = 0
    while (true) {
      val admin = AdminClient.create(props)
      try {
        admin
          .createTopics(Collections.singletonList(new NewTopic(topic, partitions, 1.toShort)))
          .all()
          .get()
        println(s"topic '$topic' created ($partitions partitions)")
        return
      } catch {
        case e: java.util.concurrent.ExecutionException
            if e.getCause.isInstanceOf[TopicExistsException] =>
          println(s"topic '$topic' already exists")
          return
        case _: Exception =>
          attempt += 1
          if (attempt >= 30) sys.error(s"kafka not reachable at $bootstrap")
          println(s"kafka not ready yet, retry $attempt")
          Thread.sleep(2000)
      } finally admin.close(Duration.ofSeconds(5))
    }
  }
}
