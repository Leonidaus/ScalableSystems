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
 * Streams the mock DEBS CSVs line by line into a Kafka topic, then loops.
 * Raw CSV lines are published as-is; parsing stays in the Flink job.
 */
object TickPublisher {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic     = sys.env.getOrElse("TOPIC", "ticks")
    val inputDir  = Paths.get(sys.env.getOrElse("INPUT", "/mock_data"))
    val delayMs   = sys.env.getOrElse("DELAY_MS", "2").toLong

    val csvs = listCsvs(inputDir)
    if (csvs.isEmpty) {
      println(s"no CSVs under $inputDir")
      sys.exit(1)
    }

    createTopic(bootstrap, topic)

    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    var pass = 0
    // The mock data is a single week, so the producer replays it forever to
    // keep the pipeline streaming. A real deployment would consume a live
    // feed instead of looping over files.
    while (true) {
      pass += 1
      var sent = 0
      for (csv <- csvs; line <- Files.readAllLines(csv, StandardCharsets.UTF_8).asScala) {
        if (line.nonEmpty) {
          producer.send(new ProducerRecord[String, String](topic, line))
          sent += 1
        }
        Thread.sleep(delayMs)
      }
      producer.flush()
      println(s"[pass $pass] $sent lines published to '$topic', replaying")
    }
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

  /** Creates the topic with 3 partitions, retrying until the broker is up. */
  private def createTopic(bootstrap: String, topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrap)
    var attempt = 0
    while (true) {
      val admin = AdminClient.create(props)
      try {
        admin
          .createTopics(Collections.singletonList(new NewTopic(topic, 3, 1.toShort)))
          .all()
          .get()
        println(s"topic '$topic' created (3 partitions)")
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
