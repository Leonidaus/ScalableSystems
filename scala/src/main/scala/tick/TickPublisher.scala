package tick

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.{Duration, Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Collections
import java.util.Properties
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.errors.TopicExistsException
import scala.jdk.CollectionConverters._

/**
 * Streams the DEBS CSVs line by line into a Kafka topic.
 * Only trade lines are published, cut after the last column the job reads;
 * they stay raw CSV, so parsing stays in the Flink job.
 */
object TickPublisher {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic     = sys.env.getOrElse("TOPIC", "ticks")
    val inputDir  = Paths.get(sys.env.getOrElse("INPUT", "/data/mock"))
    val speed     = parseSpeed(sys.env.getOrElse("SPEED", "500"))
    // Data gaps longer than this (nights, the weekend) are shortened to it; 0 keeps them.
    val maxGapS   = sys.env.getOrElse("MAX_GAP_S", "300").toLong
    val loop      = sys.env.getOrElse("LOOP", "false").toBoolean
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
    val clock    = new ReplayClock(speed, maxGapS * 1000)
    println(
      if (speed > 0) s"publishing at ${speed}x the data's pace, gaps capped at ${maxGapS}s"
      else "publishing at max speed")
    var pass = 0
    do {
      pass += 1
      clock.reset()
      var sent = 0L
      var arrival = -1L
      var lastReport = System.nanoTime()
      var sentAtReport = 0L
      // Streamed line by line: the full-day files are several GB.
      for (csv <- csvs) {
        val reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)
        try {
          var line = reader.readLine()
          while (line != null) {
            if (TickParser.parse(line).isDefined) {
              if (speed > 0) {
                arrival = TickParser.arrivalMillis(line)
                clock.await(arrival)
              }
              producer.send(new ProducerRecord[String, String](topic, trim(line)))
              sent += 1
              val now = System.nanoTime()
              if (now - lastReport >= ReportEveryNanos) {
                val rate = (sent - sentAtReport) * 1e9 / (now - lastReport)
                val paced =
                  if (speed > 0 && arrival >= 0)
                    s", data time ${ReportTime.format(Instant.ofEpochMilli(arrival))}, lag ${clock.lagMs} ms"
                  else ""
                println(f"[replay] $sent%d sent, $rate%.0f/s$paced")
                lastReport = now
                sentAtReport = sent
              }
            }
            line = reader.readLine()
          }
        } finally reader.close()
      }
      producer.flush()
      println(s"[pass $pass] $sent trades published to '$topic'${if (loop) ", replaying" else ""}")
    } while (loop)
    producer.close()
  }

  private val ReportEveryNanos = 10L * 1000000000
  private val ReportTime = DateTimeFormatter.ofPattern("dd-MM HH:mm:ss").withZone(ZoneId.of("Europe/Paris"))

  /** SPEED: "max" or 0 for no pacing, otherwise a positive factor over real time. */
  private[tick] def parseSpeed(s: String): Double = {
    val v =
      if (s.trim.equalsIgnoreCase("max")) 0.0
      else s.trim.toDoubleOption.getOrElse(Double.NaN)
    if (!(v >= 0) || v.isInfinite)
      throw new IllegalArgumentException(s"SPEED must be 'max' or a number >= 0, got '$s'")
    v
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
