package tick

import java.nio.charset.StandardCharsets
import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.KafkaSinkContext
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * Query 1: 5-minute tumbling event-time windows per symbol
 * (first window starts at 00:00 CEST — epoch-based timestamps align to it).
 * Each window emits the highest-timestamp tick it contains, i.e. the window's
 * close; a keyed fold then chains closes into the spec's EMA38/EMA100 per
 * symbol, skipping closes whose window a replay pass has already folded.
 * Query 2: the fold also flags EMA38/EMA100 crossovers as BUY/SELL advisories.
 * Every snapshot goes to the `ema-results` topic, crossovers alone to
 * `advisories`; both keyed by symbol, JSON values ([[EmaResultJson]]).
 */
object EmaJob {

  def main(args: Array[String]): Unit = {
    val bootstrap     = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic         = sys.env.getOrElse("TOPIC", "ticks")
    val emaTopic      = sys.env.getOrElse("EMA_TOPIC", "ema-results")
    val advisoryTopic = sys.env.getOrElse("ADVISORY_TOPIC", "advisories")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource
      .builder[String]()
      .setBootstrapServers(bootstrap)
      .setTopics(topic)
      .setGroupId("tick-job")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val results = build(env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafka-ticks"))
    results.sinkTo(kafkaSink(bootstrap, emaTopic)).name("ema-results-sink")
    results
      .filter(_.advisory.nonEmpty)
      .name("crossovers")
      .sinkTo(kafkaSink(bootstrap, advisoryTopic))
      .name("advisories-sink")

    env.execute("ema-job")
  }

  /** Query 1 pipeline: raw CSV lines → per-symbol EMA snapshots. */
  def build(lines: DataStream[String]): DataStream[EmaResult] =
    lines
      .flatMap(new FlatMapFunction[String, TickEvent] {
        override def flatMap(line: String, out: Collector[TickEvent]): Unit =
          TickParser.parse(line).foreach(out.collect)
      })
      .name("parse-ticks")
      // Event time from the tick's own CEST timestamp. Ticks arrive out of
      // order: the task says per-symbol order cannot be assumed, and Kafka's
      // round-robin partitioning shuffles arrivals too.
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[TickEvent](Duration.ofSeconds(5))
          // With -p 4 and only 3 Kafka partitions, one source subtask is
          // idle and would otherwise pin the watermark at Long.MinValue,
          // freezing every window. Idleness excludes such channels from the
          // downstream watermark minimum.
          .withIdleness(Duration.ofSeconds(10))
          .withTimestampAssigner(new SerializableTimestampAssigner[TickEvent] {
            override def extractTimestamp(e: TickEvent, recordTs: Long): Long = e.timestamp
          })
      )
      .keyBy(_.symbol)
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      // The producer replays the same week over and over; 7 days of lateness
      // keeps already-fired windows re-firable so the demo stays continuous
      // (identical data re-emits identical closes). On the full dataset,
      // consumed once, this has no effect.
      .allowedLateness(Duration.ofDays(7))
      .reduce((a: TickEvent, b: TickEvent) => if (a.timestamp >= b.timestamp) a else b)
      .name("5min-close")
      // Chained per-symbol EMA38/EMA100 over the window closes. Dedupes by
      // window start: the producer's replay loop re-fires windows (7-day
      // lateness), and folding a window twice would corrupt the chain.
      .keyBy(_.symbol)
      .flatMap(new KeyedEmaFold)
      .name("ema-38-100")

  /**
   * JSON records keyed by symbol, so each symbol's results stay ordered within
   * a partition. AT_LEAST_ONCE only flushes on checkpoints, which are off, so
   * delivery is effectively best effort; a restarted job recomputes from the
   * earliest offset, and consumers dedupe by (symbol, window_start).
   */
  private def kafkaSink(bootstrap: String, topic: String): KafkaSink[EmaResult] =
    KafkaSink
      .builder[EmaResult]()
      .setBootstrapServers(bootstrap)
      .setRecordSerializer(new KafkaRecordSerializationSchema[EmaResult] {
        // No record timestamp, so the producer stamps wall-clock time. The
        // builder's schema would carry the 2021 event time over, and Kafka's
        // 7-day time retention then deletes every record as soon as its
        // segment rolls.
        override def serialize(
            r: EmaResult,
            ctx: KafkaSinkContext,
            timestamp: java.lang.Long
        ): ProducerRecord[Array[Byte], Array[Byte]] =
          new ProducerRecord(
            topic,
            r.symbol.getBytes(StandardCharsets.UTF_8),
            EmaResultJson(r).getBytes(StandardCharsets.UTF_8)
          )
      })
      .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
}
