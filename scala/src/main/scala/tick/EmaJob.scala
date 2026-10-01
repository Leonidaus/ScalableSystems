package tick

import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector

/**
 * Query 1 skeleton: 5-minute tumbling event-time windows per symbol
 * (first window starts at 00:00 CEST — epoch-based timestamps align to it).
 * Each window emits the highest-timestamp tick it contains, i.e. the window's
 * close. The EMA state comes next.
 */
object EmaJob {

  def main(args: Array[String]): Unit = {
    val bootstrap = sys.env.getOrElse("KAFKA_BOOTSTRAP", "kafka:9092")
    val topic     = sys.env.getOrElse("TOPIC", "ticks")

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource
      .builder[String]()
      .setBootstrapServers(bootstrap)
      .setTopics(topic)
      .setGroupId("tick-job")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    env
      .fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafka-ticks")
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
      .print()

    env.execute("ema-job")
  }
}
