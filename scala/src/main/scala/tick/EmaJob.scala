package tick

import java.nio.charset.StandardCharsets
import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerRecord

object EmaJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val source = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setTopics("ticks")
      .setGroupId("tick-job")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val lines = env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "kafka-ticks")
    val results = build(lines)
    results.sinkTo(kafkaSink("ema-results"))
    results.filter(_.advisory.nonEmpty).sinkTo(kafkaSink("advisories"))

    env.execute("ema-job")
  }

  def build(lines: DataStream[String]): DataStream[EmaResult] =
    lines
      .flatMap(new FlatMapFunction[String, TickEvent] {
        override def flatMap(line: String, out: Collector[TickEvent]): Unit =
          TickParser.parse(line).foreach(out.collect)
      })
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness[TickEvent](Duration.ofSeconds(5))
          // an idle subtask would pin the watermark at Long.MinValue
          .withIdleness(Duration.ofSeconds(10))
          .withTimestampAssigner(new SerializableTimestampAssigner[TickEvent] {
            override def extractTimestamp(e: TickEvent, recordTs: Long): Long = e.timestamp
          })
      )
      .keyBy(_.symbol)
      .window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
      .reduce((a, b) => if (a.timestamp >= b.timestamp) a else b)
      .keyBy(_.symbol)
      .flatMap(new KeyedEmaFold)

  private def kafkaSink(topic: String): KafkaSink[EmaResult] =
    KafkaSink.builder[EmaResult]()
      .setBootstrapServers("kafka:9092")
      .setRecordSerializer(new KafkaRecordSerializationSchema[EmaResult] {
        override def serialize(
            r: EmaResult,
            ctx: KafkaRecordSerializationSchema.KafkaSinkContext,
            timestamp: java.lang.Long
        ): ProducerRecord[Array[Byte], Array[Byte]] =
          new ProducerRecord(
            topic,
            r.symbol.getBytes(StandardCharsets.UTF_8),
            EmaResultJson(r).getBytes(StandardCharsets.UTF_8)
          )
      })
      .build()
}
