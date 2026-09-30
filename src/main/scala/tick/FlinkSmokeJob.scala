package tick

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/** Per-symbol event count — the smoke test. */
final case class SymbolCount(symbol: String, events: Long)

/**
 * Minimal Flink job proving the toolchain end to end
 */
object FlinkSmokeJob {

  def main(args: Array[String]): Unit = {
    val input = if (args.nonEmpty) args(0) else "mock_data"

    // Bounded input, using batch mode
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    // How Flink serialises our Scala case classes — printed because it decides
    // throughput on the full dataset. GenericType == Kryo fallback.
    println(s"[types] TickEvent   -> ${TypeInformation.of(classOf[TickEvent])}")
    println(s"[types] SymbolCount -> ${TypeInformation.of(classOf[SymbolCount])}")

    val source = FileSource
      .forRecordStreamFormat(new TextLineInputFormat(), new Path(input))
      .build()

    val lines = env.fromSource(source, WatermarkStrategy.noWatermarks[String](), "csv-source")

    // Counts events per symbol, keyed by symbol.
    val counts = lines
      .flatMap(new FlatMapFunction[String, SymbolCount] {
        def flatMap(line: String, out: Collector[SymbolCount]): Unit =
          TickParser.parse(line).foreach(ev => out.collect(SymbolCount(ev.symbol, 1L)))
      })
      .keyBy(new KeySelector[SymbolCount, String] {
        def getKey(c: SymbolCount): String = c.symbol
      })
      .reduce(new ReduceFunction[SymbolCount] {
        def reduce(a: SymbolCount, b: SymbolCount): SymbolCount =
          SymbolCount(a.symbol, a.events + b.events)
      })

    counts.print()

    env.execute("flink-smoke-job")
  }
}
