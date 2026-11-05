package tick

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/** Bounded local run of both queries, no Kafka. Usage: LocalRun <input.csv> <output.csv> */
object LocalRun {

  def main(args: Array[String]): Unit = {
    val in  = Paths.get(args(0))
    val out = Paths.get(args(1))
    Files.deleteIfExists(out)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val lines   = env.fromCollection(Files.readAllLines(in, StandardCharsets.UTF_8))
    val results = EmaJob.build(lines).executeAndCollect()

    val w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)
    try
      while (results.hasNext) {
        val r = results.next()
        w.write(s"${r.symbol},${r.windowStart},${r.windowEnd},${r.close},${r.ema38},${r.ema100},${r.advisory}\n")
      }
    finally {
      results.close()
      w.close()
    }
  }
}
