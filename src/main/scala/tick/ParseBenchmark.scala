package tick

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.util.Using

/**
 * Single-threaded parse benchmark
 */
object ParseBenchmark {

  private val BufferSize = 1 << 20
  private val Preview    = 5

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("usage: runMain tick.ParseBenchmark <trading-data.csv>")
      sys.exit(1)
    }
    run(Paths.get(args(0)))
  }

  private def run(path: Path): Unit = {
    var linesRead = 0L
    var kept      = 0L
    val t0        = System.nanoTime()

    Using.resource(open(path)) { in =>
      var line = in.readLine()
      while (line != null) {
        linesRead += 1
        TickParser.parse(line) match {
          case Some(ev) =>
            kept += 1
            if (kept <= Preview) println(s"$ev\n    <- $line")
          case None =>
        }
        line = in.readLine()
      }
    }

    val secs = (System.nanoTime() - t0) / 1e9
    println(f"lines read : $linesRead%,d")
    if (linesRead == 0) {
      println("events kept: 0 (empty input)")
      println(f"elapsed    : $secs%.1f s")
    } else {
      println(f"events kept: $kept%,d (${100.0 * kept / linesRead}%.1f%%)")
      if (secs > 0) println(f"elapsed    : $secs%.1f s  (${linesRead / secs}%,.0f lines/s)")
      else println(f"elapsed    : $secs%.1f s")
    }
  }

  private def open(path: Path): BufferedReader =
    new BufferedReader(Files.newBufferedReader(path, StandardCharsets.UTF_8), BufferSize)
}
