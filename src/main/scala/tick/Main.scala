package tick

import java.nio.file.Paths

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("usage: run <trading-data.csv[.gz]>")
      sys.exit(1)
    }
    val path = Paths.get(args(0))

    var linesRead = 0L
    var kept      = 0L
    val preview   = 5
    val t0        = System.nanoTime()

    TickSource.lines(path).foreach { line =>
      linesRead += 1
      TickParser.parse(line) match {
        case Some(ev) =>
          kept += 1
          if (kept <= preview) println(s"$ev\n    <- $line")
        case None =>
      }
    }

    val secs = (System.nanoTime() - t0) / 1e9
    println(f"lines read : $linesRead%,d")
    println(f"events kept: $kept%,d (${100.0 * kept / linesRead}%.1f%%)")
    println(f"elapsed    : $secs%.1f s  (${linesRead / secs}%,.0f lines/s)")
  }
}
