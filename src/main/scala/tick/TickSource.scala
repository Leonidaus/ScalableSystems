package tick

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** Reads a trading-data CSV and yields only the price events. */
object TickSource {

  private val BufferSize = 1 << 20

  def lines(path: Path): Iterator[String] = {
    val reader = new BufferedReader(Files.newBufferedReader(path, StandardCharsets.UTF_8), BufferSize)
    Iterator.continually(reader.readLine()).takeWhile(_ != null)
  }

  def fromFile(path: Path): Iterator[TickEvent] =
    lines(path).flatMap(TickParser.parse)
}
