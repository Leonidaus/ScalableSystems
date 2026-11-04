package tick

import org.junit.Assert._
import org.junit.Test

class TickPublisherTest {

  // Full-width (39-column) trade line with the parser's columns set.
  private val line =
    Array.fill(39)("x")
      .updated(0, "RDSA.NL")
      .updated(1, "E")
      .updated(2, "08-11-2021")
      .updated(21, "20.5328")
      .updated(23, "09:00:48.9772")
      .updated(26, "08-11-2021")
      .mkString(",")

  @Test
  def trimKeepsColumnsUpToTradingDate(): Unit = {
    val trimmed = TickPublisher.trim(line)
    assertEquals(27, trimmed.split(",", -1).length)
    assertTrue(trimmed.endsWith(",08-11-2021"))
    assertEquals(TickParser.parse(line), TickParser.parse(trimmed))
  }

  @Test
  def trimLeavesShortLinesAlone(): Unit = {
    val short = line.split(",", -1).take(27).mkString(",")
    assertEquals(short, TickPublisher.trim(short))
  }
}
