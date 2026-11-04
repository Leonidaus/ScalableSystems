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
      .updated(3, "09:00:48.2803")
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

  @Test
  def arrivalMillisReadsSystemDateAndTime(): Unit = {
    val expected = java.time.LocalDateTime.of(2021, 11, 8, 9, 0, 48, 280000000)
      .atZone(java.time.ZoneId.of("Europe/Paris")).toInstant.toEpochMilli
    assertEquals(expected, TickParser.arrivalMillis(line))
  }

  @Test
  def arrivalMillisMalformedIsMinusOne(): Unit = {
    assertEquals(-1L, TickParser.arrivalMillis(line.replace("09:00:48.2803", "x")))
    assertEquals(-1L, TickParser.arrivalMillis("ID,SecType,Date,Time"))
    assertEquals(-1L, TickParser.arrivalMillis("# comment"))
  }

  @Test
  def parseSpeedAcceptsMaxAndPositiveNumbers(): Unit = {
    assertEquals(0.0, TickPublisher.parseSpeed("max"), 0.0)
    assertEquals(0.0, TickPublisher.parseSpeed("MAX"), 0.0)
    assertEquals(0.0, TickPublisher.parseSpeed("0"), 0.0)
    assertEquals(2.5, TickPublisher.parseSpeed("2.5"), 0.0)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def parseSpeedRejectsNegative(): Unit = TickPublisher.parseSpeed("-1")

  @Test(expected = classOf[IllegalArgumentException])
  def parseSpeedRejectsGarbage(): Unit = TickPublisher.parseSpeed("fast")
}
