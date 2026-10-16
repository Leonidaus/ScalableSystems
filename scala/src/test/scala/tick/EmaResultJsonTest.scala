package tick

import org.junit.Assert._
import org.junit.Test

class EmaResultJsonTest {

  @Test
  def serializesAllFieldsWithIsoUtcTimestamps(): Unit = {
    // 2021-11-08 09:00 CET = 08:00 UTC.
    val r = EmaResult("RDSA.NL", 1636358400000L, 1636358700000L, 20.5, 1.25, 0.5, Advisory.Buy)
    assertEquals(
      """{"symbol":"RDSA.NL","window_start":"2021-11-08T08:00:00Z","window_end":"2021-11-08T08:05:00Z",""" +
        """"close":20.5,"ema38":1.25,"ema100":0.5,"advisory":"BUY"}""",
      EmaResultJson(r)
    )
  }

  @Test
  def escapesQuotesAndBackslashesInStrings(): Unit = {
    val r = EmaResult("A\"B\\C", 0L, 300000L, 1.0, 1.0, 1.0, Advisory.None)
    assertTrue(EmaResultJson(r).startsWith("""{"symbol":"A\"B\\C","""))
    assertTrue(EmaResultJson(r).endsWith(""""advisory":""}"""))
  }
}
