package tick

import java.time.Instant

/**
 * Hand-rolled JSON for [[EmaResult]], the value format of both output topics.
 * Timestamps are ISO-8601 UTC; doubles use Double.toString, which is valid JSON
 * for the finite values the fold emits.
 */
object EmaResultJson {

  def apply(r: EmaResult): String =
    s"""{"symbol":${quote(r.symbol)},""" +
      s""""window_start":"${Instant.ofEpochMilli(r.windowStart)}",""" +
      s""""window_end":"${Instant.ofEpochMilli(r.windowEnd)}",""" +
      s""""close":${r.close},"ema38":${r.ema38},"ema100":${r.ema100},""" +
      s""""advisory":${quote(r.advisory)}}"""

  private def quote(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
}
