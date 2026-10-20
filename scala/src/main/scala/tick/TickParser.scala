package tick

import java.time.{LocalDate, ZoneId}
import java.util.concurrent.ConcurrentHashMap

/**
 * Parses one raw CSV line of the DEBS 2022 trading data into a [[TickEvent]].
 * Only the fields ID, SecType, Date, Last, Trading time and Trading date are looked at.
 */
object TickParser {

  private val Zone = ZoneId.of("Europe/Paris")

  // Column indices in the raw file.
  private val IdCol          = 0
  private val SecTypeCol     = 1
  private val DateCol        = 2
  private val LastCol        = 21
  private val TradingTimeCol = 23
  private val TradingDateCol = 26

  def parse(line: String): Option[TickEvent] = {
    if (line.isEmpty || line.charAt(0) == '#' || line.startsWith("ID,")) return None

    // Walk the fields left to right, remembering only the ones we need.
    var symbol: String      = null
    var secType: Char       = 0
    var date: String        = null
    var last: String        = null
    var tradingTime: String = null
    var tradingDate: String = null

    var col   = 0
    var start = 0
    val len   = line.length
    while (col <= TradingDateCol && start <= len) {
      var end = line.indexOf(',', start)
      if (end < 0) end = len
      col match {
        case IdCol          => symbol = line.substring(start, end)
        case SecTypeCol     => if (end > start) secType = line.charAt(start)
        case DateCol        => date = line.substring(start, end)
        case LastCol        => if (end == start) return None else last = line.substring(start, end)
        case TradingTimeCol => tradingTime = line.substring(start, end)
        case TradingDateCol => tradingDate = line.substring(start, end)
        case _              =>
      }
      col += 1
      start = end + 1
    }
    if (col <= TradingDateCol || symbol.isEmpty || tradingTime.isEmpty) return None

    val effectiveDate = if (tradingDate.isEmpty) date else tradingDate
    val price         = parseDouble(last)
    val timeMillis    = parseTimeMillis(tradingTime)
    val dayMillis     = dayStartMillis(effectiveDate)
    // A zero or NaN price is not a trade.
    if (!(price > 0) || timeMillis < 0 || dayMillis == Long.MinValue) None
    else Some(TickEvent(symbol, secType, price, effectiveDate, tradingTime, dayMillis + timeMillis))
  }

  private def parseDouble(s: String): Double =
    try s.toDouble catch { case _: NumberFormatException => Double.NaN }

  // Millis since midnight, or -1 if malformed.
  private def parseTimeMillis(t: String): Long = {
    if (t.length < 8 || t.charAt(2) != ':' || t.charAt(5) != ':') return -1
    val h = digits2(t, 0);
    val m = digits2(t, 3);
    val s = digits2(t, 6)
    if (h < 0 || m < 0 || s < 0) return -1
    var ms = 0
    if (t.length > 9 && t.charAt(8) == '.') {
      var i = 9; var scale = 100
      while (i < t.length && scale > 0) {
        val d = t.charAt(i) - '0'
        if (d < 0 || d > 9) return -1
        ms += d * scale; scale /= 10; i += 1
      }
    }
    ((h * 60L + m) * 60L + s) * 1000L + ms
  }

  private def digits2(s: String, at: Int): Int = {
    val a = s.charAt(at) - '0'; val b = s.charAt(at + 1) - '0'
    if (a < 0 || a > 9 || b < 0 || b > 9) -1 else a * 10 + b
  }

  // The file has one date per file (plus a handful of neighbours), so cache midnight per date string.
  // Concurrent: this object is shared by every parsing subtask in a TaskManager JVM.
  private val dayCache = new ConcurrentHashMap[String, java.lang.Long]()

  // "DD-MM-YYYY" -> epoch millis of local midnight in Europe/Paris, or Long.MinValue if malformed.
  private def dayStartMillis(d: String): Long =
    dayCache.computeIfAbsent(d, d => {
      if (d.length != 10 || d.charAt(2) != '-' || d.charAt(5) != '-') Long.MinValue
      else
        try {
          val day   = d.substring(0, 2).toInt
          val month = d.substring(3, 5).toInt
          val year  = d.substring(6, 10).toInt
          LocalDate.of(year, month, day).atStartOfDay(Zone).toInstant.toEpochMilli
        } catch { case _: RuntimeException => Long.MinValue }
    })
}
