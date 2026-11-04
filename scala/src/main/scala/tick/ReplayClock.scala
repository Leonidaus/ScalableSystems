package tick

/**
 * Paces a replay by the data's own arrival timestamps, `speed` times faster
 * than real time; speed <= 0 means no pacing at all. Every event's due time is
 * measured from one anchor (first event, first wall instant), so rounding never
 * accumulates. Data gaps longer than `maxGapMs` (nights, the weekend) are
 * shortened to it; 0 keeps them as they are. Events older than the newest one
 * seen, even by hours (real data has such stale rows), are due at once and
 * leave the schedule untouched, so input must be in date order.
 */
final class ReplayClock(
    speed: Double,
    maxGapMs: Long,
    nanoTime: () => Long = () => System.nanoTime(),
    sleep: Long => Unit = Thread.sleep
) {
  private var anchored  = false
  private var dataStart = 0L
  private var wallStart = 0L
  private var dataMax   = 0L
  private var skippedMs = 0L
  private var lag       = 0L

  /** Blocks until the event that arrived at `arrivalMs` is due; -1 (unknown) passes straight through. */
  def await(arrivalMs: Long): Unit = {
    if (speed <= 0 || arrivalMs < 0) return
    if (!anchored) {
      anchored = true
      dataStart = arrivalMs; dataMax = arrivalMs
      wallStart = nanoTime()
      skippedMs = 0
      lag = 0
      return
    }
    // Slightly out of order: an event at or before one already sent is due now.
    if (arrivalMs <= dataMax) return
    val gap = arrivalMs - dataMax
    if (maxGapMs > 0 && gap > maxGapMs) skippedMs += gap - maxGapMs
    dataMax = arrivalMs

    val due   = wallStart + ((arrivalMs - dataStart - skippedMs) * 1e6 / speed).toLong
    val ahead = due - nanoTime()
    lag = math.max(0L, -ahead) / 1000000
    if (ahead >= 1000000) sleep(ahead / 1000000)
  }

  /** How far (ms) the last paced event was behind its due time. */
  def lagMs: Long = lag

  /** Forgets the anchor, e.g. when a loop pass restarts at the first file. */
  def reset(): Unit = {
    anchored = false
    skippedMs = 0
    lag = 0
  }
}
