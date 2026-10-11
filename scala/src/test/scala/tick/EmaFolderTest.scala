package tick

import org.junit.Assert._
import org.junit.Test

/**
 * Hand-computed fixtures for the spec formula
 * `EMA_j(w) = Close(w)·2/(1+j) + EMA_j(w−1)·(1−2/(1+j))`, EMA_0 = 0,
 * over closes 100 → 200 → 150:
 *
 *   w1: ema38 = 100·2/39,               ema100 = 100·2/101
 *   w2: ema38 = 200·2/39 + (200/39)·(37/39),
 *       ema100 = 200·2/101 + (200/101)·(99/101)
 */
class EmaFolderTest {

  // Monday 2021-11-08 09:00/09:05/09:10 Europe/Paris (CET), 5-min aligned.
  private val W1 = 1636358400000L
  private val W2 = 1636358700000L
  private val W3 = 1636359000000L

  private def tick(symbol: String, ts: Long, last: Double): TickEvent =
    TickEvent(symbol, 'E', last, "08-11-2021", "09:00:00.000", ts)

  @Test
  def firstWindowSeedsEmaToZero(): Unit = {
    val r = new EmaFolder().fold(tick("MOCK.NL", W1, 100.0)).get
    assertEquals("MOCK.NL", r.symbol)
    assertEquals(W1, r.windowStart)
    assertEquals(W1 + 300000L, r.windowEnd)
    assertEquals(100.0, r.close, 0.0)
    // Seed EMA_0 = 0: first EMA is just close·2/(1+j).
    assertEquals(100.0 * 2.0 / 39.0, r.ema38, 1e-12)
    assertEquals(100.0 * 2.0 / 101.0, r.ema100, 1e-12)
  }

  @Test
  def chainMatchesHandComputedSeries(): Unit = {
    val folder = new EmaFolder
    val r1 = folder.fold(tick("MOCK.NL", W1, 100.0)).get
    val r2 = folder.fold(tick("MOCK.NL", W2, 200.0)).get
    val r3 = folder.fold(tick("MOCK.NL", W3, 150.0)).get

    assertEquals(100.0 * 2.0 / 39.0, r1.ema38, 1e-12)
    assertEquals(100.0 * 2.0 / 101.0, r1.ema100, 1e-12)

    val w2_38  = 200.0 * 2.0 / 39.0 + (200.0 / 39.0) * (37.0 / 39.0)
    val w2_100 = 200.0 * 2.0 / 101.0 + (200.0 / 101.0) * (99.0 / 101.0)
    assertEquals(w2_38, r2.ema38, 1e-12)
    assertEquals(w2_100, r2.ema100, 1e-12)

    val w3_38  = 150.0 * 2.0 / 39.0 + w2_38 * (37.0 / 39.0)
    val w3_100 = 150.0 * 2.0 / 101.0 + w2_100 * (99.0 / 101.0)
    assertEquals(w3_38, r3.ema38, 1e-12)
    assertEquals(w3_100, r3.ema100, 1e-12)
  }

  @Test
  def unalignedTickFoldsIntoRoundedWindowStart(): Unit = {
    // 09:03:17.123 belongs to the 09:00 window.
    val ts = W1 + 3L * 60000L + 17123L
    val r  = new EmaFolder().fold(tick("MOCK.NL", ts, 100.0)).get
    assertEquals(W1, r.windowStart)
    assertEquals(W1 + 300000L, r.windowEnd)
  }

  @Test
  def duplicateWindowIsSkippedEvenOutOfOrder(): Unit = {
    val folder = new EmaFolder
    // Immediate repeat and out-of-order repeat of an already-folded window.
    assertTrue(folder.fold(tick("MOCK.NL", W1, 100.0)).isDefined)
    assertTrue(folder.fold(tick("MOCK.NL", W2, 200.0)).isDefined)
    assertTrue(folder.fold(tick("MOCK.NL", W1 + 1, 111.0)).isEmpty)
    assertTrue(folder.fold(tick("MOCK.NL", W1, 111.0)).isEmpty)
    // The skipped closes must not have advanced the chain.
    val r = folder.fold(tick("MOCK.NL", W3, 150.0)).get
    val w2_38 = 200.0 * 2.0 / 39.0 + (200.0 / 39.0) * (37.0 / 39.0)
    assertEquals(150.0 * 2.0 / 39.0 + w2_38 * (37.0 / 39.0), r.ema38, 1e-12)
  }

  @Test
  def foldersAreIndependentPerSymbol(): Unit = {
    val a = new EmaFolder
    val b = new EmaFolder
    a.fold(tick("MOCK.NL", W1, 100.0))
    b.fold(tick("RDSA.NL", W1, 22.0))
    a.fold(tick("MOCK.NL", W2, 200.0))
    val rb = b.fold(tick("RDSA.NL", W2, 23.0)).get
    // b only ever saw its own two closes.
    assertEquals(22.0 * 2.0 / 39.0 * (37.0 / 39.0) + 23.0 * 2.0 / 39.0, rb.ema38, 1e-12)
  }
}
