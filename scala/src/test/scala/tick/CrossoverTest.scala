package tick

import org.junit.Assert._
import org.junit.Test

/** Spec Eqs. 1–4 on synthetic EMA pairs: (prev38, prev100) → (ema38, ema100). */
class CrossoverTest {

  private def cross(prev38: Double, prev100: Double, ema38: Double, ema100: Double): String =
    EmaMath.crossover(prev38, prev100, ema38, ema100)

  @Test
  def bullishCrossIsBuy(): Unit =
    assertEquals(Advisory.Buy, cross(1.0, 2.0, 3.0, 2.5))

  @Test
  def bearishCrossIsSell(): Unit =
    assertEquals(Advisory.Sell, cross(3.0, 2.0, 1.0, 1.5))

  @Test
  def staysAboveOrBelowIsNothing(): Unit = {
    assertEquals(Advisory.None, cross(3.0, 2.0, 3.5, 2.1))
    assertEquals(Advisory.None, cross(1.0, 2.0, 1.5, 2.1))
  }

  @Test
  def equalBeforeCountsAsStartingSide(): Unit = {
    // ≤ / ≥ at w−1: leaving equality fires in either direction.
    assertEquals(Advisory.Buy, cross(2.0, 2.0, 2.1, 2.0))
    assertEquals(Advisory.Sell, cross(2.0, 2.0, 1.9, 2.0))
    // The EMA_0 = 0 seed is this case: the first window is a BUY.
    assertEquals(Advisory.Buy, cross(0.0, 0.0, 100.0 * 2 / 39, 100.0 * 2 / 101))
  }

  @Test
  def reachingEqualityIsNothing(): Unit = {
    // Strict > / < at w: touching without crossing never fires.
    assertEquals(Advisory.None, cross(3.0, 2.0, 2.0, 2.0))
    assertEquals(Advisory.None, cross(1.0, 2.0, 2.0, 2.0))
    assertEquals(Advisory.None, cross(2.0, 2.0, 2.0, 2.0))
  }

  @Test
  def repeatedCrossingsAlternate(): Unit = {
    // EMA38 − EMA100 per window: −1, +1, +2, −1, −1, 0, +1, −2.
    val series = Seq((1.0, 2.0), (3.0, 2.0), (4.0, 2.0), (1.0, 2.0), (1.0, 2.0), (2.0, 2.0), (3.0, 2.0), (0.0, 2.0))
    val advisories = series.sliding(2).map { case Seq((p38, p100), (e38, e100)) => cross(p38, p100, e38, e100) }.toSeq
    assertEquals(Seq("BUY", "", "SELL", "", "", "BUY", "SELL"), advisories)
  }
}
