package tick

import org.junit.Assert._
import org.junit.Test
import scala.collection.mutable.ArrayBuffer

class ReplayClockTest {

  // Fake wall clock in nanos; sleeping advances it.
  private var now   = 0L
  private val slept = ArrayBuffer.empty[Long]
  private def clock(speed: Double, maxGapMs: Long = 0) =
    new ReplayClock(speed, maxGapMs, () => now, ms => { slept += ms; now += ms * 1000000 })

  private val t0 = 1636358400000L // 08-11-2021 09:00 CET

  @Test
  def maxSpeedNeverSleeps(): Unit = {
    val c = clock(0)
    c.await(t0); c.await(t0 + 60000)
    assertTrue(slept.isEmpty)
  }

  @Test
  def realTimeSleepsDataDelta(): Unit = {
    val c = clock(1)
    c.await(t0); c.await(t0 + 500)
    assertEquals(Seq(500L), slept.toSeq)
  }

  @Test
  def speedDividesDelay(): Unit = {
    val c = clock(10)
    c.await(t0); c.await(t0 + 10000)
    assertEquals(Seq(1000L), slept.toSeq)
  }

  @Test
  def backwardsArrivalDoesNotSleep(): Unit = {
    val c = clock(1)
    c.await(t0); c.await(t0 + 1000); c.await(t0 + 400)
    assertEquals(Seq(1000L), slept.toSeq)
  }

  @Test
  def longGapIsCompressed(): Unit = {
    val c = clock(1, maxGapMs = 60000)
    c.await(t0); c.await(t0 + 15L * 3600 * 1000)
    assertEquals(Seq(60000L), slept.toSeq)
  }

  @Test
  def behindScheduleDoesNotSleepAndReportsLag(): Unit = {
    val c = clock(1)
    c.await(t0)
    now += 2000L * 1000000
    c.await(t0 + 500)
    assertTrue(slept.isEmpty)
    assertEquals(1500L, c.lagMs)
  }

  @Test
  def subMillisecondAheadDoesNotSleep(): Unit = {
    val c = clock(1000)
    c.await(t0); c.await(t0 + 1)
    assertTrue(slept.isEmpty)
  }

  @Test
  def unknownArrivalDoesNotSleep(): Unit = {
    val c = clock(1)
    c.await(t0); c.await(-1); c.await(t0 + 500)
    assertEquals(Seq(500L), slept.toSeq)
  }

  @Test
  def staleRowDoesNotDelayNextRow(): Unit = {
    // Real data: around the Xetra close some trades carry a Time hours behind their neighbours.
    val c = clock(1, maxGapMs = 300000)
    val staleBy = (5 * 60 + 21) * 60 * 1000L
    c.await(t0); c.await(t0 + 1000); c.await(t0 + 1000 - staleBy); c.await(t0 + 1500)
    assertEquals(Seq(1000L, 500L), slept.toSeq)
  }

  @Test
  def resetReanchors(): Unit = {
    val c = clock(1)
    c.await(t0); c.await(t0 + 500)
    c.reset()
    c.await(t0)
    assertEquals(Seq(500L), slept.toSeq)
  }
}
