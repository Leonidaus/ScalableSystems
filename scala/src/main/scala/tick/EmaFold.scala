package tick

import org.apache.flink.api.common.functions.{OpenContext, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector

/** Spec EMA constants: alpha_j = 2/(1+j) for j ∈ {38, 100}. */
object EmaMath {
  val Alpha38: Double  = 2.0 / 39.0
  val Alpha100: Double = 2.0 / 101.0

  def next(prevEma: Double, close: Double, alpha: Double): Double =
    close * alpha + prevEma * (1.0 - alpha)

  /**
   * BUY when EMA38 crosses above EMA100 between windows w−1 and w, SELL on the reverse, otherwise "".
   * Equality at w−1 counts as the starting side, equality at w never fires.
   */
  def crossover(prev38: Double, prev100: Double, ema38: Double, ema100: Double): String =
    if (ema38 > ema100 && prev38 <= prev100) Advisory.Buy
    else if (ema38 < ema100 && prev38 >= prev100) Advisory.Sell
    else Advisory.None
}

/**
 * Advisory values.
 */
object Advisory {
  val Buy  = "BUY"
  val Sell = "SELL"
  val None = ""
}

/** One per-symbol, per-window EMA snapshot emitted by [[KeyedEmaFold]]; `advisory` is BUY, SELL or "". */
final case class EmaResult(
    symbol: String,
    windowStart: Long,
    windowEnd: Long,
    close: Double,
    ema38: Double,
    ema100: Double,
    advisory: String
)

/**
 * Folds per-window closes into the spec's EMA38/EMA100 chain for a single
 * symbol: `EMA_j(w) = Close(w)·2/(1+j) + EMA_j(w−1)·(1−2/(1+j))`, seeded with
 * EMA = 0 on the first window, and detects crossovers between consecutive
 * folded windows. Windows with no events never produce a close, so the 
 * chain simply carries the previous EMA forward across them.
 */
final class EmaFolder {

  private var ema38           = 0.0
  private var ema100          = 0.0
  private var lastWindowStart = Long.MinValue

  /** @return the new EMA snapshot, or None if this close's window is not after the last folded one. */
  def fold(event: TickEvent): Option[EmaResult] = {
    val windowStart = event.timestamp - event.timestamp % 300000L
    if (windowStart <= lastWindowStart) None
    else {
      lastWindowStart = windowStart
      val prev38  = ema38
      val prev100 = ema100
      ema38  = EmaMath.next(prev38, event.last, EmaMath.Alpha38)
      ema100 = EmaMath.next(prev100, event.last, EmaMath.Alpha100)
      val advisory = EmaMath.crossover(prev38, prev100, ema38, ema100)
      Some(EmaResult(event.symbol, windowStart, windowStart + 300000L, event.last, ema38, ema100, advisory))
    }
  }
}

/**
 * Keyed adapter around [[EmaFolder]]: holds one folder per symbol in keyed
 * state so each symbol's EMA chain persists across windows.
 */
class KeyedEmaFold extends RichFlatMapFunction[TickEvent, EmaResult] {

  private var folder: ValueState[EmaFolder] = _

  override def open(ctx: OpenContext): Unit =
    folder = getRuntimeContext.getState(
      new ValueStateDescriptor[EmaFolder]("ema-folder", classOf[EmaFolder])
    )

  override def flatMap(event: TickEvent, out: Collector[EmaResult]): Unit = {
    val f = Option(folder.value()).getOrElse(new EmaFolder)
    f.fold(event).foreach { result =>
      folder.update(f)
      out.collect(result)
    }
  }
}
