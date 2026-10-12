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
}

/** One per-symbol, per-window EMA snapshot emitted by [[KeyedEmaFold]]. */
final case class EmaResult(
    symbol: String,
    windowStart: Long,
    windowEnd: Long,
    close: Double,
    ema38: Double,
    ema100: Double
)

/**
 * Folds per-window closes into the spec's EMA38/EMA100 chain for a single
 * symbol: `EMA_j(w) = Close(w)·2/(1+j) + EMA_j(w−1)·(1−2/(1+j))`, seeded with
 * EMA = 0 on the first window. Windows with no events never produce a close,
 * so the chain simply carries the previous EMA forward across them.
 *
 * The producer replays the mock week forever and the window operator allows 7
 * days of lateness, so every close arrives once per replay pass (a pass
 * restarts at the week's first window, so a plain "last window seen" counter
 * cannot detect the duplicates). Folding a window twice would corrupt the
 * chain, hence the set of already-folded window starts, bounded by the number
 * of windows that actually contain events (≤ 2016 per symbol-week).
 */
final class EmaFolder {

  private var ema38  = 0.0
  private var ema100 = 0.0
  private val seen   = new java.util.HashSet[java.lang.Long]()

  /** @return the new EMA snapshot, or None if this close's window was already folded. */
  def fold(event: TickEvent): Option[EmaResult] = {
    val windowStart = event.timestamp - event.timestamp % 300000L
    if (!seen.add(windowStart)) None
    else {
      ema38  = EmaMath.next(ema38, event.last, EmaMath.Alpha38)
      ema100 = EmaMath.next(ema100, event.last, EmaMath.Alpha100)
      Some(EmaResult(event.symbol, windowStart, windowStart + 300000L, event.last, ema38, ema100))
    }
  }
}

/**
 * Keyed adapter around [[EmaFolder]]: holds one folder per symbol in keyed
 * state so each symbol's EMA chain persists across windows and replay passes.
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
