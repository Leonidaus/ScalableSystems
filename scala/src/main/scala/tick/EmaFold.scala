package tick

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
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
 * Advisory values. Plain strings rather than case objects or Option: records
 * fall back to Kryo, which deserializes Scala singletons into fresh instances
 * and breaks `==` against them.
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
 * folded windows. Taken literally, the seed makes every symbol's first window
 * a BUY (0 ≤ 0 before, EMA38 > EMA100 after for any positive close). 
 * Windows with no events never produce a close, so the chain simply carries 
 * the previous EMA forward across them.
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
 * Window adapter around [[EmaFolder]]: fused onto the window's reduce, it
 * receives each fired window's single pre-reduced close and folds it into
 * the per-symbol EMA chain. The folder lives in globalState — per-key state
 * that persists across windows of this operator — so the pipeline needs only
 * the one keyBy upstream of the window. (The previous keyBy after the window
 * re-keyed by the same key and paid a shuffle per window close for nothing.)
 */
final class EmaFoldWindowFunction
    extends ProcessWindowFunction[TickEvent, EmaResult, String, TimeWindow] {

  private[this] val descriptor =
    new ValueStateDescriptor[EmaFolder]("ema-folder", classOf[EmaFolder])

  override def process(
      key: String,
      context: ProcessWindowFunction[TickEvent, EmaResult, String, TimeWindow]#Context,
      elements: java.lang.Iterable[TickEvent],
      out: Collector[EmaResult]
  ): Unit = {
    val state = context.globalState().getState(descriptor)
    val f     = Option(state.value()).getOrElse(new EmaFolder)
    elements.forEach { event =>
      f.fold(event).foreach { result =>
        state.update(f)
        out.collect(result)
      }
    }
  }
}
