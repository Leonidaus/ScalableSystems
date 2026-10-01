package tick

final case class TickEvent(
    symbol: String,
    secType: Char,
    last: Double,
    tradingDate: String,
    tradingTime: String,
    timestamp: Long
)
