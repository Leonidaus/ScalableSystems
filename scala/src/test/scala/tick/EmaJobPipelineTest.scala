package tick

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.junit.Assert._
import org.junit.Test
import scala.jdk.CollectionConverters._

/**
 * End-to-end wiring of the Query 1 pipeline over a bounded in-memory source —
 * stands in for the docker-compose Kafka path on machines without Docker.
 * Timestamps are Monday 2021-11-08 morning, Europe/Paris (CET).
 */
class EmaJobPipelineTest {

  private val W1 = 1636358400000L // 09:00
  private val W2 = W1 + 300000L   // 09:05

  // Builds a raw CSV line with the parser's columns (ID 0, SecType 1, Date 2,
  // Last 21, Trading time 23, Trading date 26) set and the rest empty.
  private def line(symbol: String, tradingTime: String, last: String): String =
    Array.fill(37)("")
      .updated(0, symbol)
      .updated(1, "E")
      .updated(2, "08-11-2021")
      .updated(21, last)
      .updated(23, tradingTime)
      .updated(26, "08-11-2021")
      .mkString(",")

  @Test
  def windowsThenFoldsClosesIntoPerSymbolEmaChain(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val lines: DataStream[String] = env.fromData(Seq(
      line("MOCK.NL", "09:00:13.9459", "100.0"),
      line("MOCK.NL", "09:00:48.2803", "101.0"), // window-1 close: latest ts
      line("MOCK.NL", "09:05:01.0000", "110.0"),
      line("MOCK.NL", "09:05:59.9999", "120.0"), // window-2 close
      line("RDSA.NL", "09:00:00.0001", "22.0")
    ).asJava)

    val it   = EmaJob.build(lines).executeAndCollect()
    val bySymbol = it.asScala.toSeq.groupBy(_.symbol)
    it.close()

    val mock = bySymbol("MOCK.NL").sortBy(_.windowStart)
    assertEquals(2, mock.size)
    assertEquals(W1, mock.head.windowStart)
    assertEquals(101.0, mock.head.close, 0.0)
    // Seed EMA_0 = 0: first EMA is close·2/(1+j).
    assertEquals(101.0 * 2.0 / 39.0, mock.head.ema38, 1e-9)
    assertEquals(101.0 * 2.0 / 101.0, mock.head.ema100, 1e-9)
    assertEquals(W2, mock(1).windowStart)
    assertEquals(120.0, mock(1).close, 0.0)
    assertEquals(120.0 * 2.0 / 39.0 + (101.0 * 2.0 / 39.0) * (37.0 / 39.0), mock(1).ema38, 1e-9)

    val rdsa = bySymbol("RDSA.NL")
    assertEquals(1, rdsa.size)
    assertEquals(W1, rdsa.head.windowStart)
    assertEquals(22.0 * 2.0 / 39.0, rdsa.head.ema38, 1e-9)
  }
}
