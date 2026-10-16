# ScalableSystems

CS-E4780 course project: detect trading trends in DEBS 2022 financial tick
data — EMA 38/100 crossovers with buy/sell advisories, computed per symbol
over 5-minute tumbling windows.

## Data flow

```
data/mock/*.csv  ->  tick.TickPublisher  ->  Kafka 'ticks'  ->  tick.EmaJob  ->  5-min windows  ->  EMA38/100 + crossover  ->  Kafka 'ema-results' (every window)
one week of ticks      Scala producer          3 partitions      Flink job    per symbol close   keyed fold                  Kafka 'advisories'  (BUY/SELL only)
                      replays forever
```

The producer republishes the mock CSVs line by line, forever. The Flink job
consumes the topic, parses each line into a `TickEvent`, groups events by
symbol, and buckets each symbol's ticks into 5-minute event-time tumbling
windows. Each window emits one result: the last trade by event time in that
window (the window's close). A keyed fold (`KeyedEmaFold`) then chains each
symbol's closes into the spec's EMA38/EMA100 — seeded EMA₀ = 0, deduping by
window start so the producer's replay loop can't fold the same window twice —
flags crossovers between consecutive windows as BUY/SELL advisories. Every 
`EmaResult` snapshot goes to the `ema-results` topic, thecrossovers alone to 
`advisories`; both are keyed by symbol with JSON values:

```json
{"symbol":"RDSA.NL","window_start":"2021-11-08T08:00:00Z","window_end":"2021-11-08T08:05:00Z","close":20.5328,"ema38":1.0529641025641026,"ema100":0.406590099009901,"advisory":"BUY"}
```

`advisory` is `"BUY"`, `"SELL"` or `""`. The spec is applied literally: with
EMA₀ = 0 the previous window counts as EMA38 ≤ EMA100, so every symbol's
first window is a BUY.

## How to run

```sh
docker compose up -d --build                                  # build + start everything
# watch the advisories (swap in --topic ema-results for every window's EMAs)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic advisories --from-beginning --property print.key=true
docker compose down                                            # stop
```

No tools needed on the host — the fat JAR is built by an sbt stage inside
Docker (`scala/docker/Dockerfile`). Flink dashboard: http://localhost:8081
(`server` on :3001 and `client` on :5173 also start, still fed by mock data).

## TODO

1. **EMA per symbol (Query 1)** — done: `KeyedEmaFold` folds each window
   close into `EMA_j(w) = Close(w)·2/(1+j) + EMA_j(w−1)·(1−2/(1+j))`, j ∈ {38,
   100}, EMA₀ = 0, in keyed per-symbol state, skipping closes whose window
   start (`windowStart = ts − ts % 300000`) a replay pass has already folded —
   the producer's replay loop re-fires windows within the 7-day lateness, and
   folding a window twice would corrupt the chain. Unit tests: `EmaFolderTest`,
   `EmaJobPipelineTest`.
2. **Crossover advisories (Query 2)** — done: `EmaMath.crossover` flags BUY
   when EMA38 crosses above EMA100 and SELL on the reverse, comparing window
   w and w−1 (Eqs. 1–4), published to the `advisories` topic. Unit tests:
   `CrossoverTest`, `EmaFolderTest`, `EmaResultJsonTest`.
3. **Visualization** — feed advisories/EMAs to the server+client (currently
   replaying generated mock data) by consuming `ema-results`/`advisories`.
4. **Full dataset** — run over `data/full/` (289M events) for the
   performance-evaluation section of the report.
5. **Report** — 4-5 pages, ACM template: architecture, implementation,
   performance (correctness, scalability, resource utilization).
