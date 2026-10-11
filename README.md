# ScalableSystems

CS-E4780 course project: detect trading trends in DEBS 2022 financial tick
data — EMA 38/100 crossovers with buy/sell advisories, computed per symbol
over 5-minute tumbling windows.

## Data flow

```
data/mock/*.csv  ->  tick.TickPublisher  ->  Kafka 'ticks'  ->  tick.EmaJob  ->  5-min windows  ->  EMA38/100  ->  print()
one week of ticks      Scala producer          3 partitions      Flink job    per symbol close   keyed fold     TM stdout
                      replays forever
```

The producer republishes the mock CSVs line by line, forever. The Flink job
consumes the topic, parses each line into a `TickEvent`, groups events by
symbol, and buckets each symbol's ticks into 5-minute event-time tumbling
windows. Each window emits one result: the last trade by event time in that
window (the window's close). A keyed fold (`KeyedEmaFold`) then chains each
symbol's closes into the spec's EMA38/EMA100 — seeded EMA₀ = 0, deduping by
window start so the producer's replay loop can't fold the same window twice —
and prints one `EmaResult` snapshot per symbol per window to TaskManager
stdout. The crossover logic is not implemented yet.

## How to run

```sh
docker compose up -d --build                                  # build + start everything
docker compose logs -f taskmanager | grep EmaResult            # watch the EMA snapshots
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
2. **Crossover advisories (Query 2)** — buy when EMA38 crosses above EMA100,
   sell on the reverse, comparing window w and w-1 (Eq. 1-4).
3. **Visualization** — feed advisories/EMAs to the server+client (currently
   replaying generated mock data); publish results to a second Kafka topic.
4. **Full dataset** — run over `data/full/` (289M events) for the
   performance-evaluation section of the report.
5. **Report** — 4-5 pages, ACM template: architecture, implementation,
   performance (correctness, scalability, resource utilization).
