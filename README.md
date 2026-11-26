# ScalableSystems

CS-E4780 course project: detect trading trends in DEBS 2022 financial tick
data — EMA 38/100 crossovers with buy/sell advisories, computed per symbol
over 5-minute tumbling windows.

## Data flow

```
data/mock/*.csv                    one week of ticks
       │
       ▼
tick.TickPublisher                 Scala producer, paced replay
       │
       ▼
Kafka 'ticks'                      raw CSV trade lines, 12 partitions
       │
       ▼
tick.EmaJob  (Flink)
   ├─ parse                        CSV line → TickEvent
   ├─ 5-min tumbling windows       per symbol, emits the window's close
   └─ KeyedEmaFold                 EMA38 / EMA100 + BUY/SELL crossover
       │
       ├────────────────────────────────┐
       ▼                                ▼
Kafka 'ema-results'                Kafka 'advisories'
every window                       BUY/SELL only
       │
       ▼
server  (Node, kafkajs)            per-symbol index of EMA rows
       │  WebSocket: history, then live rows
       ▼
client  (React + Chart.js)
```

The producer republishes the mock CSVs' trade lines. The Flink job
consumes the topic, parses each line into a `TickEvent`, groups events by
symbol, and buckets each symbol's ticks into 5-minute event-time tumbling
windows. Each window emits one result: the last trade by event time in that
window (the window's close). A keyed fold (`KeyedEmaFold`) then chains each
symbol's closes into the spec's EMA38/EMA100 (EMA₀ = 0) and flags crossovers 
between consecutive windows as BUY/SELL advisories. Every `EmaResult` snapshot
goes to the `ema-results` topic, the crossovers alone to `advisories`;
both are keyed by symbol with JSON values:

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

Real data instead of mock: download the DEBS CSVs (~25 GB, gitignored) into
`data/full/` and point the producer at them.

```sh
scripts/download-data.sh                 # all 7 days; or e.g. scripts/download-data.sh 08-11-21
docker compose down                      # start from an empty 'ticks' topic
TICKS_INPUT=/data/full SPEED=500 docker compose up -d --build
```

### Replay speed

The producer paces publishing by each line's arrival time (the system
`Date`/`Time` columns, which the files are ordered by); the job's windows
still use `Trading time` (event time). One caveat: the job's watermark
idleness timeout (10 s) runs on wall-clock time, so at very low speeds which
late events get dropped can differ from a max-speed run.

- `SPEED` — `1` real time, `N` N× faster, `max` (or `0`) no pacing.
  Default `500`.
- `MAX_GAP_S` — data gaps longer than this many seconds (nights, the weekend)
  are shortened to it; `0` keeps them. Default `300`.

Every 10 s the producer logs the trades sent, the achieved rate and, when paced,
the current data time and how far behind schedule it is (`lag`).

No tools needed on the host — the fat JAR is built by an sbt stage inside
Docker (`scala/docker/Dockerfile`). Flink dashboard: http://localhost:8081
(`server` on :3001 and `client` on :5173 also start)

## Correctness validation

`scripts/reference.py` implements both queries independently of the Flink job
and `scripts/compare.py` diffs it against a real run of the pipeline
(`tick.LocalRun`, the same `EmaJob.build` code path without Kafka) over a
sample of real data. `prepare_sample.py` picks symbols across the dataset's
long tail (head, mid, sparse, bid/ask-only) and pre-sorts them by event time,
so no event is late for its window.

```sh
scripts/download-data.sh 08-11-21
python3 scripts/prepare_sample.py data/full/debs2022-gc-trading-day-08-11-21.csv data/samples/sample-real-08-11.csv
python3 scripts/reference.py data/samples/sample-real-08-11.csv output/ref-real.csv
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/data/samples:/samples" -v "$PWD/output:/out" \
  scalablesystems/flink:2.3.0-scalafree \
  java -cp "/jobs/scalable-systems-0.1.0.jar:/opt/flink/lib/*" \
  tick.LocalRun /samples/sample-real-08-11.csv /out/actual-real.csv
python3 scripts/compare.py output/ref-real.csv output/actual-real.csv data/samples/sample-real-08-11.csv
```

On 08-11-21 (40 symbols, 511k lines): 2009/2009 evaluated windows match
exactly (closes, EMAs, advisories); 59k non-trade lines are filtered
identically by both sides; 20 of 38 symbols cross empty windows between
trades; the 2 bid/ask-only symbols produce no output. Only BUY advisories 
can fire within one day from the EMA₀ = 0 seed; the SELL path is covered by
`CrossoverTest`/`EmaFolderTest`.