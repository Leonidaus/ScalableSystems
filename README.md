# ScalableSystems

School project for CS-E4780 — a streaming pipeline that computes EMA 38/100
crossovers with BUY/SELL advisories from DEBS 2022 trading data and replays them
to a live web dashboard.

**Current status:** the Flink toolchain is set up and proven end to end (E0.1,
E0.2). The job that exists today just counts events per symbol — the EMA logic is
Epic 1. The web stack still runs on generated mock data; wiring real job output
into it is E3.1. See [`TICKETS.md`](TICKETS.md).

## Architecture

Two halves, currently connected only by a shared CSV schema:

```
mock_data/*.csv   ->  src/main/scala/tick   ->  (Epic 1: EMA windows)
DEBS 2022 raw ticks   Flink job + parser

server/mock/data/sample.csv  ->  server (Express + ws)  ->  client (React + Vite)
generated EMA windows            GET /api/symbols           symbol picker
                                 WS /ws (300 ms replay)     live EMA chart
```

The Vite dev server proxies `/api` and `/ws` to the backend, so the browser only
ever talks to one origin and no CORS setup is needed.

- **`src/main/scala/tick/`** — sbt project (Scala 2.13). `TickParser` turns a raw
  DEBS CSV line into a `TickEvent`; `FlinkSmokeJob` is the Flink job that
  consumes it; `ParseBenchmark` is a single-threaded throughput harness.
- **`server/`** — Node.js service (Express + `ws`). Serves the symbol list over
  HTTP and replays a symbol's EMA windows over a WebSocket, one window per 300 ms.
- **`server/mock/`** — `generate-data.js` produces the `sample.csv` the server
  replays (two symbols, synthetic sine-wave EMAs with crossovers).
- **`client/`** — React + Vite app using `lightweight-charts`. Draws the two EMA
  lines with BUY/SELL arrow markers.

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| JDK | **17** | Flink reflects into JDK internals; `build.sbt` passes the required `--add-opens` flags for local runs. The Docker images set the same flags. |
| sbt | 1.10.1 | pinned in `project/build.properties` |
| Docker | with Compose v2 | `docker compose`, not `docker-compose` |
| Node | 18+ | Vite 6's floor; only needed for the `server/` and `client/` halves |

Scala 2.13.16 and Flink 2.3.0 are pinned in `build.sbt` and fetched automatically.

## Quick start

```sh
# 1. Flink job against the committed sample CSVs, no Docker needed
sbt run

# 2. The whole stack in Docker
docker compose up --build
```

- Client: <http://localhost:5173>
- Server API: <http://localhost:3001>
- Flink dashboard: <http://localhost:8081>

The Flink services come up **idle** — they are a runtime waiting for a job, so
nothing happens until you submit one.

## Running the Flink job

`tick.FlinkSmokeJob` reads the CSVs with a bounded `FileSource`, parses them with
`TickParser`, and counts price events per symbol. It is the E0.1 proof that the
toolchain works; there is no EMA logic yet.

### Entry points

Two classes have a `main`, so `build.sbt` pins which one each sbt task uses.
**`sbt run` is the Flink job** — the parse benchmark must be named explicitly:

| Command | Runs |
|---|---|
| `sbt run` | `tick.FlinkSmokeJob` over `mock_data` (the default argument) |
| `sbt "run data"` | the job over another path |
| `sbt "runMain tick.FlinkSmokeJob mock_data"` | the job, named explicitly |
| `sbt "runMain tick.ParseBenchmark <csv>"` | the single-threaded benchmark |

Without those pins sbt would stop and ask you to pick a class by number, so do
not remove them while two `main` methods exist.

### Local runs (the normal dev loop)

sbt runs the job against an **embedded MiniCluster** in-process. No Docker, no
JAR, breakpoints work:

```sh
sbt "runMain tick.FlinkSmokeJob mock_data"
```

Expected output, which doubles as the E0.1 acceptance check:

```
[types] TickEvent   -> GenericType<tick.TickEvent>
[types] SymbolCount -> GenericType<tick.SymbolCount>
SymbolCount(TTE.FR,724)
SymbolCount(MOCK.NL,2520)
SymbolCount(RDSA.NL,864)
SymbolCount(SIE.ETR,837)
SymbolCount(SX5E.FR,592)
```

Row order varies between runs — keyed results arrive as subtasks finish — but the
counts are stable and match an independent `awk` pass (5537 price events total):

```sh
awk -F',' 'FNR==1{next} $22!="" && $22+0>0 {n[$1]++} END{for(s in n) print s"="n[s]}' mock_data/*.csv
```

The `GenericType<...>` lines are printed on purpose: they mean Flink is falling
back to Kryo serialisation for the case classes rather than its fast POJO path.
Harmless at this scale, a real cost over the full dataset — E1.1 should address it.

### Cluster runs

Build the fat JAR first. `target/scala-2.13` is mounted into the cluster as
`/jobs`, so a code change needs `sbt assembly` only — never an image rebuild:

```sh
sbt assembly
docker compose up -d --build jobmanager taskmanager
```

`--build` matters on the first run. The cluster uses a small custom image
(`docker/flink/Dockerfile`) that deletes `flink-scala_2.12` from the stock Flink
image. **No published Flink image is Scala-free** — not even in 2.x, where that
jar is nothing but the Scala 2.12 standard library. Flink resolves `scala.*`
parent-first, so those 2.12 classes shadow the 2.13 library inside our JAR and a
Scala 2.13 job dies at submission with
`NoSuchMethodError: scala.Predef$.refArrayOps`.

Submit via the **CLI**:

```sh
# explicit entry class
docker compose exec jobmanager flink run -p 4 \
  -c tick.FlinkSmokeJob /jobs/scalable-systems-0.1.0.jar /mock_data

# without -c: uses Main-Class from the JAR manifest (the same job)
docker compose exec jobmanager flink run /jobs/scalable-systems-0.1.0.jar /mock_data

# detached — returns immediately; use this for long runs
docker compose exec jobmanager flink run -d -p 4 \
  -c tick.FlinkSmokeJob /jobs/scalable-systems-0.1.0.jar /mock_data
```

Add `-T` (`docker compose exec -T ...`) when scripting without a TTY.

Job management:

```sh
docker compose exec jobmanager flink list      # running + scheduled
docker compose exec jobmanager flink list -a   # include finished
docker compose exec jobmanager flink cancel <JobID>
```

Or use the **web dashboard** at <http://localhost:8081> → *Submit New Job* →
*Add New*, uploading `target/scala-2.13/scalable-systems-0.1.0.jar` with entry
class `tick.FlinkSmokeJob`. The dashboard is also where throughput, backpressure
and the flame graph live, which is what E5.1 needs.

### Where the job output goes

**Cluster runs print nothing to your terminal, and that is expected.** `main()`
runs in the submitting client (so you see the `[types]` lines), but `print()` is
a *sink* — it executes on a TaskManager, in another container, and writes to that
process's stdout:

```sh
docker compose logs taskmanager | grep SymbolCount      # all of it
docker compose logs -f taskmanager                      # follow live
docker compose logs taskmanager | grep -oE "SymbolCount\([A-Z0-9.]+,[0-9]+\)" | sort -u
```

Or in the dashboard: **Task Managers** → pick one → **Stdout**.

When parallelism > 1, Flink prefixes each line with the subtask index (`3> ...`).
With `--scale taskmanager=2` the output is split across both containers, though
`docker compose logs taskmanager` still aggregates them. `print()` is a debugging sink only.

### Mounts and parallelism

| Host | Container | Notes |
|---|---|---|
| `mock_data/` | `/mock_data` | read-only, committed sample CSVs |
| `out/` | `/out` | job output, writable |
| `target/scala-2.13/` | `/jobs` | read-only, the assembled JAR |

The JobManager and every TaskManager mount the same input paths on purpose: a
TaskManager opens the files itself, so a path visible only to the JobManager
fails at runtime rather than at submission.

Each TaskManager provides 4 slots. For the E5.1 scaling runs, add TaskManagers
and match `-p` to the total slots — `-p` above the available slots leaves the job
queued waiting for resources:

```sh
docker compose up -d --scale taskmanager=2     # 2 x 4 = 8 slots
docker compose exec jobmanager flink run -p 8 \
  -c tick.FlinkSmokeJob /jobs/scalable-systems-0.1.0.jar /mock_data
```

## The parse benchmark

`tick.ParseBenchmark` reads a CSV with one plain thread and reports parse
throughput:

```sh
sbt "runMain tick.ParseBenchmark data/sample.csv"
```

## Running the web stack

```sh
cd server && npm install && npm start      # backend on :3001
cd client && npm install && npm run dev    # frontend on :5173
```

Regenerate the server's sample data:

```sh
cd server && npm run generate-data
```

The server currently replays `server/mock/data/sample.csv`, which is generated,
not computed. Replacing it with real Flink output is E3.1 — and is blocked on B1,
since the server loads the whole CSV into memory.

## How the build is put together

Worth knowing before editing `build.sbt`:

- **Flink dependencies are `Provided`.** The cluster already ships
  `flink-streaming-java`, `flink-clients` and `flink-connector-files` in
  `/opt/flink/lib`. Bundling a second copy into the fat JAR causes classloader
  conflicts at submission, so the JAR carries only our code plus `scala-library`.
  Consequence: `java -jar target/.../scalable-systems-0.1.0.jar` **cannot work** —
  it fails with `NoClassDefFoundError`. The JAR only runs where Flink is supplied.
- **`sbt run` still works locally anyway.** `Provided` dependencies are normally
  dropped from the run classpath, which would break the MiniCluster loop, so
  `build.sbt` overrides `Compile / run` and `Compile / runMain` to use the
  Compile classpath instead.
- **`fork := true` is required.** Running in sbt's own JVM makes Flink's threads
  inherit sbt's layered classloader, which cannot see `flink-core`.
- **log4j is `Provided` too**, which keeps it out of the JAR while still putting
  it on the local run classpath — otherwise local runs print
  `SLF4J: Defaulting to no-operation (NOP) logger`. `src/main/resources/log4j2.properties`
  configures local runs only; the assembly step discards it so the cluster keeps
  its own logging config.

## Repository layout

```
src/main/scala/tick/   Flink jobs + tick parser (sbt project root)
src/main/resources/    log4j2.properties (local runs only)
project/               sbt's own build definition (+ Metals-generated files)
server/                Node.js backend (Express + WebSocket replay)
client/                React + Vite frontend (lightweight-charts)
mock_data/             Raw DEBS 2022 trading-data CSVs (committed)
data/                  Full DEBS dataset (gitignored, not in the repo)
out/                   Flink job output (gitignored)
docker/flink/          Scala-stripped Flink image (removes flink-scala_2.12)
docker-compose.yml     Compose stack: Flink (JM + TM) + server + client
TICKETS.md             Work breakdown and open bugs
```
