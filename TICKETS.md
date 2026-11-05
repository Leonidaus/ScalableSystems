# Tickets — CS-E4780 Course Project

Deadline: **27.11.2026** · Decisions locked: **Flink as SPE**, **everything runs locally** · Repo: `Leonidaus/ScalableSystems` (public)

---

## Epic 0 — Setup & decisions

- [x] **E0.1 Add Flink to sbt** — done: Flink 2.3.0, Java DataStream API from Scala 2.13, smoke job `tick.FlinkSmokeJob` verified locally.

  The repo is currently a plain sbt project on Scala 2.13.16 (`build.sbt`), but Flink's first-class Scala support targets 2.12, and 2.13 artifact coverage varies by Flink version. Add the Flink dependencies (streaming API, runtime, test harness) to `build.sbt` and resolve the compatibility question first: either downgrade the project to Scala 2.12 or use the Java DataStream API from Scala, which is the most future-proof route. Acceptance: a minimal hello-world Flink job compiles and runs locally, and the chosen approach is recorded in the README or a short ADR note.

- [x] **E0.2 Add Flink to docker-compose** — done: JobManager + TaskManager (4 slots) in `docker-compose.yml`, custom Scala-free image in `scala/docker/`, submission workflow (CLI + dashboard) in the README. Verified: full stack up, job submitted at `-p 4`, results match the local run.

  The whole project runs locally, so the Flink runtime needs to live in the local docker stack alongside the existing `server` and `client` services. Add a JobManager and at least one TaskManager to `docker-compose.yml` with sensible memory limits and a mounted volume for job jars/results, and document the job submission workflow (`flink run` CLI vs. web dashboard) in the README. Acceptance: `docker compose up --build` brings up the full stack and a packaged job can be submitted to it following only the README.

- [x] **E0.3 DECIDE event ordering strategy** — done: correctness runs
  pre-sort the sample by event time (`prepare_sample.py`), the live pipeline
  keeps 5-s bounded out-of-orderness plus the fold's window-start dedupe.

  The spec states the total order of events per symbol cannot be assumed, which directly affects watermarking in Flink: an event arriving after the watermark has passed its timestamp is dropped, and dropped price events silently corrupt the EMA chain. Evaluate the two realistic options — a one-time pre-sort pass of the CSVs by timestamp (simplest, makes watermarks trivial since the bounded source is then in order) versus a watermark strategy with generous allowed lateness (messy, because a re-fired window would need to retract its contribution to the EMA chain). Recommendation is the pre-sort pass, implemented either as a small script or a Flink batch job. Output: the decision written down, since the report's correctness section will be judged on how ordering was handled.

- [x] **E0.4 DECIDE empty-window semantics** — done: carry-forward. An empty
  window emits nothing, the EMA stays unchanged until the symbol's next
  evaluated window, and crossovers compare consecutive evaluated windows.

  The spec defines `Close_{s,wi}` only when a price event exists in the window, but says nothing about windows where a symbol has no events — and with 5504 symbols over 5-minute windows, empty windows are common. Decide whether the EMA stays unchanged (carry-forward, the standard convention and the recommendation) or decays toward zero, and record the decision explicitly. This choice affects the correctness claims in the report, so it must be stated and defended, not left implicit.

- [ ] **E0.5 DECIDE pipeline topology**

  Decide how computed results reach the UI: (a) the Flink job writes a results CSV that the existing Node server replays over its WebSocket — matching the current `sample.csv` interchange schema exactly, so the server and client need zero changes — or (b) Flink streams results live to the server over socket/HTTP. Recommendation is (a): it keeps the demo replay-speed knob in the UI, decouples the Flink work from the web stack, and the `generate-data.js` format already defines the target schema. Output: documented decision plus a diagram in the README if topology (b) surprises anyone later.

- [ ] **E0.6 Check window anchoring**

  The spec anchors `w0` at 0:00 CEST, but Flink's tumbling windows align to the epoch by default, which would put window boundaries at 0:00 UTC — an hour off for this dataset. The good news is the data week (8–14 Nov 2021) is entirely CET (UTC+1) because DST ended 31 Oct 2021, so a fixed +1h window offset should produce correct 0:00-midnight boundaries. Verify this empirically on a small slice and document it, so the report can state the anchoring precisely instead of hand-waving it.

## Epic 1 — Query 1 (EMA)

- [ ] **E1.1 Flink CSV source**

  Implement the job input: a bounded `FileSource` over the `data/mock/*.csv` files, parsing each line with the same column mapping the existing `TickParser` uses (ID+exchange at index 0, SecType at 1, Last at 21, Trading time at 23, Trading date at 26, with the system Date as fallback). Events must carry event-time timestamps built in the Europe/Paris zone, and the watermark strategy must follow the ordering decision from E0.3. Rows without a positive Last price are not trades and must be filtered out, consistent with the existing parser.

- [ ] **E1.2 Tumbling 5-min windows**

  Key the event stream by symbol and apply event-time tumbling windows of 5 minutes with the offset validated in E0.6, implementing spec relaxations 4–6 (5-minute length, anchored at midnight, non-overlapping). `Close_{s,wi}` is the last price event observed in the window, which means the window function must track the latest event by timestamp within the window — and since event order follows the pre-sorted source, "latest" is well-defined. Acceptance: windowed output shows correct boundaries at 0:00 CET and the correct Close per window on a hand-checkable slice.

- [x] **E1.3 EMA38/EMA100 state** — done: `EmaFold.scala` (`EmaMath`, `EmaFolder`, `KeyedEmaFold`) folds window closes into per-symbol EMA38/EMA100 chains in keyed `ValueState`, seeded EMA = 0, deduping replayed closes by window start (`ts − ts % 300000`); wired into `EmaJob` after the window close. Unit-tested in `EmaFolderTest`, pipeline wiring in `EmaJobPipelineTest` (bounded in-memory source, no Kafka needed).

  Implement the spec's EMA formula — `EMA_j = Close·(2/(1+j)) + EMA_prev·(1−2/(1+j))` for j ∈ {38, 100} — which chains across windows, so it cannot live in a stateless window function. Use per-symbol keyed state holding the previous window's EMA38/EMA100 (or an empty symbol registry), apply the empty-window rule from E0.4, and seed with EMA = 0 for a symbol's first window as the spec defines. A `KeyedProcessFunction` with event-time timers is the natural fit; window evaluation happens when the next window starts, per relaxation 7. (The landed implementation keeps the window operator instead and folds at window-fire time, which for a continuous stream coincides with the next window starting; the fold lives behind `EmaJob.build` so the topology can be swapped later if relaxation 7 ever demands timer-based evaluation.)

- [ ] **E1.4 Output in sample.csv schema**

  The job's sink must emit rows in exactly the schema `generate-data.js` already produces — `symbol,window_end,ema38,ema100` plus the advisory column once E2.1 lands — so the Node server consumes real output with zero code changes (drop-in replacement for `server/mock/data/sample.csv`). Emit as CSV via a FileSink, and keep the `window_end` in the ISO format the server already parses. Acceptance: swapping the generated sample for job output requires no edits outside the data file itself.

- [ ] **E1.5 Tests**

  Unit-test the EMA math and windowing against hand-computed fixtures: the exact spec formula on a short synthetic series, midnight anchoring, EMA=0 seeding on first windows, the empty-window carry-forward, and timestamp ties within a window. Follow with a small end-to-end test on a truncated real CSV slice where expected output is computed by hand or by the pandas reference from E5.2. These tests are the backbone of the report's correctness section, so keep them readable and self-explanatory.

## Epic 2 — Query 2 (crossovers)

- [x] **E2.1 BUY/SELL detection** — done: `EmaMath.crossover` (spec Eqs. 1–4) runs inside `EmaFolder.fold` against the previous window's EMAs, so the replay dedupe also guarantees one advisory per crossover. Results go to Kafka as JSON keyed by symbol: every window to `ema-results`, crossovers to `advisories`. Spec applied literally: the EMA₀ = 0 seed makes each symbol's first window a BUY.

  Implement crossover detection exactly per spec Eqs. 1–4: a BUY advisory when EMA38 crosses from ≤ to > EMA100 between consecutive evaluated windows, a SELL advisory on the opposite cross. The logic should live in the same Flink job as Query 1, comparing each window's EMAs against that symbol's previous window — the same sign-change rule `generate-data.js` already implements, which makes it a useful cross-check. Exactly one advisory event per crossover, emitted at window-evaluation time.

- [x] **E2.2 Crossover tests** — done: `CrossoverTest` (both directions, flat stretches, equality on either side, repeated crossings), `EmaFolderTest` (first-window BUY, hand-derived SELL, no advisory on skipped duplicates), `EmaJobPipelineTest` advisory asserts.

  Test with synthetic EMA series covering both crossover directions, flat/no-crossover stretches, repeated crossings, and the boundary case where EMA38 equals EMA100 (the spec's ≤/≥ conditions decide whether that fires). Verify exactly one advisory per qualifying crossover and none otherwise. These cases map directly onto the report's correctness claims and onto the markers the chart renders.

## Epic 3 — Integration

- [ ] **E3.1 Wire real output into server**

  Replace the `npm run generate-data` workflow with a documented path from Flink job output to the file the server replays, and update the README accordingly (the TICKETS-era README still presents the two tracks as unconnected). Depends on B1 — the server cannot consume ~11M real rows until its memory/rescan problem is fixed. Acceptance: a fresh clone following the README goes from raw `data/mock/` to a live chart of real computed data.

- [ ] **E3.2 Verify subscription semantics**

  Spec relaxation 3 requires all metrics to be computed for every symbol but updates delivered only for subscribed ones. Confirm the current design satisfies this: the Flink job computes all 5504 symbols, and the server's per-symbol WebSocket replay means a client only receives the symbol it asks for. Write the confirmation down for the report's correctness section, since it's an explicit spec requirement that's easy to claim but should be demonstrated.

- [ ] **E3.3 Finalize compose stack**

  With Flink services from E0.2 in place, verify the entire system comes up with `docker compose up --build` — Flink runtime, Node server, and Vite client — and that each service's health/readiness is checkable (basic healthcheck or documented log signal). The README's run instructions must match the final stack. Any leftover gaps from the mock era, like the `.gz` mismatch (B3), get cleaned up here.

## Epic 4 — Visualization (bonus)

- [ ] **E4.1 Smart-viz upgrade**

  The baseline markers already earn the smart-viz points; this ticket deliberately keeps the bonus effort small and high-value. Replace the plain dropdown with a searchable picker (5504 symbols won't fit a `<select>`), add the raw Close price alongside the two EMA lines for context, and show a small advisory summary (counts and latest BUY/SELL). Scope guard: no multi-chart dashboards or heavy analytics — polish what exists rather than rebuilding it, and fix B2 (fitContent on every row) while touching this code.

## Epic 5 — Evaluation & report

- [ ] **E5.1 Performance runs**

  Run the evaluation the report's performance section demands, all on local hardware: throughput (events/s) over the full 289M-event week, scaling runs at parallelism 1/2/4/8 via TaskManager slots, and CPU/memory utilization captured with local tooling (Flink metrics plus OS-level profiling). Record the exact machine specs since there's no cloud dimension to describe. Start this by mid-October — it's the canary that tells you whether the Flink setup from Epic 0 performs or needs rethinking, and it feeds directly into report figures.

- [x] **E5.2 Correctness validation** — done: `scripts/reference.py` is an
  independent implementation of both queries; it matches `tick.LocalRun` (the
  real pipeline, bounded, parallelism 1) exactly on 2009/2009 windows over a
  stratified 40-symbol, 511k-line sample of 08-11-21. See README
  "Correctness validation".

  Build an independent reference implementation of both queries (e.g., pandas over per-symbol groups) and diff its output against the Flink job over a sample of symbols, including some with sparse events where carry-forward actually triggers. An independent implementation is what makes the correctness claim credible — self-referential tests alone won't convince a grader. Reuse this reference in E1.5 rather than hand-computing fixtures twice.

- [ ] **E5.3 Report**

  Write the 4–5 page report in the ACM template following §4.1: abstract, introduction, system architecture (design choices from E0.3–E0.5 as short ADR-style notes), implementation (local hardware, Flink, tools, GUI), performance evaluation (E5.1/E5.2 numbers), conclusion. Also add the DEBS 2022 dataset's CC BY-NC-SA attribution to the repo — the data is already public but currently uncredited — and make sure the repo is public for submission.

## Bugs

- [ ] **B1 — Server holds whole CSV in memory and rescans per request**

  `server/index.js` loads the entire replay CSV at startup, `rowsFor()` filters all rows for every WebSocket subscription, and `/api/symbols` rebuilds the symbol set on every HTTP request. Fine for the 100-row sample — but the real full-week output is roughly 11M window rows (5504 symbols × 2016 windows), which means multi-GB heap usage and an O(n) scan per client action. Fix by indexing rows by symbol once at startup (`Map<symbol, rows>`) or streaming lazily. Blocks E3.1.

- [ ] **B2 — Chart refits on every incoming row**

  `client/src/Chart.jsx` calls `chart.timeScale().fitContent()` inside `onmessage`, so every replayed window resets the viewport and the user can never zoom or pan during a replay. Fit once on the first points (or on symbol change) instead.

- [x] **B3 — `.gz` input claimed but unsupported** — fixed: usage string now reads `runMain tick.ParseBenchmark <trading-data.csv>`, no `.gz` claim. Flink reads the CSVs via `FileSource`, so gzip support was never needed here.

  `Main.scala` advertises `run <trading-data.csv[.gz]>` but `TickSource` opens files with a plain `BufferedReader`, so a gzipped file would be read as garbage. Either pipe through `GZIPInputStream` or fix the usage string to match reality — whichever survives contact with how the Flink job will actually read data (likely the latter).

- [ ] **B4 — WebSocket URL hardcodes `ws://`**

  `Chart.jsx` builds `ws://${location.host}/ws`, which fails on any https-served deployment (it needs `wss://`). Derive the scheme from `location.protocol`. Low priority given the local-only plan, but it's a one-line fix.

- [x] **B5 — File reader never closed** — fixed: `TickSource` deleted and its reader inlined into `ParseBenchmark` under `Using.resource`, which closes on both normal exit and exception.

  `TickSource.lines()` creates a `BufferedReader` that is never closed on iterator exhaustion, leaking the underlying file handle. Mostly harmless in one-shot CLI runs, but it matters once this parsing code is reused by the Flink source or longer-running jobs. Wrap in `Using`-style management.

- [ ] **B6 — Sub-millisecond precision silently truncated**

  `TickParser` parses at most 3 fractional digits, but the data carries 4 (`HH:MM:SS.ssss`). Events landing in the same millisecond then fall back to file order — and the spec says per-symbol order can't be assumed. Probably harmless at 5-minute window granularity, but the truncation must be stated in the report so it can't be attacked as a correctness gap.

- [ ] **B7 — Fragile startup and fetch error handling**

  The server dies with an uncaught stack trace if the replay CSV is missing (`readFileSync` at `server/index.js`), and the client's `/api/symbols` fetch has no error path, leaving a permanently empty dropdown on failure. Add graceful errors on both sides — cheap insurance for the live demo.

- [x] **B8 — NaN stats on empty input** — fixed: `ParseBenchmark` guards `linesRead == 0` and prints `events kept: 0 (empty input)`. Verified against an empty file.

  `Main.scala` divides by `linesRead` without a zero guard, so running against an empty file prints `NaN%`. Two-line fix while touching the parser.

---

### Suggested order

E0.1–E0.6 first (unblock everything) → Epic 1 → E2 → B1 before E3.1 → E5.1 by mid-October → report last. Bugs B3/B5/B8 are quick fillers anytime.
