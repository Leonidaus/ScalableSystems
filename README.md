# ScalableSystems

School project for CS-E4780 — a small streaming pipeline that replays trading
indicator data (EMA 38/100 crossovers with BUY/SELL advisories) to a live web
dashboard.

## Architecture

Two parts:

```
mock_data/*.csv  ->  src/main/scala/tick  (tick parsing, sbt)
DEBS 2022 raw ticks   sbt "run <csv>"
```

```
server/mock/data/sample.csv  ->  server (Express + ws)  ->  client (React + Vite)
generated EMA windows            GET /api/symbols           symbol picker
                                 WS /ws (300 ms replay)     live EMA chart
```

The Vite dev server proxies `/api` and `/ws` to the backend, so the browser
only ever talks to one origin.

- **`src/main/scala/`** — sbt project (Scala 2.13) that reads a raw DEBS 2022
  trading-data CSV and parses price events (`TickEvent`). Run with
  `sbt "run mock_data/trading_data_08-11-2021.csv"`.
- **`server/`** — Node.js service built on Express + `ws`. Serves the list of
  symbols over HTTP and replays that symbol's precomputed EMA windows over a
  WebSocket, one window every 300 ms.
- **`server/mock/`** — `generate-data.js` produces the `sample.csv` the server
  replays (two symbols, synthetic sine-wave EMAs with crossovers).
- **`client/`** — React + Vite app using `lightweight-charts`. Fetches the
  symbol list, streams the selected symbol over the WebSocket, and draws the
  two EMA lines with BUY/SELL arrow markers.

## Run with Docker

```sh
docker compose up --build     # build and start
docker compose down           # stop
```

- Client: http://localhost:5173
- Server API: http://localhost:3001

## Run locally (development)

```sh
cd server && npm install && npm start      # backend on :3001
cd client && npm install && npm run dev    # frontend on :5173
```

Regenerate the server's sample data:

```sh
cd server && npm run generate-data
```

## Repository layout

```
src/main/scala/tick/   Scala tick parser (sbt project root)
server/                Node.js backend (Express + WebSocket replay)
client/                React + Vite frontend (lightweight-charts)
mock_data/             Raw DEBS 2022 trading-data CSVs
docker-compose.yml     Compose stack: server + client
```
