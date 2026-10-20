// Express serves the symbol list; the EMA rows are streamed over a WebSocket.
import express from 'express';
import { WebSocketServer } from 'ws';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const PORT = process.env.PORT ?? 3001;
const INTERVAL_MS = 300; // how fast the recorded windows are replayed
const CSV_PATH = join(dirname(fileURLToPath(import.meta.url)), 'mock', 'data', 'sample.csv');

// Tiny CSV parser: first line is the header, no quoting/escaping support.
function parseCsv(text) {
  const [header, ...lines] = text.trim().split('\n');
  const cols = header.split(',');
  return lines.map((line) => {
    const values = line.split(',');
    return Object.fromEntries(cols.map((c, i) => [c, values[i] ?? '']));
  });
}

// Index rows by symbol once at startup: converting and filtering the whole
// array on every WebSocket subscription is O(n) per client action.
const rowsBySymbol = new Map();
for (const raw of parseCsv(readFileSync(CSV_PATH, 'utf8'))) {
  if (!rowsBySymbol.has(raw.symbol)) rowsBySymbol.set(raw.symbol, []);
  rowsBySymbol.get(raw.symbol).push({
    // Lightweight Charts wants the time as unix seconds.
    time: Math.floor(new Date(raw.window_end).getTime() / 1000),
    ema38: Number(raw.ema38),
    ema100: Number(raw.ema100),
    advisory: raw.advisory,
  });
}
const symbols = [...rowsBySymbol.keys()];

function rowsFor(symbol) {
  return rowsBySymbol.get(symbol) ?? [];
}

const app = express();
app.get('/api/symbols', (_req, res) => {
  res.json(symbols);
});

const server = app.listen(PORT, () => console.log(`listening on http://localhost:${PORT}`));

// The client sends a symbol, we replay that symbol's windows one at a time.
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (socket) => {
  let timer;
  socket.on('message', (data) => {
    clearInterval(timer);
    const rows = rowsFor(String(data));
    let i = 0;
    timer = setInterval(() => {
      if (i >= rows.length) return clearInterval(timer);
      socket.send(JSON.stringify(rows[i++]));
    }, INTERVAL_MS);
  });
  socket.on('close', () => clearInterval(timer));
});
