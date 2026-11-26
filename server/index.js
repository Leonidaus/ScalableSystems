import express from 'express';
import { WebSocketServer } from 'ws';
import { Kafka } from 'kafkajs';

const PORT = process.env.PORT ?? 3001;
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP ?? 'localhost:9092';
const TOPIC = process.env.TOPIC ?? 'ema-results';
const SPEED = Number(process.env.SPEED);

// symbol -> rows in arrival (= window) order, as sent to the client
const rowsBySymbol = new Map();
// clients currently watching a symbol
const listeners = new Set();
// wall clock + data time of the first row, for the UI's simulation clock
let firstRow = null;

function index(row) {
  const rows = rowsBySymbol.get(row.symbol) ?? [];
  rows.push(row);
  rowsBySymbol.set(row.symbol, rows);
  if (!firstRow) firstRow = { wall: Date.now(), data: row.time * 1000 };
  for (const l of listeners) if (l.symbol === row.symbol) l.send({ type: 'row', row });
}

// Kafka or the topic may not be up yet, so retry until the consumer starts.
async function consume() {
  const kafka = new Kafka({ clientId: 'viz-server', brokers: [KAFKA_BOOTSTRAP] });
  for (;;) {
    // fresh group id each start, so the topic is read from the beginning
    const consumer = kafka.consumer({ groupId: `viz-server-${Date.now()}` });
    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [TOPIC], fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const r = JSON.parse(message.value.toString());
          index({
            symbol: r.symbol,
            // Lightweight Charts wants unix seconds
            time: Math.floor(Date.parse(r.window_end) / 1000),
            close: r.close,
            ema38: r.ema38,
            ema100: r.ema100,
            advisory: r.advisory,
          });
        },
      });
      return;
    } catch (err) {
      console.error(`consumer failed: ${err.message}, retrying in 5s`);
      await consumer.disconnect().catch(() => {});
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

const app = express();
app.get('/api/symbols', (_req, res) => res.json([...rowsBySymbol.keys()]));
app.get('/api/status', (_req, res) =>
  res.json({
    startedAt: firstRow?.wall ?? null,
    dataStart: firstRow?.data ?? null,
    speed: Number.isFinite(SPEED) && SPEED > 0 ? SPEED : 'max',
  })
);

const server = app.listen(PORT, () => console.log(`listening on http://localhost:${PORT}`));

// Client sends a symbol, we reply with its history, then stream new rows.
const wss = new WebSocketServer({ server, path: '/ws' });
wss.on('connection', (socket) => {
  let listener = null;
  socket.on('message', (data) => {
    if (listener) listeners.delete(listener);
    listener = { symbol: String(data), send: (msg) => socket.send(JSON.stringify(msg)) };
    listeners.add(listener);
    listener.send({ type: 'history', rows: rowsBySymbol.get(listener.symbol) ?? [] });
  });
  socket.on('close', () => {
    if (listener) listeners.delete(listener);
  });
});

consume();
