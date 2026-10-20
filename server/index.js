// Express serves the symbol list; EMA rows are streamed over a WebSocket.
// Rows come from the Flink job via the 'ema-results' Kafka topic and are
// indexed by symbol once as they arrive (no rescan per request).
import express from 'express';
import { WebSocketServer } from 'ws';
import { Kafka } from 'kafkajs';

const PORT = process.env.PORT ?? 3001;
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP ?? 'localhost:9092';
const TOPIC = process.env.TOPIC ?? 'ema-results';

// symbol -> array of rows in arrival (= window) order, as sent to the client.
const rowsBySymbol = new Map();

// One entry per connected client waiting for live rows of its symbol.
const listeners = new Set();

function index(row) {
  const rows = rowsBySymbol.get(row.symbol) ?? [];
  rows.push(row);
  rowsBySymbol.set(row.symbol, rows);
  for (const l of listeners) if (l.symbol === row.symbol) l.send({ type: 'row', row });
}

// Kafka may be down or the topic not yet created (the Flink job creates it on
// first write), so retry on startup failures; the HTTP/WebSocket side stays up
// regardless. Once running, kafkajs restarts the consumer on its own after a
// crash, so nothing is retried past this point.
async function consume() {
  const kafka = new Kafka({ clientId: 'viz-server', brokers: [KAFKA_BOOTSTRAP] });
  for (;;) {
    const consumer = kafka.consumer({ groupId: 'viz-server' });
    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [TOPIC], fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ message }) => {
          const r = JSON.parse(message.value.toString());
          index({
            symbol: r.symbol,
            // Lightweight Charts wants the time as unix seconds.
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
app.get('/api/symbols', (_req, res) => {
  res.json([...rowsBySymbol.keys()]);
});

const server = app.listen(PORT, () => console.log(`listening on http://localhost:${PORT}`));

// The client sends a symbol, we reply with everything we have for it in one
// history message, then keep streaming new rows as they arrive.
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
