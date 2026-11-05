import { useEffect, useState } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

// Draws "BUY"/"SELL" next to the advisory markers on the close series.
const advisoryLabels = {
  id: 'advisoryLabels',
  afterDatasetsDraw(chart) {
    const advisories = chart.data.datasets[0].advisories;
    if (!advisories) return;
    const meta = chart.getDatasetMeta(0);
    const { ctx } = chart;
    advisories.forEach((adv, i) => {
      if (!adv) return;
      const point = meta.data[i];
      if (!point) return;
      ctx.save();
      ctx.fillStyle = adv === 'SELL' ? '#ef5350' : '#26a69a';
      ctx.font = '11px sans-serif';
      ctx.fillText(
        adv,
        point.x - 12,
        adv === 'SELL' ? point.y - 12 : point.y + 22
      );
      ctx.restore();
    });
  },
};

// Streams the selected symbol over a WebSocket and draws each window as it
// arrives. One dataset per series, one point per 5-minute window.
export default function Chart({ symbol }) {
  const [rows, setRows] = useState([]);

  useEffect(() => {
    if (!symbol) return;
    setRows([]);

    const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
    ws.onopen = () => ws.send(symbol);
    let lastTime = 0;
    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'history') {
        const history = msg.rows.filter((r) => r.time >= lastTime).sort((a, b) => a.time - b.time);
        if (history.length) {
          lastTime = history[history.length - 1].time;
          setRows(history);
        }
      } else if (msg.row.time >= lastTime) {
        lastTime = msg.row.time;
        setRows((rows) => [...rows, msg.row]);
      }
    };
    return () => ws.close();
  }, [symbol]);

  // Collapsed sessions: one category per evaluated window, so closed hours and
  // weekends leave no flat dead time on the axis (the price never actually
  // held there — those windows emit no rows). Spacing is thus not
  // time-proportional.
  const fmt = (t) =>
    new Date(t * 1000).toLocaleString('en-GB', {
      weekday: 'short',
      hour: '2-digit',
      minute: '2-digit',
    });
  // The EMA axis fits the converged part of the chains: the 0-seed warm-up
  // ramp would otherwise stretch the range and hide the movement.
  const tail = rows.slice(-100);
  const emas = tail.flatMap((r) => [r.ema38, r.ema100]);
  const lo = Math.min(...emas);
  const hi = Math.max(...emas);
  const pad = Math.max((hi - lo) * 0.2, Math.abs(hi) * 0.05) || 1;
  const data = {
    labels: rows.map((r) => fmt(r.time)),
    datasets: [
      {
        label: 'close',
        data: rows.map((r) => r.close),
        borderColor: '#787b86',
        backgroundColor: '#787b86',
        borderWidth: 1,
        pointRadius: rows.map((r) => (r.advisory ? 6 : 0)),
        pointStyle: rows.map((r) => (r.advisory === 'SELL' ? 'triangle' : 'circle')),
        pointRotation: rows.map((r) => (r.advisory === 'SELL' ? 180 : 0)),
        pointBackgroundColor: rows.map((r) =>
          r.advisory === 'SELL' ? '#ef5350' : '#26a69a'
        ),
        advisories: rows.map((r) => r.advisory),
        yAxisID: 'y',
      },
      {
        label: 'ema38',
        data: rows.map((r) => r.ema38),
        borderColor: '#2962ff',
        backgroundColor: '#2962ff',
        borderWidth: 1,
        pointRadius: 0,
        yAxisID: 'y1',
      },
      {
        label: 'ema100',
        data: rows.map((r) => r.ema100),
        borderColor: '#ef6c00',
        backgroundColor: '#ef6c00',
        borderWidth: 1,
        pointRadius: 0,
        yAxisID: 'y1',
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    scales: {
      x: {
        ticks: { maxRotation: 0, autoSkip: true },
      },
      // Close gets its own scale; the EMA warm-up ramp from 0 would otherwise
      // stretch the shared range and hide the price movement.
      y: {
        position: 'right',
        ticks: { callback: (v) => Number(v.toPrecision(6)) },
      },
      y1: {
        position: 'left',
        min: tail.length ? lo - pad : undefined,
        max: tail.length ? hi + pad : undefined,
      },
    },
  };

  if (!symbol) return null;

  return (
    <div style={{ height: 400 }}>
      <Line data={data} options={options} plugins={[advisoryLabels]} />
    </div>
  );
}
