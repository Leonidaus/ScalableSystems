import { useEffect, useRef } from 'react';
import { createChart, LineSeries, createSeriesMarkers } from 'lightweight-charts';

// Streams the selected symbol over a WebSocket and draws each window as it arrives.
export default function Chart({ symbol }) {
  const containerRef = useRef(null);

  useEffect(() => {
    if (!symbol) return;

    const chart = createChart(containerRef.current, {
      height: 400,
      timeScale: { timeVisible: true },
    });
    const close = chart.addSeries(LineSeries, { color: '#787b86', title: 'close' });
    const ema38 = chart.addSeries(LineSeries, { color: '#2962ff', title: 'ema38' });
    const ema100 = chart.addSeries(LineSeries, { color: '#ef6c00', title: 'ema100' });

    const markers = [];
    const markersPlugin = createSeriesMarkers(ema38, markers);

    const applyRow = (row) => {
      close.update({ time: row.time, value: row.close });
      ema38.update({ time: row.time, value: row.ema38 });
      ema100.update({ time: row.time, value: row.ema100 });

      // BUY -> up arrow below the bar, SELL -> down arrow above the bar.
      if (row.advisory) {
        markers.push({
          time: row.time,
          position: row.advisory === 'BUY' ? 'belowBar' : 'aboveBar',
          shape: row.advisory === 'BUY' ? 'arrowUp' : 'arrowDown',
          color: row.advisory === 'BUY' ? '#26a69a' : '#ef5350',
          text: row.advisory,
        });
        markersPlugin.setMarkers(markers);
      }
    };

    const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
    ws.onopen = () => ws.send(symbol);
    // Rows can arrive out of order (late windows, re-consumed duplicates); the
    // chart only accepts points newer than the last drawn one.
    let lastTime = 0;
    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);
      if (msg.type === 'history') {
        const rows = [...msg.rows].sort((a, b) => a.time - b.time);
        for (const row of rows) if (row.time >= lastTime) {
          applyRow(row);
          lastTime = row.time;
        }
        // Fit once to the whole history; live rows must not reset the viewport.
        chart.timeScale().fitContent();
      } else if (msg.row.time >= lastTime) {
        applyRow(msg.row);
        lastTime = msg.row.time;
      }
    };

    // Closing the socket stops the server-side replay.
    return () => {
      ws.close();
      chart.remove();
    };
  }, [symbol]);

  return <div ref={containerRef} />;
}
