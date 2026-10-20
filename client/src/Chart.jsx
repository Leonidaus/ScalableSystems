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
    const ema38 = chart.addSeries(LineSeries, { color: '#2962ff', title: 'ema38' });
    const ema100 = chart.addSeries(LineSeries, { color: '#ef6c00', title: 'ema100' });

    const markers = [];
    const markersPlugin = createSeriesMarkers(ema38, markers);

    // Fit the viewport to the first data point, then follow the live edge;
    // refitting on every row (bug B2) made the chart jump constantly.
    let fitted = false;

    const ws = new WebSocket(`ws://${location.host}/ws`);
    ws.onopen = () => ws.send(symbol);
    ws.onmessage = (event) => {
      const row = JSON.parse(event.data);
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
      if (!fitted) {
        chart.timeScale().fitContent();
        fitted = true;
      } else {
        chart.timeScale().scrollToRealTime();
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
