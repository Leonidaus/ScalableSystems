import { useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts/core';
import { LineChart, ScatterChart } from 'echarts/charts';
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

echarts.use([LineChart, ScatterChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

// Streams the selected symbol over a WebSocket and draws each window as it
// arrives. One series per price/EMA, one point per 5-minute window, plus a
// scatter series that marks BUY/SELL advisories on the close line.
export default function Chart({ symbol }) {
  const [rows, setRows] = useState([]);
  const containerRef = useRef(null);
  const chartRef = useRef(null);

  // One chart instance per mount, disposed with the component.
  useEffect(() => {
    const chart = echarts.init(containerRef.current);
    chartRef.current = chart;
    const onResize = () => chart.resize();
    window.addEventListener('resize', onResize);
    return () => {
      window.removeEventListener('resize', onResize);
      chart.dispose();
      chartRef.current = null;
    };
  }, []);

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

  useEffect(() => {
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

    // Alert markers sit on the close line: triangles pointing up for BUY,
    // down for SELL, labeled at the window's position.
    const advisories = rows
      .map((r, i) =>
        r.advisory
          ? {
              value: [i, r.close],
              symbolRotate: r.advisory === 'SELL' ? 180 : 0,
              itemStyle: { color: r.advisory === 'SELL' ? '#ef5350' : '#26a69a' },
              label: {
                show: true,
                formatter: r.advisory,
                position: r.advisory === 'SELL' ? 'bottom' : 'top',
                color: r.advisory === 'SELL' ? '#ef5350' : '#26a69a',
              },
            }
          : null
      )
      .filter(Boolean);

    chartRef.current?.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      legend: { top: 0 },
      grid: { top: 30, left: 10, right: 20, bottom: 10, containLabel: true },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: rows.map((r) => fmt(r.time)),
        axisLabel: { hideOverlap: true },
      },
      yAxis: [
        {
          // The EMA warm-up ramp from 0 would otherwise stretch the range
          // and hide the movement; the tail fit zooms to the converged part
          // of the chains.
          type: 'value',
          position: 'left',
          min: tail.length ? lo - pad : undefined,
          max: tail.length ? hi + pad : undefined,
        },
        {
          // scale: true keeps the close axis from starting at zero, which
          // would flatten the up/down movement of prices around 20.
          type: 'value',
          position: 'right',
          scale: true,
          axisLabel: { formatter: (v) => Number(v.toPrecision(6)) },
        },
      ],
      series: [
        {
          name: 'ema38',
          type: 'line',
          yAxisIndex: 0,
          data: rows.map((r) => r.ema38),
          showSymbol: false,
          lineStyle: { width: 1 },
          color: '#2962ff',
        },
        {
          name: 'ema100',
          type: 'line',
          yAxisIndex: 0,
          data: rows.map((r) => r.ema100),
          showSymbol: false,
          lineStyle: { width: 1 },
          color: '#ef6c00',
        },
        {
          name: 'close',
          type: 'line',
          yAxisIndex: 1,
          data: rows.map((r) => r.close),
          showSymbol: false,
          lineStyle: { width: 2 },
          color: '#525866',
        },
        {
          name: 'alerts',
          type: 'scatter',
          yAxisIndex: 1,
          data: advisories,
          symbol: 'triangle',
          symbolSize: 9,
        },
      ],
    });
  }, [rows]);

  if (!symbol) return null;
  return <div ref={containerRef} style={{ height: 400 }} />;
}
