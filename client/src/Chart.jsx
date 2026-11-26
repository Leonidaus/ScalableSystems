import { useEffect, useRef, useState } from 'react';
import * as echarts from 'echarts/core';
import { LineChart, ScatterChart } from 'echarts/charts';
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

echarts.use([LineChart, ScatterChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

export default function Chart({ symbol }) {
  const [rows, setRows] = useState([]);
  const containerRef = useRef(null);
  const chartRef = useRef(null);

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
    // x axis is one category per window, so closed hours and weekends
    // don't leave dead time on the axis
    const fmt = (t) =>
      new Date(t * 1000).toLocaleString('en-GB', {
        weekday: 'short',
        hour: '2-digit',
        minute: '2-digit',
      });
    // the EMA warm-up ramp from 0 would stretch the y range,
    // so fit the axis to the last 100 windows instead
    const tail = rows.slice(-100);
    const emas = tail.flatMap((r) => [r.ema38, r.ema100]);
    const lo = Math.min(...emas);
    const hi = Math.max(...emas);
    const pad = Math.max((hi - lo) * 0.2, Math.abs(hi) * 0.05) || 1;

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
          type: 'value',
          position: 'left',
          min: tail.length ? lo - pad : undefined,
          max: tail.length ? hi + pad : undefined,
        },
        {
          // scale keeps the close axis from starting at zero
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
