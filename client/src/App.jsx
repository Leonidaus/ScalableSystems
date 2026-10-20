import { useEffect, useState } from 'react';
import Chart from './Chart.jsx';

export default function App() {
  const [symbols, setSymbols] = useState([]);
  const [symbol, setSymbol] = useState('');

  // Symbols appear as the Flink job produces output, so keep asking until
  // the list is no longer empty.
  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const list = await (await fetch('/api/symbols')).json();
        if (list.length > 0) {
          setSymbols(list);
          setSymbol((s) => s || list[0]);
          clearInterval(timer);
        }
      } catch {
        // Server not up yet; try again on the next tick.
      }
    }, 2000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div style={{ fontFamily: 'sans-serif', padding: 16 }}>
      <h1>Trading Indicators</h1>
      <label>
        Symbol:{' '}
        <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
          {symbols.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>
      <Chart symbol={symbol} />
    </div>
  );
}
