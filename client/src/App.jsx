import { useEffect, useState } from 'react';
import Chart from './Chart.jsx';

export default function App() {
  const [symbols, setSymbols] = useState([]);
  const [symbol, setSymbol] = useState('');

  // Load the list of available symbols once, and select the first one.
  useEffect(() => {
    fetch('/api/symbols')
      .then((r) => r.json())
      .then((list) => {
        setSymbols(list);
        setSymbol(list[0] ?? '');
      });
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
