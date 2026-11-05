import { useEffect, useState } from 'react';
import Chart from './Chart.jsx';

export default function App() {
  const [symbols, setSymbols] = useState([]);
  const [symbol, setSymbol] = useState('');
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);

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

  // A plain dropdown cannot hold 5504 symbols; recommend matches while the
  // user types, prefix hits ranked before plain substring hits.
  const q = query.trim().toLowerCase();
  const starts = [];
  const contains = [];
  for (const s of symbols) {
    const l = s.toLowerCase();
    if (l.startsWith(q)) starts.push(s);
    else if (q && l.includes(q)) contains.push(s);
  }
  const matches = (q ? [...starts, ...contains] : symbols).slice(0, 5);
  const pick = (s) => {
    setSymbol(s);
    setQuery(s);
    setOpen(false);
  };

  return (
    <div style={{ fontFamily: 'sans-serif', padding: 16 }}>
      <h1>Trading Indicators</h1>
      <label>
        Symbol:{' '}
        <input
          value={query}
          placeholder="type to search..."
          autoComplete="off"
          onChange={(e) => {
            setQuery(e.target.value);
            setActive(0);
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          // Firing on mousedown (not click) lets a match win the race with
          // the input's blur, which closes the list.
          onBlur={() => setOpen(false)}
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault();
              setActive((i) => Math.min(i + 1, matches.length - 1));
            } else if (e.key === 'ArrowUp') {
              e.preventDefault();
              setActive((i) => Math.max(i - 1, 0));
            } else if (e.key === 'Enter' && open && matches[active]) {
              e.preventDefault();
              pick(matches[active]);
            } else if (e.key === 'Escape') {
              setOpen(false);
            }
          }}
          style={{ width: 200 }}
        />
      </label>
      {open && matches.length > 0 && (
        <ul
          style={{
            margin: 0,
            padding: 0,
            listStyle: 'none',
            border: '1px solid #ccc',
            width: 208,
            maxHeight: 150,
            overflowY: 'auto',
          }}
        >
          {matches.map((s, i) => (
            <li
              key={s}
              onMouseDown={() => pick(s)}
              style={{
                padding: '4px 8px',
                cursor: 'pointer',
                background: i === active ? '#e8f0fe' : 'none',
              }}
            >
              {s}
            </li>
          ))}
        </ul>
      )}
      <p style={{ color: '#666' }}>Showing: {symbol || '—'}</p>
      <Chart symbol={symbol} />
    </div>
  );
}
