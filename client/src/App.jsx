import { useEffect, useState } from 'react';
import Chart from './Chart.jsx';

export default function App() {
  const [symbols, setSymbols] = useState([]);
  const [symbol, setSymbol] = useState('');
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const [clock, setClock] = useState('');
  const [status, setStatus] = useState(null);

  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const s = await (await fetch('/api/status')).json();
        if (s.startedAt) {
          setStatus(s);
          clearInterval(timer);
        }
      } catch {
        // server not up yet, try again on the next tick
      }
    }, 500);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    const timer = setInterval(() => {
      if (!status) {
        setClock('');
      } else if (typeof status.speed === 'number') {
        const ms = status.dataStart + (Date.now() - status.startedAt) * status.speed;
        setClock(
          new Date(ms).toLocaleString('en-GB', {
            weekday: 'short',
            day: '2-digit',
            month: 'short',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          }) + ` (sim, ${status.speed}x)`
        );
      } else {
        setClock('max speed');
      }
    }, 1000);
    return () => clearInterval(timer);
  }, [status]);

  // symbols appear gradually as the job produces each one's first result
  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const list = await (await fetch('/api/symbols')).json();
        if (list.length > 0) {
          setSymbols((prev) => (prev.length === list.length ? prev : list));
          setSymbol((s) => s || list[0]);
        }
      } catch {
        // server not up yet, try again on the next tick
      }
    }, 2000);
    return () => clearInterval(timer);
  }, []);

  // a dropdown can't hold 5000+ symbols, so match while typing,
  // prefix hits ranked before substring hits
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
    <div style={{ padding: 16 }}>
      <h1 style={{ display: 'flex', justifyContent: 'space-between' }}>
        Live EMA signals
        <span style={{ fontSize: '0.6em' }}>{clock}</span>
      </h1>
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
          // onMouseDown wins the race against the input's blur
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
      {symbol ? (
        <>
          <p>Showing: {symbol}</p>
          <Chart symbol={symbol} />
        </>
      ) : (
        <p>
          No tickers yet, the Flink job takes ~15 s to start after
          docker compose up, and the first windows close after 5 minutes of
          data time (a few seconds at replay speeds). This page updates
          itself, no reload needed.
        </p>
      )}
    </div>
  );
}
