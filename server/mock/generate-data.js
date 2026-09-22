// Generates server/mock/data/sample.csv with dummy EMA data for two symbols.
// Run with: npm run generate-data
import { writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROWS = 50;                                  // rows per symbol
const START = Date.UTC(2021, 10, 8, 0, 0, 0);     // 2021-11-08 00:00:00 UTC
const STEP_MS = 5 * 60 * 1000;                    // one 5-minute window

// Two sine waves per symbol: a fast one (ema38) and a slow one (ema100).
// Different periods make them cross each other several times.
const SYMBOLS = [
  { symbol: 'RDSA.NL', base: 22, fastAmp: 0.9, fastDiv: 4.5, slowAmp: 0.35, slowDiv: 13, phase: 0.8 },
  { symbol: 'SIE.ETR', base: 142, fastAmp: 2.5, fastDiv: 5.5, slowAmp: 1.0, slowDiv: 15, phase: 2.1 },
];

const rows = [];
for (const s of SYMBOLS) {
  let prevDiff = null;
  for (let i = 0; i < ROWS; i++) {
    const ema38 = s.base + s.fastAmp * Math.sin(i / s.fastDiv);
    const ema100 = s.base + s.slowAmp * Math.sin(i / s.slowDiv + s.phase);
    const diff = ema38 - ema100;

    // A crossover is a sign change of (ema38 - ema100) between two windows.
    let advisory = '';
    if (prevDiff !== null && prevDiff <= 0 && diff > 0) advisory = 'BUY';
    if (prevDiff !== null && prevDiff >= 0 && diff < 0) advisory = 'SELL';
    prevDiff = diff;

    rows.push([
      s.symbol,
      new Date(START + i * STEP_MS).toISOString(),
      ema38.toFixed(4),
      ema100.toFixed(4),
      advisory,
    ].join(','));
  }
}

const csv = ['symbol,window_end,ema38,ema100,advisory', ...rows].join('\n') + '\n';
const out = join(dirname(fileURLToPath(import.meta.url)), 'data', 'sample.csv');
writeFileSync(out, csv);
console.log(`Wrote ${rows.length} rows to ${out}`);
