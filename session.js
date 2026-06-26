"use strict";
// ================================================================
// session.js  v2.1.0  |  PRONTO-AI
// Only XAUUSD and US100/NAS100 — all other symbols blocked
//
// Supported brokers (set env var BROKER):
//   ftmo    → XAUUSD       / US100.cash   (default)
//   vantage → XAUUSD+      / NAS100
//   maven   → XAUUSD       / US100
// ================================================================

const TIMEZONE = "Europe/Brussels";

// Risk: 0.0375% of equity per trade
const DEFAULT_RISK_PCT = 0.000375;

// SL buffer: webhook gives sl_pct (e.g. 0.003 = 0.3%)
// We multiply by 1.5 to account for spread + timing lag
const SL_BUFFER_MULT = 1.5;

// ── Broker detection ─────────────────────────────────────────────
// Set BROKER env var to "ftmo" | "vantage" | "maven"
// Defaults to "ftmo" if not set.
const BROKER = (process.env.BROKER || "ftmo").toLowerCase().trim();

// Per-broker MT5 symbol names
// catalog key → { type, mt5, pip }
const BROKER_SYMBOL_MAP = {
  ftmo: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD",     pip: 0.01 },
    "US100.cash": { type: "index",     mt5: "US100.cash", pip: 0.10 },
  },
  vantage: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD+",    pip: 0.01 },
    "US100.cash": { type: "index",     mt5: "NAS100",     pip: 0.10 },
  },
  maven: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD",     pip: 0.01 },
    "US100.cash": { type: "index",     mt5: "US100",      pip: 0.10 },
  },
};

if (!BROKER_SYMBOL_MAP[BROKER]) {
  throw new Error(`[session.js] Unknown BROKER="${BROKER}". Must be: ftmo | vantage | maven`);
}

// Active symbol catalog — resolved from BROKER env var
const SYMBOL_CATALOG = BROKER_SYMBOL_MAP[BROKER];

console.log(`[session.js] Broker="${BROKER}" — MT5 symbols: XAUUSD→"${SYMBOL_CATALOG["XAUUSD"].mt5}", US100→"${SYMBOL_CATALOG["US100.cash"].mt5}"`);

// All TradingView aliases that map to our 2 pairs
const SYMBOL_ALIASES = {
  "GOLD":        "XAUUSD",
  "XAUUSD":      "XAUUSD",
  "XAU/USD":     "XAUUSD",
  "XAUUSD.":     "XAUUSD",
  "US100":       "US100.cash",
  "US100.CASH":  "US100.cash",
  "NAS100":      "US100.cash",
  "NAS100USD":   "US100.cash",
  "NASDAQ":      "US100.cash",
  "NDX":         "US100.cash",
  "USTEC":       "US100.cash",
  "US100USD":    "US100.cash",
  "NASDAQ100":   "US100.cash",
};

// Brussels time helpers
function getBrusselsComponents(date = null) {
  const d = date ? new Date(date) : new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    weekday: "long", year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  }).formatToParts(d);
  const get = (type) => parts.find(p => p.type === type)?.value;
  const dayName = get("weekday");
  const dayMap  = { Sunday:0, Monday:1, Tuesday:2, Wednesday:3, Thursday:4, Friday:5, Saturday:6 };
  const day     = dayMap[dayName] ?? 0;
  const hour    = parseInt(get("hour")) % 24;
  const minute  = parseInt(get("minute"));
  const second  = parseInt(get("second"));
  const hhmm    = hour * 100 + minute;
  return { day, hour, minute, second, hhmm };
}

function getBrusselsDateStr(date = null) {
  const d = date ? new Date(date) : new Date();
  return new Intl.DateTimeFormat("sv-SE", { timeZone: TIMEZONE }).format(d);
}

// Session based on Brussels time
// Asia:   02:00–08:00
// London: 08:00–15:30
// NY:     15:30–02:00
function getSession(date = null) {
  const { hhmm } = getBrusselsComponents(date);
  if (hhmm >= 200  && hhmm < 800)  return "asia";
  if (hhmm >= 800  && hhmm < 1530) return "london";
  return "ny";
}

function isWeekend(date = null) {
  const { day } = getBrusselsComponents(date);
  return day === 0 || day === 6;
}

// Normalize raw symbol from TradingView to our catalog key
function normalizeSymbol(raw) {
  if (!raw) return null;
  const upper = raw.toString().toUpperCase().trim().replace(/[^A-Z0-9./]/g, "");
  if (SYMBOL_ALIASES[upper]) return SYMBOL_ALIASES[upper];
  // Also try without dot
  const noDot = upper.replace(/\./g, "");
  for (const [alias, target] of Object.entries(SYMBOL_ALIASES)) {
    if (alias.replace(/[./]/g, "") === noDot) return target;
  }
  return null;
}

function getSymbolInfo(raw) {
  const key = normalizeSymbol(raw);
  if (!key) return null;
  return { ...SYMBOL_CATALOG[key], key };
}

function getVwapPosition(price, vwapMid) {
  if (price == null || vwapMid == null || vwapMid === 0) return "unknown";
  return parseFloat(price) >= parseFloat(vwapMid) ? "above" : "below";
}

// Optimizer key: "XAUUSD_london_buy_above"
function buildOptimizerKey(symbol, session, direction, vwapPos) {
  return `${symbol}_${session}_${direction}_${vwapPos}`;
}

// Daily trade label: "01/06-#3"
function buildDailyLabel(date, count) {
  const s = getBrusselsDateStr(date);
  const dd = s.slice(8, 10);
  const mm = s.slice(5, 7);
  return `${dd}/${mm}-#${count}`;
}

// canOpen: only blocked on weekends or unknown symbol
// Also explicitly block index signals we don't trade
const BLOCKED_SYMBOLS = new Set([
  "US30USD","US30","DOW","DJI","DJIA",
  "DE30EUR","DE30","DAX","GER30","GER40",
  "UK100GBP","UK100","FTSE","FTSE100",
  "SP500","SPX","US500","SPX500",
  "JP225","JPN225","NIKKEI",
]);

// Time-based block windows (Brussels wall-clock, hhmm format, end-exclusive)
// XAUUSD     blocked 14:00–15:00
// US100.cash blocked 11:00–14:00
const TIME_BLOCK_WINDOWS = {
  "XAUUSD":     [{ start: 1400, end: 1500 }],
  "US100.cash": [{ start: 1100, end: 1400 }],
};

// Returns the matching window if the symbol is time-blocked right now, else null
function isTimeBlocked(symbolKey, date = null) {
  const windows = TIME_BLOCK_WINDOWS[symbolKey];
  if (!windows) return null;
  const { hhmm } = getBrusselsComponents(date);
  for (const w of windows) {
    if (hhmm >= w.start && hhmm < w.end) return w;
  }
  return null;
}

function _fmtHHMM(n) {
  const s = String(n).padStart(4, "0");
  return s.slice(0, 2) + ":" + s.slice(2);
}

// TP risk-reward per symbol per Brussels time window (end-exclusive).
// Anything not matched uses DEFAULT_TP_RR.
//   XAUUSD     09:00-11:00 -> 1.0 RR ,  15:00-17:00 -> 3.0 RR
//   US100.cash 08:00-10:00 -> 2.0 RR
const DEFAULT_TP_RR = 1.5;
const TP_RR_WINDOWS = {
  "XAUUSD": [
    { start: 900,  end: 1100, rr: 1.0 },
    { start: 1500, end: 1700, rr: 3.0 },
  ],
  "US100.cash": [
    { start: 800,  end: 1000, rr: 2.0 },
  ],
};

// Resolve the TP RR for a symbol at a given time (defaults to DEFAULT_TP_RR)
function getTpRR(symbolKey, date = null) {
  const windows = TP_RR_WINDOWS[symbolKey];
  if (windows) {
    const { hhmm } = getBrusselsComponents(date);
    for (const w of windows) {
      if (hhmm >= w.start && hhmm < w.end) return w.rr;
    }
  }
  return DEFAULT_TP_RR;
}

function canOpenNewTrade(rawSymbol, date = null) {
  if (isWeekend(date)) return { allowed: false, reason: "WEEKEND" };
  const upper = (rawSymbol || "").toString().toUpperCase().trim().replace(/[^A-Z0-9./]/g,"");
  if (BLOCKED_SYMBOLS.has(upper)) return { allowed: false, reason: `SYMBOL_NOT_ALLOWED: "${rawSymbol}" — explicitly blocked` };
  const sym = normalizeSymbol(rawSymbol);
  if (!sym) return { allowed: false, reason: `SYMBOL_NOT_ALLOWED: "${rawSymbol}" — only XAUUSD and US100.cash` };
  const blk = isTimeBlocked(sym, date);
  if (blk) return { allowed: false, reason: `TIME_BLOCK: ${sym} blocked ${_fmtHHMM(blk.start)}\u2013${_fmtHHMM(blk.end)} Brussels` };
  return { allowed: true, reason: null };
}

module.exports = {
  TIMEZONE, DEFAULT_RISK_PCT, SL_BUFFER_MULT,
  BROKER, BROKER_SYMBOL_MAP,
  SYMBOL_CATALOG, SYMBOL_ALIASES,
  getBrusselsComponents, getBrusselsDateStr,
  getSession, isWeekend,
  normalizeSymbol, getSymbolInfo,
  getVwapPosition, buildOptimizerKey,
  buildDailyLabel, canOpenNewTrade,
  TIME_BLOCK_WINDOWS, isTimeBlocked,
  DEFAULT_TP_RR, TP_RR_WINDOWS, getTpRR,
};
