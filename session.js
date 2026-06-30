"use strict";
// ================================================================
// session.js  v2.1.1  |  PRONTO-AI
// Only XAUUSD and US100/NAS100 — all other symbols blocked
//
// Supported brokers (set env var BROKER):
//   ftmo    → XAUUSD       / US100.cash   (default)
//   vantage → XAUUSD       / NAS100
//   maven   → XAUUSD       / US100
// ================================================================

const TIMEZONE = "Europe/Brussels";

// Risk: per trade as fraction of equity
// 0.09 = 9% → ≈€18 risk at €200 equity, scales with balance
const DEFAULT_RISK_PCT = 0.09;

// SL buffer: webhook gives sl_pct (e.g. 0.003 = 0.3%)
// We multiply by 1.5 to account for spread + timing lag
const SL_BUFFER_MULT = 1.5;

// Per-symbol lot size multiplier — applied AFTER rounding to volStep
// (i.e. round normally, then scale the result). Gold stays at normal
// size; US100/NAS100 gets doubled.
const LOT_MULTIPLIER = {
  "XAUUSD":     1,
  "US100.cash": 2,
};

// ── Broker detection ─────────────────────────────────────────────
const BROKER = (process.env.BROKER || "ftmo").toLowerCase().trim();

// Per-broker MT5 symbol names + volume constraints
// volMin  = minimum lot size allowed by broker
// volStep = lot size must be a multiple of this value
const BROKER_SYMBOL_MAP = {
  ftmo: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD",     pip: 0.01, volMin: 0.01, volStep: 0.01 },
    "US100.cash": { type: "index",     mt5: "US100.cash", pip: 0.10, volMin: 0.01, volStep: 0.01 },
  },
  vantage: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD",     pip: 0.01, volMin: 0.01, volStep: 0.01 },
    "US100.cash": { type: "index",     mt5: "NAS100",     pip: 0.10, volMin: 0.10, volStep: 0.10 },
  },
  maven: {
    "XAUUSD":     { type: "commodity", mt5: "XAUUSD",     pip: 0.01, volMin: 0.01, volStep: 0.01 },
    "US100.cash": { type: "index",     mt5: "US100",      pip: 0.10, volMin: 0.01, volStep: 0.01 },
  },
};

if (!BROKER_SYMBOL_MAP[BROKER]) {
  throw new Error(`[session.js] Unknown BROKER="${BROKER}". Must be: ftmo | vantage | maven`);
}

// Active symbol catalog — resolved from BROKER env var
const SYMBOL_CATALOG = BROKER_SYMBOL_MAP[BROKER];

console.log(`[session.js] Broker="${BROKER}" — MT5 symbols: XAUUSD→"${SYMBOL_CATALOG["XAUUSD"].mt5}", US100→"${SYMBOL_CATALOG["US100.cash"].mt5}"`);

// ── Volume rounding helper ────────────────────────────────────────
// Rounds lots DOWN to nearest volStep, then enforces volMin.
// Decimal precision is derived from volStep itself, so a 0.10 step
// (e.g. Vantage NAS100) always yields exactly 1 decimal (0.10, 0.20, 0.30...)
// instead of leaking floating-point noise like 0.30000000000000004.
function roundLots(rawLots, symInfo) {
  const step = symInfo.volStep ?? 0.01;
  const min  = symInfo.volMin  ?? 0.01;
  // Decimals implied by the step itself (0.10 → 1, 0.01 → 2)
  const stepStr = step.toString();
  const decimals = stepStr.includes(".") ? stepStr.split(".")[1].length : 0;
  // Round DOWN to nearest step (never risk more than calculated),
  // using integer math to avoid floating-point drift
  const stepsCount = Math.floor(rawLots / step + 1e-9); // tiny epsilon guards against e.g. 0.299999999
  const stepped = parseFloat((stepsCount * step).toFixed(decimals));
  // Enforce minimum, keep the step's own precision (not hardcoded to 2dp)
  let result = Math.max(min, stepped);

  // Apply per-symbol lot multiplier AFTER rounding (gold x1, NAS/US100 x2).
  // symInfo.key is set by getSymbolInfo(); falls back to no multiplier
  // (x1) if key is missing, e.g. when callers pass a raw symInfo object.
  const mult = LOT_MULTIPLIER[symInfo.key] ?? 1;
  if (mult !== 1) {
    result = parseFloat((result * mult).toFixed(decimals));
  }

  return parseFloat(result.toFixed(decimals));
}

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

function normalizeSymbol(raw) {
  if (!raw) return null;
  const upper = raw.toString().toUpperCase().trim().replace(/[^A-Z0-9./]/g, "");
  if (SYMBOL_ALIASES[upper]) return SYMBOL_ALIASES[upper];
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

function buildOptimizerKey(symbol, session, direction, vwapPos) {
  return `${symbol}_${session}_${direction}_${vwapPos}`;
}

function buildDailyLabel(date, count) {
  const s = getBrusselsDateStr(date);
  const dd = s.slice(8, 10);
  const mm = s.slice(5, 7);
  return `${dd}/${mm}-#${count}`;
}

const BLOCKED_SYMBOLS = new Set([
  "US30USD","US30","DOW","DJI","DJIA",
  "DE30EUR","DE30","DAX","GER30","GER40",
  "UK100GBP","UK100","FTSE","FTSE100",
  "SP500","SPX","US500","SPX500",
  "JP225","JPN225","NIKKEI",
]);

const TIME_BLOCK_WINDOWS = {
  "US100.cash": [{ start: 1100, end: 1600 }],
};

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

const DEFAULT_TP_RR = 1.5;
const TP_RR_WINDOWS = {
  "XAUUSD": [
    { start: 1300, end: 1500, rr: 1.25 },
    { start: 1500, end: 1700, rr: 3.0 },
    // 17:00–08:00 (rest) falls through to DEFAULT_TP_RR (1.5R)
  ],
  "US100.cash": [
    { start: 800,  end: 1100, rr: 2.25 },
    { start: 1600, end: 1800, rr: 1.25 },
    { start: 1800, end: 2300, rr: 2.75 },
    // 23:00–08:00 (rest) falls through to DEFAULT_TP_RR (1.5R)
  ],
};

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
  roundLots, LOT_MULTIPLIER,
};
