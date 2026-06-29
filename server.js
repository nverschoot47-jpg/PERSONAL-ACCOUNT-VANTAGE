"use strict";
// ================================================================
// server.js  v2.0.0  |  PRONTO-AI
//
// Flow:
// 1. TradingView webhook → /webhook
//    - symbol filter (XAUUSD / US100 only)
//    - SL + TP calculation (sl_pct × 1.5 × execPrice)
//    - lot calculation (riskEUR / slDist)
//    - placeOrder on MT5 via MetaAPI
//    - start ghost tracker
//    - log to signal_log
//
// 2. syncPositions() every 5s
//    - poll MT5 for open positions
//    - update ghost tracker with current price
//    - track 0.1R milestones (-1.0 → +max)
//    - detect MT5 close (TP or SL)
//    - if MT5 SL: finalize ghost (phantom SL = MT5 SL)
//    - if MT5 TP: keep ghost running until phantom SL
//
// 3. Ghost phantom SL:
//    - price crosses SL level
//    - backfill all ADV milestones proportionally
//    - save to ghost_trades
//    - delete from ghost_state
//
// 4. Dashboard: server.js contains all HTML/JS inline
// ================================================================

const express = require("express");
const helmet  = require("helmet");
const cron    = require("node-cron");

const db = require("./db");
const {
  DEFAULT_RISK_PCT, SL_BUFFER_MULT,
  BROKER, BROKER_SYMBOL_MAP,
  getBrusselsDateStr, getSession,
  normalizeSymbol, getSymbolInfo,
  getVwapPosition, buildOptimizerKey,
  buildDailyLabel, canOpenNewTrade,
  getTpRR, roundLots,
} = require("./session");

const VERSION = "2.1.0";

// ── Safe numeric parser (handles NaN, null, undefined, "") ────────
function safeNum(val) {
  if (val === null || val === undefined || val === "") return null;
  const n = parseFloat(val);
  return isNaN(n) ? null : n;
}

// ── Config from Railway env vars ─────────────────────────────────
const PORT           = process.env.PORT           || 3000;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "";
const META_API_TOKEN = process.env.META_API_TOKEN || "";
const META_ACCOUNT   = process.env.META_ACCOUNT   || "";
const META_BASE      = process.env.META_BASE
  || "https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai";

// ── App state ────────────────────────────────────────────────────
let dbReady       = false;
let openPositions = new Map();
let latestEquity  = 50000;
let latestCurrency = "USD";
let _acctCache    = null;
let _acctCacheTs  = 0;
let _syncRunning  = false;
let _lastEquitySave = 0;

// ── Express: start immediately so Railway health check passes ────
const app = express();
app.use(helmet({ contentSecurityPolicy: false }));
app.use((req, res, next) => {
  if (req.method === "POST" && req.headers["content-type"]?.includes("application/json")) {
    let raw = "";
    req.on("data", chunk => raw += chunk);
    req.on("end", () => {
      try {
        const sanitized = raw.replace(/: *NaN/g, ": null").replace(/: *nan/g, ": null");
        req.body = JSON.parse(sanitized);
      } catch { try { req.body = JSON.parse(raw); } catch { req.body = {}; } }
      next();
    });
  } else {
    express.json({ limit: "1mb" })(req, res, next);
  }
});

const server = app.listen(PORT, () => {
  console.log(`[PRONTO-AI v${VERSION}] port ${PORT} | broker=${BROKER}`);
});

// ── MetaAPI helpers ───────────────────────────────────────────────
let _metaFails = 0;
let _circuitOpen = false;
let _circuitOpenAt = 0;
const CIRCUIT_THRESHOLD = 15;
const _recentWebhooks = new Map();
const _zeroDealsCount = new Map();
const _processingWebhooks = new Set();

function isDuplicateWebhook(sym, dir) {
  const key = sym+"_"+dir, now = Date.now();
  if (_processingWebhooks.has(key)) return true;
  const last = _recentWebhooks.get(key);
  if (last && now-last < 60000) return true;
  _processingWebhooks.add(key);
  setTimeout(() => _processingWebhooks.delete(key), 5000);
  for (const [k,v] of _recentWebhooks) if (now-v>120000) _recentWebhooks.delete(k);
  return false;
}

function markWebhookPlaced(sym, dir) {
  const key = sym+"_"+dir;
  _recentWebhooks.set(key, Date.now());
}

const CIRCUIT_RESET_MS = 45000;

function circuitOpen() {
  if (!_circuitOpen) return false;
  if (Date.now() - _circuitOpenAt > CIRCUIT_RESET_MS) {
    _circuitOpen = false; _metaFails = 0;
    console.log("[MetaAPI] Circuit reset");
    return false;
  }
  return true;
}

async function metaFetch(path, method = "GET", body = null, retries = 2) {
  if (circuitOpen()) throw new Error("MetaAPI circuit open");
  const url  = `${META_BASE}${path}`;
  const opts = {
    method,
    headers: { "auth-token": META_API_TOKEN, "Content-Type": "application/json" },
    signal: AbortSignal.timeout(12000),
  };
  if (body) opts.body = JSON.stringify(body);
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url, opts);
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`${res.status} ${txt.slice(0, 100)}`);
      }
      _metaFails = 0;
      return res.json().catch(() => null);
    } catch (e) {
      if (i < retries) { await new Promise(r => setTimeout(r, 1000 * (i + 1))); continue; }
      const isServerDown = e.message.includes('503') || e.message.includes('Service Unavailable');
      if (!isServerDown) {
        _metaFails++;
        if (_metaFails >= CIRCUIT_THRESHOLD) { _circuitOpen = true; _circuitOpenAt = Date.now(); console.error("[MetaAPI] Circuit OPEN"); }
      } else {
        console.warn("[MetaAPI] 503 outage — not counting toward circuit");
      }
      throw e;
    }
  }
}

async function getAccountInfo() {
  const now = Date.now();
  if (_acctCache && now - _acctCacheTs < 60000) return _acctCache;
  if (!META_API_TOKEN || !META_ACCOUNT) return null;
  try {
    const d = await metaFetch(`/users/current/accounts/${META_ACCOUNT}/account-information`);
    if (d?.balance !== undefined) {
      _acctCache = d; _acctCacheTs = now;
      latestEquity   = parseFloat(d.equity ?? d.balance ?? latestEquity);
      latestCurrency = d.currency ?? latestCurrency;
    }
    return d;
  } catch (e) { return _acctCache ?? null; }
}

async function getPositions() {
  if (!META_API_TOKEN || !META_ACCOUNT) return [];
  try {
    const d = await metaFetch(`/users/current/accounts/${META_ACCOUNT}/positions`);
    return Array.isArray(d) ? d : [];
  } catch { return []; }
}

async function placeOrder(order) {
  const result = await metaFetch(`/users/current/accounts/${META_ACCOUNT}/trade`, "POST", order);
  if (result) console.log(`[PlaceOrder] Response:`, JSON.stringify(result).slice(0,200));
  return result;
}

async function getDeals(positionId) {
  if (!META_API_TOKEN || !META_ACCOUNT) return [];
  if (circuitOpen()) return [];
  try {
    const from = new Date(Date.now() - 30 * 86400000).toISOString();
    const to   = new Date().toISOString();
    const url  = `${META_BASE}/users/current/accounts/${META_ACCOUNT}/history-deals/position/${positionId}?from=${from}&to=${to}`;
    const res  = await fetch(url, {
      headers: { "auth-token": META_API_TOKEN, "Content-Type": "application/json" },
      signal: AbortSignal.timeout(8000),
    });
    if (!res.ok) {
      if (res.status === 503) console.warn(`[MetaAPI] getDeals 503 for ${positionId} — MetaAPI outage`);
      return [];
    }
    const d = await res.json().catch(() => null);
    return Array.isArray(d) ? d : (d?.deals ?? []);
  } catch { return []; }
}

// ── Ghost tracker ─────────────────────────────────────────────────
function initGhost(pos) {
  return {
    positionId:    pos.positionId,
    dailyLabel:    pos.dailyLabel,
    optimizerKey:  pos.optimizerKey,
    symbol:        pos.symbol,
    assetType:     pos.assetType,
    direction:     pos.direction,
    session:       pos.session,
    vwapPosition:  pos.vwapPosition,
    entry:         pos.entry,
    sl:            pos.sl,
    tp:            pos.tp,
    lots:          pos.lots,
    riskEur:       pos.riskEur,
    slPct:         pos.slPct,
    slDist:        pos.slDist,
    vwapMid:       pos.vwapMid,
    vwapUpper:     pos.vwapUpper,
    vwapLower:     pos.vwapLower,
    vwapBandPct:   pos.vwapBandPct,
    sessionHigh:   pos.sessionHigh,
    sessionLow:    pos.sessionLow,
    dayHigh:       pos.dayHigh,
    dayLow:        pos.dayLow,
    tvEntry:       pos.tvEntry,
    mt5Comment:    pos.mt5Comment,
    openedAt:      pos.openedAt,
    maxRR:         0,
    peakRRPos:     0,
    peakRRNeg:     0,
    rrMilestones:  {},
    mt5ClosedTP:   false,
    mt5CloseAt:    null,
    mt5CloseReason: null,
    phantomSLHit:  false,
    slHitAt:       null,
    timeToSLMin:   null,
  };
}

function updateGhost(ghost, currentPrice) {
  if (ghost.phantomSLHit) return false;
  const price  = parseFloat(currentPrice);
  const entry  = parseFloat(ghost.entry);
  const sl     = parseFloat(ghost.sl);
  const slDist = Math.abs(entry - sl);
  if (slDist <= 0) return false;
  const isBuy  = ghost.direction === "buy";

  const fav = isBuy ? price - entry : entry - price;
  const rr  = fav / slDist;
  if (rr > ghost.maxRR)     ghost.maxRR     = rr;
  if (rr > ghost.peakRRPos) ghost.peakRRPos = rr;

  const adv    = isBuy ? entry - price : price - entry;
  const advPct = Math.max(0, adv / slDist * 100);
  if (advPct > ghost.peakRRNeg) ghost.peakRRNeg = advPct;

  for (let v = 0.1; v <= 20.0 + 1e-9; v = Math.round((v + 0.1) * 10) / 10) {
    const key = "+" + v.toFixed(1);
    if (!ghost.rrMilestones[key] && rr >= v - 1e-9) {
      ghost.rrMilestones[key] = Date.now();
    }
  }

  const advRR = isBuy ? (entry - price) / slDist : (price - entry) / slDist;
  for (let v = 0.1; v <= 1.0 + 1e-9; v = Math.round((v + 0.1) * 10) / 10) {
    const key = "-" + v.toFixed(1);
    if (!ghost.rrMilestones[key] && advRR >= v - 1e-9) {
      ghost.rrMilestones[key] = Date.now();
    }
  }

  const hitSL = isBuy ? price <= sl : price >= sl;
  if (hitSL) {
    ghost.phantomSLHit = true;
    ghost.slHitAt      = new Date().toISOString();
    const openedTs     = ghost.openedAt ? new Date(ghost.openedAt).getTime() : Date.now() - 60000;
    ghost.timeToSLMin  = Math.round((Date.now() - openedTs) / 60000);
    const elapsed = Math.max(1, ghost.timeToSLMin);
    for (let v = 1.0; v >= 0.1 - 1e-9; v = Math.round((v - 0.1) * 10) / 10) {
      const key = "-" + v.toFixed(1);
      if (!ghost.rrMilestones[key]) {
        const ts = openedTs + Math.round(elapsed * v * 60000);
        ghost.rrMilestones[key] = ts;
      }
    }
    return true;
  }
  return false;
}

function msToElapsed(rrMilestones, openedAt) {
  const openedTs = openedAt ? new Date(openedAt).getTime() : null;
  if (!openedTs) return rrMilestones;
  const result = {};
  for (const [key, val] of Object.entries(rrMilestones)) {
    const tsMs = typeof val === "number" ? val : new Date(val).getTime();
    const elMin = Math.round((tsMs - openedTs) / 60000);
    const el = Math.max(0, elMin);
    if (el < 60) result[key] = el + "m";
    else {
      const h = Math.floor(el / 60), m = el % 60;
      result[key] = h + "h" + (m ? String(m).padStart(2, "0") + "m" : "");
    }
  }
  return result;
}

async function finalizeGhost(ghost) {
  const elapsedMilestones = msToElapsed(ghost.rrMilestones, ghost.openedAt);
  await db.saveGhostTrade({
    positionId:     ghost.positionId,
    dailyLabel:     ghost.dailyLabel,
    optimizerKey:   ghost.optimizerKey,
    symbol:         ghost.symbol,
    assetType:      ghost.assetType,
    direction:      ghost.direction,
    session:        ghost.session,
    vwapPosition:   ghost.vwapPosition,
    entry:          ghost.entry,
    sl:             ghost.sl,
    tp:             ghost.tp,
    lots:           ghost.lots,
    riskEur:        ghost.riskEur,
    slPct:          ghost.slPct,
    slDist:         ghost.slDist,
    vwapMid:        ghost.vwapMid,
    vwapUpper:      ghost.vwapUpper,
    vwapLower:      ghost.vwapLower,
    vwapBandPct:    ghost.vwapBandPct,
    sessionHigh:    ghost.sessionHigh,
    sessionLow:     ghost.sessionLow,
    dayHigh:        ghost.dayHigh,
    dayLow:         ghost.dayLow,
    tvEntry:        ghost.tvEntry,
    mt5Comment:     ghost.mt5Comment,
    peakRRPos:      ghost.peakRRPos,
    rrMilestones:   elapsedMilestones,
    timeToSLMin:    ghost.timeToSLMin,
    mt5CloseReason: ghost.mt5CloseReason,
    openedAt:       ghost.openedAt,
    closedAt:       ghost.slHitAt ?? new Date().toISOString(),
  });
  await db.deleteGhostState(ghost.positionId);
  const pos = openPositions.get(ghost.positionId);
  if (pos) {
    pos.finalizedAt = Date.now();
    pos.ghostFinalized = true;
    pos.ghost.finalizedAt = Date.now();
  }
  console.log(`[Ghost] Finalized ${ghost.positionId} ${ghost.symbol} peakRR=${ghost.peakRRPos.toFixed(2)}R SL=${ghost.timeToSLMin}m`);
}

function cleanupFinalizedGhosts() {}

// ── syncPositions ─────────────────────────────────────────────────
async function syncPositions() {
  if (!dbReady || _syncRunning || circuitOpen()) return;
  _syncRunning = true;
  try {
    const now = Date.now();
    if (now - _acctCacheTs > 60000) {
      const acct = await getAccountInfo();
      if (acct?.equity) {
        latestEquity = parseFloat(acct.equity);
        if (now - _lastEquitySave > 300000) {
          _lastEquitySave = now;
          const openPnl = [...openPositions.values()].reduce((s, p) => s + (p.livePnl ?? 0), 0);
          db.saveEquity(acct.balance, acct.equity, openPnl, openPositions.size).catch(() => {});
        }
      }
    }

    const liveMT5 = await getPositions();
    const liveIds = new Set(
      (liveMT5.length === 0 && openPositions.size > 0 && !_circuitOpen)
        ? (console.warn(`[Sync] MetaAPI 0 positions but ${openPositions.size} in memory — skipping close detection`), [])
        : liveMT5.map(p => String(p.id))
    );

    const closedIds = [...openPositions.keys()].filter(id => !liveIds.has(id));
    await Promise.all(closedIds.map(async id => {
      const pos = openPositions.get(id);
      if (!pos) return;
      if (pos.mt5Closed || pos.ghostFinalized) return;

      const ageMs = pos.openedAt ? Date.now() - new Date(pos.openedAt).getTime() : 999999;
      if (ageMs < 90000) {
        console.log(`[Sync] Skipping close check for ${id} — only ${Math.round(ageMs/1000)}s old`);
        return;
      }

      let closeReason = "sl";
      try {
        if (_circuitOpen) { return; }
        const deals = await getDeals(id);

        if (!deals.length) {
          const zeroCount = (_zeroDealsCount.get(id) || 0) + 1;
          _zeroDealsCount.set(id, zeroCount);
          if (zeroCount >= 3) {
            console.warn(`[Sync] ${id} 0 deals for ${zeroCount} syncs — forcing mt5Closed`);
            const pos2 = openPositions.get(id);
            if (pos2 && !pos2.mt5Closed) {
              pos2.mt5Closed = true;
              if (pos2.ghost) { pos2.ghost.mt5ClosedTP = true; pos2.ghost.mt5CloseReason = "unknown"; }
            }
            _zeroDealsCount.delete(id);
          } else {
            console.log(`[Sync] ${id} 0 deals (${zeroCount}/3) — skipping`);
          }
          return;
        }

        const outDeals = deals.filter(d =>
          (d.entryType || "").toUpperCase().includes("OUT") ||
          (d.type || "").toUpperCase().includes("OUT") ||
          (d.entry || "").toUpperCase().includes("OUT")
        );

        if (!outDeals.length) {
          console.log(`[Sync] ${id} missing from live but no OUT deal found — keeping open`);
          return;
        }

        const closing = outDeals.sort((a, b) => new Date(b.time || 0) - new Date(a.time || 0))[0];
        if (closing) {
          const r = (closing.reason || "").toUpperCase();
          if (r.includes("TP") || r.includes("TAKE_PROFIT")) closeReason = "tp";
          else if (r.includes("SL") || r.includes("STOP_LOSS")) closeReason = "sl";
          else if (closing.profit != null) closeReason = parseFloat(closing.profit) > 0 ? "tp" : "sl";
          if (closing.price) pos._exitPrice = parseFloat(closing.price);
        }
      } catch {}

      if (pos._exitPrice && pos.tp && pos.sl) {
        const exitP = pos._exitPrice;
        const tp    = parseFloat(pos.tp);
        const sl    = parseFloat(pos.sl);
        const entry = parseFloat(pos.entry);
        const slDist = Math.abs(entry - sl);
        const tpDist = Math.abs(entry - tp);
        const distToTP = Math.abs(exitP - tp);
        const distToSL = Math.abs(exitP - sl);
        if (distToTP < slDist * 0.10) closeReason = "tp";
        else if (distToSL < slDist * 0.10) closeReason = "sl";
        const tpRRActual = slDist > 0 ? tpDist / slDist : (pos.tpRR ?? 1.5);
        const ghost = pos.ghost;
        if (ghost && ghost.peakRRPos >= tpRRActual - 0.2) closeReason = "tp";
      } else if (pos.ghost && pos.ghost.peakRRPos >= (pos.tpRR ?? 1.5) - 0.2) {
        closeReason = "tp";
      }

      const ghost = pos.ghost;
      await db.saveClosedTrade({
        positionId: id, dailyLabel: pos.dailyLabel, symbol: pos.symbol,
        assetType: pos.assetType, direction: pos.direction, session: pos.session,
        vwapPosition: pos.vwapPosition, optimizerKey: pos.optimizerKey,
        entry: pos.entry, sl: pos.sl, tp: pos.tp, lots: pos.lots,
        riskPct: pos.riskPct, riskEur: pos.riskEur, slPct: pos.slPct,
        slPoints: pos.slPoints, slDist: pos.slDist, vwapMid: pos.vwapMid,
        vwapUpper: pos.vwapUpper, vwapLower: pos.vwapLower, vwapBandPct: pos.vwapBandPct,
        sessionHigh: pos.sessionHigh, sessionLow: pos.sessionLow,
        dayHigh: pos.dayHigh, dayLow: pos.dayLow, tvEntry: pos.tvEntry,
        executionPrice: pos.executionPrice, slippage: pos.slippage,
        exitPrice: pos._exitPrice ?? null, closeReason,
        peakRRPos: ghost?.peakRRPos ?? 0, peakRRNeg: ghost?.peakRRNeg ?? 0,
        mt5Comment: pos.mt5Comment, openedAt: pos.openedAt,
        closedAt: new Date().toISOString(),
      });

      if (closeReason === "sl") {
        if (ghost && !ghost.phantomSLHit) {
          ghost.phantomSLHit  = true;
          ghost.slHitAt       = new Date().toISOString();
          ghost.timeToSLMin   = Math.round((Date.now() - new Date(ghost.openedAt).getTime()) / 60000);
          const elapsed       = Math.max(1, ghost.timeToSLMin);
          const openedTs      = new Date(ghost.openedAt).getTime();
          for (let v = 1.0; v >= 0.1 - 1e-9; v = Math.round((v - 0.1) * 10) / 10) {
            const key = "-" + v.toFixed(1);
            if (!ghost.rrMilestones[key])
              ghost.rrMilestones[key] = openedTs + Math.round(elapsed * v * 60000);
          }
        }
        ghost.mt5CloseReason = "sl";
        await finalizeGhost(ghost);
      } else {
        if (ghost) {
          ghost.mt5ClosedTP    = true;
          ghost.mt5CloseAt     = new Date().toISOString();
          ghost.mt5CloseReason = "tp";
          pos.mt5Closed = true;
          await db.saveGhostState(ghost);
          console.log(`[Ghost] MT5 TP hit for ${id} ${pos.symbol} — ghost tracking on`);
        } else {
          openPositions.delete(id);
        }
      }
    }));

    for (const lp of liveMT5) {
      const id  = String(lp.id);
      const pos = openPositions.get(id);

      if (!pos) { await adoptPosition(lp); continue; }

      if (pos.mt5Closed && !pos.ghostFinalized) {
        console.log(`[Sync] Resetting false-close for ${id} ${pos.symbol}`);
        pos.mt5Closed = false;
        if (pos.ghost) {
          pos.ghost.mt5ClosedTP = false;
          pos.ghost.mt5CloseAt = null;
          pos.ghost.mt5CloseReason = null;
        }
      }

      if (lp.volume != null) pos.lots = parseFloat(lp.volume);
      if (lp.currentPrice)   pos.currentPrice = parseFloat(lp.currentPrice);
      const rawPnl = lp.profit ?? lp.unrealizedProfit ?? null;
      if (rawPnl != null) pos.livePnl = parseFloat(rawPnl);

      if (pos.ghost && lp.currentPrice) {
        const prevPeak = pos.ghost.peakRRPos;
        const prevMsCount = Object.keys(pos.ghost.rrMilestones).length;
        const justHit = updateGhost(pos.ghost, lp.currentPrice);
        if (justHit) {
          pos.ghost.mt5CloseReason = pos.mt5Closed ? "tp" : "sl";
          await finalizeGhost(pos.ghost);
          continue;
        }
        const changed = pos.ghost.peakRRPos !== prevPeak
          || Object.keys(pos.ghost.rrMilestones).length !== prevMsCount;
        if (changed) await db.saveGhostState(pos.ghost);
      }
    }

    const _now30 = Date.now();
    const _skipGhost = syncPositions._lastGhostPriceFetch && _now30 - syncPositions._lastGhostPriceFetch < 30000;
    if (!_skipGhost) {
      syncPositions._lastGhostPriceFetch = _now30;
      const ghostOnlySyms = new Set(
        [...openPositions.values()]
          .filter(p => p.mt5Closed && p.ghost && !p.ghost.phantomSLHit)
          .map(p => p.symbol)
      );
      const symPrices = new Map();
      for (const sym of ghostOnlySyms) {
        try {
          const symInfo = getSymbolInfo(sym);
          if (!symInfo) continue;
          const q = await metaFetch(`/users/current/accounts/${META_ACCOUNT}/symbols/${symInfo.mt5}/current-price`);
          if (q?.bid && q?.ask) symPrices.set(sym, { bid: parseFloat(q.bid), ask: parseFloat(q.ask) });
        } catch {}
      }
      for (const [id, pos] of openPositions) {
        if (!pos.mt5Closed || !pos.ghost || pos.ghost.phantomSLHit) continue;
        const prices = symPrices.get(pos.symbol);
        if (!prices) continue;
        const curPrice = pos.direction === "buy" ? prices.bid : prices.ask;
        pos.currentPrice = curPrice;
        const justHit = updateGhost(pos.ghost, curPrice);
        if (justHit) {
          pos.ghost.mt5CloseReason = "tp";
          await finalizeGhost(pos.ghost);
        } else {
          await db.saveGhostState(pos.ghost);
        }
      }
    }

  } catch(syncErr) {
    console.warn('[Sync] Non-critical error:', syncErr.message);
  } finally {
    _syncRunning = false;
  }
}

// ── Adopt MT5 position not in memory ─────────────────────────────
const MT5_TO_CATALOG = Object.fromEntries(
  Object.entries(BROKER_SYMBOL_MAP[BROKER]).map(([key, val]) => [val.mt5, key])
);

async function adoptPosition(lp) {
  const id     = String(lp.id);
  const rawSym = lp.symbol || "";
  const symbol = MT5_TO_CATALOG[rawSym] ?? normalizeSymbol(rawSym) ?? rawSym;
  const symInfo = getSymbolInfo(symbol);
  if (!symInfo) return;

  const lpType  = (lp.type || lp.positionType || "").toString().toUpperCase();
  const isBuy   = lpType.includes("BUY") || lpType === "POSITION_TYPE_BUY";
  const direction = isBuy ? "buy" : "sell";
  const entry   = parseFloat(lp.openPrice ?? lp.currentPrice ?? 0);
  const sl      = parseFloat(lp.stopLoss ?? 0);
  const tp      = parseFloat(lp.takeProfit ?? 0) || null;
  const lots    = parseFloat(lp.volume ?? 0);
  const openedAt = lp.time ? new Date(lp.time).toISOString() : new Date().toISOString();
  const session  = getSession(new Date(openedAt));
  const slDist   = Math.abs(entry - sl);
  const slPct    = entry > 0 && slDist > 0 ? slDist / entry : 0.003;
  let vwapPos = "unknown";
  if (lp.comment) {
    if (lp.comment.includes("ABV")) vwapPos = "above";
    else if (lp.comment.includes("BLW")) vwapPos = "below";
  }
  const optimizerKey = buildOptimizerKey(symbol, session, direction, vwapPos);

  const pos = {
    positionId: id, dailyLabel: lp.comment?.match(/\d{2}\/\d{2}-#\d+/)?.[0] ?? null,
    symbol, assetType: symInfo.type, direction, session,
    vwapPosition: "unknown", optimizerKey, entry, sl, tp, lots,
    riskPct: DEFAULT_RISK_PCT, riskEur: null, slPct, slDist, slPoints: null,
    vwapMid: null, vwapUpper: null, vwapLower: null, vwapBandPct: null,
    sessionHigh: null, sessionLow: null, dayHigh: null, dayLow: null,
    tvEntry: entry, executionPrice: entry, slippage: 0,
    mt5Comment: lp.comment ?? null, openedAt,
    currentPrice: parseFloat(lp.currentPrice ?? entry),
    livePnl: parseFloat(lp.profit ?? 0), mt5Closed: false,
  };
  pos.ghost = initGhost(pos);
  openPositions.set(id, pos);
  if (dbReady) await db.saveGhostState(pos.ghost);
  if (dbReady) {
    try {
      const sig = await db.pool.query(
        "SELECT vwap_mid,vwap_upper,vwap_lower,vwap_band_pct,session_high,session_low,day_high,day_low,tv_entry,sl_pct FROM signal_log WHERE position_id=$1 LIMIT 1", [id]
      );
      if (sig.rows.length) {
        const s=sig.rows[0];
        const en={vwapMid:parseFloat(s.vwap_mid)||null,vwapUpper:parseFloat(s.vwap_upper)||null,
          vwapLower:parseFloat(s.vwap_lower)||null,vwapBandPct:parseFloat(s.vwap_band_pct)||null,
          sessionHigh:parseFloat(s.session_high)||null,sessionLow:parseFloat(s.session_low)||null,
          dayHigh:parseFloat(s.day_high)||null,dayLow:parseFloat(s.day_low)||null,
          tvEntry:parseFloat(s.tv_entry)||pos.tvEntry,slPct:parseFloat(s.sl_pct)||pos.slPct};
        Object.assign(pos,en);if(pos.ghost)Object.assign(pos.ghost,en);
      }
    } catch(e){}
  }
  console.log(`[Adopt] ${id} ${symbol} ${direction} entry=${entry}`);
}

// ── Webhook secret check ──────────────────────────────────────────
function checkSecret(req, res) {
  if (!WEBHOOK_SECRET) { res.status(401).json({ error: "WEBHOOK_SECRET not set" }); return false; }
  const provided = req.headers["x-webhook-secret"] || req.headers["x-secret"]
    || req.body?.secret || req.query?.secret;
  if (provided !== WEBHOOK_SECRET) { res.status(401).json({ error: "Unauthorized" }); return false; }
  return true;
}

// ── Routes ────────────────────────────────────────────────────────
app.get("/", (req, res) => { res.setHeader("Content-Type", "text/html; charset=utf-8"); res.send(dashboardHTML()); });
app.get("/dashboard", (req, res) => { res.setHeader("Content-Type", "text/html; charset=utf-8"); res.send(dashboardHTML()); });
app.get("/health", (req, res) => {
  res.json({ ok: true, version: VERSION, broker: BROKER, dbReady, openPositions: openPositions.size, circuitOpen: _circuitOpen, uptime: Math.round(process.uptime()), ts: new Date().toISOString() });
});
app.get("/status", async (req, res) => {
  const acct = circuitOpen() ? _acctCache : await getAccountInfo().catch(() => _acctCache);
  res.json({ version: VERSION, broker: BROKER, dbReady, openPositions: openPositions.size, account: acct ? { balance: acct.balance, equity: acct.equity, currency: acct.currency } : null, ts: new Date().toISOString() });
});

// ── Main webhook ──────────────────────────────────────────────────
app.post("/webhook", async (req, res) => {
  const t0 = Date.now();
  if (!checkSecret(req, res)) return;
  if (!dbReady) return res.status(503).json({ error: "DB not ready, retry shortly" });
  console.log(`[Webhook] Received: ${JSON.stringify(req.body).slice(0,120)}`);

  const body = req.body ?? {};
  const { symbol: rawSym, direction: _dir, action: _action, sl_pct, sl_points, vwap, vwap_upper, vwap_lower, session_high, session_low, day_high, day_low } = body;
  const tvClose = body.close ?? body.entry ?? null;
  const direction = (_dir ?? _action ?? "").toLowerCase().trim();
  if (direction !== "buy" && direction !== "sell") return res.status(400).json({ error: `Invalid direction: "${direction}"` });

  if (isDuplicateWebhook(rawSym||"", direction)) {
    console.log(`[Webhook] Duplicate skipped: ${rawSym} ${direction}`);
    await db.logSignal({ symbol: rawSym, direction, session: getSession(), outcome: "DUPLICATE", rejectReason: "Duplicate signal within 60s window", tvEntry: safeNum(tvClose), slPct: safeNum(sl_pct), latencyMs: Date.now() - t0 }).catch(() => {});
    return res.json({ ok:false, reason:"DUPLICATE_SIGNAL" });
  }

  if (_circuitOpen) {
    const circuitAge = Date.now() - _circuitOpenAt;
    if (circuitAge > 30000) { console.warn(`[Webhook] Circuit was open ${Math.round(circuitAge/1000)}s — resetting`); _circuitOpen = false; _metaFails = 0; }
    else { console.warn(`[Webhook] Circuit OPEN — order blocked for ${rawSym} ${direction}`); }
  }

  const { allowed, reason: blockReason } = canOpenNewTrade(rawSym);
  if (!allowed) {
    const blockOutcome = blockReason.startsWith("SYMBOL") ? "SYMBOL_NOT_ALLOWED" : blockReason.startsWith("TIME_BLOCK") ? "TIME_BLOCKED" : "WEEKEND";
    await db.logSignal({ symbol: rawSym, direction, session: getSession(), outcome: blockOutcome, rejectReason: blockReason, tvEntry: safeNum(tvClose), slPct: safeNum(sl_pct), latencyMs: Date.now() - t0, slPoints: safeNum(sl_points), vwapMid: safeNum(vwap), vwapUpper: safeNum(vwap_upper), vwapLower: safeNum(vwap_lower), sessionHigh: safeNum(session_high), sessionLow: safeNum(session_low), dayHigh: safeNum(day_high), dayLow: safeNum(day_low) });
    return res.json({ ok: false, reason: blockReason });
  }

  const symbol   = normalizeSymbol(rawSym);
  const symInfo  = getSymbolInfo(symbol);
  const session  = getSession();
  const tvEntry  = safeNum(tvClose);
  const vwapMid  = safeNum(vwap);
  const vwapPos  = getVwapPosition(tvEntry, vwapMid);
  const optKey   = buildOptimizerKey(symbol, session, direction, vwapPos);
  const slPct    = safeNum(sl_pct) ?? 0.003;

  const _sH = safeNum(session_high), _sL = safeNum(session_low);
  const wh = {
    slPoints:    safeNum(sl_points),
    vwapUpper:   safeNum(vwap_upper),
    vwapLower:   safeNum(vwap_lower),
    sessionHigh: _sH ?? safeNum(day_high) ?? null,
    sessionLow:  _sL ?? safeNum(day_low) ?? null,
    dayHigh:     safeNum(day_high),
    dayLow:      safeNum(day_low),
  };

  let vwapBandPct = null;
  if (tvEntry != null && vwapMid != null && wh.vwapUpper != null) {
    const halfBand = Math.abs(wh.vwapUpper - vwapMid);
    if (halfBand > 0.001) vwapBandPct = parseFloat(((Math.abs(tvEntry - vwapMid) / halfBand) * 100).toFixed(2));
  }

  if (!circuitOpen()) {
    const acct = await Promise.race([getAccountInfo(), new Promise(r => setTimeout(() => r(null), 5000))]);
    if (acct?.equity) latestEquity = parseFloat(acct.equity);
  }

  // ── Live MT5 quote ────────────────────────────────────────────
  let execPrice   = tvEntry ?? 0;
  let spreadAtEntry = null;
  try {
    const q = await metaFetch(`/users/current/accounts/${META_ACCOUNT}/symbols/${symInfo.mt5}/current-price`);
    if (q?.bid && q?.ask) {
      spreadAtEntry = parseFloat((q.ask - q.bid).toFixed(6));
      execPrice     = direction === "buy" ? parseFloat(q.ask) : parseFloat(q.bid);
    }
  } catch (e) {
    if (e.message?.includes('503') || e.message?.includes('Service Unavailable')) _metaFails = Math.max(0, _metaFails - 1);
  }
  if (!execPrice && tvEntry) execPrice = tvEntry;
  const slippage = tvEntry && execPrice ? Math.abs(execPrice - tvEntry) : 0;

  // ── SL & TP calculation ───────────────────────────────────────
  const slDist  = parseFloat((slPct * SL_BUFFER_MULT * execPrice).toFixed(6));
  const slPrice = direction === "buy"
    ? parseFloat((execPrice - slDist).toFixed(6))
    : parseFloat((execPrice + slDist).toFixed(6));
  const tpRR    = getTpRR(symbol, new Date());
  const tpPrice = direction === "buy"
    ? parseFloat((execPrice + slDist * tpRR).toFixed(6))
    : parseFloat((execPrice - slDist * tpRR).toFixed(6));

  // ── Lot calculation with broker volume constraints ────────────
  const riskEur = parseFloat((latestEquity * DEFAULT_RISK_PCT).toFixed(2));
  const lotNom  = slDist > 0 ? riskEur / slDist : 0.01;
  const lotRaw  = symInfo.type === "index"
    ? parseFloat(lotNom.toFixed(2))
    : parseFloat((lotNom / 100).toFixed(2));
  const lots    = roundLots(lotRaw, symInfo);

  const dateStr    = getBrusselsDateStr();
  const dailyCount = await db.getNextDailyCount(dateStr).catch(() => 1);
  const dailyLabel = buildDailyLabel(null, dailyCount);

  const sessMap = { ny: "NY", london: "LD", asia: "AS" };
  const vwapMap = { above: "ABV", below: "BLW", unknown: "UNK" };
  const mt5Comment = `${symbol.slice(0, 6)} ${direction === "buy" ? "B" : "S"}-${sessMap[session] ?? "NY"}-${vwapMap[vwapPos] ?? "UNK"} ${dailyLabel}`;

  console.log(`[Webhook] ${symbol} ${direction.toUpperCase()} | exec=${execPrice} slDist=${slDist.toFixed(4)} (${(slPct * 100).toFixed(3)}%×${SL_BUFFER_MULT}) | lots=${lots} riskEur=${riskEur} | ${dailyLabel}`);

  // ── Place order ───────────────────────────────────────────────
  let positionId;
  try {
    const r = await placeOrder({
      symbol: symInfo.mt5,
      actionType: direction === "buy" ? "ORDER_TYPE_BUY" : "ORDER_TYPE_SELL",
      volume: lots,
      stopLoss: slPrice,
      takeProfit: tpPrice,
      comment: mt5Comment,
    });
    positionId = r?.positionId ?? r?.orderId ?? null;

    if (!positionId) {
      const placeTime = Date.now();
      for (let attempt = 0; attempt < 5; attempt++) {
        await new Promise(res => setTimeout(res, 2000));
        const liveNow = await getPositions();
        const match   = liveNow.find(lp => {
          const lpDir = (lp.type || "").includes("BUY") ? "buy" : "sell";
          const ot    = lp.time ? new Date(lp.time).getTime() : 0;
          return lp.symbol === symInfo.mt5 && lpDir === direction && ot >= placeTime - 30000 && !openPositions.has(String(lp.id));
        });
        if (match) { positionId = String(match.id); break; }
      }
    }
    if (!positionId) {
      console.warn(`[Webhook] ORDER_NOT_CONFIRMED: ${symbol} ${direction} session=${session} circuitOpen=${_circuitOpen}`);
      await db.logSignal({ dailyLabel: null, symbol, assetType: symInfo.type, direction, session, vwapPosition: vwapPos, optimizerKey: optKey, tvEntry, slPct, vwapMid, vwapBandPct, ...wh, outcome: "ORDER_NOT_CONFIRMED", rejectReason: "No positionId from MetaAPI", latencyMs: Date.now() - t0 });
      return res.status(202).json({ ok: false, reason: "ORDER_NOT_CONFIRMED" });
    }
  } catch (e) {
    console.error(`[Webhook] placeOrder error: ${e.message}`);
    await db.logSignal({ symbol, assetType: symInfo.type, direction, session, vwapPosition: vwapPos, optimizerKey: optKey, tvEntry, slPct, vwapMid, vwapBandPct, ...wh, outcome: "ERROR", rejectReason: e.message, latencyMs: Date.now() - t0 });
    return res.status(500).json({ error: e.message });
  }

  // ── Build position + ghost ────────────────────────────────────
  const pos = {
    positionId, dailyLabel, symbol, assetType: symInfo.type,
    direction, session, vwapPosition: vwapPos, optimizerKey: optKey,
    entry: execPrice, sl: slPrice, tp: tpPrice, lots, tpRR,
    riskPct: DEFAULT_RISK_PCT, riskEur, slPct, slDist,
    tvEntry, executionPrice: execPrice, slippage,
    vwapMid, vwapBandPct, ...wh,
    mt5Comment, openedAt: new Date().toISOString(),
    currentPrice: execPrice, livePnl: 0, mt5Closed: false,
  };
  pos.ghost = initGhost(pos);
  openPositions.set(positionId, pos);

  await db.saveGhostState(pos.ghost);
  await db.logSignal({ dailyLabel, symbol, assetType: symInfo.type, direction, session, vwapPosition: vwapPos, optimizerKey: optKey, tvEntry, slPct, vwapMid, vwapBandPct, ...wh, outcome: "PLACED", latencyMs: Date.now() - t0, positionId });

  console.log(`[Placed] ${positionId} ${symbol} ${direction} lots=${lots} entry=${execPrice} sl=${slPrice} tp=${tpPrice} ${dailyLabel}`);
  markWebhookPlaced(rawSym||"", direction);
  res.json({ ok: true, positionId, symbol, direction, lots, entry: execPrice, sl: slPrice, tp: tpPrice, riskEur, dailyLabel, mt5Comment, latencyMs: Date.now() - t0 });
});

// ── API endpoints ─────────────────────────────────────────────────
app.get("/api/open-positions", (req, res) => {
  const out = [];
  for (const [id, pos] of openPositions) {
    const g = pos.ghost;
    out.push({ positionId: id, dailyLabel: pos.dailyLabel, symbol: pos.symbol, assetType: pos.assetType, direction: pos.direction, session: pos.session, vwapPosition: pos.vwapPosition, optimizerKey: pos.optimizerKey, entry: pos.entry, sl: pos.sl, tp: pos.tp, lots: pos.lots, riskEur: pos.riskEur, slPct: pos.slPct, slDist: pos.slDist, tvEntry: pos.tvEntry, vwapMid: pos.vwapMid, vwapUpper: pos.vwapUpper, vwapLower: pos.vwapLower, vwapBandPct: pos.vwapBandPct, sessionHigh: pos.sessionHigh, sessionLow: pos.sessionLow, dayHigh: pos.dayHigh, dayLow: pos.dayLow, mt5Comment: pos.mt5Comment, openedAt: pos.openedAt, currentPrice: pos.currentPrice ?? null, livePnl: pos.livePnl ?? null, mt5Closed: pos.mt5Closed ?? false, ghostFinalized: pos.ghostFinalized ?? false, mt5CloseReason: pos.ghost?.mt5CloseReason ?? null,
      ghost: g ? { maxRR: g.maxRR, peakRRPos: g.peakRRPos, peakRRNeg: g.peakRRNeg, rrMilestones: msToElapsed(g.rrMilestones, g.openedAt), mt5ClosedTP: g.mt5ClosedTP ?? false, phantomSLHit: g.phantomSLHit, mt5CloseReason: g.mt5CloseReason ?? null, timeToSLMin: g.timeToSLMin ?? null, slHitAt: g.slHitAt ?? null } : null,
    });
  }
  res.json(out);
});

app.get("/api/closed-trades", async (req, res) => { if (!dbReady) return res.json([]); res.json(await db.loadClosedTrades(parseInt(req.query.limit) || 200)); });
app.get("/api/signal-log", async (req, res) => { if (!dbReady) return res.json([]); res.json(await db.loadSignalLog(parseInt(req.query.limit) || 200)); });
app.get("/api/ghost-active", (req, res) => { res.redirect("/api/open-positions"); });
app.get("/api/ghost-history", async (req, res) => { if (!dbReady) return res.json([]); res.json(await db.loadGhostTrades(req.query.from ?? null, req.query.to ?? null, parseInt(req.query.limit) || 300)); });
app.get("/api/equity-curve", async (req, res) => { if (!dbReady) return res.json([]); res.json(await db.loadEquityCurve(200)); });
app.get("/api/performance", async (req, res) => {
  if (!dbReady) return res.json({});
  const trades = await db.loadClosedTrades(500);
  const tp  = trades.filter(t => t.closeReason === "tp").length;
  const sl  = trades.filter(t => t.closeReason === "sl").length;
  const wr  = trades.length ? (tp / trades.length * 100).toFixed(1) : "0.0";
  const peakAvg = trades.length ? (trades.reduce((s, t) => s + (t.peakRRPos || 0), 0) / trades.length).toFixed(2) : "0.00";
  res.json({ total: trades.length, tp, sl, winRate: parseFloat(wr), avgPeakRR: parseFloat(peakAvg), balance: latestEquity, currency: latestCurrency });
});
app.get("/api/performance-by-key", async (req, res) => { if (!dbReady) return res.json([]); res.json(await db.loadPerformanceByKey()); });

app.get("/api/db-inspect", async (req, res) => {
  if (!db.DB_ENABLED) return res.json({ dbEnabled: false, message: "Running without a database", openPositionsInMemory: openPositions.size });
  try {
    const tables = await db.pool.query(`SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`);
    const counts = {};
    for (const t of tables.rows) { try { const r = await db.pool.query(`SELECT COUNT(*) AS n FROM "${t.tablename}"`); counts[t.tablename] = parseInt(r.rows[0].n); } catch { counts[t.tablename] = -1; } }
    res.json({ tables: counts, openPositionsInMemory: openPositions.size });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.post("/api/force-sync", async (req, res) => { if (!checkSecret(req, res)) return; await syncPositions(); res.json({ ok: true, openPositions: openPositions.size }); });
app.post("/api/recover", async (req, res) => {
  if (!checkSecret(req, res)) return;
  const live = await getPositions();
  let adopted = 0;
  for (const lp of live) { if (!openPositions.has(String(lp.id))) { await adoptPosition(lp); adopted++; } }
  res.json({ ok: true, adopted, total: openPositions.size });
});

// ── Dashboard HTML ────────────────────────────────────────────────
function dashboardHTML() {
  return `<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>PRONTO·AI v${VERSION} | ${BROKER.toUpperCase()}</title>
<style>*{box-sizing:border-box;margin:0;padding:0}body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',system-ui,sans-serif;background:#0d1117;color:#e6edf3;font-size:12px}.hdr{background:#161b22;border-bottom:1px solid rgba(139,148,158,.15);padding:6px 14px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;position:sticky;top:0;z-index:100}.brand{font-size:13px;font-weight:700}.brand span{color:#bc8cff}.hkv{font-size:10px;color:#8b949e;white-space:nowrap}.hkv b{color:#e6edf3}.hkv.cg b{color:#3fb950}.hkv.cr b{color:#f85149}.hkv.cb b{color:#388bfd}.hkv.cp b{color:#bc8cff}.hstat{margin-left:auto;display:flex;align-items:center;gap:8px;font-size:10px}.dot-g{width:7px;height:7px;border-radius:50%;background:#3fb950;display:inline-block;animation:blink 2s infinite}.dot-r{width:7px;height:7px;border-radius:50%;background:#f85149;display:inline-block}@keyframes blink{0%,100%{opacity:1}50%{opacity:.4}}.nav{background:#161b22;border-bottom:1px solid rgba(139,148,158,.15);display:flex;padding:0 14px;overflow-x:auto}.ntab{padding:9px 14px;font-size:11px;color:#8b949e;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap}.ntab:hover{color:#e6edf3}.ntab.on{color:#3fb950;border-bottom-color:#3fb950;font-weight:600}.pg{display:none;padding:12px 14px}.pg.on{display:block}.card{background:#161b22;border:1px solid rgba(139,148,158,.15);border-radius:6px;margin-bottom:10px;overflow:hidden}.chdr{padding:7px 10px;border-bottom:1px solid rgba(139,148,158,.1);display:flex;align-items:center;gap:8px;flex-wrap:wrap}.ctitle{font-size:11px;font-weight:600;color:#e6edf3;display:flex;align-items:center;gap:6px}.dot{width:7px;height:7px;border-radius:50%;flex-shrink:0}.dot.g{background:#3fb950}.dot.r{background:#f85149}.dot.b{background:#388bfd}.cm{margin-left:auto;font-size:9px;color:#6e7681}.tw{width:100%;overflow-x:auto}table{border-collapse:collapse;width:100%}th{text-align:left;font-size:9px;font-weight:500;color:#6e7681;padding:4px 5px;border-bottom:1px solid rgba(139,148,158,.15);white-space:nowrap;background:#161b22}td{padding:4px 5px;border-bottom:1px solid rgba(139,148,158,.08);font-size:10px;vertical-align:middle;white-space:nowrap}tr:hover td{background:rgba(139,148,158,.04)}.nd{text-align:center;color:#6e7681;padding:20px;font-size:11px}.bd{display:inline-flex;align-items:center;padding:1px 5px;border-radius:3px;font-size:9px;font-weight:700;white-space:nowrap}.bd-buy{background:rgba(63,185,80,.15);color:#3fb950;border:1px solid rgba(63,185,80,.3)}.bd-sell{background:rgba(248,81,73,.15);color:#f85149;border:1px solid rgba(248,81,73,.3)}.bd-ab{background:rgba(63,185,80,.1);color:#3fb950}.bd-bw{background:rgba(248,81,73,.1);color:#f85149}.bd-idx{background:rgba(57,211,242,.15);color:#39d3f2;border:1px solid rgba(57,211,242,.3)}.bd-com{background:rgba(188,140,255,.15);color:#bc8cff;border:1px solid rgba(188,140,255,.3)}.bd-placed{background:rgba(63,185,80,.15);color:#3fb950;border:1px solid rgba(63,185,80,.3)}.bd-nopos{background:rgba(248,81,73,.15);color:#f85149;border:1px solid rgba(248,81,73,.3)}.bd-err{background:rgba(248,81,73,.3);color:#ff4444;border:1px solid #f85149;font-weight:700}.bd-live{background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.25);padding:2px 7px;font-size:9px;font-weight:700}.bd-k{background:rgba(139,148,158,.1);color:#e6edf3;border:1px solid rgba(139,148,158,.25);font-size:9px;font-weight:700;padding:1px 5px;border-radius:3px;display:inline-flex}.kst{display:grid;gap:6px;padding:8px 10px}.ks{background:#0d1117;border-radius:4px;padding:6px 10px;border:1px solid rgba(139,148,158,.1)}.ksl{font-size:9px;color:#8b949e;text-transform:uppercase;letter-spacing:.4px;margin-bottom:2px}.ksv{font-size:16px;font-weight:700;color:#e6edf3}.cg{color:#3fb950}.cr{color:#f85149}.cb{color:#388bfd}.cp{color:#bc8cff}.cy{color:#d29922}.cd{color:#8b949e}.cw{color:#e6edf3}.fw{font-weight:700}.segs{display:flex;background:#0d1117;border:1px solid rgba(139,148,158,.2);border-radius:4px;overflow:hidden;margin-left:auto}.seg{padding:3px 10px;background:none;border:none;color:#6e7681;cursor:pointer;font-size:10px}.seg.on{background:#21262d;color:#e6edf3}::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:#0d1117}::-webkit-scrollbar-thumb{background:#30363d;border-radius:2px}</style>
</head><body>
<div class="hdr">
  <div class="brand">PRONTO<span>·</span>AI <span style="font-size:10px;color:#6e7681;font-weight:400">v${VERSION} | ${BROKER.toUpperCase()}</span></div>
  <div class="hkv">Balance <b id="h-bal">--</b></div>
  <div class="hkv cg">Equity <b id="h-eq">--</b></div>
  <div class="hkv cb">Open <b id="h-open">--</b></div>
  <div class="hkv cp">Ghost <b id="h-ghost">--</b></div>
  <div class="hkv" id="h-db">DB init...</div>
  <div class="hstat"><span id="h-sess-dot" class="dot-g"></span><span id="h-sess" style="font-size:10px;color:#8b949e">--</span><span id="h-time" style="font-size:10px;color:#6e7681;margin-left:4px">--</span></div>
</div>
<div class="nav">
  <div class="ntab on" onclick="go('ov',this)">Overview</div>
  <div class="ntab" onclick="go('sig',this)">Signals <span style="background:rgba(139,148,158,.15);color:#8b949e;border-radius:8px;padding:1px 5px;font-size:9px" id="nb-sig">0</span></div>
  <div class="ntab" onclick="go('gh',this)">Ghost Tracker <span style="background:rgba(188,140,255,.15);color:#bc8cff;border-radius:8px;padding:1px 5px;font-size:9px" id="nb-gh">0</span></div>
  <div class="ntab" onclick="go('perf',this)">Performance</div>
</div>
<div style="padding:12px 14px">
<div class="pg on" id="p-ov">
  <div class="card"><div class="chdr"><div class="ctitle"><div class="dot g"></div>Trades</div><span style="font-size:9px;background:rgba(56,139,253,.1);color:#388bfd;border:1px solid rgba(56,139,253,.25);padding:1px 6px;border-radius:3px;margin-left:8px" id="ov-open-badge">0 open</span><span style="font-size:9px;background:rgba(139,148,158,.1);color:#6e7681;border:1px solid rgba(139,148,158,.2);padding:1px 6px;border-radius:3px" id="ov-closed-badge">0 closed</span></div>
  <div class="tw"><table><thead><tr><th>#</th><th>Symbol</th><th>Type</th><th>Dir</th><th>VWAP</th><th>Session</th><th>Entry</th><th style="color:#f85149">SL</th><th style="color:#3fb950">TP</th><th>Lots</th><th>Opened</th><th>Status</th></tr></thead><tbody id="ov-body"><tr><td colspan="12" class="nd">Loading...</td></tr></tbody></table></div></div>
</div>
<div class="pg" id="p-sig">
  <div class="card"><div class="chdr"><div class="ctitle"><div class="dot b"></div>Signal Log</div>
  <div class="segs"><button class="seg on" onclick="filterSig('all',this)">All</button><button class="seg" onclick="filterSig('placed',this)">Placed</button><button class="seg" onclick="filterSig('errors',this)">Errors</button></div></div>
  <div class="tw"><table><thead><tr><th>Time</th><th>Daily#</th><th>Symbol</th><th>Dir</th><th>Session</th><th>VWAP</th><th>Entry</th><th>SL%</th><th>Outcome</th><th>Latency</th></tr></thead><tbody id="sig-body"><tr><td colspan="10" class="nd">Loading...</td></tr></tbody></table></div></div>
</div>
<div class="pg" id="p-gh">
  <div class="card"><div class="chdr"><div class="ctitle"><div class="dot g"></div>Ghost Tracker</div></div>
  <div class="tw"><table><thead><tr><th>Status</th><th>#</th><th>Symbol</th><th>Dir</th><th>Session</th><th>Peak+</th><th>Entry</th><th>SL</th><th>TP</th><th>Lots</th><th>Opened</th></tr></thead><tbody id="gh-body"><tr><td colspan="11" class="nd">Loading...</td></tr></tbody></table></div></div>
</div>
<div class="pg" id="p-perf">
  <div class="card"><div class="chdr"><div class="ctitle"><div class="dot b"></div>Performance</div></div>
  <div class="kst" style="grid-template-columns:repeat(5,1fr)" id="perf-kpis"></div></div>
</div>
</div>
<script>
'use strict';
const $=id=>document.getElementById(id);
const fmt=(v,d=2)=>v==null||isNaN(v)?'--':Number(v).toFixed(d);
const fmtTs=s=>!s?'--':new Date(s).toLocaleString('nl-BE',{timeZone:'Europe/Brussels',day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
function bdDir(d){return d==='buy'?'<span class="bd bd-buy">BUY</span>':'<span class="bd bd-sell">SELL</span>';}
function bdType(t){return t==='commodity'?'<span class="bd bd-com">COM</span>':'<span class="bd bd-idx">IDX</span>';}
function bdVwap(v){return v==='above'?'<span class="bd bd-ab">ABOVE</span>':'<span class="bd bd-bw">BELOW</span>';}
function bdSess(s){const m={ny:'NEW YORK',london:'LONDON',asia:'ASIA'};const c={ny:'#f0883e',london:'#3fb950',asia:'#8b949e'};return '<span style="color:'+(c[s]||'#8b949e')+';font-size:10px;font-weight:500">'+(m[s]||s||'--')+'</span>';}
async function api(u){try{const r=await fetch(u);if(!r.ok)return null;return await r.json();}catch{return null;}}
function tick(){const now=new Date();const t=now.toLocaleTimeString('nl-BE',{timeZone:'Europe/Brussels',hour:'2-digit',minute:'2-digit',second:'2-digit'});const h=parseInt(now.toLocaleString('nl-BE',{timeZone:'Europe/Brussels',hour:'2-digit',hour12:false}));const m=now.getMinutes();const isNY=(h>=15&&h<21)||(h===15&&m>=30),isLD=(h>=8&&h<15)||(h===15&&m<30);if($('h-sess'))$('h-sess').textContent=isNY?'NEW YORK':isLD?'LONDON':'ASIA';if($('h-time'))$('h-time').textContent=t;}
setInterval(tick,1000);tick();
function go(pg,el){document.querySelectorAll('.pg').forEach(p=>p.classList.remove('on'));document.querySelectorAll('.ntab').forEach(t=>t.classList.remove('on'));const p=$('p-'+pg);if(p)p.classList.add('on');if(el)el.classList.add('on');if(pg==='ov')loadOv();if(pg==='sig')loadSig();if(pg==='gh')loadGh();if(pg==='perf')loadPerf();}
async function loadHeader(){const s=await api('/status');if(s){if(s.dbReady&&$('h-db'))$('h-db').textContent='DB ✓';if($('h-open'))$('h-open').textContent=s.openPositions||0;if(s.account){if($('h-bal'))$('h-bal').textContent=Math.round(s.account.balance||0).toLocaleString()+' '+s.account.currency;if($('h-eq'))$('h-eq').textContent=Math.round(s.account.equity||0).toLocaleString()+' '+s.account.currency;}}}
async function loadOv(){const[pos,closed]=await Promise.all([api('/api/open-positions'),api('/api/closed-trades')]);const _p=Array.isArray(pos)?pos:[];const _c=Array.isArray(closed)?closed:[];const open=_p.filter(p=>!p.mt5Closed&&!p.ghostFinalized);if($('ov-open-badge'))$('ov-open-badge').textContent=open.length+' open';if($('ov-closed-badge'))$('ov-closed-badge').textContent=_c.length+' closed';if($('nb-gh'))$('nb-gh').textContent=_p.length;const body=$('ov-body');if(!body)return;const rows=open.map(p=>'<tr><td><span class="bd-k">'+(p.dailyLabel||'--')+'</span></td><td class="cw fw">'+p.symbol+'</td><td>'+bdType(p.assetType)+'</td><td>'+bdDir(p.direction)+'</td><td>'+bdVwap(p.vwapPosition)+'</td><td>'+bdSess(p.session)+'</td><td class="cd">'+fmt(p.entry,p.assetType==='index'?2:4)+'</td><td class="cr">'+fmt(p.sl,p.assetType==='index'?2:4)+'</td><td class="cg">'+fmt(p.tp,p.assetType==='index'?2:4)+'</td><td class="cd">'+fmt(p.lots,2)+'</td><td class="cd" style="font-size:9px">'+fmtTs(p.openedAt)+'</td><td><span class="bd-live">● LIVE</span></td></tr>');if(_c.length)rows.push('<tr><td colspan="12" style="padding:5px 10px;font-size:9px;color:#6e7681;background:rgba(248,81,73,.05);border-top:1px solid rgba(139,148,158,.15)">Closed — '+_c.length+' trades</td></tr>');_c.forEach(t=>{const isTP=t.closeReason==='tp';rows.push('<tr><td><span class="bd-k">'+(t.dailyLabel||'--')+'</span></td><td class="cw fw">'+t.symbol+'</td><td>'+bdType(t.assetType)+'</td><td>'+bdDir(t.direction)+'</td><td>'+bdVwap(t.vwapPosition)+'</td><td>'+bdSess(t.session)+'</td><td class="cd">'+fmt(t.entry,t.assetType==='index'?2:4)+'</td><td class="cr">'+fmt(t.sl,t.assetType==='index'?2:4)+'</td><td class="cg">'+fmt(t.tp,t.assetType==='index'?2:4)+'</td><td class="cd">'+fmt(t.lots,2)+'</td><td class="cd" style="font-size:9px">'+fmtTs(t.closedAt)+'</td><td>'+(isTP?'<span class="bd" style="background:rgba(63,185,80,.2);color:#3fb950;border:1px solid rgba(63,185,80,.4)">TP</span>':'<span class="bd" style="background:rgba(248,81,73,.2);color:#f85149;border:1px solid rgba(248,81,73,.4)">SL</span>')+'</td></tr>');});body.innerHTML=rows.join('')||'<tr><td colspan="12" class="nd">No trades yet</td></tr>';}
let _sigAll=[],_sigFilter='all';
async function loadSig(){_sigAll=await api('/api/signal-log?limit=500')||[];if($('nb-sig'))$('nb-sig').textContent=_sigAll.length;renderSig();}
function filterSig(f,el){_sigFilter=f;document.querySelectorAll('.seg').forEach(b=>b.classList.remove('on'));if(el)el.classList.add('on');renderSig();}
function renderSig(){const data=_sigFilter==='placed'?_sigAll.filter(s=>s.outcome==='PLACED'):_sigFilter==='errors'?_sigAll.filter(s=>['ERROR','ORDER_NOT_CONFIRMED'].includes(s.outcome)):_sigAll;const body=$('sig-body');if(!body)return;if(!data.length){body.innerHTML='<tr><td colspan="10" class="nd">No signals yet</td></tr>';return;}body.innerHTML=data.map(s=>{let ob;if(s.outcome==='PLACED')ob='<span class="bd bd-placed">PLACED</span>';else if(s.outcome==='ERROR')ob='<span class="bd bd-err">ERROR</span>';else if(s.outcome==='ORDER_NOT_CONFIRMED')ob='<span class="bd bd-nopos">No Pos</span>';else ob='<span class="bd" style="background:rgba(240,136,62,.15);color:#f0883e;border:1px solid rgba(240,136,62,.3)">'+s.outcome+'</span>';return'<tr><td class="cd" style="font-size:9px">'+fmtTs(s.receivedAt)+'</td><td class="cw">'+(s.dailyLabel||'—')+'</td><td class="cw fw">'+(s.symbol||'--')+'</td><td>'+bdDir(s.direction)+'</td><td>'+bdSess(s.session)+'</td><td>'+bdVwap(s.vwapPosition||'unknown')+'</td><td class="cd">'+fmt(s.tvEntry,s.assetType==='index'?2:5)+'</td><td class="cd">'+(s.slPct?(s.slPct*100).toFixed(3)+'%':'--')+'</td><td>'+ob+'</td><td class="cd">'+(s.latencyMs!=null?s.latencyMs+'ms':'--')+'</td></tr>';}).join('');}
async function loadGh(){const pos=await api('/api/open-positions')||[];const body=$('gh-body');if(!body)return;if(!pos.length){body.innerHTML='<tr><td colspan="11" class="nd">No ghost trades yet</td></tr>';return;}body.innerHTML=pos.map(p=>{const g=p.ghost||{};const pkp=g.peakRRPos||0;let sb;if(p.ghostFinalized)sb='<span class="bd" style="background:rgba(139,148,158,.15);color:#e6edf3;border:1px solid rgba(139,148,158,.4);padding:2px 7px;font-size:9px;font-weight:700">FINISHED</span>';else if(p.mt5Closed)sb='<span class="bd" style="background:rgba(188,140,255,.15);color:#bc8cff;border:1px solid rgba(188,140,255,.3);padding:2px 7px;font-size:9px;font-weight:700">GHOST</span>';else sb='<span class="bd bd-live">● LIVE</span>';return'<tr><td>'+sb+'</td><td><span class="bd-k">'+(p.dailyLabel||'--')+'</span></td><td class="cw fw">'+p.symbol+'</td><td>'+bdDir(p.direction)+'</td><td>'+bdSess(p.session)+'</td><td>'+(pkp>0?'<span class="cg fw">+'+pkp.toFixed(2)+'R</span>':'--')+'</td><td class="cd">'+fmt(p.entry,p.assetType==='index'?2:4)+'</td><td class="cr">'+fmt(p.sl,p.assetType==='index'?2:4)+'</td><td class="cg">'+fmt(p.tp,p.assetType==='index'?2:4)+'</td><td class="cd">'+fmt(p.lots,2)+'</td><td class="cd" style="font-size:9px">'+fmtTs(p.openedAt)+'</td></tr>';}).join('');}
async function loadPerf(){const perf=await api('/api/performance');if(perf&&$('perf-kpis')){const kpis=[['Total',perf.total,'cw'],['TP',perf.tp,'cg'],['SL',perf.sl,'cr'],['Win Rate',(perf.winRate||0).toFixed(1)+'%','cy'],['Balance',perf.balance?Math.round(perf.balance).toLocaleString()+' '+perf.currency:'--','cb']];$('perf-kpis').innerHTML=kpis.map(x=>'<div class="ks"><div class="ksl">'+x[0]+'</div><div class="ksv '+x[2]+'">'+(x[1]!=null?x[1]:'--')+'</div></div>').join('');}}
loadHeader();loadOv();
setInterval(loadHeader,15000);
setInterval(()=>{const a=document.querySelector('.pg.on');if(!a)return;if(a.id==='p-ov')loadOv();if(a.id==='p-gh')loadGh();},5000);
setInterval(()=>{const a=document.querySelector('.pg.on');if(a?.id==='p-sig')loadSig();},30000);
</script></body></html>`;
}

// ── Background init ───────────────────────────────────────────────
async function initBackground() {
  console.log(db.DB_ENABLED ? "[PRONTO-AI] DATABASE_URL is set — persistence enabled" : "[PRONTO-AI] No DATABASE_URL — running in-memory only, no persistence across restarts");
  let retries = 0;
  while (retries < 5) {
    try { await db.initDB(); break; }
    catch (e) { retries++; console.error(`[DB] init failed (${retries}/5): ${e.message}`); if (retries < 5) await new Promise(r => setTimeout(r, 5000 * retries)); else throw e; }
  }

  try {
    const states = await db.loadAllGhostStates();
    for (const g of states) {
      if (!g.positionId || !g.entry || !g.sl) continue;
      const pos = { positionId: g.positionId, dailyLabel: g.dailyLabel, symbol: g.symbol, assetType: g.assetType, direction: g.direction, session: g.session, vwapPosition: g.vwapPosition, optimizerKey: g.optimizerKey, entry: g.entry, sl: g.sl, tp: g.tp, lots: g.lots, riskEur: g.riskEur, slPct: g.slPct, slDist: g.slDist, vwapMid: g.vwapMid, vwapUpper: g.vwapUpper, vwapLower: g.vwapLower, vwapBandPct: g.vwapBandPct, sessionHigh: g.sessionHigh, sessionLow: g.sessionLow, dayHigh: g.dayHigh, dayLow: g.dayLow, tvEntry: g.tvEntry, mt5Comment: g.mt5Comment, openedAt: g.openedAt, mt5Closed: g.mt5ClosedTP ?? false, currentPrice: g.entry, livePnl: 0,
        ghost: { positionId: g.positionId, dailyLabel: g.dailyLabel, optimizerKey: g.optimizerKey, symbol: g.symbol, assetType: g.assetType, direction: g.direction, session: g.session, vwapPosition: g.vwapPosition, entry: g.entry, sl: g.sl, tp: g.tp, lots: g.lots, riskEur: g.riskEur, slPct: g.slPct, slDist: g.slDist, vwapMid: g.vwapMid, vwapUpper: g.vwapUpper, vwapLower: g.vwapLower, vwapBandPct: g.vwapBandPct, sessionHigh: g.sessionHigh, sessionLow: g.sessionLow, dayHigh: g.dayHigh, dayLow: g.dayLow, tvEntry: g.tvEntry, mt5Comment: g.mt5Comment, openedAt: g.openedAt, maxRR: g.maxRR ?? 0, peakRRPos: g.peakRRPos ?? 0, peakRRNeg: g.peakRRNeg ?? 0, rrMilestones: g.rrMilestones ?? {}, mt5ClosedTP: g.mt5ClosedTP ?? false, mt5CloseAt: g.mt5CloseAt ?? null, mt5CloseReason: g.mt5ClosedTP ? "tp" : null, phantomSLHit: g.phantomSLHit ?? false, slHitAt: g.slHitAt ?? null, timeToSLMin: g.timeToSLMin ?? null },
      };
      openPositions.set(g.positionId, pos);
    }
    console.log(`[DB] Restored ${openPositions.size} ghost states`);
  } catch (e) { console.error("[DB] restore failed:", e.message); }

  dbReady = true;
  console.log("[PRONTO-AI] DB ready");

  if (META_API_TOKEN && META_ACCOUNT) {
    try {
      const acct = await Promise.race([
        metaFetch(`/users/current/accounts/${META_ACCOUNT}/account-information`),
        new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 20000)),
      ]);
      if (acct?.balance !== undefined) {
        latestEquity   = parseFloat(acct.equity ?? acct.balance);
        latestCurrency = acct.currency ?? "USD";
        _acctCache = acct; _acctCacheTs = Date.now();
        console.log(`[MetaAPI] Connected — ${acct.balance} ${acct.currency}`);
        const live = await getPositions();
        for (const lp of live) { if (!openPositions.has(String(lp.id))) await adoptPosition(lp); }
      }
    } catch (e) { console.error(`[MetaAPI] Startup failed: ${e.message}`); _metaFails = 0; _circuitOpen = false; }
  } else {
    console.warn("[MetaAPI] META_API_TOKEN or META_ACCOUNT not set — no MetaAPI connection");
  }

  cron.schedule("*/10 * * * * *", syncPositions);
  cron.schedule("*/5 * * * *", cleanupFinalizedGhosts);
  console.log("[PRONTO-AI] Cron active — 10s sync");
}

initBackground().catch(e => { console.error("[FATAL] initBackground:", e.message); });
