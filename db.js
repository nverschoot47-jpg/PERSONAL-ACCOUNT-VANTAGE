"use strict";
// ================================================================
// db.js  v2.0.0  |  PRONTO-AI
// Clean schema for XAUUSD & US100 ghost trading
// ================================================================

const { Pool } = require("pg");

const DB_URL = process.env.DATABASE_URL;
const DB_ENABLED = !!DB_URL;

if (!DB_ENABLED) {
  console.warn("[DB] DATABASE_URL not set — running WITHOUT persistence (in-memory only). " +
               "Signal log, closed trades, ghost history, and equity curve will NOT survive a restart.");
}

const pool = DB_ENABLED
  ? new Pool({
      connectionString:        DB_URL,
      ssl:                     DB_URL.includes(".railway.internal") ? false : { rejectUnauthorized: false },
      max:                     8,
      connectionTimeoutMillis: 15000,
      idleTimeoutMillis:       30000,
      statement_timeout:       10000,
    })
  : null;

if (pool) pool.on("error", (err) => console.error("[DB Pool] error:", err.message));

// ── initDB ────────────────────────────────────────────────────────
async function initDB() {
  if (!DB_ENABLED) {
    console.log("[DB] Skipping initDB — running without persistence");
    return;
  }
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // signal_log: every webhook that arrives (PLACED or blocked)
    await client.query(`
      CREATE TABLE IF NOT EXISTS signal_log (
        id            SERIAL       PRIMARY KEY,
        received_at   TIMESTAMPTZ  DEFAULT NOW(),
        daily_label   TEXT,                         -- "01/06-#3" only for PLACED
        symbol        TEXT,
        asset_type    TEXT,
        direction     TEXT,
        session       TEXT,
        vwap_position TEXT,
        optimizer_key TEXT,
        tv_entry      NUMERIC,
        sl_pct        NUMERIC,
        sl_points     NUMERIC,
        vwap_mid      NUMERIC,
        vwap_upper    NUMERIC,
        vwap_lower    NUMERIC,
        vwap_band_pct NUMERIC,
        session_high  NUMERIC,
        session_low   NUMERIC,
        day_high      NUMERIC,
        day_low       NUMERIC,
        outcome       TEXT,                         -- PLACED / SYMBOL_NOT_ALLOWED / WEEKEND / ORDER_NOT_CONFIRMED / ERROR / DUPLICATE
        reject_reason TEXT,
        latency_ms    INTEGER,
        position_id   TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_signal_log_ts  ON signal_log (received_at DESC);
      CREATE INDEX IF NOT EXISTS idx_signal_log_sym ON signal_log (symbol, received_at DESC);
      CREATE INDEX IF NOT EXISTS idx_signal_log_out ON signal_log (outcome);
    `);

    // closed_trades: MT5 position closed (TP or SL)
    await client.query(`
      CREATE TABLE IF NOT EXISTS closed_trades (
        id              SERIAL       PRIMARY KEY,
        position_id     TEXT         UNIQUE,
        daily_label     TEXT,
        symbol          TEXT         NOT NULL,
        asset_type      TEXT,
        direction       TEXT         NOT NULL,
        session         TEXT,
        vwap_position   TEXT,
        optimizer_key   TEXT,
        entry           NUMERIC      NOT NULL,
        sl              NUMERIC      NOT NULL,
        tp              NUMERIC,
        lots            NUMERIC,
        risk_pct        NUMERIC,
        risk_eur        NUMERIC,
        sl_pct          NUMERIC,
        sl_points       NUMERIC,
        sl_dist         NUMERIC,
        vwap_mid        NUMERIC,
        vwap_upper      NUMERIC,
        vwap_lower      NUMERIC,
        vwap_band_pct   NUMERIC,
        session_high    NUMERIC,
        session_low     NUMERIC,
        day_high        NUMERIC,
        day_low         NUMERIC,
        tv_entry        NUMERIC,
        execution_price NUMERIC,
        slippage        NUMERIC,
        exit_price      NUMERIC,
        close_reason    TEXT,                       -- "tp" or "sl"
        peak_rr_pos     NUMERIC      DEFAULT 0,
        peak_rr_neg     NUMERIC      DEFAULT 0,
        mt5_comment     TEXT,
        opened_at       TIMESTAMPTZ,
        closed_at       TIMESTAMPTZ  DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_closed_trades_opened ON closed_trades (opened_at DESC);
      CREATE INDEX IF NOT EXISTS idx_closed_trades_key    ON closed_trades (optimizer_key);
      CREATE INDEX IF NOT EXISTS idx_closed_trades_sym    ON closed_trades (symbol);
    `);

    // ghost_state: active ghost trackers (persist across restarts)
    await client.query(`
      CREATE TABLE IF NOT EXISTS ghost_state (
        position_id     TEXT         PRIMARY KEY,
        daily_label     TEXT,
        optimizer_key   TEXT         NOT NULL,
        symbol          TEXT         NOT NULL,
        asset_type      TEXT,
        direction       TEXT         NOT NULL,
        session         TEXT,
        vwap_position   TEXT,
        entry           NUMERIC      NOT NULL,
        sl              NUMERIC      NOT NULL,
        tp              NUMERIC,
        lots            NUMERIC,
        risk_eur        NUMERIC,
        sl_pct          NUMERIC,
        sl_dist         NUMERIC,
        vwap_mid        NUMERIC,
        vwap_upper      NUMERIC,
        vwap_lower      NUMERIC,
        vwap_band_pct   NUMERIC,
        session_high    NUMERIC,
        session_low     NUMERIC,
        day_high        NUMERIC,
        day_low         NUMERIC,
        tv_entry        NUMERIC,
        mt5_comment     TEXT,
        max_rr          NUMERIC      DEFAULT 0,
        peak_rr_pos     NUMERIC      DEFAULT 0,
        peak_rr_neg     NUMERIC      DEFAULT 0,
        rr_milestones   JSONB        DEFAULT '{}',
        mt5_closed_tp   BOOLEAN      DEFAULT FALSE,
        mt5_close_at    TIMESTAMPTZ,
        phantom_sl_hit  BOOLEAN      DEFAULT FALSE,
        sl_hit_at       TIMESTAMPTZ,
        time_to_sl_min  INTEGER,
        opened_at       TIMESTAMPTZ,
        updated_at      TIMESTAMPTZ  DEFAULT NOW()
      );
    `);

    // ghost_trades: finalized ghost trackers (phantom SL hit)
    await client.query(`
      CREATE TABLE IF NOT EXISTS ghost_trades (
        id              SERIAL       PRIMARY KEY,
        position_id     TEXT         UNIQUE,
        daily_label     TEXT,
        optimizer_key   TEXT         NOT NULL,
        symbol          TEXT         NOT NULL,
        asset_type      TEXT,
        direction       TEXT         NOT NULL,
        session         TEXT,
        vwap_position   TEXT,
        entry           NUMERIC      NOT NULL,
        sl              NUMERIC      NOT NULL,
        tp              NUMERIC,
        lots            NUMERIC,
        risk_eur        NUMERIC,
        sl_pct          NUMERIC,
        sl_dist         NUMERIC,
        vwap_mid        NUMERIC,
        vwap_upper      NUMERIC,
        vwap_lower      NUMERIC,
        vwap_band_pct   NUMERIC,
        session_high    NUMERIC,
        session_low     NUMERIC,
        day_high        NUMERIC,
        day_low         NUMERIC,
        tv_entry        NUMERIC,
        mt5_comment     TEXT,
        peak_rr_pos     NUMERIC      DEFAULT 0,
        rr_milestones   JSONB        DEFAULT '{}',
        time_to_sl_min  INTEGER,
        mt5_close_reason TEXT,                      -- "tp" or "sl" — the MT5 close, ghost always ends on phantom SL
        opened_at       TIMESTAMPTZ,
        closed_at       TIMESTAMPTZ,                -- when phantom SL was hit
        created_at      TIMESTAMPTZ  DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_key    ON ghost_trades (optimizer_key);
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_sym    ON ghost_trades (symbol);
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_opened ON ghost_trades (opened_at DESC);
    `);

    // equity_curve: balance/equity snapshots every 5 min
    await client.query(`
      CREATE TABLE IF NOT EXISTS equity_curve (
        id          SERIAL       PRIMARY KEY,
        balance     NUMERIC,
        equity      NUMERIC,
        open_pnl    NUMERIC      DEFAULT 0,
        open_count  INTEGER      DEFAULT 0,
        recorded_at TIMESTAMPTZ  DEFAULT NOW()
      );
      CREATE INDEX IF NOT EXISTS idx_equity_curve_ts ON equity_curve (recorded_at DESC);
    `);

    // daily_counter: track trade # per day
    await client.query(`
      CREATE TABLE IF NOT EXISTS daily_counter (
        date_str    TEXT         PRIMARY KEY,        -- "2026-06-01"
        count       INTEGER      DEFAULT 0
      );
    `);

    // ── Step 2: Migrations (add missing columns to existing tables) ──
    await client.query(`
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS optimizer_key   TEXT;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS daily_label     TEXT;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS vwap_position   TEXT;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS vwap_mid        NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS vwap_upper      NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS vwap_lower      NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS vwap_band_pct   NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS session_high    NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS session_low     NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS day_high        NUMERIC;
      ALTER TABLE signal_log     ADD COLUMN IF NOT EXISTS day_low         NUMERIC;
    `);
    await client.query(`
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS optimizer_key   TEXT;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS daily_label     TEXT;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS asset_type      TEXT;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS vwap_position   TEXT;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS vwap_band_pct   NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS session_high    NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS session_low     NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS day_high        NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS day_low         NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS peak_rr_pos     NUMERIC DEFAULT 0;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS peak_rr_neg     NUMERIC DEFAULT 0;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS mt5_comment     TEXT;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS tv_entry        NUMERIC;
      ALTER TABLE closed_trades  ADD COLUMN IF NOT EXISTS sl_dist         NUMERIC;
    `);
    await client.query(`
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS optimizer_key   TEXT;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS daily_label     TEXT;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS asset_type      TEXT;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS vwap_position   TEXT;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS vwap_band_pct   NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS session_high    NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS session_low     NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS day_high        NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS day_low         NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS tv_entry        NUMERIC;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS mt5_comment     TEXT;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS mt5_close_at    TIMESTAMPTZ;
      ALTER TABLE ghost_state    ADD COLUMN IF NOT EXISTS time_to_sl_min  INTEGER;
    `);
    await client.query(`
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS optimizer_key   TEXT;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS daily_label     TEXT;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS asset_type      TEXT;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS vwap_position   TEXT;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS vwap_band_pct   NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS session_high    NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS session_low     NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS day_high        NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS day_low         NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS tv_entry        NUMERIC;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS mt5_comment     TEXT;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS time_to_sl_min  INTEGER;
      ALTER TABLE ghost_trades   ADD COLUMN IF NOT EXISTS mt5_close_reason TEXT;
    `);

    // Fix: drop phantom_sl_hit NOT NULL (no-op if already nullable/default)
    await client.query(`ALTER TABLE ghost_trades ALTER COLUMN phantom_sl_hit DROP NOT NULL`).catch(()=>{});
    // Fix: tp nullable in ghost_state
    await client.query(`ALTER TABLE ghost_state ALTER COLUMN tp DROP NOT NULL`).catch(()=>{});
    // Fix: UNIQUE constraint on ghost_trades.position_id (required for ON CONFLICT)
    await client.query(`
      DO $d$ BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM pg_constraint c
          JOIN pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=c.conkey[1]
          WHERE c.conrelid='ghost_trades'::regclass
            AND c.contype IN ('u','p') AND a.attname='position_id'
        ) THEN
          ALTER TABLE ghost_trades ADD CONSTRAINT ghost_trades_position_id_key UNIQUE (position_id);
        END IF;
      END $d$
    `).catch(()=>{});

    // Data recovery: copy closed_trades → ghost_trades on every startup
    // Ensures FINISHED data survives every redeploy forever
    await client.query(`
      INSERT INTO ghost_trades (
        position_id, daily_label, optimizer_key, symbol, asset_type,
        direction, session, vwap_position,
        entry, sl, tp, lots, risk_eur, sl_pct, sl_dist,
        vwap_mid, vwap_upper, vwap_lower, vwap_band_pct,
        session_high, session_low, day_high, day_low,
        tv_entry, mt5_comment,
        peak_rr_pos, peak_rr_neg,
        rr_milestones, mt5_close_reason, opened_at, closed_at
      )
      SELECT
        ct.position_id, ct.daily_label, ct.optimizer_key, ct.symbol, ct.asset_type,
        ct.direction, ct.session, ct.vwap_position,
        ct.entry, ct.sl, ct.tp, ct.lots, ct.risk_eur, ct.sl_pct, ct.sl_dist,
        ct.vwap_mid, ct.vwap_upper, ct.vwap_lower, ct.vwap_band_pct,
        ct.session_high, ct.session_low, ct.day_high, ct.day_low,
        ct.tv_entry, ct.mt5_comment,
        COALESCE(ct.peak_rr_pos, 0),
        COALESCE(ct.peak_rr_neg, 0),
        '{}',
        ct.close_reason,
        ct.opened_at, ct.closed_at
      FROM closed_trades ct
      WHERE ct.position_id IS NOT NULL
        AND ct.opened_at IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM ghost_trades gt WHERE gt.position_id = ct.position_id
        )
    `).catch(()=>{});
    console.log("[DB] Migrations applied + data recovery done");

    // ── Step 3: Indexes (now safe — columns exist) ─────────────────
    await client.query(`
      CREATE INDEX IF NOT EXISTS idx_signal_log_ts     ON signal_log     (received_at DESC);
      CREATE INDEX IF NOT EXISTS idx_signal_log_sym    ON signal_log     (symbol, received_at DESC);
      CREATE INDEX IF NOT EXISTS idx_signal_log_out    ON signal_log     (outcome);
      CREATE INDEX IF NOT EXISTS idx_closed_trades_opened ON closed_trades (opened_at DESC);
      CREATE INDEX IF NOT EXISTS idx_closed_trades_key    ON closed_trades (optimizer_key);
      CREATE INDEX IF NOT EXISTS idx_closed_trades_sym    ON closed_trades (symbol);
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_key     ON ghost_trades  (optimizer_key);
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_sym     ON ghost_trades  (symbol);
      CREATE INDEX IF NOT EXISTS idx_ghost_trades_opened  ON ghost_trades  (opened_at DESC);
      CREATE INDEX IF NOT EXISTS idx_equity_curve_ts      ON equity_curve  (recorded_at DESC);
    `);

    await client.query("COMMIT");
    console.log("[DB] Schema v2.0 ready");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

// ── Daily trade counter ────────────────────────────────────────────
const _memDailyCounter = {};
async function getNextDailyCount(dateStr) {
  if (!DB_ENABLED) {
    _memDailyCounter[dateStr] = (_memDailyCounter[dateStr] || 0) + 1;
    return _memDailyCounter[dateStr];
  }
  const r = await pool.query(`
    INSERT INTO daily_counter (date_str, count) VALUES ($1, 1)
    ON CONFLICT (date_str) DO UPDATE SET count = daily_counter.count + 1
    RETURNING count
  `, [dateStr]);
  return parseInt(r.rows[0].count);
}

// ── Signal log ─────────────────────────────────────────────────────
async function logSignal(data) {
  if (!DB_ENABLED) return;
  try {
    await pool.query(`
      INSERT INTO signal_log (
        daily_label, symbol, asset_type, direction, session, vwap_position, optimizer_key,
        tv_entry, sl_pct, sl_points, vwap_mid, vwap_upper, vwap_lower, vwap_band_pct,
        session_high, session_low, day_high, day_low,
        outcome, reject_reason, latency_ms, position_id
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
    `, [
      data.dailyLabel    ?? null,
      data.symbol        ?? null, data.assetType  ?? null,
      data.direction     ?? null, data.session    ?? null,
      data.vwapPosition  ?? null, data.optimizerKey ?? null,
      data.tvEntry       ?? null, data.slPct      ?? null, data.slPoints  ?? null,
      data.vwapMid       ?? null, data.vwapUpper  ?? null, data.vwapLower ?? null,
      data.vwapBandPct   ?? null,
      data.sessionHigh   ?? null, data.sessionLow ?? null,
      data.dayHigh       ?? null, data.dayLow     ?? null,
      data.outcome       ?? null, data.rejectReason ?? null,
      data.latencyMs     ?? null, data.positionId ?? null,
    ]);
  } catch (e) { console.warn("[!] logSignal:", e.message); }
}

async function loadSignalLog(limit = 200) {
  if (!DB_ENABLED) return [];
  try {
    const r = await pool.query(`
      SELECT
        id, received_at AS "receivedAt", daily_label AS "dailyLabel",
        symbol, asset_type AS "assetType", direction, session,
        vwap_position AS "vwapPosition", optimizer_key AS "optimizerKey",
        CAST(tv_entry     AS FLOAT) AS "tvEntry",
        CAST(sl_pct       AS FLOAT) AS "slPct",
        CAST(sl_points    AS FLOAT) AS "slPoints",
        CAST(vwap_mid     AS FLOAT) AS "vwapMid",
        CAST(vwap_upper   AS FLOAT) AS "vwapUpper",
        CAST(vwap_lower   AS FLOAT) AS "vwapLower",
        CAST(vwap_band_pct AS FLOAT) AS "vwapBandPct",
        CAST(session_high AS FLOAT) AS "sessionHigh",
        CAST(session_low  AS FLOAT) AS "sessionLow",
        CAST(day_high     AS FLOAT) AS "dayHigh",
        CAST(day_low      AS FLOAT) AS "dayLow",
        outcome, reject_reason AS "rejectReason",
        latency_ms AS "latencyMs", position_id AS "positionId"
      FROM signal_log
      ORDER BY received_at DESC
      LIMIT $1
    `, [limit]);
    return r.rows;
  } catch (e) { console.warn("[!] loadSignalLog:", e.message); return []; }
}

// ── Closed trades ──────────────────────────────────────────────────
async function saveClosedTrade(t) {
  if (!DB_ENABLED) return;
  try {
    await pool.query(`
      INSERT INTO closed_trades (
        position_id, daily_label, symbol, asset_type, direction, session, vwap_position, optimizer_key,
        entry, sl, tp, lots, risk_pct, risk_eur, sl_pct, sl_points, sl_dist,
        vwap_mid, vwap_upper, vwap_lower, vwap_band_pct,
        session_high, session_low, day_high, day_low,
        tv_entry, execution_price, slippage, exit_price, close_reason,
        peak_rr_pos, peak_rr_neg, mt5_comment, opened_at, closed_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,$16,$17,
        $18,$19,$20,$21,$22,$23,$24,$25,
        $26,$27,$28,$29,$30,$31,$32,$33,$34,$35
      )
      ON CONFLICT (position_id) DO UPDATE SET
        exit_price    = EXCLUDED.exit_price,
        close_reason  = EXCLUDED.close_reason,
        peak_rr_pos   = EXCLUDED.peak_rr_pos,
        peak_rr_neg   = EXCLUDED.peak_rr_neg,
        closed_at     = EXCLUDED.closed_at
    `, [
      t.positionId, t.dailyLabel,
      t.symbol, t.assetType, t.direction, t.session, t.vwapPosition, t.optimizerKey,
      t.entry, t.sl, t.tp ?? null, t.lots ?? null, t.riskPct ?? null, t.riskEur ?? null,
      t.slPct ?? null, t.slPoints ?? null, t.slDist ?? null,
      t.vwapMid ?? null, t.vwapUpper ?? null, t.vwapLower ?? null, t.vwapBandPct ?? null,
      t.sessionHigh ?? null, t.sessionLow ?? null, t.dayHigh ?? null, t.dayLow ?? null,
      t.tvEntry ?? null, t.executionPrice ?? null, t.slippage ?? null,
      t.exitPrice ?? null, t.closeReason ?? "sl",
      t.peakRRPos ?? 0, t.peakRRNeg ?? 0,
      t.mt5Comment ?? null, t.openedAt ?? null, t.closedAt ?? new Date().toISOString(),
    ]);
  } catch (e) { console.warn("[!] saveClosedTrade:", e.message); }
}

async function loadClosedTrades(limit = 200) {
  if (!DB_ENABLED) return [];
  try {
    const r = await pool.query(`
      SELECT
        position_id AS "positionId", daily_label AS "dailyLabel",
        symbol, asset_type AS "assetType", direction, session,
        vwap_position AS "vwapPosition", optimizer_key AS "optimizerKey",
        CAST(entry AS FLOAT) AS entry, CAST(sl AS FLOAT) AS sl, CAST(tp AS FLOAT) AS tp,
        CAST(lots AS FLOAT) AS lots, CAST(risk_eur AS FLOAT) AS "riskEur",
        CAST(sl_pct AS FLOAT) AS "slPct", CAST(sl_points AS FLOAT) AS "slPoints",
        CAST(sl_dist AS FLOAT) AS "slDist",
        CAST(vwap_mid AS FLOAT) AS "vwapMid",
        CAST(vwap_band_pct AS FLOAT) AS "vwapBandPct",
        CAST(session_high AS FLOAT) AS "sessionHigh", CAST(session_low AS FLOAT) AS "sessionLow",
        CAST(day_high AS FLOAT) AS "dayHigh", CAST(day_low AS FLOAT) AS "dayLow",
        CAST(tv_entry AS FLOAT) AS "tvEntry",
        CAST(exit_price AS FLOAT) AS "exitPrice",
        close_reason AS "closeReason",
        CAST(peak_rr_pos AS FLOAT) AS "peakRRPos",
        CAST(peak_rr_neg AS FLOAT) AS "peakRRNeg",
        mt5_comment AS "mt5Comment",
        opened_at AS "openedAt", closed_at AS "closedAt"
      FROM closed_trades
      ORDER BY opened_at DESC NULLS LAST
      LIMIT $1
    `, [limit]);
    return r.rows;
  } catch (e) { console.warn("[!] loadClosedTrades:", e.message); return []; }
}

// ── Ghost state ────────────────────────────────────────────────────
async function saveGhostState(g) {
  if (!DB_ENABLED) return;
  try {
    await pool.query(`
      INSERT INTO ghost_state (
        position_id, daily_label, optimizer_key, symbol, asset_type, direction, session, vwap_position,
        entry, sl, tp, lots, risk_eur, sl_pct, sl_dist,
        vwap_mid, vwap_upper, vwap_lower, vwap_band_pct,
        session_high, session_low, day_high, day_low, tv_entry, mt5_comment,
        max_rr, peak_rr_pos, peak_rr_neg, rr_milestones,
        mt5_closed_tp, mt5_close_at, phantom_sl_hit, sl_hit_at, time_to_sl_min,
        opened_at, updated_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,
        $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,
        $26,$27,$28,$29,$30,$31,$32,$33,$34,$35,NOW()
      )
      ON CONFLICT (position_id) DO UPDATE SET
        max_rr          = EXCLUDED.max_rr,
        peak_rr_pos     = EXCLUDED.peak_rr_pos,
        peak_rr_neg     = EXCLUDED.peak_rr_neg,
        rr_milestones   = EXCLUDED.rr_milestones,
        mt5_closed_tp   = EXCLUDED.mt5_closed_tp,
        mt5_close_at    = EXCLUDED.mt5_close_at,
        phantom_sl_hit  = EXCLUDED.phantom_sl_hit,
        sl_hit_at       = EXCLUDED.sl_hit_at,
        time_to_sl_min  = EXCLUDED.time_to_sl_min,
        lots            = COALESCE(EXCLUDED.lots, ghost_state.lots),
        updated_at      = NOW()
    `, [
      g.positionId, g.dailyLabel,
      g.optimizerKey, g.symbol, g.assetType, g.direction, g.session, g.vwapPosition,
      g.entry, g.sl, g.tp ?? null, g.lots ?? null, g.riskEur ?? null,
      g.slPct ?? null, g.slDist ?? null,
      g.vwapMid ?? null, g.vwapUpper ?? null, g.vwapLower ?? null, g.vwapBandPct ?? null,
      g.sessionHigh ?? null, g.sessionLow ?? null, g.dayHigh ?? null, g.dayLow ?? null,
      g.tvEntry ?? null, g.mt5Comment ?? null,
      g.maxRR ?? 0, g.peakRRPos ?? 0, g.peakRRNeg ?? 0,
      JSON.stringify(g.rrMilestones ?? {}),
      g.mt5ClosedTP ?? false, g.mt5CloseAt ?? null,
      g.phantomSLHit ?? false, g.slHitAt ?? null, g.timeToSLMin ?? null,
      g.openedAt ?? null,
    ]);
  } catch (e) { console.warn("[!] saveGhostState:", e.message); }
}

async function loadAllGhostStates() {
  if (!DB_ENABLED) return [];
  try {
    const r = await pool.query(`
      SELECT
        position_id AS "positionId", daily_label AS "dailyLabel",
        optimizer_key AS "optimizerKey", symbol, asset_type AS "assetType",
        direction, session, vwap_position AS "vwapPosition",
        CAST(entry AS FLOAT) AS entry, CAST(sl AS FLOAT) AS sl, CAST(tp AS FLOAT) AS tp,
        CAST(lots AS FLOAT) AS lots, CAST(risk_eur AS FLOAT) AS "riskEur",
        CAST(sl_pct AS FLOAT) AS "slPct", CAST(sl_dist AS FLOAT) AS "slDist",
        CAST(vwap_mid AS FLOAT) AS "vwapMid",
        CAST(vwap_upper AS FLOAT) AS "vwapUpper",
        CAST(vwap_lower AS FLOAT) AS "vwapLower",
        CAST(vwap_band_pct AS FLOAT) AS "vwapBandPct",
        CAST(session_high AS FLOAT) AS "sessionHigh",
        CAST(session_low AS FLOAT) AS "sessionLow",
        CAST(day_high AS FLOAT) AS "dayHigh",
        CAST(day_low AS FLOAT) AS "dayLow",
        CAST(tv_entry AS FLOAT) AS "tvEntry",
        mt5_comment AS "mt5Comment",
        CAST(max_rr AS FLOAT) AS "maxRR",
        CAST(peak_rr_pos AS FLOAT) AS "peakRRPos",
        CAST(peak_rr_neg AS FLOAT) AS "peakRRNeg",
        rr_milestones AS "rrMilestones",
        mt5_closed_tp AS "mt5ClosedTP", mt5_close_at AS "mt5CloseAt",
        phantom_sl_hit AS "phantomSLHit", sl_hit_at AS "slHitAt",
        time_to_sl_min AS "timeToSLMin",
        opened_at AS "openedAt"
      FROM ghost_state
    `);
    return r.rows;
  } catch (e) { console.warn("[!] loadAllGhostStates:", e.message); return []; }
}

async function deleteGhostState(positionId) {
  if (!DB_ENABLED) return;
  try {
    await pool.query("DELETE FROM ghost_state WHERE position_id = $1", [positionId]);
  } catch (e) { console.warn("[!] deleteGhostState:", e.message); }
}

// ── Ghost trades (finalized) ───────────────────────────────────────
async function saveGhostTrade(g) {
  if (!DB_ENABLED) return;
  try {
    await pool.query(`
      INSERT INTO ghost_trades (
        position_id, daily_label, optimizer_key, symbol, asset_type, direction, session, vwap_position,
        entry, sl, tp, lots, risk_eur, sl_pct, sl_dist,
        vwap_mid, vwap_upper, vwap_lower, vwap_band_pct,
        session_high, session_low, day_high, day_low, tv_entry, mt5_comment,
        peak_rr_pos, rr_milestones, time_to_sl_min,
        mt5_close_reason, opened_at, closed_at
      ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15,
        $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,
        $26,$27,$28,$29,$30,$31
      )
      ON CONFLICT (position_id) DO UPDATE SET
        peak_rr_pos     = EXCLUDED.peak_rr_pos,
        rr_milestones   = EXCLUDED.rr_milestones,
        time_to_sl_min  = EXCLUDED.time_to_sl_min,
        closed_at       = EXCLUDED.closed_at,
        lots            = COALESCE(EXCLUDED.lots, ghost_trades.lots)
    `, [
      g.positionId, g.dailyLabel,
      g.optimizerKey, g.symbol, g.assetType, g.direction, g.session, g.vwapPosition,
      g.entry, g.sl, g.tp ?? null, g.lots ?? null, g.riskEur ?? null,
      g.slPct ?? null, g.slDist ?? null,
      g.vwapMid ?? null, g.vwapUpper ?? null, g.vwapLower ?? null, g.vwapBandPct ?? null,
      g.sessionHigh ?? null, g.sessionLow ?? null, g.dayHigh ?? null, g.dayLow ?? null,
      g.tvEntry ?? null, g.mt5Comment ?? null,
      g.peakRRPos ?? 0,
      JSON.stringify(g.rrMilestones ?? {}),
      g.timeToSLMin ?? null,
      g.mt5CloseReason ?? null,
      g.openedAt ?? null, g.closedAt ?? new Date().toISOString(),
    ]);
  } catch (e) {
    if (e.message.includes('ON CONFLICT') || e.message.includes('constraint')) {
      // Fallback: plain UPDATE if unique constraint not yet in DB
      try {
        await pool.query(
          `UPDATE ghost_trades SET
            peak_rr_pos=GREATEST(peak_rr_pos,$1), rr_milestones=$2,
            time_to_sl_min=COALESCE($3,time_to_sl_min),
            closed_at=COALESCE($4,closed_at),
            lots=COALESCE($5,lots)
           WHERE position_id=$6`,
          [g.peakRRPos??0, JSON.stringify(g.rrMilestones??{}),
           g.timeToSLMin??null, g.closedAt??null, g.lots??null, g.positionId]
        );
      } catch(e2) { console.warn("[!] saveGhostTrade fallback:", e2.message); }
    } else { console.warn("[!] saveGhostTrade:", e.message); }
  }
}

async function loadGhostTrades(from = null, to = null, limit = 300) {
  if (!DB_ENABLED) return [];
  try {
    const params = [];
    const conds  = [];
    if (from) { params.push(from); conds.push(`opened_at >= $${params.length}`); }
    if (to)   { params.push(to);   conds.push(`opened_at <= $${params.length}`); }
    const where = conds.length ? "WHERE " + conds.join(" AND ") : "";
    params.push(limit);
    const r = await pool.query(`
      SELECT
        position_id AS "positionId", daily_label AS "dailyLabel",
        optimizer_key AS "optimizerKey", symbol, asset_type AS "assetType",
        direction, session, vwap_position AS "vwapPosition",
        CAST(entry AS FLOAT) AS entry, CAST(sl AS FLOAT) AS sl,
        CAST(tp AS FLOAT) AS tp,
        CAST(lots AS FLOAT) AS lots,
        CAST(sl_pct AS FLOAT) AS "slPct",
        CAST(sl_dist AS FLOAT) AS "slDist",
        CAST(tv_entry AS FLOAT) AS "tvEntry",
        CAST(vwap_mid AS FLOAT) AS "vwapMid",
        CAST(vwap_upper AS FLOAT) AS "vwapUpper",
        CAST(vwap_lower AS FLOAT) AS "vwapLower",
        CAST(vwap_band_pct AS FLOAT) AS "vwapBandPct",
        CAST(session_high AS FLOAT) AS "sessionHigh",
        CAST(session_low AS FLOAT) AS "sessionLow",
        CAST(day_high AS FLOAT) AS "dayHigh",
        CAST(day_low AS FLOAT) AS "dayLow",
        CAST(peak_rr_pos AS FLOAT) AS "peakRRPos",
        rr_milestones AS "rrMilestones",
        time_to_sl_min AS "timeToSLMin",
        mt5_close_reason AS "mt5CloseReason",
        mt5_comment AS "mt5Comment",
        opened_at AS "openedAt", COALESCE(closed_at, sl_hit_at, created_at) AS "closedAt"
      FROM ghost_trades
      ${where}
      ORDER BY opened_at DESC NULLS LAST
      LIMIT $${params.length}
    `, params);
    return r.rows;
  } catch (e) { console.warn("[!] loadGhostTrades:", e.message); return []; }
}

// ── Equity curve ───────────────────────────────────────────────────
async function saveEquity(balance, equity, openPnl, openCount) {
  if (!DB_ENABLED) return;
  try {
    await pool.query(
      "INSERT INTO equity_curve (balance, equity, open_pnl, open_count) VALUES ($1,$2,$3,$4)",
      [balance, equity, openPnl ?? 0, openCount ?? 0]
    );
  } catch (e) { console.warn("[!] saveEquity:", e.message); }
}

async function loadEquityCurve(limit = 200) {
  if (!DB_ENABLED) return [];
  try {
    const r = await pool.query(`
      SELECT
        CAST(balance AS FLOAT), CAST(equity AS FLOAT),
        CAST(open_pnl AS FLOAT) AS "openPnl",
        open_count AS "openCount",
        recorded_at AS "recordedAt"
      FROM equity_curve
      ORDER BY recorded_at DESC
      LIMIT $1
    `, [limit]);
    return r.rows.reverse();
  } catch (e) { return []; }
}

// ── Performance stats per optimizer key ───────────────────────────
async function loadPerformanceByKey() {
  if (!DB_ENABLED) return [];
  try {
    // Get all ghost trades grouped by optimizer key
    const r = await pool.query(`
      SELECT
        optimizer_key AS "optimizerKey",
        symbol,
        COUNT(*) AS trades,
        AVG(peak_rr_pos) AS avg_peak,
        MAX(peak_rr_pos) AS max_peak,
        COUNT(*) FILTER (WHERE mt5_close_reason = 'tp') AS mt5_tp_count,
        CAST(AVG(time_to_sl_min) AS FLOAT) AS avg_time_to_sl
      FROM ghost_trades
      WHERE optimizer_key IS NOT NULL
      GROUP BY optimizer_key, symbol
      ORDER BY symbol, optimizer_key
    `);
    return r.rows;
  } catch (e) { return []; }
}

module.exports = {
  pool, initDB, DB_ENABLED,
  getNextDailyCount,
  logSignal, loadSignalLog,
  saveClosedTrade, loadClosedTrades,
  saveGhostState, loadAllGhostStates, deleteGhostState,
  saveGhostTrade, loadGhostTrades,
  saveEquity, loadEquityCurve,
  loadPerformanceByKey,
};
