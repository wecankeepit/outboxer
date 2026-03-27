#!/usr/bin/env -S node --import tsx
/**
 * chat-viewer.ts — Local chat interface for the Gmail timeline database.
 *
 * Serves a web UI at http://localhost:3847 that lets you browse channels
 * and view message timelines in a chat-bubble interface.
 *
 * Usage:
 *   node --import tsx scripts/chat-viewer.ts
 */

import { createRequire } from "node:module";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { join } from "node:path";
import { homedir } from "node:os";
import { existsSync } from "node:fs";
import { createHmac, randomBytes } from "node:crypto";
import { SEED_EMAILS } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");

const DB_PATH = process.env.DB_PATH || join(homedir(), ".outboxer", "takeout", "gmail.db");
const PORT = parseInt(process.env.PORT || "3847", 10);
const AUTH_SECRET = process.env.AUTH_SECRET || ""; // passphrase for login when served publicly
/** When non-empty, app is served under this URL prefix (e.g. /outboxer via reverse proxy). */
const BASE_PATH = (process.env.BASE_PATH || "").replace(/\/$/, "");

function stripBasePath(pathname: string): string {
  if (!BASE_PATH) return pathname;
  if (pathname === BASE_PATH || pathname === `${BASE_PATH}/`) return "/";
  if (pathname.startsWith(`${BASE_PATH}/`)) return pathname.slice(BASE_PATH.length);
  return pathname;
}

if (!existsSync(DB_PATH)) {
  console.error(`Database not found: ${DB_PATH}`);
  console.error("Run the import first: node --import tsx src/import-gmail-takeout.ts import");
  process.exit(1);
}

const db = new DatabaseSync(DB_PATH);
db.exec("PRAGMA journal_mode = WAL");

// ─── Schema Migrations ──────────────────────────────────────────────────────

// Add source column for multi-platform messages
try { db.exec("ALTER TABLE messages ADD COLUMN source TEXT NOT NULL DEFAULT 'gmail_takeout'"); } catch { /* exists */ }

// Fix legacy 'gmail' source tag
const legacyMsgCount = (
  db.prepare("SELECT COUNT(*) as c FROM messages WHERE source = 'gmail'").get() as { c: number }
).c;
if (legacyMsgCount > 0) {
  db.exec("UPDATE messages SET source = 'gmail_takeout' WHERE source = 'gmail'");
}

// Phone-based identity resolution table (for Google Voice contacts)
db.exec(`CREATE TABLE IF NOT EXISTS contact_phones (
  phone      TEXT PRIMARY KEY,
  contact_id INTEGER NOT NULL REFERENCES contacts(id)
)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_contact_phones_contact ON contact_phones(contact_id)`);

// ─── Authentication ─────────────────────────────────────────────────────────

const COOKIE_NAME = "outboxer_session";
const SESSION_MAX_AGE = 60 * 60 * 24 * 30; // 30 days in seconds

function makeSessionToken(secret: string): string {
  // HMAC the secret with a fixed key — the token IS the proof-of-knowledge
  return createHmac("sha256", "outboxer-session-v1").update(secret).digest("hex");
}

function parseCookies(header: string | undefined): Record<string, string> {
  if (!header) return {};
  const cookies: Record<string, string> = {};
  for (const pair of header.split(";")) {
    const [k, ...v] = pair.trim().split("=");
    if (k) cookies[k.trim()] = v.join("=").trim();
  }
  return cookies;
}

function isAuthenticated(req: IncomingMessage): boolean {
  if (!AUTH_SECRET) return true; // no secret configured = local dev, no auth
  const cookies = parseCookies(req.headers.cookie);
  return cookies[COOKIE_NAME] === makeSessionToken(AUTH_SECRET);
}

function serveLoginPage(res: ServerResponse, error?: string): void {
  const errorHtml = error ? `<p style="color:#ff6b6b;margin-bottom:16px">${error}</p>` : "";
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(/* html */ `<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Outboxer — Login</title>
<style>
  :root { --font-scale: 1; }
  html { font-size: calc(14px); }
  body { background: #1a1a2e; color: #e0e0e0; font-family: system-ui, sans-serif;
         display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
  .card { background: #16213e; border-radius: 12px; padding: 40px; max-width: 360px; width: 100%;
          box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
  h1 { margin: 0 0 24px; font-size: calc(24px * var(--font-scale)); text-align: center; }
  input[type="password"] { width: 100%; padding: 12px; border: 1px solid #333; border-radius: 8px;
         background: #0f3460; color: #fff; font-size: calc(16px * var(--font-scale)); box-sizing: border-box; }
  button { width: 100%; padding: 12px; margin-top: 16px; border: none; border-radius: 8px;
           background: #4a6fa5; color: #fff; font-size: calc(16px * var(--font-scale)); cursor: pointer; }
  button:hover { background: #5a8fd5; }
</style></head><body>
<div class="card">
  <h1>Outboxer</h1>
  ${errorHtml}
  <form method="POST" action="${BASE_PATH}/login">
    <input type="password" name="passphrase" placeholder="Passphrase" autofocus required>
    <button type="submit">Sign in</button>
  </form>
</div></body></html>`);
}

function handleLogin(req: IncomingMessage, res: ServerResponse): void {
  let body = "";
  req.on("data", (chunk: Buffer) => { body += chunk.toString(); });
  req.on("end", () => {
    const params = new URLSearchParams(body);
    const passphrase = params.get("passphrase") || "";
    if (passphrase === AUTH_SECRET) {
      const token = makeSessionToken(AUTH_SECRET);
      const home = BASE_PATH ? `${BASE_PATH}/` : "/";
      const cookiePath = BASE_PATH || "/";
      const secure = BASE_PATH ? "; Secure" : "";
      res.writeHead(302, {
        Location: home,
        "Set-Cookie": `${COOKIE_NAME}=${token}; Path=${cookiePath}; HttpOnly; SameSite=Strict; Max-Age=${SESSION_MAX_AGE}${secure}`,
      });
      res.end();
    } else {
      serveLoginPage(res, "Wrong passphrase.");
    }
  });
}

// ─── Contact Merges ──────────────────────────────────────────────────────────

db.exec(`CREATE TABLE IF NOT EXISTS contact_merges (
  secondary_id INTEGER PRIMARY KEY REFERENCES contacts(id),
  primary_id   INTEGER NOT NULL REFERENCES contacts(id)
)`);

// ─── Sparkline precomputation ────────────────────────────────────────────────

db.exec(`CREATE TABLE IF NOT EXISTS sparkline_cache (
  contact_id   INTEGER PRIMARY KEY,
  monthly_json TEXT NOT NULL DEFAULT '[]',  -- JSON array of [month_index, switches] pairs (sparse)
  max_switches INTEGER NOT NULL DEFAULT 0,
  out_pct           INTEGER NOT NULL DEFAULT 0,   -- out/(in+out) as 0-100
  intra_conv_days   REAL NOT NULL DEFAULT 0,      -- median intra-conversation gap in days
  inter_conv_days   REAL NOT NULL DEFAULT 0,      -- median inter-conversation gap in days
  cutpoint_days     REAL NOT NULL DEFAULT 0,      -- effective conversation cutpoint in days
  initiation_pct    INTEGER NOT NULL DEFAULT 0,   -- user-initiated conversation proportion 0-100
  slope_pct         INTEGER NOT NULL DEFAULT 50,  -- slope percentile rank 0-100 among all people
  computed_at  TEXT NOT NULL DEFAULT ''
)`);

// Migration: add new columns if missing
for (const col of [
  ["out_pct", "INTEGER NOT NULL DEFAULT 0"],
  ["intra_conv_days", "REAL NOT NULL DEFAULT 0"],
  ["inter_conv_days", "REAL NOT NULL DEFAULT 0"],
  ["cutpoint_days", "REAL NOT NULL DEFAULT 0"],
  ["initiation_pct", "INTEGER NOT NULL DEFAULT 0"],
  ["slope_pct", "INTEGER NOT NULL DEFAULT 50"],
]) {
  try { db.exec(`ALTER TABLE sparkline_cache ADD COLUMN ${col[0]} ${col[1]}`); } catch (_) {}
}

/**
 * Min-gap rolling conversation clustering.
 * Walks exchange ticks left-to-right, tracking the minimum inter-tick gap
 * within the current conversation. The window for accepting the next tick
 * is min(minGap * MULT, 7 days). If the next gap exceeds this window,
 * a new conversation begins. The minimum gap can only shrink within a
 * conversation, preventing drift.
 */
const CONV_MULT = 2.5;
const CONV_MAX_WINDOW_MS = 7 * 86400000; // 7 days

type ConvResult = {
  convos: Array<{ startIdx: number; endIdx: number; startedByUser: boolean }>;
  intraGaps: number[];
  interGaps: number[];
  cutpointMs: number; // effective cutpoint = minGap * MULT used most often
};

function clusterConversations(ticks: Array<{ ts: number; fromUser: boolean }>): ConvResult {
  const empty: ConvResult = { convos: [], intraGaps: [], interGaps: [], cutpointMs: 0 };
  if (ticks.length === 0) return empty;
  if (ticks.length === 1) {
    return { convos: [{ startIdx: 0, endIdx: 0, startedByUser: ticks[0].fromUser }], intraGaps: [], interGaps: [], cutpointMs: 0 };
  }

  const convos: ConvResult["convos"] = [];
  const intraGaps: number[] = [];
  const interGaps: number[] = [];
  const cutpoints: number[] = []; // track per-conversation effective cutpoints
  let cs = 0;
  let minGap = Infinity;

  for (let i = 1; i < ticks.length; i++) {
    const gap = ticks[i].ts - ticks[i - 1].ts;

    if (i === cs + 1) {
      // First gap of a new conversation
      if (gap >= CONV_MAX_WINDOW_MS) {
        convos.push({ startIdx: cs, endIdx: i - 1, startedByUser: ticks[cs].fromUser });
        interGaps.push(gap);
        cs = i;
        minGap = Infinity;
      } else {
        intraGaps.push(gap);
        minGap = gap;
      }
    } else {
      const window = Math.min(minGap * CONV_MULT, CONV_MAX_WINDOW_MS);
      if (gap <= window) {
        intraGaps.push(gap);
        minGap = Math.min(minGap, gap);
      } else {
        cutpoints.push(Math.min(minGap * CONV_MULT, CONV_MAX_WINDOW_MS));
        convos.push({ startIdx: cs, endIdx: i - 1, startedByUser: ticks[cs].fromUser });
        interGaps.push(gap);
        cs = i;
        minGap = Infinity;
      }
    }
  }
  // Flush last conversation
  if (minGap < Infinity) cutpoints.push(Math.min(minGap * CONV_MULT, CONV_MAX_WINDOW_MS));
  convos.push({ startIdx: cs, endIdx: ticks.length - 1, startedByUser: ticks[cs].fromUser });

  // Effective cutpoint: median of per-conversation cutpoints
  const cutpointMs = cutpoints.length > 0 ? median(cutpoints) : 0;

  return { convos, intraGaps, interGaps, cutpointMs };
}

/**
 * Precompute sparkline data + exchange metrics for all bidirectional contacts.
 * Months are indexed from 2000-01 = 0. Only non-zero months are stored (sparse).
 */
function precomputeSparklines(): void {
  const startYear = 2000;
  const now = new Date();
  const currentMonthIdx = (now.getFullYear() - startYear) * 12 + now.getMonth();

  console.log("Precomputing sparkline + exchange metrics...");
  const t0 = Date.now();

  // ── Phase 1: Monthly sparkline data (unchanged) ──
  const rows = db.prepare(`
    WITH person_messages AS (
      SELECT
        COALESCE(cm.primary_id, cp.contact_id) as person_id,
        m.date,
        m.is_from_user,
        m.channel_id
      FROM messages m
      JOIN channel_participants cp ON cp.channel_id = m.channel_id
      JOIN contacts ct ON ct.id = cp.contact_id
      LEFT JOIN contact_merges cm ON cm.secondary_id = ct.id
      WHERE m.date >= '2000-01-01'
        AND (m.is_from_user = 1
             OR m.sender_contact_id = cp.contact_id
             OR m.sender_contact_id IN (SELECT secondary_id FROM contact_merges WHERE primary_id = COALESCE(cm.primary_id, cp.contact_id)))
    ),
    with_prev AS (
      SELECT
        person_id,
        date,
        is_from_user,
        LAG(is_from_user) OVER (PARTITION BY person_id ORDER BY date) as prev_dir
      FROM person_messages
    ),
    monthly_runs AS (
      SELECT
        person_id,
        (CAST(SUBSTR(date, 1, 4) AS INTEGER) - ${startYear}) * 12
          + CAST(SUBSTR(date, 6, 2) AS INTEGER) - 1 as month_idx,
        SUM(CASE WHEN is_from_user = 1 AND (prev_dir IS NULL OR prev_dir = 0) THEN 1 ELSE 0 END) as out_runs,
        SUM(CASE WHEN is_from_user = 0 AND (prev_dir IS NULL OR prev_dir = 1) THEN 1 ELSE 0 END) as in_runs
      FROM with_prev
      GROUP BY person_id, month_idx
    )
    SELECT person_id, month_idx, MIN(out_runs, in_runs) as switches
    FROM monthly_runs
    WHERE MIN(out_runs, in_runs) > 0
    ORDER BY person_id, month_idx
  `).all() as Array<{ person_id: number; month_idx: number; switches: number }>;

  const personSparkMap = new Map<number, Array<[number, number]>>();
  for (const row of rows) {
    if (row.month_idx < 0 || row.month_idx > currentMonthIdx) continue;
    let arr = personSparkMap.get(row.person_id);
    if (!arr) { arr = []; personSparkMap.set(row.person_id, arr); }
    arr.push([row.month_idx, row.switches]);
  }

  // ── Phase 2: Exchange tick analysis ──
  // Get all messages per person, ordered by date, to build exchange ticks.
  // An exchange tick = one "run" of consecutive same-direction messages.
  // The tick timestamp = the latest message timestamp in that run.
  const msgRows = db.prepare(`
    SELECT
      COALESCE(cm.primary_id, cp.contact_id) as person_id,
      m.date,
      m.is_from_user
    FROM messages m
    JOIN channel_participants cp ON cp.channel_id = m.channel_id
    JOIN contacts ct ON ct.id = cp.contact_id
    LEFT JOIN contact_merges cm ON cm.secondary_id = ct.id
    WHERE m.date >= '2000-01-01'
      AND (m.is_from_user = 1
           OR m.sender_contact_id = cp.contact_id
           OR m.sender_contact_id IN (SELECT secondary_id FROM contact_merges WHERE primary_id = COALESCE(cm.primary_id, cp.contact_id)))
    ORDER BY COALESCE(cm.primary_id, cp.contact_id), m.date
  `).all() as Array<{ person_id: number; date: string; is_from_user: number }>;

  // Build exchange ticks per person: [{ts: epoch_ms, fromUser: boolean}]
  type Tick = { ts: number; fromUser: boolean };
  const personTicks = new Map<number, Tick[]>();

  let curPerson = -1;
  let curDir = -1;
  let curTs = 0;
  for (const msg of msgRows) {
    const ts = new Date(msg.date).getTime();
    if (isNaN(ts)) continue;
    if (msg.person_id !== curPerson) {
      // Flush previous person's last tick
      if (curPerson >= 0 && curTs > 0) {
        personTicks.get(curPerson)!.push({ ts: curTs, fromUser: curDir === 1 });
      }
      curPerson = msg.person_id;
      curDir = msg.is_from_user;
      curTs = ts;
      personTicks.set(curPerson, []);
    } else if (msg.is_from_user !== curDir) {
      // Direction changed — flush the completed run as a tick
      personTicks.get(curPerson)!.push({ ts: curTs, fromUser: curDir === 1 });
      curDir = msg.is_from_user;
      curTs = ts;
    } else {
      // Same direction — update the run's latest timestamp
      curTs = Math.max(curTs, ts);
    }
  }
  // Flush last run
  if (curPerson >= 0 && curTs > 0) {
    personTicks.get(curPerson)!.push({ ts: curTs, fromUser: curDir === 1 });
  }

  // ── Phase 3: Per-person conversation metrics via min-gap rolling clustering ──
  const MS_PER_DAY = 86400000;

  type PersonMetrics = {
    outPct: number;          // 0-100
    intraConvDays: number;   // median intra-conversation gap in days
    interConvDays: number;   // median inter-conversation gap in days
    cutpointDays: number;    // effective conversation cutpoint in days
    initiationPct: number;   // 0-100
  };

  const personMetrics = new Map<number, PersonMetrics>();

  for (const [personId, ticks] of personTicks) {
    // out_pct: proportion of ticks that are from user
    const outTicks = ticks.filter(t => t.fromUser).length;
    const totalTicks = ticks.length;
    const outPct = totalTicks > 0 ? Math.round(100 * outTicks / totalTicks) : 0;

    // Run min-gap rolling conversation clustering
    const { convos, intraGaps, interGaps, cutpointMs } = clusterConversations(ticks);

    const intraConvDays = intraGaps.length > 0 ? median(intraGaps) / MS_PER_DAY : 0;
    const interConvDays = interGaps.length > 0 ? median(interGaps) / MS_PER_DAY : 0;
    const cutpointDays = cutpointMs / MS_PER_DAY;

    const initiatedByUser = convos.filter(c => c.startedByUser).length;
    const initiationPct = convos.length > 0 ? Math.round(100 * initiatedByUser / convos.length) : 0;

    personMetrics.set(personId, { outPct, intraConvDays, interConvDays, cutpointDays, initiationPct });
  }

  // ── Phase 3b: Linear slope of exchanges/month, then global percentile ranking ──
  // For each person, compute slope of best-fit line on their sparse monthly exchanges data
  const personSlopes = new Map<number, number>();
  for (const [personId, months] of personSparkMap) {
    if (months.length < 2) { personSlopes.set(personId, 0); continue; }
    // Fill dense array from first to last non-zero month, 0 for missing months
    const firstIdx = months[0][0];
    const lastIdx = months[months.length - 1][0];
    const n = lastIdx - firstIdx + 1;
    if (n < 2) { personSlopes.set(personId, 0); continue; }
    // Simple linear regression: y = exchanges, x = month index (0-based from first)
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    const lookup = new Map(months.map(m => [m[0], m[1]]));
    for (let i = 0; i < n; i++) {
      const y = lookup.get(firstIdx + i) || 0;
      sumX += i;
      sumY += y;
      sumXY += i * y;
      sumX2 += i * i;
    }
    const denom = n * sumX2 - sumX * sumX;
    const slope = denom !== 0 ? (n * sumXY - sumX * sumY) / denom : 0;
    personSlopes.set(personId, slope);
  }

  // Convert slopes to percentile ranks (0-100)
  const slopeEntries = [...personSlopes.entries()].filter(([, s]) => s !== 0 || personSparkMap.has(s));
  const sortedSlopes = [...slopeEntries].sort((a, b) => a[1] - b[1]);
  const slopeRank = new Map<number, number>();
  for (let i = 0; i < sortedSlopes.length; i++) {
    slopeRank.set(sortedSlopes[i][0], Math.round(100 * i / (sortedSlopes.length - 1 || 1)));
  }

  // ── Phase 4: Write to cache ──
  const nowIso = new Date().toISOString();
  const upsert = db.prepare(
    `INSERT OR REPLACE INTO sparkline_cache
       (contact_id, monthly_json, max_switches, out_pct, intra_conv_days, inter_conv_days, cutpoint_days, initiation_pct, slope_pct, computed_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
  );

  db.exec("BEGIN");
  let count = 0;
  const allPersonIds = new Set([...personSparkMap.keys(), ...personMetrics.keys()]);
  for (const personId of allPersonIds) {
    const months = personSparkMap.get(personId) || [];
    const maxSw = months.reduce((mx, m) => Math.max(mx, m[1]), 0);
    const metrics = personMetrics.get(personId) || { outPct: 0, intraConvDays: 0, interConvDays: 0, cutpointDays: 0, initiationPct: 0 };
    const slopePct = slopeRank.get(personId) ?? 50;
    upsert.run(
      personId,
      JSON.stringify(months),
      maxSw,
      metrics.outPct,
      Math.round(metrics.intraConvDays * 10) / 10,
      Math.round(metrics.interConvDays * 10) / 10,
      Math.round(metrics.cutpointDays * 10) / 10,
      metrics.initiationPct,
      slopePct,
      nowIso,
    );
    count++;
    if (count % 500 === 0) { db.exec("COMMIT"); db.exec("BEGIN"); }
  }
  db.exec("COMMIT");

  console.log(`  Sparklines + metrics computed for ${count} people in ${Date.now() - t0}ms`);
}

function median(arr: number[]): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 !== 0 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

// Check if sparklines need recomputing (stale or missing)
let sparklineReady = false;
function ensureSparklines(): void {
  if (sparklineReady) return;
  const row = db.prepare(
    "SELECT computed_at, slope_pct FROM sparkline_cache LIMIT 1"
  ).get() as { computed_at: string; slope_pct?: number } | undefined;

  const staleMs = 60 * 60 * 1000;
  const needsRecompute = !row || !row.computed_at
    || Date.now() - new Date(row.computed_at).getTime() > staleMs
    || row.slope_pct === undefined || row.slope_pct === null;
  if (needsRecompute) {
    precomputeSparklines();
  }
  sparklineReady = true;
}

// Sparkline precomputation deferred to after server.listen().

// ─── Ensure Google Contacts tables exist (may not if sync hasn't run yet) ───

db.exec(`
  CREATE TABLE IF NOT EXISTS google_contacts (
    resource_name TEXT PRIMARY KEY,
    etag          TEXT,
    display_name  TEXT NOT NULL DEFAULT '',
    given_name    TEXT DEFAULT '',
    family_name   TEXT DEFAULT '',
    nickname      TEXT DEFAULT '',
    birthday      TEXT DEFAULT '',
    photo_url     TEXT DEFAULT '',
    organizations TEXT DEFAULT '[]',
    addresses     TEXT DEFAULT '[]',
    update_time       TEXT DEFAULT '',
    update_time_human TEXT DEFAULT '',
    update_time_google TEXT DEFAULT '',
    synced_at         TEXT NOT NULL DEFAULT ''
  );
  CREATE TABLE IF NOT EXISTS google_contact_emails (
    resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
    email         TEXT NOT NULL COLLATE NOCASE,
    type          TEXT DEFAULT '',
    PRIMARY KEY (resource_name, email)
  );
  CREATE TABLE IF NOT EXISTS google_contact_phones (
    resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
    phone         TEXT NOT NULL,
    raw_phone     TEXT DEFAULT '',
    type          TEXT DEFAULT '',
    PRIMARY KEY (resource_name, phone)
  );
  CREATE TABLE IF NOT EXISTS google_contact_groups (
    resource_name TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    group_type    TEXT DEFAULT ''
  );
  CREATE TABLE IF NOT EXISTS google_contact_memberships (
    contact_resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
    group_resource_name   TEXT NOT NULL,
    PRIMARY KEY (contact_resource_name, group_resource_name)
  );
  CREATE INDEX IF NOT EXISTS idx_gc_emails_email ON google_contact_emails(email);
  CREATE INDEX IF NOT EXISTS idx_gc_phones_phone ON google_contact_phones(phone);
`);

// ── Ensure contacts table has 'hidden' column (migration) ──
try {
  db.exec("ALTER TABLE contacts ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0");
} catch (_) {
  // Column already exists — ignore
}

/** Resolve a contact ID through the merge table (follows one level). */
function resolveContactId(id: number): number {
  const row = db
    .prepare("SELECT primary_id FROM contact_merges WHERE secondary_id = ?")
    .get(id) as { primary_id: number } | undefined;
  return row ? row.primary_id : id;
}

/** Get all contact IDs that map to this primary (including itself). */
function detectUserContactId(): number {
  const findEmail = db.prepare("SELECT contact_id FROM contact_emails WHERE email = ?");
  const counts = new Map<number, number>();
  let bestId = -1, bestCount = 0;
  for (const seed of SEED_EMAILS) {
    const row = findEmail.get(seed) as { contact_id: number } | undefined;
    if (row) {
      const c = (counts.get(row.contact_id) || 0) + 1;
      counts.set(row.contact_id, c);
      if (c > bestCount) { bestCount = c; bestId = row.contact_id; }
    }
  }
  return bestId;
}

function getMergedIds(primaryId: number): number[] {
  const rows = db
    .prepare("SELECT secondary_id FROM contact_merges WHERE primary_id = ?")
    .all(primaryId) as { secondary_id: number }[];
  return [primaryId, ...rows.map((r) => r.secondary_id)];
}

function mergeContacts(secondaryId: number, primaryId: number): void {
  // Prevent self-merge and circular merges
  if (secondaryId === primaryId) return;
  const resolvedPrimary = resolveContactId(primaryId);
  // If secondary is already a primary for others, re-point them
  db.exec(`UPDATE contact_merges SET primary_id = ${resolvedPrimary} WHERE primary_id = ${secondaryId}`);
  // Insert/replace the merge
  db.prepare("INSERT OR REPLACE INTO contact_merges (secondary_id, primary_id) VALUES (?, ?)")
    .run(secondaryId, resolvedPrimary);
}

function unmergeSingle(secondaryId: number): void {
  db.prepare("DELETE FROM contact_merges WHERE secondary_id = ?").run(secondaryId);
}

// ─── API Handlers ────────────────────────────────────────────────────────────

function getChannels(search?: string, limit = SIDEBAR_PAGE_DEFAULT, offset = 0): unknown[] {
  // Only show channels where at least one participant is a "bidirectional person"
  // (someone the user has sent to AND received from across any channel/platform).
  // This is the channel-view counterpart of the per-person bidirectional filter
  // used in getContacts().
  const searchWhere = search ? "AND c.participant_names LIKE ?" : "";
  const params: (string | number)[] = search ? [`%${search}%`] : [];
  const lim = Math.min(Math.max(1, limit), 500);
  const off = Math.max(0, offset);
  return db
    .prepare(
      `WITH channel_runs AS (
         SELECT channel_id, is_from_user,
                LAG(is_from_user) OVER (PARTITION BY channel_id ORDER BY date, id) as prev_dir
         FROM messages
       ),
       channel_switches AS (
         SELECT channel_id,
           MIN(
             SUM(CASE WHEN is_from_user = 1 AND (prev_dir IS NULL OR prev_dir = 0) THEN 1 ELSE 0 END),
             SUM(CASE WHEN is_from_user = 0 AND (prev_dir IS NULL OR prev_dir = 1) THEN 1 ELSE 0 END)
           ) as switches
         FROM channel_runs
         GROUP BY channel_id
       ),
       bidirectional_contacts AS (
         SELECT COALESCE(cm.primary_id, ct.id) as primary_id
         FROM contacts ct
         LEFT JOIN contact_merges cm ON cm.secondary_id = ct.id
         JOIN channel_participants cp ON cp.contact_id = ct.id
         JOIN channels ch ON ch.id = cp.channel_id
         GROUP BY COALESCE(cm.primary_id, ct.id)
         HAVING SUM(ch.user_sent_count) > 0 AND SUM(ch.user_recv_count) > 0
       )
       SELECT DISTINCT c.id, c.participant_names, c.message_count,
              c.user_sent_count, c.user_recv_count,
              c.first_date, c.last_date,
              COALESCE(cs.switches, 0) as switches
       FROM channels c
       JOIN channel_participants cp ON cp.channel_id = c.id
       LEFT JOIN contact_merges cm ON cm.secondary_id = cp.contact_id
       LEFT JOIN channel_switches cs ON cs.channel_id = c.id
       WHERE COALESCE(cm.primary_id, cp.contact_id) IN (SELECT primary_id FROM bidirectional_contacts)
       ${searchWhere}
       ORDER BY c.last_date DESC
       LIMIT ? OFFSET ?`,
    )
    .all(...params, lim, off);
}

/** Default page sizes (API may cap higher client values). */
const SIDEBAR_PAGE_DEFAULT = 150;
const MSG_PAGE_DEFAULT = 500;

type MsgRow = {
  id: number;
  date: string;
  blob: string;
  subject: string;
  is_from_user: number;
  sender_name: string;
  source?: string;
  channel_id?: number;
  channel_names?: string;
};

/** Full channel timeline (ASC). Prefer getMessagesPage for UI. */
function getMessages(channelId: number): MsgRow[] {
  return db
    .prepare(
      `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
              ct.display_name as sender_name,
              m.source
       FROM messages m
       JOIN contacts ct ON ct.id = m.sender_contact_id
       WHERE m.channel_id = ?
       ORDER BY m.date ASC, m.id ASC`,
    )
    .all(channelId) as MsgRow[];
}

/** Newest-first page in DB, returned chronological (ASC) for display. */
function getMessagesPage(
  channelId: number,
  limit: number,
  before?: { date: string; id: number } | null,
): { messages: MsgRow[]; hasMoreOlder: boolean } {
  const lim = Math.min(Math.max(1, limit), 2000);
  let rows: MsgRow[];
  if (before) {
    rows = db
      .prepare(
        `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
                ct.display_name as sender_name,
                m.source
         FROM messages m
         JOIN contacts ct ON ct.id = m.sender_contact_id
         WHERE m.channel_id = ?
           AND (m.date < ? OR (m.date = ? AND m.id < ?))
         ORDER BY m.date DESC, m.id DESC
         LIMIT ?`,
      )
      .all(channelId, before.date, before.date, before.id, lim) as MsgRow[];
  } else {
    rows = db
      .prepare(
        `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
                ct.display_name as sender_name,
                m.source
         FROM messages m
         JOIN contacts ct ON ct.id = m.sender_contact_id
         WHERE m.channel_id = ?
         ORDER BY m.date DESC, m.id DESC
         LIMIT ?`,
      )
      .all(channelId, lim) as MsgRow[];
  }
  rows.reverse();
  let hasMoreOlder = false;
  if (rows.length > 0) {
    const oldest = rows[0]!;
    const more = db
      .prepare(
        `SELECT 1 as ok FROM messages
         WHERE channel_id = ?
           AND (date < ? OR (date = ? AND id < ?))
         LIMIT 1`,
      )
      .get(channelId, oldest.date, oldest.date, oldest.id) as { ok: number } | undefined;
    hasMoreOlder = !!more;
  }
  return { messages: rows, hasMoreOlder };
}

function getChannelInfo(channelId: number): unknown {
  return db
    .prepare(
      `SELECT c.*, GROUP_CONCAT(DISTINCT ce.email) as emails
       FROM channels c
       LEFT JOIN channel_participants cp ON cp.channel_id = c.id
       LEFT JOIN contact_emails ce ON ce.contact_id = cp.contact_id
       WHERE c.id = ?
       GROUP BY c.id`,
    )
    .get(channelId);
}

// ─── Person Timeline (aggregate across channels) ─────────────────────────────

function getContacts(search?: string, limit = SIDEBAR_PAGE_DEFAULT, offset = 0): unknown[] {
  // Resolve merged contacts: use COALESCE(cm.primary_id, ct.id) as the canonical ID.
  // last_date is computed from actual messages (sent by user OR by this contact),
  // NOT from channel.last_date which includes third-party activity in group channels.
  //
  // Per-person bidirectional filter: HAVING total_sent > 0 AND total_recv > 0
  // ensures we only show people the user has communicated with bidirectionally
  // across ANY platform/channel (not per-channel).
  const where = search ? "AND cg.display_name LIKE ?" : "";
  const params: (string | number)[] = search ? [`%${search}%`] : [];
  const lim = Math.min(Math.max(1, limit), 500);
  const off = Math.max(0, offset);
  return db
    .prepare(
      `WITH channel_runs AS (
         SELECT channel_id, is_from_user,
                LAG(is_from_user) OVER (PARTITION BY channel_id ORDER BY date, id) as prev_dir
         FROM messages
       ),
       channel_switches AS (
         SELECT channel_id,
           MIN(
             SUM(CASE WHEN is_from_user = 1 AND (prev_dir IS NULL OR prev_dir = 0) THEN 1 ELSE 0 END),
             SUM(CASE WHEN is_from_user = 0 AND (prev_dir IS NULL OR prev_dir = 1) THEN 1 ELSE 0 END)
           ) as switches
         FROM channel_runs
         GROUP BY channel_id
       ),
       -- Per-person message counts: recv counts only messages FROM this person
       person_recv AS (
         SELECT COALESCE(cm.primary_id, m.sender_contact_id) as person_id,
                COUNT(*) as recv_count
         FROM messages m
         LEFT JOIN contact_merges cm ON cm.secondary_id = m.sender_contact_id
         WHERE m.is_from_user = 0
         GROUP BY COALESCE(cm.primary_id, m.sender_contact_id)
       ),
       -- Per-person sent counts: messages user sent to channels this person is in
       person_sent AS (
         SELECT COALESCE(cm.primary_id, cp.contact_id) as person_id,
                COUNT(DISTINCT m.id) as sent_count
         FROM channel_participants cp
         LEFT JOIN contact_merges cm ON cm.secondary_id = cp.contact_id
         JOIN messages m ON m.channel_id = cp.channel_id AND m.is_from_user = 1
         GROUP BY COALESCE(cm.primary_id, cp.contact_id)
       ),
       contact_groups AS (
         SELECT
           COALESCE(cm.primary_id, ct.id) as primary_id,
           pri.display_name,
           COUNT(DISTINCT c.id) as channel_count,
           CAST(SUM(COALESCE(cs.switches, 0)) AS INTEGER) as switches,
           MIN(c.first_date) as first_date
         FROM contacts ct
         LEFT JOIN contact_merges cm ON cm.secondary_id = ct.id
         JOIN contacts pri ON pri.id = COALESCE(cm.primary_id, ct.id)
         JOIN channel_participants cp ON cp.contact_id = ct.id
         JOIN channels c ON c.id = cp.channel_id
         LEFT JOIN channel_switches cs ON cs.channel_id = c.id
         GROUP BY COALESCE(cm.primary_id, ct.id)
         HAVING SUM(c.user_sent_count) > 0 AND SUM(c.user_recv_count) > 0
       )
       SELECT
         cg.primary_id as id,
         cg.display_name,
         cg.channel_count,
         COALESCE(ps.sent_count, 0) + COALESCE(pr.recv_count, 0) as total_messages,
         COALESCE(ps.sent_count, 0) as total_sent,
         COALESCE(pr.recv_count, 0) as total_recv,
         cg.switches,
         cg.first_date,
         (SELECT MAX(m.date) FROM messages m
          WHERE m.channel_id IN (
            SELECT cp2.channel_id FROM channel_participants cp2
            WHERE cp2.contact_id = cg.primary_id
               OR cp2.contact_id IN (SELECT secondary_id FROM contact_merges WHERE primary_id = cg.primary_id)
          )
          AND (m.is_from_user = 1
               OR m.sender_contact_id = cg.primary_id
               OR m.sender_contact_id IN (SELECT secondary_id FROM contact_merges WHERE primary_id = cg.primary_id))
         ) as last_date,
         sc.monthly_json as sparkline,
         sc.max_switches as spark_max,
         COALESCE(sc.out_pct, 0) as out_pct,
         COALESCE(sc.intra_conv_days, 0) as intra_conv_days,
         COALESCE(sc.inter_conv_days, 0) as inter_conv_days,
         COALESCE(sc.cutpoint_days, 0) as cutpoint_days,
         COALESCE(sc.initiation_pct, 0) as initiation_pct,
         COALESCE(sc.slope_pct, 50) as slope_pct,
         pri_ct.hidden
       FROM contact_groups cg
       LEFT JOIN person_recv pr ON pr.person_id = cg.primary_id
       LEFT JOIN person_sent ps ON ps.person_id = cg.primary_id
       LEFT JOIN sparkline_cache sc ON sc.contact_id = cg.primary_id
       JOIN contacts pri_ct ON pri_ct.id = cg.primary_id
       WHERE last_date IS NOT NULL
       ${where}
       ORDER BY last_date DESC
       LIMIT ? OFFSET ?`,
    )
    .all(...params, lim, off);
}

function getContactInfo(contactId: number): unknown {
  const allIds = getMergedIds(contactId);
  const placeholders = allIds.map(() => "?").join(",");

  const contact = db
    .prepare("SELECT id, display_name, source FROM contacts WHERE id = ?")
    .get(contactId) as { id: number; display_name: string; source: string } | undefined;
  if (!contact) return null;

  const emails = db
    .prepare(`SELECT DISTINCT email FROM contact_emails WHERE contact_id IN (${placeholders})`)
    .all(...allIds) as { email: string }[];

  const phones = db
    .prepare(`SELECT DISTINCT phone FROM contact_phones WHERE contact_id IN (${placeholders})`)
    .all(...allIds) as { phone: string }[];

  const channels = db
    .prepare(
      `SELECT DISTINCT c.id, c.participant_names, c.message_count, c.first_date, c.last_date
       FROM channels c
       JOIN channel_participants cp ON cp.channel_id = c.id
       WHERE cp.contact_id IN (${placeholders})
       ORDER BY c.last_date DESC`,
    )
    .all(...allIds) as { id: number; participant_names: string }[];

  // Include merged contact names for display
  const mergedContacts = allIds.length > 1
    ? (db.prepare(`SELECT id, display_name FROM contacts WHERE id IN (${placeholders}) AND id != ?`)
        .all(...allIds, contactId) as { id: number; display_name: string }[])
    : [];

  return {
    ...contact,
    emails: emails.map((e) => e.email),
    phones: phones.map((p) => p.phone),
    channels,
    mergedContacts,
  };
}

// ─── Google Contacts Queries ────────────────────────────────────────────────

function getGoogleContacts(search?: string, limit = SIDEBAR_PAGE_DEFAULT, offset = 0): unknown[] {
  const where = search ? "AND gc.display_name LIKE ?" : "";
  const params: (string | number)[] = search ? [`%${search}%`] : [];
  const lim = Math.min(Math.max(1, limit), 500);
  const off = Math.max(0, offset);
  return db
    .prepare(
      `SELECT
         gc.resource_name,
         gc.display_name,
         gc.given_name,
         gc.family_name,
         gc.photo_url,
         gc.organizations,
         gc.update_time,
         gc.update_time_human,
         gc.update_time_google,
         gc.birthday,
         (SELECT GROUP_CONCAT(gce.email, ', ')
          FROM google_contact_emails gce WHERE gce.resource_name = gc.resource_name
         ) as emails,
         (SELECT GROUP_CONCAT(gcp.raw_phone, ', ')
          FROM google_contact_phones gcp WHERE gcp.resource_name = gc.resource_name
         ) as phones,
         (SELECT GROUP_CONCAT(gcg.name, ', ')
          FROM google_contact_memberships gcm
          JOIN google_contact_groups gcg ON gcg.resource_name = gcm.group_resource_name
          WHERE gcm.contact_resource_name = gc.resource_name
            AND gcg.group_type != 'SYSTEM_CONTACT_GROUP'
         ) as labels
       FROM google_contacts gc
       WHERE 1=1 ${where}
       ORDER BY
         CASE WHEN gc.update_time_human != '' THEN 0 ELSE 1 END,
         SUBSTR(COALESCE(NULLIF(gc.update_time_human, ''), gc.update_time_google), 1, 10) DESC,
         COALESCE(NULLIF(gc.display_name, ''),
           json_extract(gc.organizations, '$[0].name'),
           json_extract(gc.organizations, '$[0].title'),
           gc.resource_name) COLLATE NOCASE ASC
       LIMIT ? OFFSET ?`,
    )
    .all(...params, lim, off);
}

function getGoogleContactDetail(resourceName: string): unknown {
  const gc = db
    .prepare(
      `SELECT gc.*,
         (SELECT GROUP_CONCAT(gce.email, ', ')
          FROM google_contact_emails gce WHERE gce.resource_name = gc.resource_name
         ) as emails,
         (SELECT GROUP_CONCAT(gcp.raw_phone, ', ')
          FROM google_contact_phones gcp WHERE gcp.resource_name = gc.resource_name
         ) as phones,
         (SELECT GROUP_CONCAT(gcg.name, ', ')
          FROM google_contact_memberships gcm
          JOIN google_contact_groups gcg ON gcg.resource_name = gcm.group_resource_name
          WHERE gcm.contact_resource_name = gc.resource_name
            AND gcg.group_type != 'SYSTEM_CONTACT_GROUP'
         ) as labels
       FROM google_contacts gc
       WHERE gc.resource_name = ?`,
    )
    .get(resourceName) as Record<string, unknown> | undefined;
  if (!gc) return null;

  // Find linked Outboxer contact(s) via email/phone matching
  const emails = db
    .prepare(
      "SELECT email FROM google_contact_emails WHERE resource_name = ?",
    )
    .all(resourceName) as { email: string }[];

  const phones = db
    .prepare(
      "SELECT phone FROM google_contact_phones WHERE resource_name = ?",
    )
    .all(resourceName) as { phone: string }[];

  const linkedContactIds = new Set<number>();
  for (const { email } of emails) {
    const row = db
      .prepare("SELECT contact_id FROM contact_emails WHERE LOWER(email) = LOWER(?)")
      .get(email) as { contact_id: number } | undefined;
    if (row) linkedContactIds.add(row.contact_id);
  }
  for (const { phone } of phones) {
    const row = db
      .prepare("SELECT contact_id FROM contact_phones WHERE phone = ?")
      .get(phone) as { contact_id: number } | undefined;
    if (row) linkedContactIds.add(row.contact_id);
  }

  // Resolve through merges
  const resolvedIds = new Set<number>();
  for (const cid of linkedContactIds) {
    const merge = db
      .prepare("SELECT primary_id FROM contact_merges WHERE secondary_id = ?")
      .get(cid) as { primary_id: number } | undefined;
    resolvedIds.add(merge ? merge.primary_id : cid);
  }

  return {
    ...gc,
    linkedOutboxerContactIds: [...resolvedIds],
  };
}

function getPersonTimeline(contactId: number): MsgRow[] {
  const allIds = getMergedIds(contactId);
  const placeholders = allIds.map(() => "?").join(",");
  return db
    .prepare(
      `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
              m.channel_id,
              ct.display_name as sender_name,
              c.participant_names as channel_names,
              m.source
       FROM messages m
       JOIN contacts ct ON ct.id = m.sender_contact_id
       JOIN channels c ON c.id = m.channel_id
       WHERE m.channel_id IN (
         SELECT cp.channel_id FROM channel_participants cp WHERE cp.contact_id IN (${placeholders})
       )
       AND (m.is_from_user = 1 OR m.sender_contact_id IN (${placeholders}))
       ORDER BY m.date ASC, m.id ASC`,
    )
    .all(...allIds, ...allIds) as MsgRow[];
}

/** Person aggregate timeline; paging returns newest window first, then ASC for display. */
function getPersonTimelinePage(
  contactId: number,
  limit: number,
  before?: { date: string; id: number } | null,
): { messages: MsgRow[]; hasMoreOlder: boolean } {
  const allIds = getMergedIds(contactId);
  const placeholders = allIds.map(() => "?").join(",");
  const lim = Math.min(Math.max(1, limit), 2000);
  const baseWhere = `
       WHERE m.channel_id IN (
         SELECT cp.channel_id FROM channel_participants cp WHERE cp.contact_id IN (${placeholders})
       )
       AND (m.is_from_user = 1 OR m.sender_contact_id IN (${placeholders}))`;
  const paramsTwice = [...allIds, ...allIds];
  let rows: MsgRow[];
  if (before) {
    rows = db
      .prepare(
        `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
                m.channel_id,
                ct.display_name as sender_name,
                c.participant_names as channel_names,
                m.source
         FROM messages m
         JOIN contacts ct ON ct.id = m.sender_contact_id
         JOIN channels c ON c.id = m.channel_id
         ${baseWhere}
           AND (m.date < ? OR (m.date = ? AND m.id < ?))
         ORDER BY m.date DESC, m.id DESC
         LIMIT ?`,
      )
      .all(...paramsTwice, before.date, before.date, before.id, lim) as MsgRow[];
  } else {
    rows = db
      .prepare(
        `SELECT m.id, m.date, m.blob, m.subject, m.is_from_user,
                m.channel_id,
                ct.display_name as sender_name,
                c.participant_names as channel_names,
                m.source
         FROM messages m
         JOIN contacts ct ON ct.id = m.sender_contact_id
         JOIN channels c ON c.id = m.channel_id
         ${baseWhere}
         ORDER BY m.date DESC, m.id DESC
         LIMIT ?`,
      )
      .all(...paramsTwice, lim) as MsgRow[];
  }
  rows.reverse();
  let hasMoreOlder = false;
  if (rows.length > 0) {
    const oldest = rows[0]!;
    const more = db
      .prepare(
        `SELECT 1 as ok FROM messages m
         WHERE m.channel_id IN (
           SELECT cp.channel_id FROM channel_participants cp WHERE cp.contact_id IN (${placeholders})
         )
         AND (m.is_from_user = 1 OR m.sender_contact_id IN (${placeholders}))
         AND (m.date < ? OR (m.date = ? AND m.id < ?))
         LIMIT 1`,
      )
      .get(...paramsTwice, oldest.date, oldest.date, oldest.id) as { ok: number } | undefined;
    hasMoreOlder = !!more;
  }
  return { messages: rows, hasMoreOlder };
}

// ─── HTTP Server ─────────────────────────────────────────────────────────────

const server = createServer((req, res) => {
  const url = new URL(req.url || "/", `http://localhost:${PORT}`);
  const pathname = stripBasePath(url.pathname);

  // ─── Auth gate ──────────────────────────────────────────────────────────
  if (AUTH_SECRET) {
    if (req.method === "POST" && pathname === "/login") {
      handleLogin(req, res);
      return;
    }
    if (!isAuthenticated(req)) {
      serveLoginPage(res);
      return;
    }
  }

  // API: GET /api/config — expose read-only flag to the UI
  if (pathname === "/api/config") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ readOnly: false }));
    return;
  }

  // API: GET /api/user-identity — the owner's identity (name, emails, phones)
  if (pathname === "/api/user-identity") {
    const userId = detectUserContactId();
    const contact = userId > 0
      ? db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(userId) as { display_name: string } | undefined
      : undefined;
    const name = contact?.display_name || "Unknown";

    // All emails: from user contact + merged contacts
    const allIds = getMergedIds(userId);
    const ph = allIds.map(() => "?").join(",");
    const emails = (db.prepare(`SELECT DISTINCT email FROM contact_emails WHERE contact_id IN (${ph})`)
      .all(...allIds) as { email: string }[]).map(r => r.email);

    // Phones (if table exists)
    let phones: string[] = [];
    try {
      phones = (db.prepare(`SELECT DISTINCT phone FROM contact_phones WHERE contact_id IN (${ph})`)
        .all(...allIds) as { phone: string }[]).map(r => r.phone);
    } catch { /* table may not exist */ }

    // Seed emails (the configured identity list)
    const seedEmails = [...SEED_EMAILS];

    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ id: userId, name, emails, phones, seedEmails }));
    return;
  }

  // API: GET /api/channels?search=...&limit=...&offset=...
  if (pathname === "/api/channels") {
    const search = url.searchParams.get("search") || undefined;
    const limit = parseInt(url.searchParams.get("limit") || String(SIDEBAR_PAGE_DEFAULT), 10);
    const offset = parseInt(url.searchParams.get("offset") || "0", 10);
    const rows = getChannels(search, limit, offset);
    const hasMore = rows.length === Math.min(Math.max(1, limit), 500);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ channels: rows, hasMore }));
    return;
  }

  // API: GET /api/channels/:id/messages?limit=...&beforeDate=...&beforeId=...
  const msgMatch = pathname.match(/^\/api\/channels\/(\d+)\/messages$/);
  if (msgMatch) {
    const channelId = parseInt(msgMatch[1]!, 10);
    const limit = parseInt(url.searchParams.get("limit") || String(MSG_PAGE_DEFAULT), 10);
    const bd = url.searchParams.get("beforeDate");
    const bid = url.searchParams.get("beforeId");
    const before =
      bd && bid != null ? { date: bd, id: parseInt(bid, 10) } : null;
    const { messages, hasMoreOlder } = getMessagesPage(channelId, limit, before);
    const info = getChannelInfo(channelId);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ channel: info, messages, hasMoreOlder }));
    return;
  }

  // API: GET /api/counts — total bidirectional people, channels, and google contacts
  if (pathname === "/api/counts") {
    const ppl = db.prepare(
      `SELECT COUNT(*) as c FROM (
         SELECT COALESCE(cm.primary_id, ct.id) as pid
         FROM contacts ct
         LEFT JOIN contact_merges cm ON cm.secondary_id = ct.id
         JOIN channel_participants cp ON cp.contact_id = ct.id
         JOIN channels ch ON ch.id = cp.channel_id
         GROUP BY COALESCE(cm.primary_id, ct.id)
         HAVING SUM(ch.user_sent_count) > 0 AND SUM(ch.user_recv_count) > 0
       )`,
    ).get() as { c: number };
    const chs = db.prepare(
      `SELECT COUNT(DISTINCT c.id) as c
       FROM channels c
       JOIN channel_participants cp ON cp.channel_id = c.id
       LEFT JOIN contact_merges cm ON cm.secondary_id = cp.contact_id
       WHERE COALESCE(cm.primary_id, cp.contact_id) IN (
         SELECT COALESCE(cm2.primary_id, ct2.id)
         FROM contacts ct2
         LEFT JOIN contact_merges cm2 ON cm2.secondary_id = ct2.id
         JOIN channel_participants cp2 ON cp2.contact_id = ct2.id
         JOIN channels ch2 ON ch2.id = cp2.channel_id
         GROUP BY COALESCE(cm2.primary_id, ct2.id)
         HAVING SUM(ch2.user_sent_count) > 0 AND SUM(ch2.user_recv_count) > 0
       )`,
    ).get() as { c: number };
    // Google contacts count (check if table exists first)
    let gcCount = 0;
    try {
      const gc = db.prepare("SELECT COUNT(*) as c FROM google_contacts").get() as { c: number };
      gcCount = gc.c;
    } catch { /* table may not exist yet */ }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ people: ppl.c, channels: chs.c, contacts: gcCount }));
    return;
  }

  // API: GET /api/contacts?search=...&limit=...&offset=...
  if (pathname === "/api/contacts") {
    ensureSparklines();
    const search = url.searchParams.get("search") || undefined;
    const limit = parseInt(url.searchParams.get("limit") || String(SIDEBAR_PAGE_DEFAULT), 10);
    const offset = parseInt(url.searchParams.get("offset") || "0", 10);
    const rows = getContacts(search, limit, offset);
    const hasMore = rows.length === Math.min(Math.max(1, limit), 500);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ contacts: rows, hasMore }));
    return;
  }

  // API: GET /api/contacts/:id/timeline?limit=...&beforeDate=...&beforeId=...
  const timelineMatch = pathname.match(/^\/api\/contacts\/(\d+)\/timeline$/);
  if (timelineMatch) {
    const contactId = parseInt(timelineMatch[1]!, 10);
    const limit = parseInt(url.searchParams.get("limit") || String(MSG_PAGE_DEFAULT), 10);
    const bd = url.searchParams.get("beforeDate");
    const bid = url.searchParams.get("beforeId");
    const before =
      bd && bid != null ? { date: bd, id: parseInt(bid, 10) } : null;
    const { messages, hasMoreOlder } = getPersonTimelinePage(contactId, limit, before);
    const info = getContactInfo(contactId);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ contact: info, messages, hasMoreOlder }));
    return;
  }

  // API: POST /api/contacts/merge  { secondaryId, primaryId }
  if (pathname === "/api/contacts/merge" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk: Buffer) => { body += chunk.toString(); });
    req.on("end", () => {
      try {
        const { secondaryId, primaryId } = JSON.parse(body);
        mergeContacts(secondaryId, primaryId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } catch (err) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: String(err) }));
      }
    });
    return;
  }

  // API: POST /api/contacts/unmerge  { secondaryId }
  if (pathname === "/api/contacts/unmerge" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk: Buffer) => { body += chunk.toString(); });
    req.on("end", () => {
      try {
        const { secondaryId } = JSON.parse(body);
        unmergeSingle(secondaryId);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } catch (err) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: String(err) }));
      }
    });
    return;
  }

  // API: GET /api/contacts/search-merge-targets?q=...&exclude=...
  // Returns contacts that are currently primaries (not merged into another) to serve as merge targets.
  if (pathname === "/api/contacts/search-merge-targets") {
    const q = url.searchParams.get("q") || "";
    const excludeId = parseInt(url.searchParams.get("exclude") || "0", 10);
    const results = db
      .prepare(
        `SELECT ct.id, ct.display_name
         FROM contacts ct
         WHERE ct.id NOT IN (SELECT secondary_id FROM contact_merges)
           AND ct.id != ?
           AND ct.display_name LIKE ?
         ORDER BY ct.display_name
         LIMIT 20`,
      )
      .all(excludeId, `%${q}%`);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(results));
    return;
  }

  // API: POST /api/contacts/:id/hide  { hidden: 0|1 }
  const hideMatch = pathname.match(/^\/api\/contacts\/(\d+)\/hide$/);
  if (hideMatch && req.method === "POST") {
    let body = "";
    req.on("data", (c: Buffer) => (body += c));
    req.on("end", () => {
      try {
        const { hidden } = JSON.parse(body);
        const contactId = parseInt(hideMatch[1], 10);
        db.prepare("UPDATE contacts SET hidden = ? WHERE id = ?").run(
          hidden ? 1 : 0,
          contactId,
        );
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      } catch (err) {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: String(err) }));
      }
    });
    return;
  }

  // API: GET /api/google-contacts?search=...&limit=...&offset=...
  if (pathname === "/api/google-contacts") {
    const search = url.searchParams.get("search") || undefined;
    const limit = parseInt(url.searchParams.get("limit") || String(SIDEBAR_PAGE_DEFAULT), 10);
    const offset = parseInt(url.searchParams.get("offset") || "0", 10);
    const rows = getGoogleContacts(search, limit, offset);
    const hasMore = rows.length === Math.min(Math.max(1, limit), 500);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ contacts: rows, hasMore }));
    return;
  }

  // API: GET /api/google-contacts/:resourceName — detail view
  const gcDetailMatch = pathname.match(/^\/api\/google-contacts\/(.+)$/);
  if (gcDetailMatch && req.method === "GET") {
    const rn = decodeURIComponent(gcDetailMatch[1]!);
    const detail = getGoogleContactDetail(rn);
    if (!detail) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Not found" }));
    } else {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(detail));
    }
    return;
  }

  // API: POST /api/ai/draft — generate a draft message using Kimi K2.5
  if (pathname === "/api/ai/draft" && req.method === "POST") {
    let body = "";
    req.on("data", (chunk: Buffer) => { body += chunk.toString(); });
    req.on("end", async () => {
      try {
        const { contactId, channelId, userPrompt } = JSON.parse(body) as {
          contactId?: number;
          channelId?: number;
          userPrompt?: string;
        };

        type MsgRow = {
          date: string; blob: string; subject: string;
          is_from_user: number; sender_name: string;
          channel_names?: string; source: string;
        };

        let messages: MsgRow[];
        let personName: string;

        if (channelId) {
          // Channel mode: use only messages from this specific channel
          messages = getMessages(channelId) as MsgRow[];
          const info = getChannelInfo(channelId) as {
            participant_names: string;
          } | undefined;
          const names = info ? JSON.parse(info.participant_names) as string[] : [];
          personName = names.join(", ") || "Unknown";
        } else if (contactId) {
          // Person mode: aggregate across all channels for this person
          messages = getPersonTimeline(contactId) as MsgRow[];
          const contact = db.prepare(
            "SELECT display_name FROM contacts WHERE id = ?",
          ).get(contactId) as { display_name: string } | undefined;
          personName = contact?.display_name || "Unknown";
        } else {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "contactId or channelId required" }));
          return;
        }

        // Build conversation transcript for the LLM
        const transcript = messages.map((m) => {
          const dateStr = m.date?.slice(0, 16)?.replace("T", " ") || "?";
          const direction = m.is_from_user ? "Your Name" : m.sender_name;
          const text = m.blob || m.subject || "[no content]";
          const src = m.source && m.source !== "gmail"
            ? ` [${m.source}]` : "";
          return `[${dateStr}${src}] ${direction}: ${text}`;
        }).join("\n\n");

        // Determine if last message calls for a specific response
        const lastMsg = messages.length > 0 ? messages[messages.length - 1] : null;
        const lastIsFromThem = lastMsg && !lastMsg.is_from_user;

        const todayStr = new Date().toISOString().slice(0, 10);

        const contextLabel = channelId
          ? `the conversation channel with ${personName}`
          : `${personName}`;

        const systemPrompt = `You are a ghostwriter for Your Name. Your job is to draft the next message the owner would send in ${contextLabel}.

TODAY'S DATE: ${todayStr}

RULES:
- Write ONLY the message text. No preamble, no explanation, no alternatives. Just the message.
- Match the owner's natural voice: direct, warm, intellectually curious, concise but substantive. He uses lowercase in casual texts. He's articulate in emails.
- Match the medium: if recent messages are short SMS/RCS texts, write a short text. If they're longer emails, write accordingly.
- Reference specific shared context from the conversation history when relevant.
- NEVER fabricate facts, events, or plans that aren't grounded in the conversation.
- Pay close attention to the DATES of messages. Consider how much time has elapsed since the last message and whether any follow-up, check-in, or gentle nudge is warranted. If someone promised to get back and hasn't, a gentle follow-up is more appropriate than starting a new topic.
- Look at the full arc of the most recent exchange: who said what, what was promised or proposed, and what's still unresolved.
${userPrompt ? `\nADDITIONAL INSTRUCTION FROM THE OWNER: ${userPrompt}` : ""}

FULL CONVERSATION HISTORY (oldest to newest):
${transcript}

Draft the owner's next message:`;

        // Call Kimi K2.5 via Fireworks (OpenAI-compatible)
        const FIREWORKS_API_KEY = "YOUR_FIREWORKS_API_KEY";
        const aiResp = await fetch(
          "https://api.fireworks.ai/inference/v1/chat/completions",
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${FIREWORKS_API_KEY}`,
            },
            body: JSON.stringify({
              model: "accounts/fireworks/models/kimi-k2p5",
              messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: "Draft the message now. Output ONLY the message text, nothing else." },
              ],
              max_tokens: 2048,
              temperature: 0.7,
              reasoning_effort: "low",
            }),
          },
        );

        if (!aiResp.ok) {
          const errText = await aiResp.text();
          res.writeHead(aiResp.status, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: `AI API error: ${aiResp.status}`, detail: errText }));
          return;
        }

        const aiData = (await aiResp.json()) as {
          choices: Array<{ message: { content: string; reasoning_content?: string } }>;
        };
        // Kimi K2.5 is a reasoning model — use content (final answer), not reasoning_content
        let draft = aiData.choices?.[0]?.message?.content || "";
        // Strip any residual <think>...</think> tags some reasoning models emit
        draft = draft.replace(/<think>[\s\S]*?<\/think>/gi, "").trim();

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ draft, personName }));
      } catch (err) {
        console.error("AI draft error:", err);
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: String(err) }));
      }
    });
    return;
  }

  // Serve the HTML UI
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(HTML);
});

server.listen(PORT, () => {
  console.log(`Chat viewer running at http://localhost:${PORT}${BASE_PATH ? ` — path prefix ${BASE_PATH}` : ""}`);
  if (BASE_PATH) console.log(`  (set BASE_PATH to match the reverse proxy, e.g. Vite /outboxer)`);
  console.log(`Database: ${DB_PATH}`);
  // Defer sparkline precomputation so the event loop can handle HTTP requests.
  setTimeout(() => ensureSparklines(), 100);
});

// ─── HTML / CSS / JS ─────────────────────────────────────────────────────────

const HTML = /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Outboxer — Communication Timeline</title>
<style>
  :root {
    --font-scale: 1;
    --bg: #0f0f0f;
    --sidebar-bg: #1a1a1a;
    --card-bg: #222;
    --hover-bg: #2a2a2a;
    --active-bg: #333;
    --border: #333;
    --text: #e0e0e0;
    --text-dim: #888;
    --text-bright: #fff;
    --accent: #6c9eff;
    --sent-bg: #1a3a5c;
    --recv-bg: #2a2a2a;
    --sent-border: #2a5a8c;
    --recv-border: #444;
  }

  /* 1rem = 14px at --font-scale 1; A−/A/A+ adjusts --font-scale on :root */
  html {
    font-size: calc(14px * var(--font-scale));
  }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    font-size: 1rem;
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    overflow: hidden;
  }

  .app {
    display: flex;
    height: 100vh;
  }

  /* ── Sidebar ── */
  .sidebar {
    width: 340px;
    min-width: 180px;
    max-width: 60vw;
    background: var(--sidebar-bg);
    border-right: none;
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
  }

  .resize-handle {
    width: 4px;
    cursor: col-resize;
    background: var(--border);
    flex-shrink: 0;
    transition: background 0.15s;
  }
  .resize-handle:hover,
  .resize-handle.dragging {
    background: var(--accent);
  }

  .sidebar-header {
    padding: 16px;
    border-bottom: 1px solid var(--border);
  }

  .sidebar-header-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 10px;
  }

  .sidebar-header h1 {
    font-size: calc(16px * var(--font-scale));
    font-weight: 600;
    color: var(--text-bright);
    margin: 0;
    flex: 1;
    min-width: 0;
  }

  .font-size-controls {
    display: flex;
    align-items: center;
    gap: 2px;
    flex-shrink: 0;
  }
  .font-size-controls button {
    padding: 2px 7px;
    font-size: calc(11px * var(--font-scale));
    line-height: 1.2;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--card-bg);
    color: var(--text-dim);
    cursor: pointer;
  }
  .font-size-controls button:hover {
    color: var(--text-bright);
    border-color: var(--accent);
  }

  .search-box {
    width: 100%;
    padding: 8px 12px;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text);
    font-size: calc(13px * var(--font-scale));
    outline: none;
    transition: border-color 0.15s;
  }
  .search-box:focus {
    border-color: var(--accent);
  }
  .search-box::placeholder { color: var(--text-dim); }

  .channel-list {
    flex: 1;
    overflow-y: auto;
    padding: 4px 0;
  }

  .channel-item {
    padding: 6px 12px;
    cursor: pointer;
    border-bottom: 1px solid var(--border);
    transition: background 0.1s;
  }
  .channel-item:hover { background: var(--hover-bg); }
  .channel-item.active { background: var(--active-bg); }

  .item-columns {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .item-info {
    flex: 1;
    min-width: 0;
  }
  .item-spark {
    flex: 0 1 25%;
    min-width: 50px;
    display: flex;
    align-items: stretch;
    opacity: 0.7;
  }
  .spark-legend {
    display: flex;
    flex-direction: column;
    align-items: flex-end;
    justify-content: space-between;
    padding-right: 3px;
    flex-shrink: 0;
  }
  .spark-legend-max {
    font-size: calc(8px * var(--font-scale));
    font-weight: 400;
    color: #4a9;
    line-height: 1;
    white-space: nowrap;
    font-family: system-ui, sans-serif;
  }
  .spark-chart {
    flex: 1;
    min-width: 0;
  }
  .spark-chart svg { display: block; width: 100%; height: 100%; }

  /* ── People row ── */
  .person-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 4px 6px;
    font-size: calc(12px * var(--font-scale));
    min-width: 0;
  }
  .person-row .name-text {
    font-weight: 500;
    color: var(--text-bright);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex-shrink: 1;
    min-width: 40px;
  }
  .person-row .yr-range {
    font-size: calc(10px * var(--font-scale));
    color: #4a9;
    opacity: 0.8;
    white-space: nowrap;
    font-weight: 400;
  }
  .person-row .hide-toggle {
    flex-shrink: 0;
    cursor: pointer;
    opacity: 0.35;
    transition: opacity 0.15s, color 0.15s;
    color: var(--text);
    line-height: 0;
    display: flex;
    align-items: center;
  }
  .channel-item:hover .hide-toggle { opacity: 0.6; }
  .channel-item.item-hidden .hide-toggle { opacity: 0.85; color: #ef4444; }
  .hide-toggle:hover { opacity: 1 !important; color: #ef4444 !important; }
  .channel-item.item-hidden .name-text {
    opacity: 0.45;
    text-decoration: line-through;
    text-decoration-thickness: 1px;
  }
  .person-row [data-field] {
    display: inline-flex;
    align-items: center;
    white-space: nowrap;
    color: var(--text-dim);
    font-size: calc(11px * var(--font-scale));
  }
  .person-row .metric-icon {
    font-variant-emoji: text;
    filter: grayscale(1) brightness(0.65);
    display: inline;
    font-size: calc(10px * var(--font-scale));
    font-style: normal;
  }

  .sf-chip-hidden .sf-chip-toggle {
    background: #7c3aed33;
    color: #c084fc;
    border: 1px solid #7c3aed55;
    font-size: calc(10px * var(--font-scale));
  }

  /* ── Channel multi-line layout (unchanged) ── */
  .channel-name {
    font-size: calc(13px * var(--font-scale));
    font-weight: 500;
    color: var(--text-bright);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .channel-meta {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-top: 3px;
    font-size: calc(11px * var(--font-scale));
    color: var(--text-dim);
  }

  .channel-counts {
    display: flex;
    gap: 8px;
  }

  .channel-counts span {
    display: inline-flex;
    align-items: center;
    gap: 2px;
  }

  /* ── Chat Panel ── */
  .chat-panel {
    flex: 1;
    display: flex;
    flex-direction: column;
    min-width: 0;
  }

  .chat-header {
    border-bottom: 1px solid var(--border);
    background: var(--sidebar-bg);
  }

  .chat-header-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 20px;
    cursor: pointer;
    user-select: none;
  }

  .chat-header-top:hover {
    background: var(--hover-bg);
  }

  .chat-header-name {
    font-size: calc(15px * var(--font-scale));
    font-weight: 600;
    color: var(--text-bright);
  }

  .chat-header-summary {
    font-size: calc(12px * var(--font-scale));
    color: var(--text-dim);
    margin-left: 12px;
    white-space: nowrap;
  }

  .chat-header-toggle {
    font-size: calc(11px * var(--font-scale));
    color: var(--text-dim);
    transition: transform 0.2s;
    flex-shrink: 0;
    margin-left: 8px;
  }

  .chat-header.expanded .chat-header-toggle {
    transform: rotate(180deg);
  }

  .chat-header-details {
    display: none;
    padding: 0 20px 12px;
  }

  .chat-header.expanded .chat-header-details {
    display: block;
  }

  .chat-header-meta {
    font-size: calc(12px * var(--font-scale));
    color: var(--text-dim);
    margin-top: 2px;
  }

  .empty-state {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--text-dim);
    font-size: calc(14px * var(--font-scale));
  }

  .messages {
    flex: 1;
    overflow-y: auto;
    padding: 16px 20px;
    display: flex;
    flex-direction: column;
    gap: 6px;
  }

  /* ── Message Bubbles ── */
  .msg {
    max-width: 72%;
    display: flex;
    flex-direction: column;
  }

  .msg.sent {
    align-self: flex-end;
    align-items: flex-end;
  }

  .msg.recv {
    align-self: flex-start;
    align-items: flex-start;
  }

  .msg-sender {
    font-size: calc(11px * var(--font-scale));
    font-weight: 600;
    color: var(--accent);
    margin-bottom: 2px;
    padding: 0 4px;
  }

  .msg.sent .msg-sender { color: #7eb8ff; }

  .msg-bubble {
    padding: 8px 12px;
    border-radius: 12px;
    font-size: calc(13px * var(--font-scale));
    line-height: 1.45;
    white-space: pre-wrap;
    word-break: break-word;
    position: relative;
  }

  .msg.sent .msg-bubble {
    background: var(--sent-bg);
    border: 1px solid var(--sent-border);
    border-bottom-right-radius: 4px;
  }

  .msg.recv .msg-bubble {
    background: var(--recv-bg);
    border: 1px solid var(--recv-border);
    border-bottom-left-radius: 4px;
  }

  .msg-time {
    font-size: calc(10px * var(--font-scale));
    color: var(--text-dim);
    margin-top: 2px;
    padding: 0 4px;
  }

  /* ── AI Compose Box ── */
  .compose-box {
    border-top: 1px solid var(--border);
    padding: 10px 16px;
    background: var(--sidebar-bg);
    display: flex;
    gap: 8px;
    align-items: flex-end;
  }
  .compose-box textarea {
    flex: 1;
    background: var(--bg);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 8px 12px;
    font-size: calc(13px * var(--font-scale));
    font-family: inherit;
    resize: none;
    min-height: 20px;
    max-height: 120px;
    line-height: 1.4;
  }
  .compose-box textarea::placeholder { color: var(--text-dim); }
  .compose-box textarea:focus { outline: none; border-color: #5b8def; }
  .compose-box button {
    background: #5b8def;
    color: #fff;
    border: none;
    border-radius: 8px;
    padding: 8px 16px;
    font-size: calc(13px * var(--font-scale));
    font-weight: 600;
    cursor: pointer;
    white-space: nowrap;
    height: 36px;
  }
  .compose-box button:hover { background: #4a7de0; }
  .compose-box button:disabled {
    background: #444;
    cursor: not-allowed;
    color: #888;
  }
  .ai-draft-msg {
    background: rgba(91, 141, 239, 0.08);
    border: 1px dashed #5b8def;
    border-radius: 12px;
    padding: 10px 14px;
    margin: 8px 60px 8px auto;
    max-width: 70%;
    font-size: calc(13px * var(--font-scale));
    line-height: 1.5;
    color: var(--text);
    position: relative;
  }
  .ai-draft-msg .draft-label {
    font-size: calc(10px * var(--font-scale));
    color: #5b8def;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 4px;
  }
  .ai-draft-msg .draft-actions {
    display: flex;
    gap: 8px;
    margin-top: 8px;
  }
  .ai-draft-msg .draft-actions button {
    font-size: calc(11px * var(--font-scale));
    padding: 4px 10px;
    border-radius: 4px;
    border: none;
    cursor: pointer;
    font-weight: 500;
  }
  .draft-actions .btn-copy {
    background: #5b8def;
    color: #fff;
  }
  .draft-actions .btn-retry {
    background: var(--bg);
    color: var(--text-dim);
    border: 1px solid var(--border) !important;
  }
  .draft-actions .btn-dismiss {
    background: transparent;
    color: var(--text-dim);
  }
  .compose-spinner {
    display: inline-block;
    width: 16px;
    height: 16px;
    border: 2px solid #5b8def44;
    border-top-color: #5b8def;
    border-radius: 50%;
    animation: spin 0.6s linear infinite;
    margin-right: 6px;
    vertical-align: middle;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* ── Date Separator ── */
  .date-sep {
    text-align: center;
    padding: 12px 0 6px;
    font-size: calc(11px * var(--font-scale));
    color: var(--text-dim);
    font-weight: 500;
  }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 6px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-thumb { background: #444; border-radius: 3px; }
  ::-webkit-scrollbar-thumb:hover { background: #555; }

  /* ── Loading ── */
  .loading {
    padding: 20px;
    text-align: center;
    color: var(--text-dim);
    font-size: calc(13px * var(--font-scale));
  }

  /* ── View Toggle ── */
  .view-toggle {
    display: flex;
    gap: 0;
    margin-bottom: 10px;
    border-radius: 8px;
    overflow: hidden;
    border: 1px solid var(--border);
  }

  .view-toggle button {
    flex: 1;
    padding: 6px 0;
    background: var(--card-bg);
    border: none;
    color: var(--text-dim);
    font-size: calc(12px * var(--font-scale));
    font-weight: 500;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
  }

  .view-toggle button.active {
    background: var(--accent);
    color: #fff;
  }

  .view-toggle button:hover:not(.active) {
    background: var(--hover-bg);
    color: var(--text);
  }

  /* ── Channel Tag on Person Timeline ── */
  .msg-channel-tag {
    font-size: calc(10px * var(--font-scale));
    color: var(--text-dim);
    padding: 0 4px;
    margin-bottom: 1px;
    font-style: italic;
  }

  .msg-channel-tag .tag-names {
    color: #9a7bcc;
  }

  /* ── Contact info in header ── */
  .chat-header-channels {
    font-size: calc(11px * var(--font-scale));
    color: var(--text-dim);
    margin-top: 4px;
  }

  .chat-header-channels span {
    display: inline-block;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 1px 6px;
    margin: 2px 4px 2px 0;
    font-size: calc(10px * var(--font-scale));
  }

  /* ── Source Badge ── */
  .msg-source {
    display: inline-block;
    font-size: calc(9px * var(--font-scale));
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
    padding: 1px 5px;
    border-radius: 3px;
    margin-left: 6px;
    vertical-align: middle;
  }
  .msg-source.src-gmail { background: #2a3a5c; color: #7eb8ff; }
  .msg-source.src-gchat { background: #1a4a2a; color: #7edf9a; }
  .msg-source.src-gvoice { background: #4a3a1a; color: #dfbf5a; }
  .msg-source.src-gmessages { background: #1a3a4a; color: #5abfdf; }

  /* ── Merge Controls ── */
  .merge-section {
    margin-top: 8px;
    padding-top: 8px;
    border-top: 1px solid var(--border);
  }

  .merge-section-title {
    font-size: calc(11px * var(--font-scale));
    color: var(--text-dim);
    margin-bottom: 4px;
    font-weight: 500;
  }

  .merge-badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 2px 8px;
    margin: 2px 4px 2px 0;
    font-size: calc(11px * var(--font-scale));
    color: var(--text);
  }

  .merge-badge .unmerge-btn {
    cursor: pointer;
    color: #e55;
    font-weight: bold;
    font-size: calc(13px * var(--font-scale));
    line-height: 1;
    border: none;
    background: none;
    padding: 0 0 0 2px;
  }
  .merge-badge .unmerge-btn:hover { color: #f77; }

  .merge-btn {
    display: inline-block;
    padding: 3px 10px;
    background: var(--card-bg);
    border: 1px solid var(--accent);
    border-radius: 4px;
    color: var(--accent);
    font-size: calc(11px * var(--font-scale));
    cursor: pointer;
    margin-top: 4px;
    transition: background 0.15s, color 0.15s;
  }
  .merge-btn:hover { background: var(--accent); color: #fff; }

  /* ── Merge Modal ── */
  .merge-modal-overlay {
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.6);
    z-index: 100;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .merge-modal {
    background: var(--sidebar-bg);
    border: 1px solid var(--border);
    border-radius: 12px;
    width: 380px;
    max-height: 450px;
    display: flex;
    flex-direction: column;
    padding: 16px;
  }

  .merge-modal h3 {
    font-size: calc(14px * var(--font-scale));
    font-weight: 600;
    color: var(--text-bright);
    margin-bottom: 10px;
  }

  .merge-modal input {
    width: 100%;
    padding: 8px 10px;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 6px;
    color: var(--text);
    font-size: calc(13px * var(--font-scale));
    outline: none;
    margin-bottom: 8px;
  }
  .merge-modal input:focus { border-color: var(--accent); }

  .merge-modal-results {
    flex: 1;
    overflow-y: auto;
    max-height: 260px;
  }

  .merge-modal-item {
    padding: 8px 10px;
    cursor: pointer;
    border-radius: 6px;
    font-size: calc(13px * var(--font-scale));
    color: var(--text);
    transition: background 0.1s;
  }
  .merge-modal-item:hover { background: var(--hover-bg); }

  .merge-modal-cancel {
    margin-top: 10px;
    text-align: center;
    font-size: calc(12px * var(--font-scale));
    color: var(--text-dim);
    cursor: pointer;
  }
  .merge-modal-cancel:hover { color: var(--text); }

  /* ── Sort/Filter Toolbar ── */
  .sf-toolbar {
    display: flex;
    align-items: center;
    gap: 4px;
    margin-top: 8px;
    flex-wrap: wrap;
    min-height: 26px;
  }

  .sf-icon {
    width: 26px;
    height: 26px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 6px;
    cursor: pointer;
    background: var(--card-bg);
    border: 1px solid var(--border);
    color: var(--text-dim);
    font-size: calc(13px * var(--font-scale));
    transition: all 0.15s;
    flex-shrink: 0;
    user-select: none;
  }

  .sf-icon:hover { color: var(--text); border-color: var(--text-dim); }
  .sf-icon.active { background: var(--accent); color: #fff; border-color: var(--accent); }

  .sf-chips {
    display: contents;
  }

  .sf-chip {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    background: var(--card-bg);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 2px 5px;
    font-size: calc(10px * var(--font-scale));
    color: var(--text);
    white-space: nowrap;
  }

  .sf-chip-label {
    color: var(--text-dim);
    font-weight: 500;
  }

  .sf-chip-toggle {
    cursor: pointer;
    color: var(--accent);
    font-weight: 600;
    font-size: calc(10px * var(--font-scale));
    padding: 0 2px;
    border: none;
    background: none;
    transition: color 0.1s;
  }
  .sf-chip-toggle:hover { color: #fff; }

  .sf-chip-op {
    cursor: pointer;
    color: var(--accent);
    font-weight: 600;
    font-size: calc(10px * var(--font-scale));
    padding: 0 2px;
    border: none;
    background: none;
    min-width: 14px;
    text-align: center;
  }
  .sf-chip-op:hover { color: #fff; }

  .sf-chip-input {
    width: 44px;
    padding: 1px 3px;
    font-size: calc(10px * var(--font-scale));
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 3px;
    color: var(--text);
    outline: none;
  }
  .sf-chip-input:focus { border-color: var(--accent); }
  .sf-chip-input.wide { width: 70px; }

  .sf-chip-remove {
    cursor: pointer;
    color: #e55;
    font-weight: bold;
    font-size: calc(11px * var(--font-scale));
    border: none;
    background: none;
    padding: 0 1px;
    line-height: 1;
  }
  .sf-chip-remove:hover { color: #f77; }

  .sf-go {
    width: 22px;
    height: 22px;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 4px;
    cursor: pointer;
    background: var(--accent);
    border: none;
    color: #fff;
    font-size: calc(10px * var(--font-scale));
    font-weight: bold;
    flex-shrink: 0;
  }
  .sf-go:hover { background: #5a8aee; }

  .sf-clear {
    cursor: pointer;
    color: var(--text-dim);
    font-size: calc(11px * var(--font-scale));
    padding: 0 2px;
    border: none;
    background: none;
    white-space: nowrap;
    line-height: 1;
  }
  .sf-clear:hover { color: #e55; }

  .sf-logic-toggle {
    cursor: pointer;
    color: var(--accent);
    font-size: calc(9px * var(--font-scale));
    font-weight: 600;
    padding: 1px 4px;
    border: 1px solid var(--accent);
    background: none;
    border-radius: 3px;
    flex-shrink: 0;
  }
  .sf-logic-toggle:hover { background: var(--accent); color: #fff; }

  /* Selection mode indicators on list items */
  .sidebar.select-mode .channel-item {
    cursor: crosshair;
  }

  .sidebar.select-mode [data-field]:hover {
    color: var(--accent) !important;
    text-decoration: underline;
    cursor: pointer;
  }
</style>
</head>
<body>
<div class="app">
  <div class="sidebar">
    <div class="sidebar-header">
      <div class="sidebar-header-top">
        <h1><a href="/" style="color:inherit;text-decoration:none;" title="Switch to IonClaw">Outboxer</a> <a id="userName" href="#" style="font-weight:400;opacity:0.6;font-size:0.81em;color:inherit;text-decoration:none;" title="Click to view your identity"></a></h1>
        <div class="font-size-controls" title="Text size">
          <button type="button" id="fontSmaller" aria-label="Smaller text">A−</button>
          <button type="button" id="fontReset" aria-label="Default text size">A</button>
          <button type="button" id="fontLarger" aria-label="Larger text">A+</button>
        </div>
      </div>
      <div class="view-toggle" id="viewToggle">
        <button class="active" data-view="people" id="tabPeople">People</button>
        <button data-view="channels" id="tabChannels">Channels</button>
        <button data-view="contacts" id="tabContacts">Contacts</button>
      </div>
      <input class="search-box" id="search" type="text"
             placeholder="Search people..." autocomplete="off">
      <div class="sf-toolbar" id="sfToolbar">
        <button class="sf-icon" id="sortIcon" title="Sort mode — click to toggle, then click fields in list" type="button">&#x21C5;</button>
        <div class="sf-chips" id="sortChips"></div>
        <button class="sf-icon" id="filterIcon" title="Filter mode — click to toggle, then click fields in list" type="button">&#x25BD;</button>
        <div class="sf-chips" id="filterChips"></div>
        <button class="sf-clear" id="sfClear" style="display:none;" title="Clear all sort &amp; filter">&#x2715;</button>
      </div>
    </div>
    <div class="channel-list" id="channelList">
      <div class="loading">Loading people...</div>
    </div>
  </div>
  <div class="resize-handle" id="resizeHandle"></div>
  <div class="chat-panel" id="chatPanel">
    <div class="empty-state" id="emptyState">Select a person to view the timeline</div>
  </div>
</div>

<script>
const BASE = ${JSON.stringify(BASE_PATH)};
const SIDEBAR_PAGE = 150;
const MSG_PAGE = 500;

(function initFontScale() {
  var s = parseFloat(localStorage.getItem('outboxer-font-scale') || '1');
  if (isNaN(s) || s < 0.75) s = 1;
  if (s > 1.6) s = 1.6;
  document.documentElement.style.setProperty('--font-scale', String(s));
})();

function setFontScale(v) {
  var x = Math.max(0.75, Math.min(1.6, v));
  document.documentElement.style.setProperty('--font-scale', String(x));
  localStorage.setItem('outboxer-font-scale', String(x));
}
function adjustFontScale(delta) {
  var cur = parseFloat(localStorage.getItem('outboxer-font-scale') || '1');
  if (isNaN(cur)) cur = 1;
  setFontScale(cur + delta);
}

const channelList = document.getElementById('channelList');
const chatPanel = document.getElementById('chatPanel');
const searchBox = document.getElementById('search');
const viewToggle = document.getElementById('viewToggle');

let activeItemId = null;
let debounceTimer = null;
let currentView = 'people'; // 'people' or 'channels'
let currentContact = null; // Holds the loaded contact info for merge operations
let isReadOnly = false; // Set from /api/config — hides merge UI on cloud

// ── Sidebar Resize ──
(function() {
  var handle = document.getElementById('resizeHandle');
  var sidebar = document.querySelector('.sidebar');
  var savedW = localStorage.getItem('outboxer-sidebar-width');
  if (savedW) sidebar.style.width = savedW + 'px';

  var startX, startW;
  handle.addEventListener('mousedown', function(e) {
    e.preventDefault();
    startX = e.clientX;
    startW = sidebar.getBoundingClientRect().width;
    handle.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    function onMove(e) {
      var newW = Math.max(180, Math.min(window.innerWidth * 0.6, startW + e.clientX - startX));
      sidebar.style.width = newW + 'px';
    }
    function onUp() {
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
      handle.classList.remove('dragging');
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      localStorage.setItem('outboxer-sidebar-width', Math.round(sidebar.getBoundingClientRect().width));
    }
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onUp);
  });
})();

// ── Sort/Filter State ──
// Per-tab state — each tab has its own sort/filter config
var tabSortVars = {
  people: [{ field: 'date', dir: 'desc' }],
  channels: [{ field: 'date', dir: 'desc' }],
  contacts: [{ field: 'source', dir: 'asc' }, { field: 'date', dir: 'desc' }]
};
var tabFilterVars = { people: [], channels: [], contacts: [] };
var tabFilterLogic = { people: 'and', channels: 'and', contacts: 'and' };
// Active references (point into per-tab state)
var sortVars = tabSortVars.people;
var filterVars = tabFilterVars.people;
var filterLogic = 'and';
let sortSelectMode = false;
let filterSelectMode = false;
let cachedData = [];     // Raw data from current API response
let sfFilterTimer = null;
var sidebarHasMore = false;
var sidebarLoadBusy = false;
var allChatMessages = [];
var chatHasMoreOlder = false;
var messageLoadBusy = false;
var timelineShowChannelTags = false;
var activeLoadMoreKind = null;
var activeLoadMoreId = null;

// Hidden filter state for People tab: 'exclude' | 'only' | 'off'
var hiddenFilterMode = 'exclude';

function sfSwitchTab(view) {
  sortVars = tabSortVars[view] || [];
  filterVars = tabFilterVars[view] || [];
  filterLogic = tabFilterLogic[view] || 'and';
  tabSortVars[view] = sortVars;
  tabFilterVars[view] = filterVars;
}

const SF_LABELS = {
  name: 'Name', switches: '\u21C4Exchanges', sent: '\u2191Sent', recv: '\u2193Recv',
  msgs: 'Msgs', channels: 'Channels', date: 'Date', fields: '\u2630Fields',
  source: '\u270D\uFE0FSource',
  outpct: '\u2191%Out', cutpoint: '\u2702\uFE0ECut', intra: '\u23F1\uFE0EIntra', inter: '\u23F1\uFE0E\u23F1\uFE0EInter', initiation: '\u21C4%Init',
  slope: '\u2197Slope', exmonths: 'ExMonths'
};
const SF_TYPES = {
  name: 'text', switches: 'numeric', sent: 'numeric', recv: 'numeric',
  msgs: 'numeric', channels: 'numeric', date: 'date', fields: 'numeric',
  source: 'numeric',
  outpct: 'numeric', cutpoint: 'numeric', intra: 'numeric', inter: 'numeric', initiation: 'numeric',
  slope: 'numeric', exmonths: 'numeric'
};
const SF_OPS = {
  numeric: ['>', '<', '=', '>=', '<='],
  text: ['contains', 'regex'],
  date: ['after', 'before']
};
const SF_DEFAULT_DIR = {
  name: 'asc', switches: 'desc', sent: 'desc', recv: 'desc',
  msgs: 'desc', channels: 'desc', date: 'desc', fields: 'desc',
  source: 'asc',
  outpct: 'desc', cutpoint: 'desc', intra: 'asc', inter: 'asc', initiation: 'desc',
  slope: 'desc'
};

// Fetch read-only config
fetch(BASE + '/api/config').then(r => r.json()).then(cfg => { isReadOnly = cfg.readOnly; });

document.getElementById('fontSmaller').addEventListener('click', function() { adjustFontScale(-0.08); });
document.getElementById('fontLarger').addEventListener('click', function() { adjustFontScale(0.08); });
document.getElementById('fontReset').addEventListener('click', function() { setFontScale(1); });

// Fetch and display tab counts
fetch(BASE + '/api/counts').then(r => r.json()).then(counts => {
  document.getElementById('tabPeople').textContent = 'People (' + counts.people + ')';
  document.getElementById('tabChannels').textContent = 'Channels (' + counts.channels + ')';
  if (counts.contacts > 0) {
    document.getElementById('tabContacts').textContent = 'Contacts (' + counts.contacts + ')';
  }
});

// Fetch user identity and show in header
var userIdentity = null;
fetch(BASE + '/api/user-identity').then(r => r.json()).then(function(u) {
  userIdentity = u;
  document.getElementById('userName').textContent = '(' + u.name + ')';
});
document.getElementById('userName').addEventListener('click', function(e) {
  e.preventDefault();
  if (!userIdentity) return;
  activeItemId = null;
  document.querySelectorAll('.channel-item').forEach(function(el) { el.classList.remove('active'); });
  var u = userIdentity;
  var html = '<div style="padding:24px;max-width:640px;">';
  html += '<h2 style="margin:0 0 16px;color:var(--text-bright);">' + esc(u.name) + '</h2>';
  html += '<p style="color:var(--text-dim);margin:0 0 16px;font-size:calc(13px * var(--font-scale));">This is the owner identity. Messages sent from any of these addresses are tagged as outgoing. Emails exchanged exclusively between these addresses are excluded (self-mail).</p>';
  html += '<h3 style="margin:0 0 8px;font-size:calc(14px * var(--font-scale));color:var(--text-bright);">Seed Emails <span style="font-weight:400;opacity:0.5;">(configured in user-config.ts)</span></h3>';
  html += '<ul style="margin:0 0 16px;padding-left:20px;list-style:none;">';
  u.seedEmails.forEach(function(e) {
    html += '<li style="padding:2px 0;font-size:calc(13px * var(--font-scale));color:var(--accent);">' + esc(e) + '</li>';
  });
  html += '</ul>';
  var otherEmails = u.emails.filter(function(e) { return u.seedEmails.indexOf(e) === -1; });
  if (otherEmails.length > 0) {
    html += '<h3 style="margin:0 0 8px;font-size:calc(14px * var(--font-scale));color:var(--text-bright);">Additional Linked Emails <span style="font-weight:400;opacity:0.5;">(' + otherEmails.length + ')</span></h3>';
    html += '<ul style="margin:0 0 16px;padding-left:20px;list-style:none;">';
    otherEmails.forEach(function(e) {
      html += '<li style="padding:2px 0;font-size:calc(13px * var(--font-scale));color:var(--text-dim);">' + esc(e) + '</li>';
    });
    html += '</ul>';
  }
  if (u.phones && u.phones.length > 0) {
    html += '<h3 style="margin:0 0 8px;font-size:calc(14px * var(--font-scale));color:var(--text-bright);">Phone Numbers <span style="font-weight:400;opacity:0.5;">(' + u.phones.length + ')</span></h3>';
    html += '<ul style="margin:0 0 16px;padding-left:20px;list-style:none;">';
    u.phones.forEach(function(p) {
      html += '<li style="padding:2px 0;font-size:calc(13px * var(--font-scale));color:var(--text-dim);">' + esc(p) + '</li>';
    });
    html += '</ul>';
  }
  html += '</div>';
  chatPanel.innerHTML = html;
});

// ── View Toggle ──
viewToggle.addEventListener('click', (e) => {
  const btn = e.target.closest('button');
  if (!btn || btn.dataset.view === currentView) return;
  currentView = btn.dataset.view;
  sfSwitchTab(currentView);
  activeItemId = null;
  const emptyLabels = { people: 'person', channels: 'channel', contacts: 'contact' };
  chatPanel.innerHTML = '<div class="empty-state">Select a ' + emptyLabels[currentView] + ' to view details</div>';
  viewToggle.querySelectorAll('button').forEach(b =>
    b.classList.toggle('active', b.dataset.view === currentView));
  const placeholders = { people: 'Search people...', channels: 'Search channels...', contacts: 'Search contacts...' };
  searchBox.placeholder = placeholders[currentView] || 'Search...';
  searchBox.value = '';
  sortSelectMode = false;
  filterSelectMode = false;
  sfUpdateSelectionUI();
  sfRenderToolbar();
  loadSidebar();
});

// ── Sidebar Loading ──
async function loadSidebar(search, append) {
  if (append === undefined) append = false;
  if (currentView === 'people') return loadContacts(search, append);
  if (currentView === 'contacts') return loadGoogleContacts(search, append);
  return loadChannels(search, append);
}

// ── Channels View ──
async function loadChannels(search, append) {
  const qs = new URLSearchParams();
  if (search) qs.set('search', search);
  qs.set('limit', String(SIDEBAR_PAGE));
  qs.set('offset', append ? String(cachedData.length) : '0');
  const res = await fetch(BASE + '/api/channels?' + qs);
  const data = await res.json();
  const rows = Array.isArray(data) ? data : (data.channels || []);
  if (append) cachedData = cachedData.concat(rows);
  else cachedData = rows;
  sidebarHasMore = Array.isArray(data) ? false : !!data.hasMore;
  sfApply();
}

function renderChannels(channels) {
  if (channels.length === 0) {
    channelList.innerHTML = '<div class="loading">No channels found</div>';
    return;
  }
  channelList.innerHTML = channels.map(ch => {
    const names = JSON.parse(ch.participant_names);
    const label = names.join(', ') || 'Unknown';
    const lastDate = ch.last_date ? ch.last_date.slice(0, 10).replace(/-/g, '') : '';
    const isActive = ch.id === activeItemId;
    return '<div class="channel-item' + (isActive ? ' active' : '') + '" data-id="' + ch.id + '" data-type="channel" role="button" tabindex="0" aria-label="' + esc(label) + '">'
      + '<div class="channel-name" data-field="name" title="' + esc(label) + '">' + esc(label) + '</div>'
      + '<div class="channel-meta">'
      + '<div class="channel-counts">'
      + '<span data-field="switches">\u21C4' + ch.switches + '</span>'
      + '<span data-field="sent">\u2191' + ch.user_sent_count + '</span>'
      + '<span data-field="recv">\u2193' + ch.user_recv_count + '</span>'
      + '<span data-field="msgs">\u2709' + ch.message_count + '</span>'
      + '</div>'
      + '<span data-field="date">' + lastDate + '</span>'
      + '</div>'
      + '</div>';
  }).join('');
}

// ── People View ──
async function loadContacts(search, append) {
  const qs = new URLSearchParams();
  if (search) qs.set('search', search);
  qs.set('limit', String(SIDEBAR_PAGE));
  qs.set('offset', append ? String(cachedData.length) : '0');
  const res = await fetch(BASE + '/api/contacts?' + qs);
  const data = await res.json();
  const rows = Array.isArray(data) ? data : (data.contacts || []);
  if (append) cachedData = cachedData.concat(rows);
  else cachedData = rows;
  sidebarHasMore = Array.isArray(data) ? false : !!data.hasMore;
  sfApply();
}

var SPARK_START_YEAR = 2000;

function buildSparkline(sparklineJson, sparkMax) {
  if (!sparklineJson || !sparkMax) return null;
  var pairs;
  try { pairs = JSON.parse(sparklineJson); } catch(e) { return null; }
  if (!pairs.length) return null;

  // Pad: one zero month on each side of the data range
  var minIdx = pairs[0][0] - 1;
  var maxIdx = pairs[pairs.length - 1][0] + 1;
  var padded = [[minIdx, 0]].concat(pairs).concat([[maxIdx, 0]]);
  var span = maxIdx - minIdx;
  if (span <= 0) span = 1;

  // Year range (2-digit) — based on data range, not pads
  var startYr = Math.floor((minIdx + 1) / 12) + SPARK_START_YEAR;
  var endYr = Math.floor((maxIdx - 1) / 12) + SPARK_START_YEAR;
  var yrLabel = String(startYr).slice(-2) + '-' + String(endYr).slice(-2);

  // Pure chart SVG — viewBox spans only the chart area
  var w = 100, h = 30;
  var xScale = w / span;
  var yScale = (h - 2) / sparkMax;
  var points = padded.map(function(p) {
    var x = (p[0] - minIdx) * xScale;
    var y = h - p[1] * yScale;
    return x.toFixed(1) + ',' + y.toFixed(1);
  });

  var svg = '<svg viewBox="0 0 ' + w + ' ' + h
    + '" preserveAspectRatio="none">'
    + '<polyline points="' + points.join(' ')
    + '" fill="none" stroke="#4a9" stroke-width="1.5" vector-effect="non-scaling-stroke"/>'
    + '</svg>';

  // Legend HTML (max only — year range moved to person-row)
  var legend = '<div class="spark-legend">'
    + '<span class="spark-legend-max">' + sparkMax + '</span>'
    + '</div>';

  // Exchange-months: count of months with at least 1 exchange in the raw data
  var exchangeMonths = pairs.length;

  return { legend: legend, svg: svg, yrLabel: yrLabel, exchangeMonths: exchangeMonths };
}

function renderContacts(contacts) {
  if (contacts.length === 0) {
    channelList.innerHTML = '<div class="loading">No people found</div>';
    return;
  }
  channelList.innerHTML = contacts.map(ct => {
    const lastDate = ct.last_date ? ct.last_date.slice(0, 10).replace(/-/g, '') : '';
    const isActive = ct.id === activeItemId;
    const spark = buildSparkline(ct.sparkline, ct.spark_max);
    var hiddenClass = ct.hidden ? ' item-hidden' : '';
    var eyeTitle = ct.hidden ? 'Unhide this person' : 'Hide this person';
    var exMonths = spark ? spark.exchangeMonths : 0;
    var exYears = (exMonths / 12).toFixed(1);
    var yrLabel = spark ? spark.yrLabel : '';
    var realOutPct = (Number(ct.total_sent) + Number(ct.total_recv)) > 0
      ? Math.round(100 * Number(ct.total_sent) / (Number(ct.total_sent) + Number(ct.total_recv)))
      : 0;
    return '<div class="channel-item' + (isActive ? ' active' : '') + hiddenClass + '" data-id="' + ct.id + '" data-type="contact" role="button" tabindex="0" aria-label="' + esc(ct.display_name) + '">'
      + '<div class="item-columns">'
      + '<div class="item-info">'
      + '<div class="person-row">'
      + '<span class="name-text" data-field="name" title="' + esc(ct.display_name) + '">' + esc(ct.display_name) + '</span>'
      + (yrLabel ? '<span class="yr-range" data-field="exmonths" title="' + exMonths + ' months (' + exYears + ' years) with at least one exchange, between 20' + yrLabel.split('-')[0] + ' and 20' + yrLabel.split('-')[1] + '">' + exYears + ' (' + yrLabel + ')</span>' : '')
      + '<span class="hide-toggle" data-hide-id="' + ct.id + '" data-hidden="' + (ct.hidden ? '1' : '0') + '" title="' + eyeTitle + '">'
      + '<svg class="eye-icon" viewBox="0 0 20 12" width="14" height="9">'
      + (ct.hidden
        ? '<path d="M1 6 C4 1,16 1,19 6 C16 11,4 11,1 6Z" fill="none" stroke="currentColor" stroke-width="1.5"/>'
          + '<circle cx="10" cy="6" r="2.5" fill="none" stroke="currentColor" stroke-width="1.5"/>'
          + '<line x1="2" y1="11" x2="18" y2="1" stroke="currentColor" stroke-width="1.5"/>'
        : '<path d="M1 6 C4 1,16 1,19 6 C16 11,4 11,1 6Z" fill="none" stroke="currentColor" stroke-width="1.5"/>'
          + '<circle cx="10" cy="6" r="2.5" fill="currentColor"/>')
      + '</svg>'
      + '</span>'
      + '<span data-field="switches" title="Total bidirectional exchanges (direction switches in conversation)">\u21C4' + ct.switches + '</span>'
      + '<span data-field="cutpoint" title="Conversation cutpoint: median gap (days) that separates distinct conversations"><i class="metric-icon">\u2702\uFE0E</i>' + (Number(ct.cutpoint_days) || 0).toFixed(1) + '</span>'
      + '<span data-field="intra" title="Average gap (days) between messages within a single conversation"><i class="metric-icon">\u23F1\uFE0E</i>' + (Number(ct.intra_conv_days) || 0).toFixed(1) + '</span>'
      + '<span data-field="inter" title="Average gap (days) between separate conversations"><i class="metric-icon">\u23F1\uFE0E\u23F1\uFE0E</i>' + (Number(ct.inter_conv_days) || 0).toFixed(1) + '</span>'
      + '<span data-field="initiation" title="% of conversations initiated by you (vs. by this person)">\u21C4%' + ct.initiation_pct + '</span>'
      + '<span data-field="sent" title="Total messages you sent to this person">\u2191' + ct.total_sent + '</span>'
      + '<span data-field="recv" title="Total messages received from this person">\u2193' + ct.total_recv + '</span>'
      + '<span data-field="outpct" title="% of all messages that were outbound (sent by you): ' + ct.total_sent + '/(' + ct.total_sent + '+' + ct.total_recv + ')">\u2191%' + realOutPct + '</span>'
      + '<span data-field="channels" title="Number of distinct channels (participant sets) shared with this person">ch' + ct.channel_count + '</span>'
      + '<span data-field="slope" title="Trend percentile (0-100): how this person\u2019s exchange frequency is trending vs. all contacts">\u2197' + ct.slope_pct + '</span>'
      + '<span data-field="date" title="Date of most recent message exchanged with this person">' + lastDate + '</span>'
      + '</div>'
      + '</div>'
      + (spark ? '<div class="item-spark">' + spark.legend + '<div class="spark-chart">' + spark.svg + '</div></div>' : '')
      + '</div>'
      + '</div>';
  }).join('');
}

// ── Google Contacts View ──
async function loadGoogleContacts(search, append) {
  const qs = new URLSearchParams();
  if (search) qs.set('search', search);
  qs.set('limit', String(SIDEBAR_PAGE));
  qs.set('offset', append ? String(cachedData.length) : '0');
  const res = await fetch(BASE + '/api/google-contacts?' + qs);
  const data = await res.json();
  const rows = Array.isArray(data) ? data : (data.contacts || []);
  if (append) cachedData = cachedData.concat(rows);
  else cachedData = rows;
  sidebarHasMore = Array.isArray(data) ? false : !!data.hasMore;
  sfApply();
}

function renderGoogleContacts(contacts) {
  if (contacts.length === 0) {
    channelList.innerHTML = '<div class="loading">No contacts found</div>';
    return;
  }
  channelList.innerHTML = contacts.map(function(gc) {
    var isHuman = gc.update_time_human && gc.update_time_human !== '';
    var displayDate = isHuman
      ? gc.update_time_human.slice(0, 10).replace(/-/g, '')
      : (gc.update_time_google ? gc.update_time_google.slice(0, 10).replace(/-/g, '') : '');
    var dateStyle = isHuman ? '' : ' style="opacity:0.4"';
    var isActive = gc.resource_name === activeItemId;
    var fieldCount = (gc.emails ? gc.emails.split(', ').length : 0)
      + (gc.phones ? gc.phones.split(', ').length : 0)
      + (gc.labels ? gc.labels.split(', ').length : 0);
    var name = gcDisplayName(gc);
    return '<div class="channel-item' + (isActive ? ' active' : '') + '" data-id="' + escAttr(gc.resource_name) + '" data-type="gcontact" role="button" tabindex="0" aria-label="' + esc(name) + '">'
      + '<div class="channel-name" data-field="name" title="' + esc(name) + '">' + esc(name) + '</div>'
      + '<div class="channel-meta">'
      + '<div class="channel-counts">'
      + '<span data-field="fields">\u2630' + fieldCount + '</span>'
      + '</div>'
      + '<span data-field="date"' + dateStyle + '>' + displayDate + '</span>'
      + '</div>'
      + '</div>';
  }).join('');
}

async function loadGoogleContactDetail(resourceName) {
  activeItemId = resourceName;
  highlightActive();
  chatPanel.innerHTML = '<div class="loading">Loading contact...</div>';

  var res = await fetch(BASE + '/api/google-contacts/' + encodeURIComponent(resourceName));
  var gc = await res.json();

  var orgs = [];
  try { orgs = JSON.parse(gc.organizations || '[]'); } catch(e) {}
  var addrs = [];
  try { addrs = JSON.parse(gc.addresses || '[]'); } catch(e) {}

  var displayName = gcDisplayName(gc);
  var isHumanDate = gc.update_time_human && gc.update_time_human !== '';
  var primaryDate = isHumanDate ? gc.update_time_human.slice(0, 10) : '';
  var secondaryDate = gc.update_time_google ? gc.update_time_google.slice(0, 10) : '';
  var dateDisplay = primaryDate
    ? 'Updated ' + primaryDate
    : (secondaryDate ? 'Auto-updated ' + secondaryDate : '');

  var html = '<div style="padding:20px;max-width:640px;">';
  html += '<h2 style="margin:0 0 4px 0;font-size:calc(20px * var(--font-scale));color:var(--text);">' + esc(displayName) + '</h2>';
  html += '<div style="color:var(--text-dim);font-size:calc(13px * var(--font-scale));margin-bottom:16px;">' + dateDisplay + '</div>';

  // Contact details as a clean list
  html += '<div style="display:flex;flex-direction:column;gap:10px;">';

  if (gc.given_name || gc.family_name) {
    html += gcDetailRow('\u{1F464}', 'Name', esc(gc.given_name || '') + ' ' + esc(gc.family_name || ''));
  }
  if (gc.nickname) {
    html += gcDetailRow('\u{1F4AC}', 'Nickname', esc(gc.nickname));
  }
  if (gc.birthday) {
    html += gcDetailRow('\u{1F382}', 'Birthday', esc(gc.birthday));
  }
  if (gc.emails) {
    var emailList = gc.emails.split(', ');
    html += gcDetailRow('\u2709', 'Email' + (emailList.length > 1 ? 's' : ''), emailList.map(function(e) { return esc(e); }).join('<br>'));
  }
  if (gc.phones) {
    var phoneList = gc.phones.split(', ');
    html += gcDetailRow('\u260E', 'Phone' + (phoneList.length > 1 ? 's' : ''), phoneList.map(function(p) { return esc(p); }).join('<br>'));
  }
  for (var i = 0; i < orgs.length; i++) {
    var o = orgs[i];
    var orgParts = [];
    if (o.title) orgParts.push('<strong>' + esc(o.title) + '</strong>');
    if (o.department) orgParts.push(esc(o.department));
    if (o.name) orgParts.push(esc(o.name));
    if (orgParts.length) html += gcDetailRow('\u{1F3E2}', 'Organization', orgParts.join(' \u2014 '));
  }
  for (var j = 0; j < addrs.length; j++) {
    if (addrs[j].formatted) {
      var addrLabel = 'Address' + (addrs[j].type ? ' (' + esc(addrs[j].type) + ')' : '');
      html += gcDetailRow('\u{1F4CD}', addrLabel, esc(addrs[j].formatted).replace(/\\n/g, '<br>'));
    }
  }
  if (gc.labels) {
    html += gcDetailRow('\u{1F3F7}', 'Labels', gc.labels.split(', ').map(function(l) {
      return '<span style="display:inline-block;background:var(--card-bg);border:1px solid var(--border);border-radius:4px;padding:1px 6px;margin:1px 2px;font-size:calc(12px * var(--font-scale));">' + esc(l) + '</span>';
    }).join(' '));
  }
  if (gc.update_time_human) {
    html += gcDetailRow('\u{1F4C5}', 'Last updated (human)', esc(gc.update_time_human));
  }
  if (gc.update_time_google) {
    html += gcDetailRow('\u{1F4C5}', 'Last updated (auto)', '<span style="opacity:0.5">' + esc(gc.update_time_google) + '</span>');
  }
  if (!gc.update_time_human && !gc.update_time_google && gc.update_time) {
    html += gcDetailRow('\u{1F4C5}', 'Last updated', esc(gc.update_time));
  }

  html += '</div>'; // end detail rows

  // Show linked Outboxer contacts
  if (gc.linkedOutboxerContactIds && gc.linkedOutboxerContactIds.length > 0) {
    html += '<div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border);">';
    html += '<div style="color:var(--text-dim);font-size:calc(13px * var(--font-scale));margin-bottom:6px;">Linked Outboxer People:</div>';
    for (var k = 0; k < gc.linkedOutboxerContactIds.length; k++) {
      var cid = gc.linkedOutboxerContactIds[k];
      html += '<a href="#" onclick="event.preventDefault();switchToPeopleAndLoad(' + cid + ')" style="color:var(--accent);font-size:calc(14px * var(--font-scale));">\u2192 View messaging timeline</a> ';
    }
    html += '</div>';
  }

  html += '</div>'; // end padding wrapper
  chatPanel.innerHTML = html;
  scrollToBottom();
}

function gcDetailRow(icon, label, value) {
  return '<div style="display:flex;gap:10px;align-items:baseline;">'
    + '<span style="font-size:calc(16px * var(--font-scale));min-width:20px;text-align:center;">' + icon + '</span>'
    + '<div><div style="font-size:calc(11px * var(--font-scale));color:var(--text-dim);text-transform:uppercase;letter-spacing:0.5px;">' + label + '</div>'
    + '<div style="color:var(--text);font-size:calc(14px * var(--font-scale));">' + value + '</div></div>'
    + '</div>';
}

function gcDisplayName(gc) {
  if (gc.display_name) return gc.display_name;
  // Fallback: organization name
  try {
    var orgs = JSON.parse(gc.organizations || '[]');
    if (orgs.length > 0) {
      if (orgs[0].name) return orgs[0].name;
      if (orgs[0].title) return orgs[0].title;
    }
  } catch(e) {}
  // Fallback: labels
  if (gc.labels) return gc.labels.split(', ')[0];
  // Fallback: email or phone
  if (gc.emails) return gc.emails.split(', ')[0];
  if (gc.phones) return gc.phones.split(', ')[0];
  return '(no name)';
}

function switchToPeopleAndLoad(contactId) {
  currentView = 'people';
  viewToggle.querySelectorAll('button').forEach(function(b) {
    b.classList.toggle('active', b.dataset.view === 'people');
  });
  searchBox.placeholder = 'Search people...';
  searchBox.value = '';
  loadContacts().then(function() {
    loadPersonTimeline(contactId);
  });
}

// ── Fetch & Render Channel Messages ──
async function loadChannel(channelId) {
  activeItemId = channelId;
  highlightActive();
  activeLoadMoreKind = 'channel';
  activeLoadMoreId = channelId;
  chatPanel.innerHTML = '<div class="loading">Loading messages...</div>';

  const res = await fetch(BASE + '/api/channels/' + channelId + '/messages?limit=' + MSG_PAGE);
  const data = await res.json();
  const channel = data.channel;
  const messages = data.messages || [];
  chatHasMoreOlder = !!data.hasMoreOlder;
  allChatMessages = messages;

  const names = JSON.parse(channel.participant_names);
  const emails = channel.emails || '';
  const totalCt = Number(channel.message_count) || messages.length;

  const dateRange = (channel.first_date || '').slice(0, 10) + ' → ' + (channel.last_date || '').slice(0, 10);
  var sumLine = messages.length + ' shown';
  if (totalCt > messages.length || chatHasMoreOlder) sumLine += ' of ' + totalCt;
  if (chatHasMoreOlder) sumLine += ' · scroll up for older';

  let html = '<div class="chat-header" id="chatHeader">'
    + '<div class="chat-header-top" onclick="toggleHeader()">'
    + '<div><span class="chat-header-name">' + esc(names.join(', ')) + '</span>'
    + '<span class="chat-header-summary">' + sumLine + ' · ' + dateRange + '</span></div>'
    + '<span class="chat-header-toggle">▼</span>'
    + '</div>'
    + '<div class="chat-header-details">'
    + '<div class="chat-header-meta">'
    + sumLine + ' · ' + dateRange
    + (emails ? ' · ' + esc(emails) : '')
    + '</div>'
    + '</div></div>';

  html += renderMessageList(allChatMessages, false);

  var channelLabel = esc(names.join(', '));
  html += '<div class="compose-box" id="composeBox">'
    + '<textarea id="composeInput" rows="1" placeholder="Draft a message to ' + channelLabel + '... (optional guidance)"></textarea>'
    + '<button id="composeSend" onclick="draftAiMessage()">Draft</button>'
    + '</div>';

  chatPanel.innerHTML = html;
  scrollToBottom();
  attachMessageScrollLoader();

  var ta = document.getElementById('composeInput');
  if (ta) {
    ta.addEventListener('input', function() {
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 120) + 'px';
    });
    ta.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); draftAiMessage(); }
    });
  }
}

// ── Fetch & Render Person Timeline ──
async function loadPersonTimeline(contactId) {
  activeItemId = contactId;
  highlightActive();
  activeLoadMoreKind = 'person';
  activeLoadMoreId = contactId;
  chatPanel.innerHTML = '<div class="loading">Loading timeline...</div>';

  const res = await fetch(BASE + '/api/contacts/' + contactId + '/timeline?limit=' + MSG_PAGE);
  const data = await res.json();
  const contact = data.contact;
  const messages = data.messages || [];
  chatHasMoreOlder = !!data.hasMoreOlder;
  allChatMessages = messages;
  currentContact = contact;
  timelineShowChannelTags = contact.channels.length > 1;

  const emailStr = contact.emails.length > 0 ? contact.emails.join(', ') : '';
  const totalChannels = contact.channels.length;

  const newestDate = messages.length > 0 ? (messages[messages.length - 1].date || '').slice(0, 10) : '?';
  const oldestDate = messages.length > 0 ? (messages[0].date || '').slice(0, 10) : '?';
  const dateRange = oldestDate + ' → ' + newestDate;
  const chSuffix = totalChannels !== 1 ? 's' : '';

  var sumLine = messages.length + ' shown';
  if (chatHasMoreOlder) sumLine += ' · scroll up for older';

  let html = '<div class="chat-header" id="chatHeader">'
    + '<div class="chat-header-top" onclick="toggleHeader()">'
    + '<div><span class="chat-header-name">' + esc(contact.display_name) + '</span>'
    + '<span class="chat-header-summary">' + sumLine + ' · ' + totalChannels + ' ch · ' + dateRange + '</span></div>'
    + '<span class="chat-header-toggle">▼</span>'
    + '</div>'
    + '<div class="chat-header-details">'
    + '<div class="chat-header-meta">'
    + sumLine + ' · ' + totalChannels + ' channel' + chSuffix + ' · ' + dateRange
    + (emailStr ? ' · ' + esc(emailStr) : '')
    + '</div>';

  if (totalChannels > 1) {
    html += '<div class="chat-header-channels">';
    for (const ch of contact.channels) {
      const chNames = JSON.parse(ch.participant_names);
      const chLabel = chNames.length === 1 ? chNames[0] : chNames.join(', ');
      html += '<span title="' + ch.message_count + ' msgs">' + esc(chLabel)
        + ' (' + ch.message_count + ')</span>';
    }
    html += '</div>';
  }

  if (!isReadOnly) {
    html += '<div class="merge-section">';
    if (contact.mergedContacts && contact.mergedContacts.length > 0) {
      html += '<div class="merge-section-title">Merged aliases:</div>';
      for (const mc of contact.mergedContacts) {
        html += '<span class="merge-badge">' + esc(mc.display_name)
          + '<button class="unmerge-btn" title="Unmerge" data-unmerge="' + mc.id + '" data-primary="' + contact.id + '">×</button>'
          + '</span>';
      }
    }
    html += '<div><button class="merge-btn" id="mergeBtn">Merge another contact into this one...</button></div>';
    html += '</div>';
  }

  html += '</div></div>';
  html += renderMessageList(allChatMessages, timelineShowChannelTags);

  html += '<div class="compose-box" id="composeBox">'
    + '<textarea id="composeInput" rows="1" placeholder="Draft a message to ' + esc(contact.display_name) + '... (optional guidance)"></textarea>'
    + '<button id="composeSend" onclick="draftAiMessage()">Draft</button>'
    + '</div>';

  chatPanel.innerHTML = html;
  scrollToBottom();
  attachMessageScrollLoader();

  var ta = document.getElementById('composeInput');
  if (ta) {
    ta.addEventListener('input', function() {
      this.style.height = 'auto';
      this.style.height = Math.min(this.scrollHeight, 120) + 'px';
    });
    ta.addEventListener('keydown', function(e) {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); draftAiMessage(); }
    });
  }

  const mergeBtn = document.getElementById('mergeBtn');
  if (mergeBtn) {
    mergeBtn.addEventListener('click', () => {
      if (currentContact) openMergeModal(currentContact.id, currentContact.display_name);
    });
  }

  chatPanel.querySelectorAll('.unmerge-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const secId = parseInt(btn.getAttribute('data-unmerge'));
      const priId = parseInt(btn.getAttribute('data-primary'));
      doUnmerge(secId, priId);
    });
  });
}

function replaceMessagesDom(showTags) {
  var old = document.getElementById('messagesScroll');
  if (!old) return;
  var wrap = document.createElement('div');
  wrap.innerHTML = renderMessageList(allChatMessages, showTags);
  var next = wrap.firstElementChild;
  if (next) old.replaceWith(next);
}

async function loadMoreChannelMessages(channelId) {
  if (messageLoadBusy || !chatHasMoreOlder || allChatMessages.length === 0) return;
  messageLoadBusy = true;
  var oldest = allChatMessages[0];
  try {
    var res = await fetch(BASE + '/api/channels/' + channelId + '/messages?limit=' + MSG_PAGE
      + '&beforeDate=' + encodeURIComponent(oldest.date)
      + '&beforeId=' + oldest.id);
    var data = await res.json();
    var chunk = data.messages || [];
    if (chunk.length === 0) { chatHasMoreOlder = false; return; }
    var scroller = document.getElementById('messagesScroll');
    var oldH = scroller.scrollHeight;
    var oldTop = scroller.scrollTop;
    allChatMessages = chunk.concat(allChatMessages);
    chatHasMoreOlder = !!data.hasMoreOlder;
    replaceMessagesDom(false);
    scroller = document.getElementById('messagesScroll');
    if (scroller) scroller.scrollTop = scroller.scrollHeight - oldH + oldTop;
    attachMessageScrollLoader();
  } finally {
    messageLoadBusy = false;
  }
}

async function loadMoreTimelineMessages(contactId) {
  if (messageLoadBusy || !chatHasMoreOlder || allChatMessages.length === 0) return;
  messageLoadBusy = true;
  var oldest = allChatMessages[0];
  try {
    var res = await fetch(BASE + '/api/contacts/' + contactId + '/timeline?limit=' + MSG_PAGE
      + '&beforeDate=' + encodeURIComponent(oldest.date)
      + '&beforeId=' + oldest.id);
    var data = await res.json();
    var chunk = data.messages || [];
    if (chunk.length === 0) { chatHasMoreOlder = false; return; }
    var scroller = document.getElementById('messagesScroll');
    var oldH = scroller.scrollHeight;
    var oldTop = scroller.scrollTop;
    allChatMessages = chunk.concat(allChatMessages);
    chatHasMoreOlder = !!data.hasMoreOlder;
    replaceMessagesDom(timelineShowChannelTags);
    scroller = document.getElementById('messagesScroll');
    if (scroller) scroller.scrollTop = scroller.scrollHeight - oldH + oldTop;
    attachMessageScrollLoader();
  } finally {
    messageLoadBusy = false;
  }
}

function onMessageScrollLoadMore() {
  if (!chatHasMoreOlder || messageLoadBusy) return;
  var scroller = document.getElementById('messagesScroll');
  if (!scroller || scroller.scrollTop > 80) return;
  if (activeLoadMoreKind === 'channel' && activeLoadMoreId != null) {
    loadMoreChannelMessages(activeLoadMoreId);
  } else if (activeLoadMoreKind === 'person' && activeLoadMoreId != null) {
    loadMoreTimelineMessages(activeLoadMoreId);
  }
}

function attachMessageScrollLoader() {
  var scroller = document.getElementById('messagesScroll');
  if (!scroller) return;
  scroller.removeEventListener('scroll', onMessageScrollLoadMore);
  scroller.addEventListener('scroll', onMessageScrollLoadMore);
}

// ── Shared Message Renderer ──
function renderMessageList(messages, showChannelTags) {
  let html = '<div class="messages" id="messagesScroll" tabindex="0">';
  let lastDate = '';
  let lastChannelId = null;

  for (const msg of messages) {
    const msgDate = (msg.date || '').slice(0, 10);
    if (msgDate !== lastDate) {
      lastDate = msgDate;
      html += '<div class="date-sep">' + formatDate(msgDate) + '</div>';
      lastChannelId = null; // reset channel tag after date separator
    }

    // Show channel context tag when channel changes (person timeline only)
    if (showChannelTags && msg.channel_id !== lastChannelId) {
      lastChannelId = msg.channel_id;
      const chNames = JSON.parse(msg.channel_names);
      const chLabel = chNames.length === 1
        ? 'direct'
        : 'via ' + chNames.join(', ');
      html += '<div class="msg-channel-tag"><span class="tag-names">' + esc(chLabel) + '</span></div>';
    }

    const side = msg.is_from_user ? 'sent' : 'recv';
    const time = (msg.date || '').slice(11, 16);
    const blob = msg.blob || msg.subject || '';
    const source = msg.source || 'gmail';
    const sourceLabel = source === 'gchat' ? 'chat' : source === 'gvoice' ? 'voice' : source === 'gmessages' ? 'msg' : '';
    const sourceBadge = sourceLabel
      ? '<span class="msg-source src-' + source + '">' + sourceLabel + '</span>'
      : '';

    html += '<div class="msg ' + side + '">'
      + '<div class="msg-sender">' + esc(msg.sender_name) + sourceBadge + '</div>'
      + '<div class="msg-bubble">' + esc(blob) + '</div>'
      + '<div class="msg-time">' + time + '</div>'
      + '</div>';
  }

  html += '</div>';
  return html;
}

// ── Helpers ──
function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function formatDate(iso) {
  if (!iso) return 'Unknown date';
  try {
    const d = new Date(iso + 'T00:00:00Z');
    return d.toLocaleDateString('en-US', { weekday: 'short', year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' });
  } catch { return iso; }
}

function highlightActive() {
  document.querySelectorAll('.channel-item').forEach(el => {
    var elId = el.dataset.type === 'gcontact' ? el.dataset.id : parseInt(el.dataset.id);
    el.classList.toggle('active', elId === activeItemId);
  });
}

function toggleHeader() {
  const hdr = document.getElementById('chatHeader');
  if (hdr) hdr.classList.toggle('expanded');
}

// ── AI Draft ──
async function draftAiMessage() {
  if (!activeItemId || (currentView !== 'people' && currentView !== 'channels')) return;
  var btn = document.getElementById('composeSend');
  var input = document.getElementById('composeInput');
  var scroller = document.getElementById('messagesScroll');
  if (!btn || !input) return;

  var userPrompt = input.value.trim();
  btn.disabled = true;
  btn.innerHTML = '<span class="compose-spinner"></span>Drafting...';

  // Remove any previous draft
  var prev = document.getElementById('aiDraftMsg');
  if (prev) prev.remove();

  try {
    var payload = { userPrompt: userPrompt || undefined };
    if (currentView === 'people') {
      payload.contactId = activeItemId;
    } else {
      payload.channelId = activeItemId;
    }
    var res = await fetch(BASE + '/api/ai/draft', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    var data = await res.json();
    if (!res.ok) throw new Error(data.error || 'AI request failed');

    // Insert draft message at the bottom of the messages list
    var draftDiv = document.createElement('div');
    draftDiv.id = 'aiDraftMsg';
    draftDiv.className = 'ai-draft-msg';
    draftDiv.innerHTML = '<div class="draft-label">AI Draft</div>'
      + '<div class="draft-text">' + esc(data.draft) + '</div>'
      + '<div class="draft-actions">'
      + '<button class="btn-copy" onclick="copyDraft()">Copy</button>'
      + '<button class="btn-retry" onclick="draftAiMessage()">Retry</button>'
      + '<button class="btn-dismiss" onclick="dismissDraft()">Dismiss</button>'
      + '</div>';

    if (scroller) {
      scroller.appendChild(draftDiv);
      scroller.scrollTop = scroller.scrollHeight;
    }

    input.value = '';
    input.style.height = 'auto';
  } catch (err) {
    alert('Draft failed: ' + (err.message || err));
  } finally {
    btn.disabled = false;
    btn.textContent = 'Draft';
  }
}

function copyDraft() {
  var el = document.querySelector('#aiDraftMsg .draft-text');
  if (el) {
    navigator.clipboard.writeText(el.textContent || '');
    var btn = document.querySelector('#aiDraftMsg .btn-copy');
    if (btn) { btn.textContent = 'Copied!'; setTimeout(function() { btn.textContent = 'Copy'; }, 1500); }
  }
}

function dismissDraft() {
  var el = document.getElementById('aiDraftMsg');
  if (el) el.remove();
}

function scrollToBottom() {
  const scroller = document.getElementById('messagesScroll');
  if (scroller) {
    scroller.scrollTop = scroller.scrollHeight;
    scroller.focus();
  }
}

// ── Events ──
channelList.addEventListener('click', (e) => {
  // Hide toggle
  var hideBtn = e.target.closest('.hide-toggle');
  if (hideBtn) {
    e.preventDefault();
    e.stopPropagation();
    var hid = hideBtn.dataset.hidden === '1' ? 0 : 1;
    var cid = hideBtn.dataset.hideId;
    fetch(BASE + '/api/contacts/' + cid + '/hide', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ hidden: hid })
    }).then(function() {
      var found = cachedData.find(function(c) { return String(c.id) === String(cid); });
      if (found) found.hidden = hid;
      // If no hidden items remain, reset filter to default
      if (!cachedData.some(function(c) { return !!c.hidden; })) {
        hiddenFilterMode = 'exclude';
      }
      sfRenderToolbar();
      sfApply();
    });
    return;
  }
  // In selection mode, intercept clicks on data-field elements
  if (sortSelectMode || filterSelectMode) {
    const fieldEl = e.target.closest('[data-field]');
    if (fieldEl) {
      e.preventDefault();
      e.stopPropagation();
      sfAddVar(fieldEl.dataset.field);
      return;
    }
  }
  const item = e.target.closest('.channel-item');
  if (!item) return;
  if (item.dataset.type === 'gcontact') {
    loadGoogleContactDetail(item.dataset.id);
  } else {
    const id = parseInt(item.dataset.id);
    if (item.dataset.type === 'contact') {
      loadPersonTimeline(id);
    } else {
      loadChannel(id);
    }
  }
});

searchBox.addEventListener('input', () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(() => loadSidebar(searchBox.value), 250);
});

// ── Merge Modal ──
let mergeSourceId = null; // The contact we want to merge INTO another

function openMergeModal(sourceId, sourceName) {
  mergeSourceId = sourceId;
  const overlay = document.createElement('div');
  overlay.className = 'merge-modal-overlay';
  overlay.id = 'mergeOverlay';
  overlay.innerHTML = '<div class="merge-modal">'
    + '<h3>Merge "' + esc(sourceName) + '" into...</h3>'
    + '<input type="text" id="mergeSearchInput" placeholder="Search for target contact..." autocomplete="off">'
    + '<div class="merge-modal-results" id="mergeResults"><div class="loading">Type to search</div></div>'
    + '<div class="merge-modal-cancel" onclick="closeMergeModal()">Cancel</div>'
    + '</div>';
  document.body.appendChild(overlay);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) closeMergeModal(); });

  const input = document.getElementById('mergeSearchInput');
  let mTimer = null;
  input.addEventListener('input', () => {
    clearTimeout(mTimer);
    mTimer = setTimeout(() => searchMergeTargets(input.value), 200);
  });
  input.focus();
}

function closeMergeModal() {
  const overlay = document.getElementById('mergeOverlay');
  if (overlay) overlay.remove();
  mergeSourceId = null;
}

async function searchMergeTargets(q) {
  const resultsEl = document.getElementById('mergeResults');
  if (!q || q.length < 1) {
    resultsEl.innerHTML = '<div class="loading">Type to search</div>';
    return;
  }
  const res = await fetch(BASE + '/api/contacts/search-merge-targets?q=' + encodeURIComponent(q) + '&exclude=' + mergeSourceId);
  const items = await res.json();
  if (items.length === 0) {
    resultsEl.innerHTML = '<div class="loading">No matching contacts</div>';
    return;
  }
  resultsEl.innerHTML = items.map(c =>
    '<div class="merge-modal-item" onclick="doMerge(' + mergeSourceId + ',' + c.id + ')">'
    + esc(c.display_name) + '</div>'
  ).join('');
}

async function doMerge(secondaryId, primaryId) {
  closeMergeModal();
  await fetch(BASE + '/api/contacts/merge', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ secondaryId, primaryId }),
  });
  // Reload the sidebar and open the primary contact's timeline
  await loadSidebar(searchBox.value);
  loadPersonTimeline(primaryId);
}

async function doUnmerge(secondaryId, primaryId) {
  await fetch(BASE + '/api/contacts/unmerge', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ secondaryId }),
  });
  // Reload the sidebar and refresh the primary's timeline
  await loadSidebar(searchBox.value);
  loadPersonTimeline(primaryId);
}

// ── Sort/Filter Core Functions ──

function escAttr(s) {
  return String(s).replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function sfGetFieldValue(item, field) {
  if (currentView === 'people') {
    switch (field) {
      case 'name': return item.display_name || '';
      case 'switches': return Number(item.switches) || 0;
      case 'sent': return Number(item.total_sent) || 0;
      case 'recv': return Number(item.total_recv) || 0;
      case 'msgs': return Number(item.total_messages) || 0;
      case 'channels': return Number(item.channel_count) || 0;
      case 'date': return item.last_date || '';
      case 'outpct': { var s = Number(item.total_sent)||0, r = Number(item.total_recv)||0; return (s+r) > 0 ? Math.round(100*s/(s+r)) : 0; }
      case 'cutpoint': return Number(item.cutpoint_days) || 0;
      case 'intra': return Number(item.intra_conv_days) || 0;
      case 'inter': return Number(item.inter_conv_days) || 0;
      case 'initiation': return Number(item.initiation_pct) || 0;
      case 'slope': return Number(item.slope_pct) || 0;
      case 'exmonths': try { return JSON.parse(item.sparkline || '[]').length; } catch(e) { return 0; }
    }
  } else if (currentView === 'contacts') {
    switch (field) {
      case 'name': return gcDisplayName(item);
      case 'fields': return (item.emails ? item.emails.split(', ').length : 0)
        + (item.phones ? item.phones.split(', ').length : 0)
        + (item.labels ? item.labels.split(', ').length : 0);
      case 'date': return (item.update_time_human || item.update_time_google || '').slice(0, 10);
      case 'source': return (item.update_time_human && item.update_time_human !== '') ? 0 : 1;
    }
  } else {
    switch (field) {
      case 'name': try { return JSON.parse(item.participant_names || '[]').join(', '); } catch(e) { return ''; }
      case 'switches': return Number(item.switches) || 0;
      case 'sent': return Number(item.user_sent_count) || 0;
      case 'recv': return Number(item.user_recv_count) || 0;
      case 'msgs': return Number(item.message_count) || 0;
      case 'date': return item.last_date || '';
      default: return 0;
    }
  }
  return '';
}

function sfEvalFilter(item, fv) {
  const val = sfGetFieldValue(item, fv.field);
  const ft = SF_TYPES[fv.field] || 'numeric';
  if (ft === 'numeric') {
    const num = Number(val);
    const target = Number(fv.value);
    if (isNaN(target)) return true;
    switch (fv.op) {
      case '>': return num > target;
      case '<': return num < target;
      case '=': return num === target;
      case '>=': return num >= target;
      case '<=': return num <= target;
    }
  } else if (ft === 'text') {
    const str = String(val).toLowerCase();
    if (fv.op === 'contains') return str.includes(fv.value.toLowerCase());
    if (fv.op === 'regex') {
      try { return new RegExp(fv.value, 'i').test(str); }
      catch(e) { return true; }
    }
  } else if (ft === 'date') {
    const d = String(val).slice(0, 10);
    const t = fv.value;
    if (!t) return true;
    if (fv.op === 'after') return d >= t;
    if (fv.op === 'before') return d <= t;
  }
  return true;
}

function sfApply() {
  let data = [...cachedData];

  // Apply hidden filter for People tab
  if (currentView === 'people') {
    if (hiddenFilterMode === 'exclude') {
      data = data.filter(function(item) { return !item.hidden; });
    } else if (hiddenFilterMode === 'only') {
      data = data.filter(function(item) { return !!item.hidden; });
    }
    // 'off' = show all, no filtering
  }

  // Apply filters
  if (filterVars.length > 0) {
    data = data.filter(function(item) {
      const results = filterVars.map(function(fv) { return sfEvalFilter(item, fv); });
      return filterLogic === 'and' ? results.every(Boolean) : results.some(Boolean);
    });
  }

  // Apply sorts — always append implicit name Az tiebreaker
  var effectiveSorts = sortVars.slice();
  if (!sortVars.some(function(v) { return v.field === 'name'; })) {
    effectiveSorts.push({ field: 'name', dir: 'asc' });
  }
  if (effectiveSorts.length > 0) {
    data.sort(function(a, b) {
      for (var si = 0; si < effectiveSorts.length; si++) {
        var sv = effectiveSorts[si];
        var va = sfGetFieldValue(a, sv.field);
        var vb = sfGetFieldValue(b, sv.field);
        var cmp;
        if (typeof va === 'number' && typeof vb === 'number') {
          cmp = va - vb;
        } else {
          cmp = String(va).localeCompare(String(vb), undefined, { sensitivity: 'base' });
        }
        if (sv.dir === 'desc') cmp = -cmp;
        if (cmp !== 0) return cmp;
      }
      return 0;
    });
  }

  // Re-render toolbar (hidden chip may appear/disappear based on data)
  sfRenderToolbar();

  if (currentView === 'people') renderContacts(data);
  else if (currentView === 'contacts') renderGoogleContacts(data);
  else renderChannels(data);
}

function sfAddVar(field) {
  if (sortSelectMode) {
    if (sortVars.some(function(v) { return v.field === field; })) return;
    sortVars.push({ field: field, dir: SF_DEFAULT_DIR[field] || 'asc' });
    sfRenderToolbar();
    sfApply();
  } else if (filterSelectMode) {
    var ft = SF_TYPES[field] || 'numeric';
    var ops = SF_OPS[ft] || SF_OPS.numeric;
    filterVars.push({ field: field, op: ops[0], value: '' });
    sfRenderToolbar();
  }
}

function sfRenderToolbar() {
  var sortEl = document.getElementById('sortChips');
  var filterEl = document.getElementById('filterChips');
  var hasAny = sortVars.length > 0 || filterVars.length > 0;

  // Sort chips
  var sh = '';
  sortVars.forEach(function(sv, i) {
    var dirLabel;
    if (sv.field === 'source') {
      dirLabel = sv.dir === 'asc' ? '\u270D\uFE0F1st' : '\u{1F916}1st';
    } else {
      dirLabel = sv.dir === 'asc' ? 'Az' : 'Za';
    }
    sh += '<span class="sf-chip">'
      + '<span class="sf-chip-label">' + (SF_LABELS[sv.field] || sv.field) + '</span>'
      + '<button class="sf-chip-toggle" data-action="sort-dir" data-idx="' + i + '">'
      + dirLabel + '</button>'
      + '<button class="sf-chip-remove" data-action="sort-rm" data-idx="' + i + '">\u00d7</button>'
      + '</span>';
  });
  if (sortVars.length > 0) {
    sh += '<button class="sf-go" data-action="sort-go" title="Apply sort">\u25b6</button>';
  }
  sortEl.innerHTML = sh;

  // Filter chips — start with regular filter chips
  var fh = '';
  filterVars.forEach(function(fv, i) {
    var ft = SF_TYPES[fv.field] || 'numeric';
    var inputType = ft === 'date' ? 'date' : 'text';
    var inputClass = ft === 'text' ? ' wide' : '';
    fh += '<span class="sf-chip">'
      + '<span class="sf-chip-label">' + (SF_LABELS[fv.field] || fv.field) + '</span>'
      + '<button class="sf-chip-op" data-action="filter-op" data-idx="' + i + '">' + esc(fv.op) + '</button>'
      + '<input class="sf-chip-input' + inputClass + '" data-action="filter-val" data-idx="' + i
      + '" type="' + inputType + '" value="' + escAttr(fv.value) + '">'
      + '<button class="sf-chip-remove" data-action="filter-rm" data-idx="' + i + '">\u00d7</button>'
      + '</span>';
  });

  // Hidden filter chip — only on People tab, only when there are hidden items
  if (currentView === 'people' && sfHasHiddenItems()) {
    var hiddenLabel = hiddenFilterMode === 'exclude' ? 'Hidden: filtered'
      : hiddenFilterMode === 'only' ? 'Hidden: only' : 'Hidden: off';
    fh += '<span class="sf-chip sf-chip-hidden">'
      + '<span class="sf-chip-label">\u{1F6AB}</span>'
      + '<button class="sf-chip-toggle" data-action="hidden-cycle" title="Cycle: filtered \u2192 only \u2192 off">'
      + hiddenLabel + '</button>'
      + '</span>';
    hasAny = true;
  }

  if (filterVars.length > 0) {
    fh += '<button class="sf-logic-toggle" data-action="filter-logic">' + filterLogic.toUpperCase() + '</button>';
    fh += '<button class="sf-go" data-action="filter-go" title="Apply filter">\u25b6</button>';
  }
  filterEl.innerHTML = fh;

  // Show/hide clear button
  var clearEl = document.getElementById('sfClear');
  if (clearEl) clearEl.style.display = hasAny ? '' : 'none';
}

function sfHasHiddenItems() {
  return cachedData.some(function(item) { return !!item.hidden; });
}

function sfUpdateSelectionUI() {
  document.getElementById('sortIcon').classList.toggle('active', sortSelectMode);
  document.getElementById('filterIcon').classList.toggle('active', filterSelectMode);
  document.querySelector('.sidebar').classList.toggle('select-mode', sortSelectMode || filterSelectMode);
}

// ── Sort/Filter Event Delegation ──
document.getElementById('sfToolbar').addEventListener('click', function(e) {
  var btn = e.target.closest('[data-action]');
  if (!btn) return;
  var action = btn.dataset.action;
  var idx = parseInt(btn.dataset.idx);

  if (action === 'sort-dir') {
    sortVars[idx].dir = sortVars[idx].dir === 'asc' ? 'desc' : 'asc';
    sfRenderToolbar();
    sfApply();
  } else if (action === 'sort-rm') {
    sortVars.splice(idx, 1);
    sfRenderToolbar();
    sfApply();
  } else if (action === 'sort-go') {
    sortSelectMode = false;
    sfUpdateSelectionUI();
    sfApply();
  } else if (action === 'filter-op') {
    var fv = filterVars[idx];
    var ft = SF_TYPES[fv.field] || 'numeric';
    var ops = SF_OPS[ft];
    var curIdx = ops.indexOf(fv.op);
    fv.op = ops[(curIdx + 1) % ops.length];
    sfRenderToolbar();
    if (fv.value) sfApply();
  } else if (action === 'filter-rm') {
    filterVars.splice(idx, 1);
    sfRenderToolbar();
    sfApply();
  } else if (action === 'filter-go') {
    filterSelectMode = false;
    sfUpdateSelectionUI();
    sfApply();
  } else if (action === 'filter-logic') {
    filterLogic = filterLogic === 'and' ? 'or' : 'and';
    tabFilterLogic[currentView] = filterLogic;
    sfRenderToolbar();
    sfApply();
  } else if (action === 'hidden-cycle') {
    if (hiddenFilterMode === 'exclude') hiddenFilterMode = 'only';
    else if (hiddenFilterMode === 'only') hiddenFilterMode = 'off';
    else hiddenFilterMode = 'exclude';
    sfRenderToolbar();
    sfApply();
  }
});

// Filter value input changes (debounced)
document.getElementById('sfToolbar').addEventListener('input', function(e) {
  if (e.target.dataset.action === 'filter-val') {
    var idx = parseInt(e.target.dataset.idx);
    filterVars[idx].value = e.target.value;
    clearTimeout(sfFilterTimer);
    sfFilterTimer = setTimeout(function() { sfApply(); }, 400);
  }
});

// Sort icon toggle
document.getElementById('sortIcon').addEventListener('click', function() {
  sortSelectMode = !sortSelectMode;
  if (sortSelectMode) filterSelectMode = false;
  sfUpdateSelectionUI();
});

// Filter icon toggle
document.getElementById('filterIcon').addEventListener('click', function() {
  filterSelectMode = !filterSelectMode;
  if (filterSelectMode) sortSelectMode = false;
  sfUpdateSelectionUI();
});

// Clear all sort/filter — reset to per-tab defaults
var SF_TAB_DEFAULTS = {
  people: [{ field: 'date', dir: 'desc' }],
  channels: [{ field: 'date', dir: 'desc' }],
  contacts: [{ field: 'source', dir: 'asc' }, { field: 'date', dir: 'desc' }]
};
document.getElementById('sfClear').addEventListener('click', function() {
  var defaults = SF_TAB_DEFAULTS[currentView] || [{ field: 'date', dir: 'desc' }];
  tabSortVars[currentView] = defaults.map(function(d) { return { field: d.field, dir: d.dir }; });
  tabFilterVars[currentView] = [];
  tabFilterLogic[currentView] = 'and';
  sortVars = tabSortVars[currentView];
  filterVars = tabFilterVars[currentView];
  filterLogic = 'and';
  if (currentView === 'people') hiddenFilterMode = 'exclude';
  sortSelectMode = false;
  filterSelectMode = false;
  sfUpdateSelectionUI();
  sfRenderToolbar();
  sfApply();
});

// ── Sidebar infinite scroll (load more rows) ──
channelList.addEventListener('scroll', function() {
  if (sidebarLoadBusy || !sidebarHasMore) return;
  if (channelList.scrollHeight - channelList.scrollTop - channelList.clientHeight < 100) {
    sidebarLoadBusy = true;
    loadSidebar(searchBox.value, true).finally(function() { sidebarLoadBusy = false; });
  }
});

// ── Init ──
sfRenderToolbar();
loadSidebar();
</script>
</body>
</html>`;
