#!/usr/bin/env -S node --import tsx
/**
 * sync-gmail.ts — Gmail API sync pipeline for Outboxer.
 *
 * Fetches new messages since the last MBOX takeout (or last sync) via Gmail
 * API, processes them through the same identity-resolution / channel-assignment
 * / diff-strip pipeline as the MBOX importer.
 *
 * Usage:
 *   npm run sync   # incremental sync (or backfill if first run)
 *
 * Auth is delegated to gog (gogcli). No per-pipeline tokens or --auth flow.
 * See src/lib/google-auth.ts.
 */

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";

import { simpleParser } from "mailparser";
import type { AddressObject, ParsedMail } from "mailparser";
import { google } from "googleapis";
import {
  ParagraphCorpus,
  beautify,
  splitParagraphs,
  diffStripMessage,
  buildBlob,
} from "./lib/diff-strip.js";
import { SEED_EMAILS } from "./lib/user-config.js";
import { createGoogleAuth, GoogleAuthError } from "./lib/google-auth.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths & Config ─────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");
const BATCH_SIZE = 50; // messages per Gmail API batch

// Date of latest MBOX takeout message — backfill starts here.
const MBOX_CUTOFF = process.env.SYNC_CUTOFF || "2026/02/11";

// ─── Database helpers ───────────────────────────────────────────────────────

function openDb(): DB {
  if (!existsSync(DB_PATH)) {
    console.error(`Database not found: ${DB_PATH}`);
    process.exit(1);
  }
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA foreign_keys = ON");

  // Ensure sync_state table exists
  db.exec(`CREATE TABLE IF NOT EXISTS sync_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )`);

  return db;
}

function getSyncState(db: DB, key: string): string | null {
  const row = db.prepare("SELECT value FROM sync_state WHERE key = ?").get(key) as
    | { value: string }
    | undefined;
  return row?.value ?? null;
}

function setSyncState(db: DB, key: string, value: string): void {
  db.prepare(
    "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
  ).run(key, value);
}

/**
 * Prune non-bidirectional channels (same as import Phase 4).
 * Only channels where user has both sent AND received are kept.
 */
function pruneNonBidirectionalChannels(db: DB): void {
  // A channel is retained if ANY of its participants has bidirectional
  // communication with the user across ANY channel (not just this one).
  // First, find the set of "bidirectional people" — contacts who have
  // both sent and received messages with the user in at least one channel.
  const pruneSQL = `
    DELETE FROM channels WHERE id IN (
      SELECT ch.id FROM channels ch
      WHERE ch.user_sent_count = 0 OR ch.user_recv_count = 0
      AND ch.id NOT IN (
        SELECT cp.channel_id FROM channel_participants cp
        WHERE cp.contact_id IN (
          -- Contacts with bidirectional communication in ANY channel
          SELECT DISTINCT cp2.contact_id
          FROM channel_participants cp2
          JOIN channels ch2 ON ch2.id = cp2.channel_id
          WHERE ch2.user_sent_count > 0 AND ch2.user_recv_count > 0
        )
      )
    )`;

  // Count first
  const countSQL = `
    SELECT COUNT(*) as cnt FROM channels ch
    WHERE (ch.user_sent_count = 0 OR ch.user_recv_count = 0)
    AND ch.id NOT IN (
      SELECT cp.channel_id FROM channel_participants cp
      WHERE cp.contact_id IN (
        SELECT DISTINCT cp2.contact_id
        FROM channel_participants cp2
        JOIN channels ch2 ON ch2.id = cp2.channel_id
        WHERE ch2.user_sent_count > 0 AND ch2.user_recv_count > 0
      )
    )`;

  const { cnt } = db.prepare(countSQL).get() as { cnt: number };

  if (cnt > 0) {
    console.log(`\nPruning ${cnt} non-bidirectional channels (person-level check)...`);
    // Delete messages in those channels
    db.exec(
      `DELETE FROM messages WHERE channel_id IN (
        SELECT ch.id FROM channels ch
        WHERE (ch.user_sent_count = 0 OR ch.user_recv_count = 0)
        AND ch.id NOT IN (
          SELECT cp.channel_id FROM channel_participants cp
          WHERE cp.contact_id IN (
            SELECT DISTINCT cp2.contact_id
            FROM channel_participants cp2
            JOIN channels ch2 ON ch2.id = cp2.channel_id
            WHERE ch2.user_sent_count > 0 AND ch2.user_recv_count > 0
          )
        )
      )`,
    );
    // Delete participants
    db.exec(
      `DELETE FROM channel_participants WHERE channel_id IN (
        SELECT ch.id FROM channels ch
        WHERE (ch.user_sent_count = 0 OR ch.user_recv_count = 0)
        AND ch.id NOT IN (
          SELECT cp.channel_id FROM channel_participants cp
          WHERE cp.contact_id IN (
            SELECT DISTINCT cp2.contact_id
            FROM channel_participants cp2
            JOIN channels ch2 ON ch2.id = cp2.channel_id
            WHERE ch2.user_sent_count > 0 AND ch2.user_recv_count > 0
          )
        )
      )`,
    );
    // Delete channels
    db.exec(
      `DELETE FROM channels WHERE id IN (
        SELECT ch.id FROM channels ch
        WHERE (ch.user_sent_count = 0 OR ch.user_recv_count = 0)
        AND ch.id NOT IN (
          SELECT cp.channel_id FROM channel_participants cp
          WHERE cp.contact_id IN (
            SELECT DISTINCT cp2.contact_id
            FROM channel_participants cp2
            JOIN channels ch2 ON ch2.id = cp2.channel_id
            WHERE ch2.user_sent_count > 0 AND ch2.user_recv_count > 0
          )
        )
      )`,
    );
    console.log(`  Pruned ${cnt} channels.`);
  }
}

// ─── Shared pipeline helpers (mirrored from import-gmail-takeout.ts) ────────

function extractAddresses(
  field: AddressObject | AddressObject[] | undefined,
): string[] {
  if (!field) return [];
  const fields = Array.isArray(field) ? field : [field];
  const addrs: string[] = [];
  for (const f of fields) {
    if (f.value) {
      for (const v of f.value) {
        if (v.address) addrs.push(v.address.toLowerCase().trim());
      }
    }
  }
  return addrs;
}

function normalizeSubject(subject: string): string {
  return subject
    .replace(/^(\s*(re|fwd?|fw|aw|sv|vs|ref|tr)\s*:\s*)+/gi, "")
    .replace(/\s+/g, " ")
    .trim();
}

function formatDate(date: Date | null | undefined): string | null {
  if (!date || isNaN(date.getTime())) return null;
  return date.toISOString();
}

function resolveContact(
  db: DB,
  email: string,
  displayName: string | undefined,
  findEmail: ReturnType<DB["prepare"]>,
  insertContact: ReturnType<DB["prepare"]>,
  insertEmailStmt: ReturnType<DB["prepare"]>,
  cache: Map<string, number>,
  nameToContactId: Map<string, number>,
): number {
  const lower = email.toLowerCase().trim();
  const cached = cache.get(lower);
  if (cached !== undefined) return cached;

  const row = findEmail.get(lower) as { contact_id: number } | undefined;
  if (row) {
    cache.set(lower, row.contact_id);
    return row.contact_id;
  }

  const name = displayName && displayName !== lower ? displayName : lower;
  const nameLower = name.toLowerCase().trim();
  const isRealName =
    !nameLower.includes("@") &&
    nameLower.includes(" ") &&
    nameLower.length >= 5;

  if (isRealName) {
    const existingId = nameToContactId.get(nameLower);
    if (existingId !== undefined) {
      insertEmailStmt.run(lower, existingId);
      cache.set(lower, existingId);
      return existingId;
    }
  }

  const result = insertContact.run(name, "inferred");
  const contactId = Number(result.lastInsertRowid);
  insertEmailStmt.run(lower, contactId);
  cache.set(lower, contactId);
  if (isRealName) {
    nameToContactId.set(nameLower, contactId);
  }
  return contactId;
}

// ─── Gmail API fetching ─────────────────────────────────────────────────────

interface GmailMessageRef {
  id: string;
  threadId: string;
}

/**
 * Backfill: fetch all message IDs since MBOX_CUTOFF using messages.list.
 */
async function fetchMessageIdsSinceCutoff(
  gmail: ReturnType<typeof google.gmail>,
): Promise<GmailMessageRef[]> {
  console.log(`  Fetching message list (after:${MBOX_CUTOFF})...`);
  const refs: GmailMessageRef[] = [];
  let pageToken: string | undefined;
  let pages = 0;

  do {
    const res = await gmail.users.messages.list({
      userId: "me",
      q: `after:${MBOX_CUTOFF}`,
      maxResults: 500,
      pageToken,
    });

    if (res.data.messages) {
      for (const m of res.data.messages) {
        refs.push({ id: m.id!, threadId: m.threadId! });
      }
    }
    pageToken = res.data.nextPageToken ?? undefined;
    pages++;
    console.log(`    Page ${pages}: ${refs.length} messages so far`);
  } while (pageToken);

  console.log(`  Total message refs: ${refs.length}`);
  return refs;
}

/**
 * Incremental: fetch message IDs from history.list since lastHistoryId.
 */
async function fetchMessageIdsFromHistory(
  gmail: ReturnType<typeof google.gmail>,
  startHistoryId: string,
): Promise<{ refs: GmailMessageRef[]; latestHistoryId: string }> {
  console.log(`  Fetching history since historyId=${startHistoryId}...`);
  const refs: GmailMessageRef[] = [];
  const seenIds = new Set<string>();
  let pageToken: string | undefined;
  let latestHistoryId = startHistoryId;

  do {
    const res = await gmail.users.history.list({
      userId: "me",
      startHistoryId,
      historyTypes: ["messageAdded"],
      pageToken,
    });

    latestHistoryId = res.data.historyId || latestHistoryId;

    if (res.data.history) {
      for (const h of res.data.history) {
        if (h.messagesAdded) {
          for (const ma of h.messagesAdded) {
            const msg = ma.message;
            if (msg?.id && !seenIds.has(msg.id)) {
              seenIds.add(msg.id);
              refs.push({ id: msg.id, threadId: msg.threadId || "" });
            }
          }
        }
      }
    }
    pageToken = res.data.nextPageToken ?? undefined;
  } while (pageToken);

  console.log(`  New messages from history: ${refs.length}`);
  return { refs, latestHistoryId };
}

/**
 * Fetch a single message in raw RFC 2822 format and parse it.
 */
async function fetchAndParseMessage(
  gmail: ReturnType<typeof google.gmail>,
  messageId: string,
): Promise<{ parsed: ParsedMail; gmailId: string; historyId: string } | null> {
  try {
    const res = await gmail.users.messages.get({
      userId: "me",
      id: messageId,
      format: "raw",
    });

    const raw = res.data.raw;
    if (!raw) return null;

    // Gmail returns URL-safe base64
    const buffer = Buffer.from(raw, "base64url");
    const parsed = await simpleParser(buffer, {
      skipHtmlToText: false,
      skipTextToHtml: true,
      skipImageLinks: true,
    });

    return {
      parsed,
      gmailId: res.data.id!,
      historyId: res.data.historyId || "",
    };
  } catch (err: any) {
    if (err.code === 404) return null; // message deleted
    throw err;
  }
}

// ─── Main sync pipeline ─────────────────────────────────────────────────────

async function sync(): Promise<void> {
  console.log("Outboxer Gmail Sync");
  console.log("===================\n");

  // 1. Authorize via gog
  const oauth2 = createGoogleAuth();
  const gmail = google.gmail({ version: "v1", auth: oauth2 });

  // 2. Open database
  const db = openDb();
  console.log(`Database: ${DB_PATH}`);

  // 3. Detect user emails and contact ID
  const userEmails = new Set<string>();
  const findContactByEmail = db.prepare(
    "SELECT contact_id FROM contact_emails WHERE email = ?",
  );
  let userContactId = -1;
  const contactMatchCounts = new Map<number, number>();
  for (const seed of SEED_EMAILS) {
    userEmails.add(seed);
    const row = findContactByEmail.get(seed) as { contact_id: number } | undefined;
    if (row) {
      const count = (contactMatchCounts.get(row.contact_id) || 0) + 1;
      contactMatchCounts.set(row.contact_id, count);
    }
  }
  // Pick the contact with the most seed email matches
  let bestCount = 0;
  for (const [cid, count] of contactMatchCounts) {
    if (count > bestCount) {
      bestCount = count;
      userContactId = cid;
    }
  }
  // Expand user emails from all addresses linked to the detected user contact
  if (userContactId > 0) {
    const rows = db
      .prepare("SELECT email FROM contact_emails WHERE contact_id = ?")
      .all(userContactId) as { email: string }[];
    for (const row of rows) userEmails.add(row.email);
  }
  console.log(`User contact ID: ${userContactId} (${userEmails.size} emails)\n`);

  // 4. Determine sync mode: backfill vs incremental
  const lastHistoryId = getSyncState(db, "lastHistoryId");
  let messageRefs: GmailMessageRef[];
  let newHistoryId: string | null = null;

  if (!lastHistoryId) {
    console.log("Mode: BACKFILL (no previous sync state)");
    messageRefs = await fetchMessageIdsSinceCutoff(gmail);

    // Get current historyId from profile for future incremental syncs
    const profile = await gmail.users.getProfile({ userId: "me" });
    newHistoryId = profile.data.historyId || null;
    console.log(`  Current historyId: ${newHistoryId}\n`);
  } else {
    console.log("Mode: INCREMENTAL");
    try {
      const result = await fetchMessageIdsFromHistory(gmail, lastHistoryId);
      messageRefs = result.refs;
      newHistoryId = result.latestHistoryId;
    } catch (err: any) {
      if (err.code === 404) {
        // historyId expired (> 7 days old) — fall back to backfill
        console.log("  History expired, falling back to backfill...");
        messageRefs = await fetchMessageIdsSinceCutoff(gmail);
        const profile = await gmail.users.getProfile({ userId: "me" });
        newHistoryId = profile.data.historyId || null;
      } else {
        throw err;
      }
    }
    console.log("");
  }

  if (messageRefs.length === 0) {
    console.log("No new messages to sync.");
    pruneNonBidirectionalChannels(db);
    if (newHistoryId) setSyncState(db, "lastHistoryId", newHistoryId);
    setSyncState(db, "lastSyncAt", new Date().toISOString());
    db.close();
    console.log("\nDone!");
    return;
  }

  // 5. Pre-load existing message IDs for dedup
  const existingMsgIds = new Set<string>();
  const existingRows = db
    .prepare("SELECT message_id FROM messages WHERE message_id IS NOT NULL")
    .all() as { message_id: string }[];
  for (const row of existingRows) existingMsgIds.add(row.message_id);
  console.log(`Existing messages in DB: ${existingMsgIds.size}`);

  // 6. Prepare statements (same as import pipeline)
  const findEmail = db.prepare(
    "SELECT contact_id FROM contact_emails WHERE email = ?",
  );
  const insertContact = db.prepare(
    "INSERT INTO contacts (display_name, source) VALUES (?, ?)",
  );
  const insertEmailStmt = db.prepare(
    "INSERT OR IGNORE INTO contact_emails (email, contact_id) VALUES (?, ?)",
  );
  const findChannel = db.prepare(
    "SELECT id FROM channels WHERE participant_hash = ?",
  );
  const insertChannel = db.prepare(
    `INSERT INTO channels (participant_hash, participant_names, first_date, last_date,
       message_count, user_sent_count, user_recv_count)
     VALUES (?, ?, ?, ?, 1, ?, ?)`,
  );
  const updateChannel = db.prepare(
    `UPDATE channels SET
       first_date = MIN(first_date, ?),
       last_date  = MAX(last_date, ?),
       message_count = message_count + 1,
       user_sent_count = user_sent_count + ?,
       user_recv_count = user_recv_count + ?
     WHERE id = ?`,
  );
  const insertParticipant = db.prepare(
    "INSERT OR IGNORE INTO channel_participants (channel_id, contact_id) VALUES (?, ?)",
  );
  const insertMessage = db.prepare(
    `INSERT OR IGNORE INTO messages
       (channel_id, message_id, date, sender_contact_id, is_from_user,
        subject, raw_body, blob, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, '', 'gmail_api')`,
  );
  // Content dedup: Gmail can surface multiple message resources (draft snapshots,
  // sent copies, label events) with distinct Message-IDs for a single human
  // send. Detect by sender + subject + time proximity + matching body opening,
  // and keep only the version with the longest body.
  const findSimilarMsg = db.prepare(
    `SELECT id, channel_id, COALESCE(LENGTH(raw_body), 0) as body_len
     FROM messages
     WHERE sender_contact_id = ?
       AND subject = ?
       AND source = 'gmail_api'
       AND ABS(CAST(strftime('%s', date) AS INTEGER) - CAST(strftime('%s', ?) AS INTEGER)) <= 21600
       AND REPLACE(REPLACE(TRIM(SUBSTR(COALESCE(raw_body, ''), 1, 60)), char(10), ' '), char(13), ' ') = ?
     ORDER BY COALESCE(LENGTH(raw_body), 0) DESC
     LIMIT 1`,
  );
  const updateMsgBody = db.prepare(
    `UPDATE messages SET raw_body = ?, blob = '' WHERE id = ?`,
  );

  // Contact resolution caches
  const contactCache = new Map<string, number>();
  for (const email of userEmails) {
    contactCache.set(email, userContactId);
  }
  const nameToContactId = new Map<string, number>();
  const gcRows = db
    .prepare("SELECT id, display_name FROM contacts WHERE source = 'google_contacts'")
    .all() as { id: number; display_name: string }[];
  for (const row of gcRows) {
    const key = row.display_name.toLowerCase().trim();
    if (key.includes(" ") && key.length >= 5 && !key.includes("@")) {
      nameToContactId.set(key, row.id);
    }
  }
  // Also include inferred contacts in name map
  const inferredRows = db
    .prepare("SELECT id, display_name FROM contacts WHERE source = 'inferred'")
    .all() as { id: number; display_name: string }[];
  for (const row of inferredRows) {
    const key = row.display_name.toLowerCase().trim();
    if (key.includes(" ") && key.length >= 5 && !key.includes("@")) {
      if (!nameToContactId.has(key)) nameToContactId.set(key, row.id);
    }
  }

  // 7. Fetch and process messages
  console.log(`\nFetching and processing ${messageRefs.length} messages...\n`);

  let imported = 0;
  let skipped = 0;
  let errors = 0;
  const affectedChannelIds = new Set<number>();
  const newMessageIds = new Set<number>(); // DB row IDs of newly inserted messages

  db.exec("BEGIN");

  for (let i = 0; i < messageRefs.length; i++) {
    const ref = messageRefs[i]!;

    if ((i + 1) % BATCH_SIZE === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
      console.log(
        `  [${i + 1}/${messageRefs.length}] ${imported} imported, ${skipped} skipped, ${errors} errors`,
      );
    }

    // Fetch and parse the raw message
    let result;
    try {
      result = await fetchAndParseMessage(gmail, ref.id);
    } catch (err: any) {
      // Rate limit — back off and retry once
      if (err.code === 429) {
        console.log("  Rate limited, waiting 10s...");
        await new Promise((r) => setTimeout(r, 10000));
        try {
          result = await fetchAndParseMessage(gmail, ref.id);
        } catch {
          errors++;
          continue;
        }
      } else {
        errors++;
        continue;
      }
    }

    if (!result) {
      skipped++;
      continue;
    }

    const { parsed } = result;

    const messageId =
      parsed.messageId ||
      `synth-${createHash("md5").update(ref.id).digest("hex")}`;

    if (existingMsgIds.has(messageId)) {
      skipped++;
      continue;
    }

    // Extract fields
    const fromAddrs = extractAddresses(parsed.from);
    const from = fromAddrs[0] || "";
    const to = extractAddresses(parsed.to);
    const cc = extractAddresses(parsed.cc);
    const subject = parsed.subject || "(no subject)";
    const date = parsed.date ?? null;
    const bodyText = parsed.text || "";
    const isFromUser = userEmails.has(from.toLowerCase().trim());

    // Resolve sender
    let fromDisplayName: string | undefined;
    if (parsed.from) {
      const fromField = Array.isArray(parsed.from) ? parsed.from[0] : parsed.from;
      if (fromField?.value?.[0]?.name) {
        fromDisplayName = fromField.value[0].name;
      }
    }
    const senderContactId = resolveContact(
      db, from, fromDisplayName, findEmail, insertContact, insertEmailStmt,
      contactCache, nameToContactId,
    );

    const allAddresses = [
      ...new Set([from, ...to, ...cc].map((a) => a.toLowerCase().trim()).filter(Boolean)),
    ];
    const participantContactIds: number[] = [];

    for (const addr of allAddresses) {
      if (userEmails.has(addr)) continue;
      let displayName: string | undefined;
      if (parsed.from) {
        const fromField = Array.isArray(parsed.from) ? parsed.from : [parsed.from];
        for (const f of fromField) {
          const match = f.value?.find((v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr);
          if (match?.name) { displayName = match.name; break; }
        }
      }
      if (!displayName && parsed.to) {
        const toField = Array.isArray(parsed.to) ? parsed.to : [parsed.to];
        for (const f of toField) {
          const match = f.value?.find((v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr);
          if (match?.name) { displayName = match.name; break; }
        }
      }
      if (!displayName && parsed.cc) {
        const ccField = Array.isArray(parsed.cc) ? parsed.cc : [parsed.cc];
        for (const f of ccField) {
          const match = f.value?.find((v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr);
          if (match?.name) { displayName = match.name; break; }
        }
      }
      const contactId = resolveContact(
        db, addr, displayName, findEmail, insertContact, insertEmailStmt,
        contactCache, nameToContactId,
      );
      participantContactIds.push(contactId);
    }

    if (participantContactIds.length === 0) {
      skipped++;
      continue;
    }

    // Channel key
    const sortedIds = [...new Set(participantContactIds)].sort((a, b) => a - b);
    const channelHash = createHash("sha256")
      .update(sortedIds.join("\0"))
      .digest("hex");
    const dateStr = formatDate(date);
    if (!dateStr) {
      console.log(`    Skipping message with unparseable date: ${messageId}`);
      continue;
    }

    // Find or create channel
    const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
    let channelId: number;

    if (channelRow) {
      channelId = channelRow.id;
      updateChannel.run(dateStr, dateStr, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
    } else {
      // Resolve participant names
      const participantNames: string[] = [];
      for (const cid of sortedIds) {
        const c = db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(cid) as
          | { display_name: string }
          | undefined;
        if (c) participantNames.push(c.display_name);
      }
      const res = insertChannel.run(
        channelHash, JSON.stringify(participantNames), dateStr, dateStr,
        isFromUser ? 1 : 0, isFromUser ? 0 : 1,
      );
      channelId = Number(res.lastInsertRowid);
      for (const cid of sortedIds) {
        insertParticipant.run(channelId, cid);
      }
    }

    // Content-based dedup: check for an existing message that looks like the
    // same human send (same sender, same subject, matching body opening, ±6 hrs).
    const normSubject = normalizeSubject(subject);
    const bodyPrefix = bodyText.trim().slice(0, 60).replace(/[\r\n]+/g, " ");
    const similar = findSimilarMsg.get(senderContactId, normSubject, dateStr, bodyPrefix) as
      | { id: number; channel_id: number; body_len: number }
      | undefined;

    if (similar) {
      if (bodyText.length > similar.body_len) {
        // New version has more content — upgrade the existing row's body.
        updateMsgBody.run(bodyText, similar.id);
        newMessageIds.add(similar.id); // mark for diff-strip recompute
        affectedChannelIds.add(similar.channel_id);
      }
      existingMsgIds.add(messageId);
      skipped++;
      continue;
    }

    // Insert message (blob will be computed in diff-strip phase below)
    const msgResult = insertMessage.run(
      channelId, messageId, dateStr, senderContactId, isFromUser ? 1 : 0,
      normSubject, bodyText,
    );

    const dbRowId = Number(msgResult.lastInsertRowid);
    if (msgResult.changes > 0) {
      newMessageIds.add(dbRowId);
      affectedChannelIds.add(channelId);
      existingMsgIds.add(messageId);
      imported++;
    } else {
      skipped++; // duplicate
    }
  }

  db.exec("COMMIT");

  console.log(`\nFetch complete: ${imported} imported, ${skipped} skipped, ${errors} errors`);

  // 8. Diff-strip affected channels
  if (affectedChannelIds.size > 0) {
    console.log(`\nDiff-stripping ${affectedChannelIds.size} affected channels...`);
    const updateBlob = db.prepare("UPDATE messages SET blob = ? WHERE id = ?");

    db.exec("BEGIN");
    let channelsDone = 0;

    for (const channelId of affectedChannelIds) {
      // Get ALL messages for this channel chronologically
      const messages = db
        .prepare(
          `SELECT id, raw_body, subject FROM messages
           WHERE channel_id = ? ORDER BY date ASC`,
        )
        .all(channelId) as { id: number; raw_body: string; subject: string }[];

      if (messages.length === 0) continue;

      // Build corpus from existing messages, only compute blobs for new ones
      const corpus = new ParagraphCorpus();
      for (const msg of messages) {
        if (newMessageIds.has(msg.id)) {
          // New message — compute and save blob
          const { newText } = diffStripMessage(msg.raw_body || "", corpus);
          updateBlob.run(buildBlob(msg.subject, newText), msg.id);
        } else {
          // Existing message — just train the corpus
          const cleaned = beautify(msg.raw_body || "");
          const paragraphs = splitParagraphs(cleaned);
          corpus.addAll(paragraphs);
        }
      }

      channelsDone++;
      if (channelsDone % 100 === 0) {
        db.exec("COMMIT");
        db.exec("BEGIN");
        console.log(`  ${channelsDone}/${affectedChannelIds.size} channels processed`);
      }
    }

    db.exec("COMMIT");
    console.log(`  Diff-strip complete: ${affectedChannelIds.size} channels`);
  }

  // 9. Prune non-bidirectional channels
  pruneNonBidirectionalChannels(db);

  // 10. Update sync state
  if (newHistoryId) {
    setSyncState(db, "lastHistoryId", newHistoryId);
  }
  setSyncState(db, "lastSyncAt", new Date().toISOString());
  console.log(`\nSync state updated. lastHistoryId=${newHistoryId}`);

  db.close();
  console.log("\nDone!");
}

// ─── CLI ────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

if (args.includes("--auth")) {
  console.error(
    "The --auth flag is no longer used. Gmail auth is managed by gog.\n" +
      "If gog's token is invalid, re-authorize with:\n" +
      "  gog auth add your-email@gmail.com --services gmail,contacts",
  );
  process.exit(1);
} else if (args.includes("--daemon")) {
  const INTERVAL_MS = 15 * 60 * 1000;
  console.log(`Starting Gmail sync daemon (every ${INTERVAL_MS / 60000} minutes)...\n`);

  let authFailCount = 0;
  const runOnce = async () => {
    try {
      await sync();
      authFailCount = 0;
    } catch (err) {
      if (err instanceof GoogleAuthError) {
        authFailCount++;
        if (authFailCount <= 3) {
          console.error(`[${new Date().toISOString()}] AUTH FAILED: ${err.message}`);
        } else if (authFailCount === 4) {
          console.error(`[${new Date().toISOString()}] AUTH FAILED (suppressing further repeats). Fix gog auth.`);
        }
      } else {
        console.error(`[${new Date().toISOString()}] Sync error:`, err);
      }
    }
  };

  await runOnce();
  setInterval(runOnce, INTERVAL_MS);
} else {
  try {
    await sync();
  } catch (err: unknown) {
    if (err instanceof GoogleAuthError) {
      console.error(err.message);
      process.exit(2);
    }
    throw err;
  }
}
