#!/usr/bin/env -S node --import tsx
/**
 * sync-gmessages.ts — Matrix bridge → Outboxer DB
 *
 * Polls the local Synapse homeserver for new messages arriving via the
 * mautrix-gmessages bridge and imports them into the Outboxer database.
 *
 * The bridge creates one Matrix room per Google Messages conversation.
 * Each bridged message has an `m.room.message` event with the text content,
 * and the sender is a ghost user like `@gmessagesphone_+14155551234:outboxer.local`.
 *
 * This script:
 *   1. Uses the Matrix /sync API with a since token to get incremental updates
 *   2. Extracts message text, sender phone, and timestamp
 *   3. Resolves contacts via phone numbers (same as SMS/Voice importers)
 *   4. Creates/updates channels and inserts messages with source='gmessages'
 *
 * Usage:
 *   node --import tsx src/sync-gmessages.ts              # one-shot sync
 *   node --import tsx src/sync-gmessages.ts --daemon      # continuous polling
 *
 * Environment / config:
 *   MATRIX_HOMESERVER=http://localhost:8008
 *   MATRIX_ACCESS_TOKEN=syt_... (from login)
 *   MATRIX_USER=@outboxer:outboxer.local
 */

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { SEED_EMAILS } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Config ──────────────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");
const SYNC_TOKEN_PATH = join(OUTBOXER_HOME, "matrix-sync-token");

const MATRIX_TOKEN_PATH = join(OUTBOXER_HOME, "matrix-token");

const MATRIX_HOMESERVER =
  process.env.MATRIX_HOMESERVER || "http://localhost:8008";
const MATRIX_ACCESS_TOKEN =
  process.env.MATRIX_ACCESS_TOKEN ||
  (existsSync(MATRIX_TOKEN_PATH)
    ? readFileSync(MATRIX_TOKEN_PATH, "utf-8").trim()
    : "");
const MATRIX_USER =
  process.env.MATRIX_USER || "@outboxer:outboxer.local";
const DAEMON_MODE = process.argv.includes("--daemon");
const POLL_INTERVAL_MS = 15_000; // 15 seconds in daemon mode

// ─── Text Normalization ─────────────────────────────────────────────────────

function normalizeBody(s: string): string {
  return s
    .replace(/&#(\d+);/g, (_, n) => String.fromCodePoint(Number(n)))
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .trim();
}

// ─── Bridge User Helpers ─────────────────────────────────────────────────────

/**
 * Check if a Matrix ghost user ID looks like the user's own puppet.
 * The user's puppet has a displayname matching its raw ID (e.g., "gmessages_1.140")
 * while real contacts have proper names (e.g., "John Smith").
 */
function isUserPuppet(userId: string, displayName: string): boolean {
  // The user's own puppet has a displayname that matches the localpart
  const localpart = userId.split(":")[0].replace("@", "");
  return displayName === localpart || displayName === "";
}

/**
 * Check if a Matrix user ID is a bridge ghost user.
 */
function isBridgeGhost(userId: string): boolean {
  return userId.startsWith("@gmessages_");
}

// ─── Matrix API ──────────────────────────────────────────────────────────────

async function matrixFetch(
  path: string,
  params?: Record<string, string>,
): Promise<unknown> {
  const url = new URL(path, MATRIX_HOMESERVER);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      url.searchParams.set(k, v);
    }
  }
  const resp = await fetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${MATRIX_ACCESS_TOKEN}`,
    },
  });
  if (!resp.ok) {
    throw new Error(
      `Matrix API ${path} failed: ${resp.status} ${await resp.text()}`,
    );
  }
  return resp.json();
}

interface MatrixSyncResponse {
  next_batch: string;
  rooms?: {
    join?: Record<
      string,
      {
        timeline?: {
          events?: MatrixEvent[];
        };
      }
    >;
  };
}

interface MatrixEvent {
  type: string;
  event_id: string;
  sender: string;
  origin_server_ts: number;
  content?: {
    msgtype?: string;
    body?: string;
  };
}

function loadSyncToken(): string | null {
  try {
    return readFileSync(SYNC_TOKEN_PATH, "utf-8").trim();
  } catch {
    return null;
  }
}

function saveSyncToken(token: string): void {
  writeFileSync(SYNC_TOKEN_PATH, token, "utf-8");
}

// ─── Database ────────────────────────────────────────────────────────────────

function openDb(): DB {
  if (!existsSync(DB_PATH)) {
    console.error(`Database not found: ${DB_PATH}`);
    process.exit(1);
  }
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA foreign_keys = ON");
  try {
    db.exec("ALTER TABLE messages ADD COLUMN source TEXT DEFAULT 'gmail'");
  } catch {
    /* exists */
  }
  db.exec(`CREATE TABLE IF NOT EXISTS contact_phones (
    phone      TEXT PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id)
  )`);
  db.exec(
    `CREATE INDEX IF NOT EXISTS idx_contact_phones_contact ON contact_phones(contact_id)`,
  );
  return db;
}

function detectUserContactId(db: DB): number {
  const findEmail = db.prepare(
    "SELECT contact_id FROM contact_emails WHERE email = ?",
  );
  const contactMatchCounts = new Map<number, number>();
  let bestContactId = -1;
  let bestMatchCount = 0;
  for (const seed of SEED_EMAILS) {
    const row = findEmail.get(seed) as { contact_id: number } | undefined;
    if (row) {
      const count = (contactMatchCounts.get(row.contact_id) || 0) + 1;
      contactMatchCounts.set(row.contact_id, count);
      if (count > bestMatchCount) {
        bestMatchCount = count;
        bestContactId = row.contact_id;
      }
    }
  }
  return bestContactId;
}

// ─── Sync Logic ──────────────────────────────────────────────────────────────

/**
 * Fetch all messages from a room by paginating backwards.
 */
async function fetchRoomMessages(
  roomId: string,
  limit = 500,
): Promise<MatrixEvent[]> {
  const allEvents: MatrixEvent[] = [];
  let from: string | undefined;

  while (allEvents.length < limit) {
    const params: Record<string, string> = {
      dir: "b",
      limit: String(Math.min(100, limit - allEvents.length)),
      filter: JSON.stringify({ types: ["m.room.message"] }),
    };
    if (from) params.from = from;

    const resp = (await matrixFetch(
      `/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/messages`,
      params,
    )) as { chunk: MatrixEvent[]; end?: string };

    if (!resp.chunk || resp.chunk.length === 0) break;
    allEvents.push(...resp.chunk);

    if (!resp.end) break;
    from = resp.end;
  }

  return allEvents;
}

/**
 * Get all members of a room and categorize them.
 * Returns: { contacts: Map<userId, displayName>, userPuppets: Set<userId> }
 */
async function getRoomMembers(
  roomId: string,
): Promise<{
  contacts: Map<string, string>;
  userPuppets: Set<string>;
}> {
  const contacts = new Map<string, string>();
  const userPuppets = new Set<string>();

  try {
    // Fetch ALL members (join + leave) so we detect the user's puppet even when
    // its membership is "leave" — the bridge frequently sets puppet membership
    // to leave while still routing the user's outbound messages through it.
    const resp = (await matrixFetch(
      `/_matrix/client/v3/rooms/${encodeURIComponent(roomId)}/members`,
    )) as {
      chunk: Array<{
        state_key: string;
        content: { displayname?: string; membership?: string };
      }>;
    };

    for (const m of resp.chunk || []) {
      const userId = m.state_key;
      const displayName = m.content?.displayname || "";
      const membership = m.content?.membership || "";

      if (userId === MATRIX_USER) continue;
      if (userId.startsWith("@gmessagesbot:")) continue;
      if (!isBridgeGhost(userId)) continue;

      if (isUserPuppet(userId, displayName)) {
        userPuppets.add(userId);
      } else if (membership === "join") {
        contacts.set(userId, displayName);
      }
    }
  } catch {
    // Room might not be accessible
  }

  return { contacts, userPuppets };
}

/**
 * Resolve a contact by display name, matching to existing contacts in the DB.
 */
function resolveContactByName(
  db: DB,
  displayName: string,
  insertContact: ReturnType<DB["prepare"]>,
  nameToContactId: Map<string, number>,
): number {
  const nameLower = displayName.toLowerCase().trim();

  // Try exact name match first
  if (nameLower.length >= 3) {
    const existingId = nameToContactId.get(nameLower);
    if (existingId !== undefined) return existingId;
  }

  // Create a new inferred contact
  const result = insertContact.run(displayName || "Unknown", "inferred");
  const contactId = Number(result.lastInsertRowid);
  if (nameLower.length >= 3) {
    nameToContactId.set(nameLower, contactId);
  }
  return contactId;
}

async function getGhostDisplayName(userId: string): Promise<string> {
  try {
    const resp = (await matrixFetch(
      `/_matrix/client/v3/profile/${encodeURIComponent(userId)}`,
    )) as { displayname?: string };
    return resp.displayname || "";
  } catch {
    return "";
  }
}

async function syncOnce(db: DB): Promise<number> {
  const userContactId = detectUserContactId(db);
  if (userContactId < 0) {
    console.error("Could not detect user contact ID");
    return 0;
  }

  // Prepared statements
  const insertContact = db.prepare(
    "INSERT INTO contacts (display_name, source) VALUES (?, ?)",
  );
  const findChannel = db.prepare(
    "SELECT id FROM channels WHERE participant_hash = ?",
  );
  const insertChannel = db.prepare(
    `INSERT INTO channels (participant_hash, participant_names, first_date, last_date,
       message_count, user_sent_count, user_recv_count)
     VALUES (?, ?, ?, ?, 0, 0, 0)`,
  );
  const updateChannelStats = db.prepare(
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
        subject, blob, raw_body, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'gmessages')`,
  );
  const checkMsg = db.prepare(
    "SELECT 1 FROM messages WHERE message_id = ?",
  );
  // Dedup: skip if a message with the same content, approximate timestamp, and sender
  // already exists in the same channel — regardless of source or message_id prefix.
  // Catches both cross-source dupes (SMS backup vs bridge) and bridge re-backfill dupes.
  // Uses 2-second tolerance for timestamp matching.
  const checkDupe = db.prepare(
    `SELECT 1 FROM messages
     WHERE channel_id = ?
       AND ABS(CAST(strftime('%s', date) AS INTEGER) - CAST(strftime('%s', ?) AS INTEGER)) <= 2
       AND REPLACE(REPLACE(TRIM(blob), char(10), ''), char(13), '')
         = REPLACE(REPLACE(TRIM(?), char(10), ''), char(13), '')
       AND sender_contact_id = ?`,
  );

  // Caches
  const nameToContactId = new Map<string, number>();
  const existingContacts = db
    .prepare("SELECT id, LOWER(display_name) as name FROM contacts")
    .all() as { id: number; name: string }[];
  for (const row of existingContacts) {
    if (row.name.length >= 3 && !row.name.startsWith("+")) {
      nameToContactId.set(row.name, row.id);
    }
  }
  // Map Matrix ghost userId → Outboxer contactId (resolved per-room)
  const ghostToContact = new Map<string, number>();

  // Load sync token
  let sinceToken = loadSyncToken();
  const isInitial = !sinceToken;

  let imported = 0;
  let skipped = 0;

  if (isInitial) {
    // ── Initial sync: enumerate joined rooms and paginate messages from each ──
    console.log("  Initial sync — fetching all rooms and messages...");

    // Do an initial /sync to get the room list and save the token
    const syncResp = (await matrixFetch("/_matrix/client/v3/sync", {
      timeout: "0",
      filter: JSON.stringify({
        room: { timeline: { limit: 0 }, state: { types: ["m.room.member"] } },
        presence: { types: [] },
        account_data: { types: [] },
      }),
    })) as MatrixSyncResponse;

    const joinedRooms = Object.keys(syncResp.rooms?.join || {});
    console.log(`  Found ${joinedRooms.length} joined rooms`);

    db.exec("BEGIN");

    for (const roomId of joinedRooms) {
      // Get room members categorized as contacts vs user puppets
      const { contacts, userPuppets } = await getRoomMembers(roomId);
      if (contacts.size === 0) continue; // skip management/space rooms

      // Resolve contacts by display name
      const contactIds: number[] = [];
      const contactNames: string[] = [];
      for (const [userId, displayName] of contacts) {
        if (!displayName || displayName.startsWith("gmessages_")) continue;
        const cid = resolveContactByName(db, displayName, insertContact, nameToContactId);
        contactIds.push(cid);
        ghostToContact.set(userId, cid);
        contactNames.push(displayName);
      }

      if (contactIds.length === 0) continue;

      const sortedIds = [...new Set(contactIds)].sort((a, b) => a - b);
      const channelHash = createHash("sha256")
        .update(sortedIds.join("\0"))
        .digest("hex");

      // Find or create channel
      const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
      let channelId: number;
      if (channelRow) {
        channelId = channelRow.id;
      } else {
        const result = insertChannel.run(
          channelHash, JSON.stringify(contactNames),
          "2099-01-01T00:00:00Z", "2000-01-01T00:00:00Z",
        );
        channelId = Number(result.lastInsertRowid);
        for (const cid of sortedIds) insertParticipant.run(channelId, cid);
      }

      // Fetch all messages from this room
      const events = await fetchRoomMessages(roomId, 500);
      let roomImported = 0;

      for (const event of events) {
        if (event.type !== "m.room.message") continue;
        if (!event.content?.body) continue;
        // Only import text messages; skip images, video, audio, notices, files
        if (event.content.msgtype !== "m.text") continue;

        // Skip bridge commands that were accidentally sent to portal rooms
        const body = event.content.body;
        if (body.startsWith("!gm ") || body === "login" || body.startsWith("curl ")) {
          skipped++;
          continue;
        }

        const msgId = `matrix:${event.event_id}`;
        if (checkMsg.get(msgId)) { skipped++; continue; }

        let isFromUser =
          event.sender === MATRIX_USER ||
          userPuppets.has(event.sender);

        let senderContactId: number;
        if (isFromUser) {
          senderContactId = userContactId;
        } else if (ghostToContact.has(event.sender)) {
          senderContactId = ghostToContact.get(event.sender)!;
        } else if (isBridgeGhost(event.sender)) {
          const name = await getGhostDisplayName(event.sender);
          if (name && !name.startsWith("gmessages_")) {
            senderContactId = resolveContactByName(db, name, insertContact, nameToContactId);
            ghostToContact.set(event.sender, senderContactId);
          } else if (isUserPuppet(event.sender, name)) {
            // Late-detected user puppet (e.g. membership was "leave")
            userPuppets.add(event.sender);
            senderContactId = userContactId;
            isFromUser = true;
          } else {
            skipped++;
            continue;
          }
        } else {
          skipped++;
          continue;
        }

        if (senderContactId < 0) { skipped++; continue; }

        const dateStr = new Date(event.origin_server_ts).toISOString();
        const normalizedBody = normalizeBody(event.content.body);
        if (checkDupe.get(channelId, dateStr, normalizedBody, senderContactId)) {
          skipped++;
          continue;
        }
        const result = insertMessage.run(channelId, msgId, dateStr, senderContactId, isFromUser ? 1 : 0, "", normalizedBody);
        if (result.changes > 0) {
          updateChannelStats.run(dateStr, dateStr, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
          imported++;
          roomImported++;
        } else {
          skipped++;
        }
      }

      if (roomImported > 0) {
        console.log(`    ${contactNames.join(", ") || roomId}: ${roomImported} messages`);
      }
    }

    // Fix any placeholder dates on new channels
    db.exec(`UPDATE channels SET first_date = (
      SELECT MIN(date) FROM messages WHERE messages.channel_id = channels.id
    ) WHERE first_date = '2099-01-01T00:00:00Z'`);

    db.exec("COMMIT");

    // Save sync token
    if (syncResp.next_batch) {
      saveSyncToken(syncResp.next_batch);
    }
  } else {
    // ── Incremental sync: use /sync with since token ──
    const syncParams: Record<string, string> = {
      timeout: "0",
      since: sinceToken!,
      filter: JSON.stringify({
        room: {
          timeline: { types: ["m.room.message"], limit: 1000 },
          state: { types: ["m.room.member"] },
        },
        presence: { types: [] },
        account_data: { types: [] },
      }),
    };

    console.log(`  Incremental sync from: ${sinceToken!.slice(0, 20)}...`);
    const resp = (await matrixFetch(
      "/_matrix/client/v3/sync",
      syncParams,
    )) as MatrixSyncResponse;

    if (resp.rooms?.join) {
      db.exec("BEGIN");

      for (const [roomId, roomData] of Object.entries(resp.rooms.join)) {
        // Get room members
        const { contacts, userPuppets } = await getRoomMembers(roomId);
        if (contacts.size === 0) continue;

        // Resolve contacts by display name
        const contactIds: number[] = [];
        const contactNames: string[] = [];
        for (const [userId, displayName] of contacts) {
          if (!displayName || displayName.startsWith("gmessages_")) continue;
          const cid = resolveContactByName(db, displayName, insertContact, nameToContactId);
          contactIds.push(cid);
          ghostToContact.set(userId, cid);
          contactNames.push(displayName);
        }
        if (contactIds.length === 0) continue;

        const sortedIds = [...new Set(contactIds)].sort((a, b) => a - b);
        const channelHash = createHash("sha256").update(sortedIds.join("\0")).digest("hex");

        const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
        let channelId: number;
        if (channelRow) {
          channelId = channelRow.id;
        } else {
          const nowStr = new Date().toISOString();
          const result = insertChannel.run(channelHash, JSON.stringify(contactNames), nowStr, nowStr);
          channelId = Number(result.lastInsertRowid);
          for (const cid of sortedIds) insertParticipant.run(channelId, cid);
        }

        const events = roomData.timeline?.events || [];
        for (const event of events) {
          if (event.type !== "m.room.message") continue;
          if (!event.content?.body) continue;
          if (event.content.msgtype !== "m.text") continue;

          const body = event.content.body;
          if (body.startsWith("!gm ") || body === "login" || body.startsWith("curl ")) {
            skipped++;
            continue;
          }

          const msgId = `matrix:${event.event_id}`;
          if (checkMsg.get(msgId)) { skipped++; continue; }

          let isFromUser =
            event.sender === MATRIX_USER ||
            userPuppets.has(event.sender);

          let senderContactId: number;
          if (isFromUser) {
            senderContactId = userContactId;
          } else if (ghostToContact.has(event.sender)) {
            senderContactId = ghostToContact.get(event.sender)!;
          } else if (isBridgeGhost(event.sender)) {
            const name = await getGhostDisplayName(event.sender);
            if (name && !name.startsWith("gmessages_")) {
              senderContactId = resolveContactByName(db, name, insertContact, nameToContactId);
              ghostToContact.set(event.sender, senderContactId);
            } else if (isUserPuppet(event.sender, name)) {
              userPuppets.add(event.sender);
              senderContactId = userContactId;
              isFromUser = true;
            } else {
              skipped++;
              continue;
            }
          } else {
            skipped++;
            continue;
          }

          if (senderContactId < 0) { skipped++; continue; }

          const dateStr = new Date(event.origin_server_ts).toISOString();
          const normalizedBody = normalizeBody(event.content.body);
          if (checkDupe.get(channelId, dateStr, normalizedBody, senderContactId)) {
            skipped++;
            continue;
          }
          const result = insertMessage.run(channelId, msgId, dateStr, senderContactId, isFromUser ? 1 : 0, "", normalizedBody);
          if (result.changes > 0) {
            updateChannelStats.run(dateStr, dateStr, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
            imported++;
          } else {
            skipped++;
          }
        }
      }

      db.exec("COMMIT");
    }

    if (resp.next_batch) {
      saveSyncToken(resp.next_batch);
    }
  }

  return imported;
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  if (!MATRIX_ACCESS_TOKEN) {
    console.error("Error: MATRIX_ACCESS_TOKEN environment variable is required.");
    console.error("");
    console.error("Get a token by logging in:");
    console.error(
      `  curl -s ${MATRIX_HOMESERVER}/_matrix/client/r0/login \\`,
    );
    console.error(
      '    -d \'{"type":"m.login.password","user":"outboxer","password":"YOUR_MATRIX_PASSWORD"}\' | python3 -m json.tool',
    );
    console.error("");
    console.error("Then set: export MATRIX_ACCESS_TOKEN=syt_...");
    process.exit(1);
  }

  const db = openDb();
  console.log(`sync-gmessages — ${DAEMON_MODE ? "daemon" : "one-shot"} mode`);
  console.log(`  Homeserver: ${MATRIX_HOMESERVER}`);
  console.log(`  User: ${MATRIX_USER}`);
  console.log(`  Database: ${DB_PATH}`);
  console.log("");

  if (DAEMON_MODE) {
    console.log(
      `  Polling every ${POLL_INTERVAL_MS / 1000}s — press Ctrl+C to stop`,
    );
    console.log("");
    while (true) {
      try {
        const imported = await syncOnce(db);
        if (imported > 0) {
          console.log(
            `  [${new Date().toISOString()}] Imported ${imported} new messages`,
          );
        }
      } catch (err) {
        console.error(
          `  [${new Date().toISOString()}] Sync error:`,
          (err as Error).message,
        );
      }
      await new Promise((r) => setTimeout(r, POLL_INTERVAL_MS));
    }
  } else {
    try {
      const imported = await syncOnce(db);
      console.log(`  Imported ${imported} new messages`);
    } catch (err) {
      console.error("Sync error:", (err as Error).message);
      process.exit(1);
    }
    db.close();
  }
}

main();
