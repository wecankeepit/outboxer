#!/usr/bin/env -S node --import tsx
/**
 * import-google-chat.ts — Google Chat/Hangouts Takeout → Outboxer DB
 *
 * Imports Google Chat conversations (DMs and Spaces) from a Google Takeout
 * export into the Outboxer database, resolving contacts by email address
 * against the existing contact table and creating channels using the same
 * participant-hash mechanism as the Gmail import.
 *
 * Chat messages are discrete (no quoting/threading), so no diff-stripping
 * is needed — the message text IS the blob.
 *
 * Usage:
 *   node --import tsx src/import-google-chat.ts <takeout-path>
 *   node --import tsx src/import-google-chat.ts ~/Downloads/"Takeout 2"
 */

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync } from "node:fs";
import { join, dirname, basename } from "node:path";
import { homedir } from "node:os";
import { SEED_EMAILS } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths & Config ─────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");

// ─── Date Parsing ────────────────────────────────────────────────────────────

/**
 * Parse Google Chat date format:
 *   "Saturday, October 15, 2016 at 3:16:03 PM UTC"
 * into ISO 8601 string.
 */
function parseChatDate(dateStr: string): string | null {
  if (!dateStr) return null;

  // Remove the day-of-week prefix and " at " keyword
  // "Saturday, October 15, 2016 at 3:16:03 PM UTC"
  // → "October 15, 2016 3:16:03 PM UTC"
  const cleaned = dateStr
    .replace(/^[A-Za-z]+,\s*/, "")  // remove "Saturday, "
    .replace(/\s+at\s+/, " ")        // remove " at "
    .replace(/\s+UTC\s*$/, "");      // remove trailing "UTC"

  // Parse "October 15, 2016 3:16:03 PM"
  const d = new Date(cleaned + " UTC");
  if (isNaN(d.getTime())) {
    console.warn(`  Warning: could not parse date "${dateStr}"`);
    return null;
  }
  return d.toISOString();
}

// ─── Database ────────────────────────────────────────────────────────────────

function openDb(): DB {
  if (!existsSync(DB_PATH)) {
    console.error(`Database not found: ${DB_PATH}`);
    console.error("Run the Gmail import first to set up the database.");
    process.exit(1);
  }
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA foreign_keys = ON");

  // Ensure the source column exists on messages
  try {
    db.exec("ALTER TABLE messages ADD COLUMN source TEXT DEFAULT 'gmail'");
    console.log("  Added 'source' column to messages table.");
  } catch {
    // Column already exists
  }

  return db;
}

/**
 * Detect user contact ID and expand user emails from the database.
 */
function detectUserInfo(db: DB): { userEmails: Set<string>; userContactId: number } {
  const userEmails = new Set(SEED_EMAILS);

  // Find the user contact by matching seed emails
  const findEmail = db.prepare("SELECT contact_id FROM contact_emails WHERE email = ?");
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

  // Expand user emails from all addresses linked to user contact
  if (bestContactId > 0) {
    const emailRows = db
      .prepare("SELECT email FROM contact_emails WHERE contact_id = ?")
      .all(bestContactId) as { email: string }[];
    for (const row of emailRows) userEmails.add(row.email.toLowerCase());
  }

  return { userEmails, userContactId: bestContactId };
}

// ─── Contact Resolution (mirrored from import-gmail-takeout.ts) ──────────────

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

  // No existing contact for this email. Try name-based merge.
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

  // Create new inferred contact
  const result = insertContact.run(name, "inferred");
  const contactId = Number(result.lastInsertRowid);
  insertEmailStmt.run(lower, contactId);
  cache.set(lower, contactId);
  if (isRealName) {
    nameToContactId.set(nameLower, contactId);
  }
  return contactId;
}

// ─── Chat Import Types ───────────────────────────────────────────────────────

interface ChatMember {
  name: string;
  email: string;
  user_type: string;
}

interface GroupInfo {
  name?: string;
  members: ChatMember[];
}

interface ChatMessage {
  creator: ChatMember;
  created_date: string;
  text?: string;
  annotations?: unknown[];
  attached_files?: { original_name: string; export_name: string }[];
  topic_id?: string;
  message_id: string;
}

interface MessagesFile {
  messages: ChatMessage[];
}

// ─── Import Logic ────────────────────────────────────────────────────────────

async function importGoogleChat(takeoutPath: string): Promise<void> {
  const chatDir = join(takeoutPath, "Google Chat", "Groups");
  if (!existsSync(chatDir)) {
    console.error(`Google Chat directory not found: ${chatDir}`);
    console.error("Expected: <takeout-path>/Google Chat/Groups/");
    process.exit(1);
  }

  const db = openDb();
  const { userEmails, userContactId } = detectUserInfo(db);
  console.log(`  User contact ID: ${userContactId}`);
  console.log(`  User emails: ${[...userEmails].join(", ")}`);
  console.log("");

  // Prepared statements
  const findEmail = db.prepare("SELECT contact_id FROM contact_emails WHERE email = ?");
  const insertContact = db.prepare("INSERT INTO contacts (display_name, source) VALUES (?, ?)");
  const insertEmailStmt = db.prepare("INSERT OR IGNORE INTO contact_emails (email, contact_id) VALUES (?, ?)");
  const findChannel = db.prepare("SELECT id FROM channels WHERE participant_hash = ?");
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
     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'gchat')`,
  );

  // Contact resolution caches
  const contactCache = new Map<string, number>();
  for (const email of userEmails) {
    contactCache.set(email.toLowerCase(), userContactId);
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

  // Pre-load existing message IDs for dedup
  const existingMsgIds = new Set<string>();
  const existingRows = db
    .prepare("SELECT message_id FROM messages WHERE message_id IS NOT NULL")
    .all() as { message_id: string }[];
  for (const row of existingRows) existingMsgIds.add(row.message_id);
  console.log(`  Existing messages in DB: ${existingMsgIds.size}`);

  // Enumerate conversation directories
  const conversations = readdirSync(chatDir).filter((d) => {
    const full = join(chatDir, d);
    return existsSync(join(full, "messages.json")) && existsSync(join(full, "group_info.json"));
  });

  console.log(`  Found ${conversations.length} conversations with data`);
  console.log("");

  let totalImported = 0;
  let totalSkipped = 0;
  let totalConversations = 0;
  let emptyConversations = 0;

  const startTime = Date.now();

  db.exec("BEGIN");

  for (const convDir of conversations) {
    const convPath = join(chatDir, convDir);
    const groupInfo: GroupInfo = JSON.parse(readFileSync(join(convPath, "group_info.json"), "utf-8"));
    const messagesFile: MessagesFile = JSON.parse(readFileSync(join(convPath, "messages.json"), "utf-8"));

    if (messagesFile.messages.length === 0) {
      emptyConversations++;
      continue;
    }

    // Resolve non-user members to contacts
    const participantContactIds: number[] = [];
    const participantNames: string[] = [];

    for (const member of groupInfo.members) {
      const memberEmail = member.email.toLowerCase().trim();
      if (userEmails.has(memberEmail)) continue;

      const contactId = resolveContact(
        db,
        memberEmail,
        member.name,
        findEmail,
        insertContact,
        insertEmailStmt,
        contactCache,
        nameToContactId,
      );
      participantContactIds.push(contactId);
      // Get display name from DB (may differ from group_info if already resolved)
      const c = db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(contactId) as { display_name: string } | undefined;
      if (c) participantNames.push(c.display_name);
    }

    // Skip conversations with no external participants (self-chat)
    if (participantContactIds.length === 0) {
      totalSkipped += messagesFile.messages.length;
      continue;
    }

    // Channel key: sorted contact IDs hash (same as Gmail pipeline)
    const sortedIds = [...new Set(participantContactIds)].sort((a, b) => a - b);
    const channelHash = createHash("sha256")
      .update(sortedIds.join("\0"))
      .digest("hex");

    // Find or create channel
    const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
    let channelId: number;

    if (channelRow) {
      channelId = channelRow.id;
      // Update participant names to include all names (email channels might have had different names)
      // We don't overwrite — the existing names are fine
    } else {
      const firstDate = parseChatDate(messagesFile.messages[0]?.created_date || "") || "2000-01-01T00:00:00Z";
      const lastDate = parseChatDate(messagesFile.messages[messagesFile.messages.length - 1]?.created_date || "") || "2000-01-01T00:00:00Z";
      const result = insertChannel.run(
        channelHash,
        JSON.stringify(participantNames),
        firstDate,
        lastDate,
      );
      channelId = Number(result.lastInsertRowid);

      for (const cid of sortedIds) {
        insertParticipant.run(channelId, cid);
      }
    }

    // Insert messages
    let conversationImported = 0;
    for (const msg of messagesFile.messages) {
      if (!msg.creator || !msg.created_date) continue;

      // Build a unique message_id with gchat prefix
      const msgId = `gchat:${msg.message_id}`;
      if (existingMsgIds.has(msgId)) {
        totalSkipped++;
        continue;
      }

      const creatorEmail = msg.creator.email.toLowerCase().trim();
      const isFromUser = userEmails.has(creatorEmail);

      // Resolve sender contact
      const senderContactId = userEmails.has(creatorEmail)
        ? userContactId
        : resolveContact(
            db,
            creatorEmail,
            msg.creator.name,
            findEmail,
            insertContact,
            insertEmailStmt,
            contactCache,
            nameToContactId,
          );

      const dateStr = parseChatDate(msg.created_date);
      if (!dateStr) {
        totalSkipped++;
        continue;
      }

      // Build the message text
      let text = msg.text || "";
      // If there are attached files but no text, note the attachment
      if (!text && msg.attached_files && msg.attached_files.length > 0) {
        text = `[${msg.attached_files.map((f) => f.original_name).join(", ")}]`;
      }
      // Skip completely empty messages
      if (!text) {
        totalSkipped++;
        continue;
      }

      // Insert message — for chat, blob = text directly (no diff-stripping needed)
      insertMessage.run(
        channelId,
        msgId,
        dateStr,
        senderContactId,
        isFromUser ? 1 : 0,
        "", // no subject for chat messages
        text,
      );

      // Update channel stats
      updateChannelStats.run(
        dateStr,
        dateStr,
        isFromUser ? 1 : 0,
        isFromUser ? 0 : 1,
        channelId,
      );

      existingMsgIds.add(msgId);
      conversationImported++;
      totalImported++;
    }

    if (conversationImported > 0) {
      totalConversations++;
      const isDM = convDir.startsWith("DM ");
      const label = isDM ? "DM" : "Space";
      const convName = groupInfo.name || participantNames.join(", ");
      console.log(`  ${label}: ${convName} — ${conversationImported} messages imported`);
    }
  }

  db.exec("COMMIT");

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("");
  console.log(`═══ Google Chat import complete in ${elapsed}s ═══`);
  console.log(`  Conversations with messages: ${totalConversations}`);
  console.log(`  Empty conversations skipped: ${emptyConversations}`);
  console.log(`  Messages imported: ${totalImported}`);
  console.log(`  Messages skipped (dupes/empty): ${totalSkipped}`);
  console.log(`  Total messages in DB: ${(db.prepare("SELECT COUNT(*) as c FROM messages").get() as { c: number }).c}`);
  console.log(`  Total channels in DB: ${(db.prepare("SELECT COUNT(*) as c FROM channels").get() as { c: number }).c}`);

  db.close();
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

const takeoutPath = process.argv[2];
if (!takeoutPath) {
  console.log("import-google-chat — Import Google Chat from Takeout into Outboxer");
  console.log("");
  console.log("Usage:");
  console.log('  node --import tsx src/import-google-chat.ts <takeout-path>');
  console.log("");
  console.log("Example:");
  console.log('  node --import tsx src/import-google-chat.ts ~/Downloads/"Takeout 2"');
  process.exit(0);
}

if (!existsSync(takeoutPath)) {
  console.error(`Takeout path not found: ${takeoutPath}`);
  process.exit(1);
}

console.log(`Importing Google Chat from: ${takeoutPath}`);
console.log(`Database: ${DB_PATH}`);
console.log("");

importGoogleChat(takeoutPath).catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
