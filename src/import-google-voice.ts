#!/usr/bin/env -S node --import tsx
/**
 * import-google-voice.ts — Google Voice Takeout → Outboxer DB
 *
 * Imports Google Voice text message conversations from Google Takeout HTML
 * exports into the Outboxer database. Resolves contacts by phone number
 * (with fallback to name matching against existing contacts), and creates
 * channels using the same participant-hash mechanism as Gmail/Chat imports.
 *
 * Only "Text" files are imported — calls, voicemails, and missed calls are
 * skipped per the Outboxer philosophy of focusing on written natural language.
 *
 * Usage:
 *   node --import tsx src/import-google-voice.ts <takeout-path> [<takeout-path-2> ...]
 *   node --import tsx src/import-google-voice.ts ~/Downloads/"Takeout 2" ~/Downloads/"Takeout 3"
 */

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join, basename } from "node:path";
import { homedir } from "node:os";
import { SEED_EMAILS } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths & Config ─────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");

// User's Google Voice phone numbers (from Phones.vcf)
const USER_PHONES = new Set([
  "+15551000001", // Google Voice
  "+15551000002", // Cell
  "+15551000003", // Cell
]);

// ─── HTML Parsing ────────────────────────────────────────────────────────────

interface VoiceMessage {
  date: string;       // ISO 8601
  senderPhone: string;
  senderName: string;
  isFromUser: boolean;
  text: string;
}

interface VoiceConversation {
  participants: { phone: string; name: string }[];
  messages: VoiceMessage[];
  fileName: string;
}

/**
 * Normalize a phone number to E.164 format (+1XXXXXXXXXX).
 * Strips spaces, dashes, parens, and ensures +1 prefix for US numbers.
 */
function normalizePhone(phone: string): string {
  // Strip everything except digits and leading +
  let cleaned = phone.replace(/[^\d+]/g, "");
  // If no +, assume US number
  if (!cleaned.startsWith("+")) {
    if (cleaned.length === 10) cleaned = "+1" + cleaned;
    else if (cleaned.length === 11 && cleaned.startsWith("1")) cleaned = "+" + cleaned;
  }
  return cleaned;
}

/**
 * Parse a Google Voice HTML text file into structured data.
 *
 * The HTML structure is:
 *   <div class="hChatLog hfeed">
 *     [<div class="participants">Group conversation with: ... </div>]
 *     <div class="message">
 *       <abbr class="dt" title="2026-01-10T22:02:22.670-05:00">...</abbr>:
 *       <cite class="sender vcard"><a class="tel" href="tel:+1..."><span class="fn">Name</span></a></cite>:
 *       <q>Message text</q>
 *     </div>
 *   </div>
 */
function parseVoiceHtml(html: string, fileName: string): VoiceConversation {
  const participants: { phone: string; name: string }[] = [];
  const messages: VoiceMessage[] = [];

  // Extract participants from group conversations
  const participantsMatch = html.match(/<div class="participants">([\s\S]*?)<\/div>/);
  if (participantsMatch) {
    const pBlock = participantsMatch[1]!;
    const phoneRegex = /<a class="tel" href="tel:([^"]+)"><span class="fn">([^<]*)<\/span><\/a>/g;
    const abbrRegex = /<a class="tel" href="tel:([^"]+)"><abbr class="fn"[^>]*>([^<]*)<\/abbr><\/a>/g;
    let m;
    while ((m = phoneRegex.exec(pBlock)) !== null) {
      const phone = normalizePhone(m[1]!);
      const name = m[2]! || phone;
      if (!USER_PHONES.has(phone)) {
        participants.push({ phone, name });
      }
    }
    while ((m = abbrRegex.exec(pBlock)) !== null) {
      const phone = normalizePhone(m[1]!);
      const name = m[2]! || phone;
      if (!USER_PHONES.has(phone)) {
        participants.push({ phone, name });
      }
    }
  }

  // Extract messages
  const msgRegex = /<div class="message">([\s\S]*?)<\/div>/g;
  let msgMatch;
  while ((msgMatch = msgRegex.exec(html)) !== null) {
    const block = msgMatch[1]!;

    // Date: <abbr class="dt" title="ISO_DATE">
    const dateMatch = block.match(/<abbr class="dt" title="([^"]+)">/);
    if (!dateMatch) continue;
    const rawDate = dateMatch[1]!;
    // Parse the ISO date with timezone offset
    const d = new Date(rawDate);
    if (isNaN(d.getTime())) continue; // Skip messages with unparseable dates
    const date = d.toISOString();

    // Sender phone: <a class="tel" href="tel:+1...">
    const phoneMatch = block.match(/<a class="tel" href="tel:([^"]+)">/);
    if (!phoneMatch) continue;
    const senderPhone = normalizePhone(phoneMatch[1]!);

    // Sender name: <span class="fn">Name</span> or <abbr class="fn" title="">Me</abbr>
    let senderName = "";
    const nameMatch = block.match(/<span class="fn">([^<]*)<\/span>/);
    const abbrNameMatch = block.match(/<abbr class="fn"[^>]*>([^<]*)<\/abbr>/);
    if (nameMatch) senderName = nameMatch[1]!;
    else if (abbrNameMatch) senderName = abbrNameMatch[1]!;

    const isFromUser = USER_PHONES.has(senderPhone) || senderName === "Me";

    // Message text: <q>...</q>
    const textMatch = block.match(/<q>([\s\S]*?)<\/q>/);
    if (!textMatch) continue;
    let text = textMatch[1]!;
    // Convert <br> and <br/> to newlines, strip remaining HTML tags, decode entities
    text = text
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<[^>]+>/g, "")
      .replace(/&#(\d+);/g, (_, num) => String.fromCodePoint(parseInt(num, 10)))
      .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) => String.fromCodePoint(parseInt(hex, 16)))
      .replace(/&amp;/g, "&")
      .replace(/&lt;/g, "<")
      .replace(/&gt;/g, ">")
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&nbsp;/g, " ")
      .replace(/&#8239;/g, " ") // narrow no-break space
      .trim();

    if (!text) continue;

    messages.push({ date, senderPhone, senderName, isFromUser, text });

    // If this sender is not the user and not in participants yet, add them
    if (!isFromUser && !participants.some((p) => p.phone === senderPhone)) {
      participants.push({
        phone: senderPhone,
        name: senderName || senderPhone,
      });
    }
  }

  // For DMs (no participants block), infer participants from message senders
  // (already handled above by adding non-user senders to participants)

  return { participants, messages, fileName };
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

  // Ensure source column exists on messages
  try { db.exec("ALTER TABLE messages ADD COLUMN source TEXT DEFAULT 'gmail'"); } catch { /* exists */ }

  // Create contact_phones table for phone-based identity resolution
  db.exec(`CREATE TABLE IF NOT EXISTS contact_phones (
    phone      TEXT PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_contact_phones_contact ON contact_phones(contact_id)`);

  return db;
}

/**
 * Detect user contact ID from the database.
 */
function detectUserContactId(db: DB): number {
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
  return bestContactId;
}

/**
 * Resolve a phone number to a contact ID.
 *
 * Strategy:
 *   1. Check contact_phones table (existing mapping)
 *   2. If has a name, check for existing contact with same display_name
 *   3. Create new inferred contact if no match
 */
function resolveContactByPhone(
  db: DB,
  phone: string,
  displayName: string,
  findPhone: ReturnType<DB["prepare"]>,
  insertContact: ReturnType<DB["prepare"]>,
  insertPhone: ReturnType<DB["prepare"]>,
  phoneCache: Map<string, number>,
  nameToContactId: Map<string, number>,
): number {
  const normalPhone = normalizePhone(phone);

  // Check cache first
  const cached = phoneCache.get(normalPhone);
  if (cached !== undefined) return cached;

  // Check database
  const row = findPhone.get(normalPhone) as { contact_id: number } | undefined;
  if (row) {
    phoneCache.set(normalPhone, row.contact_id);
    return row.contact_id;
  }

  // Try name-based match against existing contacts
  const name = displayName && displayName !== normalPhone ? displayName : normalPhone;
  const nameLower = name.toLowerCase().trim();
  const isRealName = !nameLower.startsWith("+") && nameLower.length >= 3;

  if (isRealName) {
    const existingId = nameToContactId.get(nameLower);
    if (existingId !== undefined) {
      // Found existing contact by name — add phone to them
      insertPhone.run(normalPhone, existingId);
      phoneCache.set(normalPhone, existingId);
      return existingId;
    }
  }

  // Create new inferred contact
  const result = insertContact.run(name, "inferred");
  const contactId = Number(result.lastInsertRowid);
  insertPhone.run(normalPhone, contactId);
  phoneCache.set(normalPhone, contactId);
  if (isRealName) {
    nameToContactId.set(nameLower, contactId);
  }
  return contactId;
}

// ─── Import Logic ────────────────────────────────────────────────────────────

async function importGoogleVoice(takeoutPaths: string[]): Promise<void> {
  const db = openDb();
  const userContactId = detectUserContactId(db);
  console.log(`  User contact ID: ${userContactId}`);
  console.log("");

  // Register user's phone numbers in contact_phones
  const insertPhoneIfNew = db.prepare("INSERT OR IGNORE INTO contact_phones (phone, contact_id) VALUES (?, ?)");
  for (const phone of USER_PHONES) {
    insertPhoneIfNew.run(phone, userContactId);
  }

  // Prepared statements
  const findPhone = db.prepare("SELECT contact_id FROM contact_phones WHERE phone = ?");
  const insertContact = db.prepare("INSERT INTO contacts (display_name, source) VALUES (?, ?)");
  const insertPhoneStmt = db.prepare("INSERT OR IGNORE INTO contact_phones (phone, contact_id) VALUES (?, ?)");
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
     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'gvoice')`,
  );

  // Phone cache
  const phoneCache = new Map<string, number>();
  for (const phone of USER_PHONES) {
    phoneCache.set(phone, userContactId);
  }

  // Name → contact ID for matching (pre-populate from existing contacts)
  const nameToContactId = new Map<string, number>();
  const existingContacts = db
    .prepare("SELECT id, LOWER(display_name) as name FROM contacts")
    .all() as { id: number; name: string }[];
  for (const row of existingContacts) {
    if (row.name.length >= 3 && !row.name.startsWith("+")) {
      nameToContactId.set(row.name, row.id);
    }
  }

  // Pre-load existing message IDs for dedup
  const existingMsgIds = new Set<string>();
  const existingRows = db
    .prepare("SELECT message_id FROM messages WHERE message_id IS NOT NULL AND source = 'gvoice'")
    .all() as { message_id: string }[];
  for (const row of existingRows) existingMsgIds.add(row.message_id);
  console.log(`  Existing Voice messages in DB: ${existingMsgIds.size}`);

  let totalFiles = 0;
  let totalImported = 0;
  let totalSkipped = 0;
  let totalConversations = 0;

  const startTime = Date.now();

  db.exec("BEGIN");

  for (const takeoutPath of takeoutPaths) {
    const voiceDir = join(takeoutPath, "Voice", "Calls");
    if (!existsSync(voiceDir)) {
      console.log(`  Skipping ${takeoutPath}: no Voice/Calls directory`);
      continue;
    }

    console.log(`  Processing: ${takeoutPath}`);

    // Find all Text HTML files
    const textFiles = readdirSync(voiceDir).filter(
      (f) => f.includes(" - Text - ") && f.endsWith(".html"),
    );

    console.log(`  Found ${textFiles.length} text conversation files`);

    for (const fileName of textFiles) {
      totalFiles++;
      const filePath = join(voiceDir, fileName);
      const html = readFileSync(filePath, "utf-8");
      const conv = parseVoiceHtml(html, fileName);

      if (conv.messages.length === 0 || conv.participants.length === 0) {
        continue;
      }

      // Resolve participants to contact IDs
      const participantContactIds: number[] = [];
      const participantNames: string[] = [];

      for (const p of conv.participants) {
        const contactId = resolveContactByPhone(
          db,
          p.phone,
          p.name,
          findPhone,
          insertContact,
          insertPhoneStmt,
          phoneCache,
          nameToContactId,
        );
        participantContactIds.push(contactId);
        const c = db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(contactId) as { display_name: string } | undefined;
        if (c) participantNames.push(c.display_name);
      }

      if (participantContactIds.length === 0) continue;

      // Channel key: sorted contact IDs hash
      const sortedIds = [...new Set(participantContactIds)].sort((a, b) => a - b);
      const channelHash = createHash("sha256")
        .update(sortedIds.join("\0"))
        .digest("hex");

      // Find or create channel
      const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
      let channelId: number;

      if (channelRow) {
        channelId = channelRow.id;
      } else {
        const firstDate = conv.messages[0]?.date || "2000-01-01T00:00:00Z";
        const lastDate = conv.messages[conv.messages.length - 1]?.date || "2000-01-01T00:00:00Z";
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
      let fileImported = 0;
      for (const msg of conv.messages) {
        // Build unique message ID: gvoice:{senderPhone}:{date}
        const msgId = `gvoice:${msg.senderPhone}:${msg.date}`;
        if (existingMsgIds.has(msgId)) {
          totalSkipped++;
          continue;
        }

        const senderContactId = msg.isFromUser
          ? userContactId
          : resolveContactByPhone(
              db,
              msg.senderPhone,
              msg.senderName,
              findPhone,
              insertContact,
              insertPhoneStmt,
              phoneCache,
              nameToContactId,
            );

        insertMessage.run(
          channelId,
          msgId,
          msg.date,
          senderContactId,
          msg.isFromUser ? 1 : 0,
          "", // no subject for text messages
          msg.text,
        );

        updateChannelStats.run(
          msg.date,
          msg.date,
          msg.isFromUser ? 1 : 0,
          msg.isFromUser ? 0 : 1,
          channelId,
        );

        existingMsgIds.add(msgId);
        fileImported++;
        totalImported++;
      }

      if (fileImported > 0) totalConversations++;

      // Periodic progress
      if (totalFiles % 500 === 0) {
        db.exec("COMMIT");
        db.exec("BEGIN");
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`    [${elapsed}s] ${totalFiles} files, ${totalImported} imported, ${totalSkipped} skipped`);
      }
    }
  }

  db.exec("COMMIT");

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("");
  console.log(`═══ Google Voice import complete in ${elapsed}s ═══`);
  console.log(`  Files processed: ${totalFiles}`);
  console.log(`  Conversations with new messages: ${totalConversations}`);
  console.log(`  Messages imported: ${totalImported}`);
  console.log(`  Messages skipped (dupes): ${totalSkipped}`);
  console.log(`  Total messages in DB: ${(db.prepare("SELECT COUNT(*) as c FROM messages").get() as { c: number }).c}`);
  console.log(`  Total channels in DB: ${(db.prepare("SELECT COUNT(*) as c FROM channels").get() as { c: number }).c}`);

  // Show contact_phones stats
  const phoneCount = (db.prepare("SELECT COUNT(*) as c FROM contact_phones").get() as { c: number }).c;
  console.log(`  Phone numbers mapped: ${phoneCount}`);

  db.close();
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

const takeoutPaths = process.argv.slice(2);
if (takeoutPaths.length === 0) {
  console.log("import-google-voice — Import Google Voice texts from Takeout into Outboxer");
  console.log("");
  console.log("Usage:");
  console.log('  node --import tsx src/import-google-voice.ts <takeout-path> [<takeout-path-2> ...]');
  console.log("");
  console.log("Example:");
  console.log('  node --import tsx src/import-google-voice.ts ~/Downloads/"Takeout 2" ~/Downloads/"Takeout 3"');
  process.exit(0);
}

for (const p of takeoutPaths) {
  if (!existsSync(p)) {
    console.error(`Takeout path not found: ${p}`);
    process.exit(1);
  }
}

console.log("Importing Google Voice texts from:");
for (const p of takeoutPaths) console.log(`  ${p}`);
console.log(`Database: ${DB_PATH}`);
console.log("");

importGoogleVoice(takeoutPaths).catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
