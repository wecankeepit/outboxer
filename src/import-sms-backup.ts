#!/usr/bin/env -S node --import tsx
/**
 * import-sms-backup.ts — SMS Backup & Restore XML → Outboxer DB
 *
 * Imports SMS and MMS messages from the "SMS Backup & Restore" Android app's
 * XML export into the Outboxer database. Resolves contacts by phone number
 * using the same contact_phones table as the Voice importer, and creates
 * channels using the participant-hash mechanism.
 *
 * Both SMS (<sms>) and MMS (<mms>) elements are processed:
 *   - SMS: type=1 received, type=2 sent; body is in the `body` attribute
 *   - MMS: msg_box=1 received, msg_box=2 sent; text in <part ct="text/plain">
 *          child elements; participants in <addr> children (type 137=sender,
 *          151=recipient/to)
 *
 * Source tag: 'gmessages' (Google Messages / RCS / SMS)
 *
 * Usage:
 *   node --import tsx src/import-sms-backup.ts <xml-file>
 *   node --import tsx src/import-sms-backup.ts ~/Downloads/sms-20260214094015.xml
 */

import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { SEED_EMAILS } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths & Config ─────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");

// User's phone numbers (same set as Voice importer + current Verizon number)
const USER_PHONES = new Set([
  "+15551000001", // Google Voice
  "+15551000002", // Cell (old)
  "+15551000003", // Cell (current Verizon)
]);

// ─── Phone Normalization ─────────────────────────────────────────────────────

function normalizePhone(phone: string): string {
  let cleaned = phone.replace(/[^\d+]/g, "");
  if (!cleaned.startsWith("+")) {
    if (cleaned.length === 10) cleaned = "+1" + cleaned;
    else if (cleaned.length === 11 && cleaned.startsWith("1")) cleaned = "+" + cleaned;
  }
  return cleaned;
}

// ─── XML Parsing (streaming regex — avoids loading 209MB DOM) ────────────────

interface SmsMessage {
  address: string;
  date: string; // ISO 8601
  type: number; // 1=recv, 2=sent
  body: string;
  contactName: string;
}

interface MmsMessage {
  date: string; // ISO 8601
  msgBox: number; // 1=recv, 2=sent
  body: string;
  contactName: string;
  senderPhone: string;
  recipientPhones: string[];
}

/**
 * Parse the XML file using streaming line-by-line approach.
 * SMS elements are self-closing single lines.
 * MMS elements span multiple lines with child <part> and <addr> elements.
 */
function* parseSmsXml(xmlPath: string): Generator<SmsMessage | MmsMessage> {
  const content = readFileSync(xmlPath, "utf-8");
  const lines = content.split("\n");

  let inMms = false;
  let currentMms: {
    date: string;
    msgBox: number;
    contactName: string;
    textParts: string[];
    senderPhone: string;
    recipientPhones: string[];
  } | null = null;

  for (const line of lines) {
    const trimmed = line.trim();

    // ── SMS (single-line self-closing) ──
    if (trimmed.startsWith("<sms ")) {
      const address = attr(trimmed, "address");
      const dateMs = attr(trimmed, "date");
      const type = parseInt(attr(trimmed, "type") || "0", 10);
      const body = attr(trimmed, "body");
      const contactName = attr(trimmed, "contact_name");

      // Skip drafts, outbox, failed, queued (only want recv=1 and sent=2)
      if (type !== 1 && type !== 2) continue;
      if (!body || !dateMs) continue;

      const d = new Date(parseInt(dateMs, 10));
      if (isNaN(d.getTime())) continue;

      yield {
        address: normalizePhone(address),
        date: d.toISOString(),
        type,
        body,
        contactName: contactName === "(Unknown)" ? "" : contactName,
      } as SmsMessage;
      continue;
    }

    // ── MMS (multi-line with children) ──
    if (trimmed.startsWith("<mms ")) {
      const dateMs = attr(trimmed, "date");
      const msgBox = parseInt(attr(trimmed, "msg_box") || "0", 10);
      const contactName = attr(trimmed, "contact_name");

      if (msgBox !== 1 && msgBox !== 2) continue;
      if (!dateMs) continue;

      const d = new Date(parseInt(dateMs, 10));
      if (isNaN(d.getTime())) continue;

      inMms = true;
      currentMms = {
        date: d.toISOString(),
        msgBox,
        contactName: contactName === "(Unknown)" ? "" : contactName,
        textParts: [],
        senderPhone: "",
        recipientPhones: [],
      };

      // Self-closing MMS (no children)?
      if (trimmed.endsWith("/>")) {
        inMms = false;
        // MMS with no parts has no text — skip
        continue;
      }
      continue;
    }

    if (inMms && currentMms) {
      // <part> elements
      if (trimmed.startsWith("<part ")) {
        const ct = attr(trimmed, "ct");
        if (ct === "text/plain") {
          const text = attr(trimmed, "text");
          if (text && text !== "null") {
            currentMms.textParts.push(text);
          }
        }
        continue;
      }

      // <addr> elements
      if (trimmed.startsWith("<addr ")) {
        const addrPhone = normalizePhone(attr(trimmed, "address"));
        const addrType = attr(trimmed, "type");
        if (addrType === "137") {
          // Sender (from)
          currentMms.senderPhone = addrPhone;
        } else if (addrType === "151" || addrType === "130") {
          // Recipient (to) or CC
          if (!USER_PHONES.has(addrPhone) && addrPhone !== currentMms.senderPhone) {
            currentMms.recipientPhones.push(addrPhone);
          }
        }
        continue;
      }

      // Closing </mms>
      if (trimmed === "</mms>") {
        inMms = false;

        // Build text body
        let body = currentMms.textParts.join("\n").trim();
        if (!body) body = "[Image/Media]";

        // Determine sender and recipients
        const senderPhone = currentMms.senderPhone;
        const isFromUser = USER_PHONES.has(senderPhone);

        // Build list of non-user participant phones
        const participantPhones: string[] = [];
        if (!isFromUser && senderPhone) {
          participantPhones.push(senderPhone);
        }
        for (const rp of currentMms.recipientPhones) {
          if (!USER_PHONES.has(rp) && !participantPhones.includes(rp)) {
            participantPhones.push(rp);
          }
        }

        if (participantPhones.length === 0) {
          currentMms = null;
          continue;
        }

        yield {
          date: currentMms.date,
          msgBox: currentMms.msgBox,
          body,
          contactName: currentMms.contactName,
          senderPhone: isFromUser ? senderPhone : senderPhone,
          recipientPhones: participantPhones,
        } as MmsMessage;

        currentMms = null;
        continue;
      }
    }
  }
}

/**
 * Extract an XML attribute value. Handles both single and double quotes,
 * and decodes basic XML entities.
 */
function attr(line: string, name: string): string {
  // Try double-quoted first, then single-quoted
  const dqRegex = new RegExp(`${name}="([^"]*)"`, "s");
  const sqRegex = new RegExp(`${name}='([^']*)'`, "s");
  const dqMatch = line.match(dqRegex);
  const sqMatch = line.match(sqRegex);
  const raw = dqMatch?.[1] ?? sqMatch?.[1] ?? "";
  return raw
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#10;/g, "\n")
    .replace(/&#13;/g, "\r");
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
  try { db.exec("ALTER TABLE messages ADD COLUMN source TEXT DEFAULT 'gmail'"); } catch { /* exists */ }
  db.exec(`CREATE TABLE IF NOT EXISTS contact_phones (
    phone      TEXT PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id)
  )`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_contact_phones_contact ON contact_phones(contact_id)`);
  return db;
}

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

function resolveContactByPhone(
  db: DB,
  phone: string,
  displayName: string,
  findPhone: ReturnType<DB["prepare"]>,
  insertContact: ReturnType<DB["prepare"]>,
  insertPhoneStmt: ReturnType<DB["prepare"]>,
  phoneCache: Map<string, number>,
  nameToContactId: Map<string, number>,
): number {
  const normalPhone = normalizePhone(phone);
  const cached = phoneCache.get(normalPhone);
  if (cached !== undefined) return cached;

  const row = findPhone.get(normalPhone) as { contact_id: number } | undefined;
  if (row) {
    phoneCache.set(normalPhone, row.contact_id);
    return row.contact_id;
  }

  const name = displayName && displayName !== normalPhone ? displayName : normalPhone;
  const nameLower = name.toLowerCase().trim();
  const isRealName = !nameLower.startsWith("+") && nameLower.length >= 3;

  if (isRealName) {
    const existingId = nameToContactId.get(nameLower);
    if (existingId !== undefined) {
      insertPhoneStmt.run(normalPhone, existingId);
      phoneCache.set(normalPhone, existingId);
      return existingId;
    }
  }

  const result = insertContact.run(name, "inferred");
  const contactId = Number(result.lastInsertRowid);
  insertPhoneStmt.run(normalPhone, contactId);
  phoneCache.set(normalPhone, contactId);
  if (isRealName) nameToContactId.set(nameLower, contactId);
  return contactId;
}

// ─── Import Logic ────────────────────────────────────────────────────────────

function importSmsBackup(xmlPath: string): void {
  const db = openDb();
  const userContactId = detectUserContactId(db);
  console.log(`  User contact ID: ${userContactId}`);

  // Register user phones
  const insertPhoneIfNew = db.prepare("INSERT OR IGNORE INTO contact_phones (phone, contact_id) VALUES (?, ?)");
  for (const phone of USER_PHONES) insertPhoneIfNew.run(phone, userContactId);

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
     VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 'gmessages')`,
  );

  // Caches
  const phoneCache = new Map<string, number>();
  for (const phone of USER_PHONES) phoneCache.set(phone, userContactId);

  const nameToContactId = new Map<string, number>();
  const existingContacts = db
    .prepare("SELECT id, LOWER(display_name) as name FROM contacts")
    .all() as { id: number; name: string }[];
  for (const row of existingContacts) {
    if (row.name.length >= 3 && !row.name.startsWith("+")) {
      nameToContactId.set(row.name, row.id);
    }
  }

  // Dedup by message_id
  const existingMsgIds = new Set<string>();
  const existingRows = db
    .prepare("SELECT message_id FROM messages WHERE source = 'gmessages'")
    .all() as { message_id: string }[];
  for (const row of existingRows) existingMsgIds.add(row.message_id);
  console.log(`  Existing Messages texts in DB: ${existingMsgIds.size}`);

  // Cross-source dedup: skip if mautrix bridge already has the same message
  // Uses 1-second tolerance because SMS backup has whole-second timestamps
  // while mautrix has millisecond precision. Strips trailing whitespace + newlines.
  const checkCrossSourceDupe = db.prepare(
    `SELECT 1 FROM messages
     WHERE message_id LIKE 'matrix:%'
       AND ABS(julianday(date) - julianday(?)) < (1.0/86400.0)
       AND REPLACE(REPLACE(TRIM(blob), char(10), ''), char(13), '')
         = REPLACE(REPLACE(TRIM(?), char(10), ''), char(13), '')
       AND sender_contact_id = ?`,
  );

  let totalImported = 0;
  let totalSkipped = 0;
  let totalSms = 0;
  let totalMms = 0;

  const startTime = Date.now();

  db.exec("BEGIN");

  for (const msg of parseSmsXml(xmlPath)) {
    if ("type" in msg) {
      // ── SMS message ──
      totalSms++;
      const smsMsg = msg as SmsMessage;
      const isFromUser = smsMsg.type === 2;
      const phone = smsMsg.address;

      if (!phone || phone.length < 4) { totalSkipped++; continue; }

      const contactId = resolveContactByPhone(
        db, phone, smsMsg.contactName, findPhone, insertContact,
        insertPhoneStmt, phoneCache, nameToContactId,
      );

      const sortedIds = [contactId];
      const channelHash = createHash("sha256").update(sortedIds.join("\0")).digest("hex");

      const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
      let channelId: number;
      if (channelRow) {
        channelId = channelRow.id;
      } else {
        const cName = db.prepare("SELECT display_name FROM contacts WHERE id = ?")
          .get(contactId) as { display_name: string } | undefined;
        const result = insertChannel.run(
          channelHash, JSON.stringify([cName?.display_name || phone]),
          smsMsg.date, smsMsg.date,
        );
        channelId = Number(result.lastInsertRowid);
        insertParticipant.run(channelId, contactId);
      }

      const msgId = `gmessages:sms:${phone}:${smsMsg.date}`;
      if (existingMsgIds.has(msgId)) { totalSkipped++; continue; }

      const senderContactId = isFromUser ? userContactId : contactId;

      // Cross-source dedup: skip if mautrix bridge already has this message
      if (checkCrossSourceDupe.get(smsMsg.date, smsMsg.body, senderContactId)) { totalSkipped++; continue; }

      insertMessage.run(channelId, msgId, smsMsg.date, senderContactId, isFromUser ? 1 : 0, "", smsMsg.body);
      updateChannelStats.run(smsMsg.date, smsMsg.date, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
      existingMsgIds.add(msgId);
      totalImported++;

    } else {
      // ── MMS message ──
      totalMms++;
      const mmsMsg = msg as MmsMessage;
      const isFromUser = mmsMsg.msgBox === 2;

      // Resolve all non-user participants
      const participantContactIds: number[] = [];
      const participantNames: string[] = [];

      // Parse contact_name for group names (comma-separated)
      const nameHints = mmsMsg.contactName.split(",").map((n) => n.trim()).filter(Boolean);

      for (let i = 0; i < mmsMsg.recipientPhones.length; i++) {
        const rp = mmsMsg.recipientPhones[i]!;
        const hint = nameHints[i] || "";
        const contactId = resolveContactByPhone(
          db, rp, hint, findPhone, insertContact,
          insertPhoneStmt, phoneCache, nameToContactId,
        );
        participantContactIds.push(contactId);
        const c = db.prepare("SELECT display_name FROM contacts WHERE id = ?")
          .get(contactId) as { display_name: string } | undefined;
        if (c) participantNames.push(c.display_name);
      }

      if (participantContactIds.length === 0) { totalSkipped++; continue; }

      const sortedIds = [...new Set(participantContactIds)].sort((a, b) => a - b);
      const channelHash = createHash("sha256").update(sortedIds.join("\0")).digest("hex");

      const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
      let channelId: number;
      if (channelRow) {
        channelId = channelRow.id;
      } else {
        const result = insertChannel.run(
          channelHash, JSON.stringify(participantNames),
          mmsMsg.date, mmsMsg.date,
        );
        channelId = Number(result.lastInsertRowid);
        for (const cid of sortedIds) insertParticipant.run(channelId, cid);
      }

      // Determine sender contact
      const senderContactId = isFromUser
        ? userContactId
        : resolveContactByPhone(
            db, mmsMsg.senderPhone, "",
            findPhone, insertContact, insertPhoneStmt, phoneCache, nameToContactId,
          );

      const msgId = `gmessages:mms:${mmsMsg.senderPhone}:${mmsMsg.date}`;
      if (existingMsgIds.has(msgId)) { totalSkipped++; continue; }

      // Cross-source dedup: skip if mautrix bridge already has this message
      if (checkCrossSourceDupe.get(mmsMsg.date, mmsMsg.body, senderContactId)) { totalSkipped++; continue; }

      insertMessage.run(channelId, msgId, mmsMsg.date, senderContactId, isFromUser ? 1 : 0, "", mmsMsg.body);
      updateChannelStats.run(mmsMsg.date, mmsMsg.date, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
      existingMsgIds.add(msgId);
      totalImported++;
    }

    // Periodic commit
    if ((totalSms + totalMms) % 500 === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
    }
  }

  db.exec("COMMIT");

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log("");
  console.log(`═══ SMS Backup import complete in ${elapsed}s ═══`);
  console.log(`  SMS processed: ${totalSms}`);
  console.log(`  MMS processed: ${totalMms}`);
  console.log(`  Messages imported: ${totalImported}`);
  console.log(`  Messages skipped (dupes/empty): ${totalSkipped}`);
  console.log(`  Total messages in DB: ${(db.prepare("SELECT COUNT(*) as c FROM messages").get() as { c: number }).c}`);
  console.log(`  Total channels in DB: ${(db.prepare("SELECT COUNT(*) as c FROM channels").get() as { c: number }).c}`);

  db.close();
}

// ─── CLI ─────────────────────────────────────────────────────────────────────

const xmlPath = process.argv[2];
if (!xmlPath) {
  console.log("import-sms-backup — Import SMS Backup & Restore XML into Outboxer");
  console.log("");
  console.log("Usage:");
  console.log("  node --import tsx src/import-sms-backup.ts <xml-file>");
  console.log("");
  console.log("Example:");
  console.log("  node --import tsx src/import-sms-backup.ts ~/Downloads/sms-20260214094015.xml");
  process.exit(0);
}

if (!existsSync(xmlPath)) {
  console.error(`File not found: ${xmlPath}`);
  process.exit(1);
}

console.log(`Importing SMS Backup from: ${xmlPath}`);
console.log(`Database: ${DB_PATH}`);
console.log("");

importSmsBackup(xmlPath);
