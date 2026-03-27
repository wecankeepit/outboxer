#!/usr/bin/env -S node --import tsx
/**
 * import-gmail-takeout.ts  –  Gmail Takeout → Chat Timeline Database
 *
 * Transforms Gmail MBOX exports into a chat-style timeline database where
 * email is reconceptualized as text blobs exchanged between individuals
 * over channels (defined by participant sets).
 *
 * Pipeline phases:
 *   1. Import contacts from Google Contacts VCF → identity resolution table
 *   2. Import MBOX messages → raw messages with identity-resolved channel assignment
 *   3. Diff-strip → extract only new content per message (chronological diffing)
 *   4. Prune → remove non-bidirectional channels (user must have sent AND received)
 *
 * Usage:
 *   node --import tsx src/import-gmail-takeout.ts import [--fresh | --fresh-source <source>]
 *   node --import tsx src/import-gmail-takeout.ts stats
 *   node --import tsx src/import-gmail-takeout.ts timeline <email-or-name>
 *   node --import tsx src/import-gmail-takeout.ts search <terms>
 *
 * Sources (expected at):
 *   Contacts: ~/.outboxer/takeout/sources/contacts/All Contacts/All Contacts.vcf
 *   MBOX:     ~/.outboxer/takeout/sources/gmail/All mail Including Spam and Trash-002.mbox
 *
 * Database:
 *   ~/.outboxer/takeout/gmail.db
 */

import { createRequire } from "node:module";
import { createReadStream, existsSync, mkdirSync, statSync, unlinkSync } from "node:fs";
import { createHash } from "node:crypto";
import { createInterface } from "node:readline";
import { dirname, join } from "node:path";
import { homedir } from "node:os";
import { simpleParser } from "mailparser";
import type { AddressObject, ParsedMail } from "mailparser";
import { parseContactsTakeout, type ParsedContact } from "./lib/parse-contacts-vcf.js";
import { diffStripChannel } from "./lib/diff-strip.js";
import { SEED_EMAILS, HOPKINS_HANDLES } from "./lib/user-config.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths ───────────────────────────────────────────────────────────────────

const TAKEOUT_BASE = join(homedir(), ".outboxer", "takeout");
const CONTACTS_DIR = join(TAKEOUT_BASE, "sources", "contacts");
const MBOX_PATH = join(
  TAKEOUT_BASE,
  "sources",
  "gmail",
  "All mail Including Spam and Trash-002.mbox",
);
const DB_PATH = join(TAKEOUT_BASE, "gmail.db");
const BATCH_SIZE = 5000;

// ─── Schema ──────────────────────────────────────────────────────────────────

const SCHEMA = [
  // Contacts: individuals (from Google Contacts or inferred from MBOX)
  `CREATE TABLE IF NOT EXISTS contacts (
    id           INTEGER PRIMARY KEY,
    display_name TEXT    NOT NULL,
    source       TEXT    NOT NULL DEFAULT 'google_contacts'
  )`,

  // Email → Contact mapping (many emails per contact)
  `CREATE TABLE IF NOT EXISTS contact_emails (
    email      TEXT    PRIMARY KEY,
    contact_id INTEGER NOT NULL REFERENCES contacts(id)
  )`,

  // Channels: defined by a set of non-user contacts
  `CREATE TABLE IF NOT EXISTS channels (
    id               INTEGER PRIMARY KEY,
    participant_hash TEXT    UNIQUE NOT NULL,
    participant_names TEXT   NOT NULL DEFAULT '[]',
    first_date       TEXT,
    last_date        TEXT,
    message_count    INTEGER NOT NULL DEFAULT 0,
    user_sent_count  INTEGER NOT NULL DEFAULT 0,
    user_recv_count  INTEGER NOT NULL DEFAULT 0
  )`,

  // Channel ↔ Contact junction
  `CREATE TABLE IF NOT EXISTS channel_participants (
    channel_id INTEGER NOT NULL REFERENCES channels(id),
    contact_id INTEGER NOT NULL REFERENCES contacts(id),
    PRIMARY KEY (channel_id, contact_id)
  )`,

  // Messages: each is a text blob in a channel timeline
  `CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY,
    channel_id      INTEGER NOT NULL REFERENCES channels(id),
    message_id      TEXT    UNIQUE,
    date            TEXT    NOT NULL,
    sender_contact_id INTEGER NOT NULL REFERENCES contacts(id),
    is_from_user    INTEGER NOT NULL DEFAULT 0,
    subject         TEXT    NOT NULL,
    blob            TEXT    NOT NULL DEFAULT '',
    raw_body        TEXT,
    source          TEXT    NOT NULL DEFAULT 'gmail_takeout'
  )`,

  // Indexes
  `CREATE INDEX IF NOT EXISTS idx_contact_emails_contact ON contact_emails(contact_id)`,
  `CREATE INDEX IF NOT EXISTS idx_channels_hash ON channels(participant_hash)`,
  `CREATE INDEX IF NOT EXISTS idx_channels_dates ON channels(last_date)`,
  `CREATE INDEX IF NOT EXISTS idx_msg_channel ON messages(channel_id, date)`,
  `CREATE INDEX IF NOT EXISTS idx_msg_date ON messages(date)`,
  `CREATE INDEX IF NOT EXISTS idx_msg_sender ON messages(sender_contact_id)`,
  `CREATE INDEX IF NOT EXISTS idx_channel_participants_contact ON channel_participants(contact_id)`,
  `CREATE INDEX IF NOT EXISTS idx_msg_source ON messages(source)`,

  // FTS5 for search
  `CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(
    subject, blob,
    content=messages, content_rowid=id
  )`,

  `CREATE TRIGGER IF NOT EXISTS messages_fts_ai AFTER INSERT ON messages BEGIN
    INSERT INTO messages_fts(rowid, subject, blob)
    VALUES (new.id, new.subject, new.blob);
  END`,

  `CREATE TRIGGER IF NOT EXISTS messages_fts_ad AFTER DELETE ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, subject, blob)
    VALUES ('delete', old.id, old.subject, old.blob);
  END`,

  `CREATE TRIGGER IF NOT EXISTS messages_fts_au AFTER UPDATE OF blob ON messages BEGIN
    INSERT INTO messages_fts(messages_fts, rowid, subject, blob)
    VALUES ('delete', old.id, old.subject, old.blob);
    INSERT INTO messages_fts(rowid, subject, blob)
    VALUES (new.id, new.subject, new.blob);
  END`,

  // Import log
  `CREATE TABLE IF NOT EXISTS import_log (
    id            INTEGER PRIMARY KEY,
    phase         TEXT    NOT NULL,
    started_at    TEXT    NOT NULL,
    finished_at   TEXT,
    detail        TEXT
  )`,
];

// ─── Helpers ─────────────────────────────────────────────────────────────────

function initDb(dbPath: string): DB {
  mkdirSync(dirname(dbPath), { recursive: true });
  const db = new DatabaseSync(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA foreign_keys = ON");
  for (const stmt of SCHEMA) db.exec(stmt);

  // ── Migrations for existing databases ──
  const msgCols = db.prepare("PRAGMA table_info(messages)").all() as { name: string }[];
  if (!msgCols.some((c) => c.name === "source")) {
    db.exec("ALTER TABLE messages ADD COLUMN source TEXT NOT NULL DEFAULT 'gmail_takeout'");
    db.exec("CREATE INDEX IF NOT EXISTS idx_msg_source ON messages(source)");
  }

  // Fix legacy 'gmail' source tag → 'gmail_takeout' (pre-source-tracking imports)
  const legacyCount = (
    db.prepare("SELECT COUNT(*) as c FROM messages WHERE source = 'gmail'").get() as { c: number }
  ).c;
  if (legacyCount > 0) {
    console.log(`Migrating ${legacyCount} messages from legacy source 'gmail' → 'gmail_takeout'...`);
    db.exec("UPDATE messages SET source = 'gmail_takeout' WHERE source = 'gmail'");
  }

  return db;
}

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

/** Extract {name, email} pairs from a mailparser AddressObject. */
function extractNameEmailPairs(
  field: AddressObject | AddressObject[] | undefined,
): { name: string; email: string }[] {
  if (!field) return [];
  const fields = Array.isArray(field) ? field : [field];
  const pairs: { name: string; email: string }[] = [];
  for (const f of fields) {
    if (f.value) {
      for (const v of f.value) {
        if (v.address) {
          pairs.push({
            name: (v.name || "").trim(),
            email: v.address.toLowerCase().trim(),
          });
        }
      }
    }
  }
  return pairs;
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

/**
 * Parse embedded headers from a forwarded-message body.
 *
 * Detects two patterns:
 *   Exchange: "From: Name\nSent: date\nTo: ...\nSubject: ...\nAuto forwarded by a Rule"
 *   Gmail:    "---------- Forwarded message ---------\nFrom: Name <email>\nDate: ...\nSubject: ...\nTo: ..."
 *
 * Returns null if no recognisable forwarded headers are found.
 */
interface ForwardedHeaders {
  fromName: string;
  fromEmail: string;
  toNames: string[];
  toEmails: string[];
  ccNames: string[];
  ccEmails: string[];
  subject: string;
  date: Date | null;
  /** The actual message content after the embedded headers are stripped. */
  body: string;
}

function parseForwardedBody(body: string): ForwardedHeaders | null {
  // Undo quoted-printable soft line breaks that split header lines
  const text = body.replace(/=\r?\n/g, "");

  // Try Exchange-style first, then Gmail-style
  return parseExchangeForward(text) || parseGmailForward(text);
}

/**
 * Exchange-style embedded headers, parsed line-by-line to handle
 * multi-line To:/Cc: fields correctly.
 *
 *   ________________________________
 *   From: Jane Doe
 *   Sent: Monday, August 30, 2010 4:32:24 PM
 *   To: Your Name; Alice; Bob; Charlie;
 *   Mehrbod Javadi; Stacey Trotter
 *   Cc: Remel Watson
 *   Subject: Duty Hours
 *   Auto forwarded by a Rule
 */
function parseExchangeForward(text: string): ForwardedHeaders | null {
  // Find the "From: ...\nSent: ..." anchor
  const anchor = /^[_\-\s]*\r?\nFrom:\s*(.+)\r?\nSent:\s*(.+)/m.exec(text);
  if (!anchor) return null;

  const fromRaw = anchor[1].trim();
  const sentRaw = anchor[2].trim();

  // Walk lines after "Sent:" to collect header fields.
  // A header line starts with a known keyword; continuation lines don't.
  const headerKeywords = /^(To|Cc|Bcc|Subject|Auto forwarded by a Rule)\s*[:]/i;
  const autoFwdLine = /^Auto forwarded by a Rule\s*$/i;

  const afterSent = text.slice(anchor.index! + anchor[0].length);
  const lines = afterSent.split(/\r?\n/);

  let currentField = "";
  const fields: Record<string, string> = {};
  let bodyStartIdx = 0; // line index where body begins
  let seenAnyField = false;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    // Skip leading blank lines (between Sent: and To:)
    if (!seenAnyField && !line.trim()) {
      bodyStartIdx = i + 1;
      continue;
    }

    if (autoFwdLine.test(line)) {
      seenAnyField = true;
      bodyStartIdx = i + 1;
      continue;
    }

    const kwMatch = headerKeywords.exec(line);
    if (kwMatch) {
      seenAnyField = true;
      currentField = kwMatch[1].toLowerCase();
      const value = line.slice(kwMatch[0].length).trim();
      fields[currentField] = value;
      bodyStartIdx = i + 1;
      continue;
    }

    // Continuation of previous field (line doesn't start with a keyword)
    if (currentField && line.trim()) {
      fields[currentField] = (fields[currentField] || "") + "; " + line.trim();
      bodyStartIdx = i + 1;
      continue;
    }

    // Blank line or non-header content → end of header block
    break;
  }

  const toRaw = fields["to"] || "";
  const ccRaw = fields["cc"] || "";
  const subjRaw = fields["subject"] || "";

  const { name: fName, email: fEmail } = parseNameEmail(fromRaw);
  const { names: tNames, emails: tEmails } = parseRecipientList(toRaw);
  const { names: cNames, emails: cEmails } = parseRecipientList(ccRaw);
  const dateObj = parseFlexDate(sentRaw);

  const msgBody = lines.slice(bodyStartIdx).join("\n").trim();

  return {
    fromName: fName, fromEmail: fEmail,
    toNames: tNames, toEmails: tEmails,
    ccNames: cNames, ccEmails: cEmails,
    subject: subjRaw, date: dateObj, body: msgBody,
  };
}

/**
 * Gmail-style: "---------- Forwarded message ---------\nFrom: ...\nDate: ...\nSubject: ...\nTo: ..."
 */
function parseGmailForward(text: string): ForwardedHeaders | null {
  const anchor = /-+\s*Forwarded message\s*-+\r?\n/m.exec(text);
  if (!anchor) return null;

  const afterAnchor = text.slice(anchor.index! + anchor[0].length);
  const lines = afterAnchor.split(/\r?\n/);

  const fields: Record<string, string> = {};
  let currentField = "";
  let bodyStartIdx = 0;
  let seenAnyField = false;

  const headerKw = /^(From|Date|Subject|To|Cc)\s*:\s*/i;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    if (!seenAnyField && !line.trim()) { bodyStartIdx = i + 1; continue; }
    const kwMatch = headerKw.exec(line);
    if (kwMatch) {
      seenAnyField = true;
      currentField = kwMatch[1].toLowerCase();
      fields[currentField] = line.slice(kwMatch[0].length).trim();
      bodyStartIdx = i + 1;
      continue;
    }
    if (currentField && line.trim()) {
      fields[currentField] = (fields[currentField] || "") + "; " + line.trim();
      bodyStartIdx = i + 1;
      continue;
    }
    break;
  }

  if (!fields["from"]) return null;

  const { name: fName, email: fEmail } = parseNameEmail(fields["from"] || "");
  const { names: tNames, emails: tEmails } = parseRecipientList(fields["to"] || "");
  const { names: cNames, emails: cEmails } = parseRecipientList(fields["cc"] || "");
  const dateObj = fields["date"] ? parseFlexDate(fields["date"]) : null;

  const msgBody = lines.slice(bodyStartIdx).join("\n").trim();

  return {
    fromName: fName, fromEmail: fEmail,
    toNames: tNames, toEmails: tEmails,
    ccNames: cNames, ccEmails: cEmails,
    subject: fields["subject"] || "", date: dateObj, body: msgBody,
  };
}

function parseNameEmail(raw: string): { name: string; email: string } {
  // "Jane Doe <jdoe@example.edu>" or "Jane Doe" or "<jdoe@example.edu>"
  const m = raw.match(/^(.*?)\s*<([^>]+)>\s*$/);
  if (m) return { name: m[1].trim().replace(/^["']|["']$/g, ""), email: m[2].toLowerCase().trim() };
  if (raw.includes("@")) return { name: "", email: raw.toLowerCase().trim() };
  return { name: raw.trim(), email: "" };
}

function parseRecipientList(raw: string): { names: string[]; emails: string[] } {
  const names: string[] = [];
  const emails: string[] = [];
  if (!raw) return { names, emails };

  // Exchange uses semicolons, Gmail uses commas.  Split on both.
  const parts = raw.split(/[;,]/).map((s) => s.trim()).filter(Boolean);
  for (const p of parts) {
    const { name, email } = parseNameEmail(p);
    if (email) emails.push(email);
    if (name) names.push(name);
    else if (!email) names.push(p);
  }
  return { names, emails };
}

function parseFlexDate(raw: string): Date | null {
  const cleaned = raw
    // Strip timezone description: "(UTC-05:00) Eastern Time (US & Canada)"
    .replace(/\(UTC[^)]*\)\s*[^,]*(,\s*)?/g, "")
    // Gmail uses "at" between date and time: "Thu, Jan 29, 2026 at 12:18 PM"
    .replace(/\bat\b/g, "")
    // Normalise unicode narrow/non-breaking spaces to regular spaces
    .replace(/[\u202F\u00A0]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const d = new Date(cleaned);
  if (!isNaN(d.getTime())) return d;
  const d2 = new Date(raw);
  return isNaN(d2.getTime()) ? null : d2;
}

async function* splitMbox(filePath: string): AsyncGenerator<string> {
  const rl = createInterface({
    input: createReadStream(filePath, { encoding: "utf-8" }),
    crlfDelay: Infinity,
  });
  let lines: string[] = [];
  let inMessage = false;
  for await (const line of rl) {
    if (line.startsWith("From ") && line.length > 5) {
      if (inMessage && lines.length > 0) {
        yield lines.join("\n");
        lines = [];
      }
      inMessage = true;
      continue;
    }
    if (inMessage) {
      lines.push(line.startsWith(">From ") ? line.slice(1) : line);
    }
  }
  if (lines.length > 0) yield lines.join("\n");
}

// ─── Phase 1: Import Contacts ────────────────────────────────────────────────

interface ContactRow {
  id: number;
  display_name: string;
  source: string;
}

/**
 * Import contacts from Google Contacts VCF into the database.
 * Returns the set of all user email addresses (auto-detected from the
 * contact with the most known user emails).
 */
function importContacts(db: DB): { userEmails: Set<string>; userContactId: number } {
  console.log("Phase 1: Importing contacts from Google Contacts VCF...");
  const startTime = Date.now();

  const contacts = parseContactsTakeout(CONTACTS_DIR);
  console.log(`  Parsed ${contacts.length} merged contacts with email addresses`);

  const insertContact = db.prepare(
    "INSERT INTO contacts (display_name, source) VALUES (?, 'google_contacts')",
  );
  const insertEmail = db.prepare(
    "INSERT OR IGNORE INTO contact_emails (email, contact_id) VALUES (?, ?)",
  );

  // Check existing contacts by email to avoid duplicates on re-import
  const findContactByEmailForDedup = db.prepare(
    "SELECT contact_id FROM contact_emails WHERE email = ?",
  );

  let inserted = 0;
  let skipped = 0;
  db.exec("BEGIN");
  for (const c of contacts) {
    // If any of this contact's emails already exist, skip the contact
    let existingId: number | null = null;
    for (const email of c.emails) {
      const row = findContactByEmailForDedup.get(email) as { contact_id: number } | undefined;
      if (row) { existingId = row.contact_id; break; }
    }
    if (existingId !== null) {
      skipped++;
      // Still register any NEW emails for this existing contact
      for (const email of c.emails) {
        insertEmail.run(email, existingId);
      }
      continue;
    }
    const result = insertContact.run(c.displayName);
    const contactId = Number(result.lastInsertRowid);
    for (const email of c.emails) {
      insertEmail.run(email, contactId);
    }
    inserted++;
  }
  db.exec("COMMIT");
  console.log(`  Contacts: ${inserted} new, ${skipped} existing (skipped)`);

  // Auto-detect the user's contact: the one named "Your Name" with the
  // most email addresses, or fallback to a known seed address
  // Find contact that owns the most seed emails
  let bestContactId = -1;
  let bestMatchCount = 0;

  const findContactByEmail = db.prepare(
    "SELECT contact_id FROM contact_emails WHERE email = ?",
  );

  const contactMatchCounts = new Map<number, number>();
  for (const seed of SEED_EMAILS) {
    const row = findContactByEmail.get(seed) as { contact_id: number } | undefined;
    if (row) {
      const count = (contactMatchCounts.get(row.contact_id) || 0) + 1;
      contactMatchCounts.set(row.contact_id, count);
      if (count > bestMatchCount) {
        bestMatchCount = count;
        bestContactId = row.contact_id;
      }
    }
  }

  // Expand user emails from all addresses linked to the detected user contact
  const userEmails = new Set<string>();
  if (bestContactId > 0) {
    const userEmailRows = db
      .prepare("SELECT email FROM contact_emails WHERE contact_id = ?")
      .all(bestContactId) as { email: string }[];
    for (const row of userEmailRows) userEmails.add(row.email);
  }
  // Also include seeds that might not be in contacts
  for (const seed of SEED_EMAILS) userEmails.add(seed);

  const userName = bestContactId > 0
    ? (db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(bestContactId) as ContactRow).display_name
    : "Your Name";

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  User contact: ${userName} (id=${bestContactId})`);
  console.log(`  User emails (${userEmails.size}): ${[...userEmails].join(", ")}`);
  console.log(`  Contacts imported in ${elapsed}s`);
  console.log("");

  return { userEmails, userContactId: bestContactId };
}

// ─── Phase 2: Import MBOX ────────────────────────────────────────────────────

/**
 * Resolve an email address to a contact ID. If no existing contact is found,
 * check if an existing inferred contact has the same display name and merge
 * into it. Otherwise create a new inferred contact.
 *
 * This ensures "John Smith <jsmith_123@example.com>" and
 * "John Smith <jsmith@example.edu>" resolve to the same contact.
 */
function resolveContact(
  db: DB,
  email: string,
  displayName: string | undefined,
  findEmail: ReturnType<DB["prepare"]>,
  insertContact: ReturnType<DB["prepare"]>,
  insertEmail: ReturnType<DB["prepare"]>,
  cache: Map<string, number>,
  /** Maps lowercased display names → contact IDs for inferred contacts. */
  nameToContactId: Map<string, number>,
): number {
  const lower = email.toLowerCase().trim();

  // When the email is empty (e.g. "From: Jane Doe" with no address),
  // resolve purely by display name — do NOT cache under the empty-string key
  // since many different senders can have no email.
  if (!lower) {
    const name = displayName?.trim() || "Unknown";
    const nameLower = name.toLowerCase();
    const isRealName =
      !nameLower.includes("@") &&
      nameLower.includes(" ") &&
      nameLower.length >= 5;

    if (isRealName) {
      const existingId = nameToContactId.get(nameLower);
      if (existingId !== undefined) return existingId;
    }

    // Check if we already have an inferred contact with this exact name
    const byName = db
      .prepare("SELECT id FROM contacts WHERE LOWER(display_name) = ? LIMIT 1")
      .get(nameLower) as { id: number } | undefined;
    if (byName) {
      if (isRealName) nameToContactId.set(nameLower, byName.id);
      return byName.id;
    }

    const result = insertContact.run(name, "inferred");
    const contactId = Number(result.lastInsertRowid);
    if (isRealName) nameToContactId.set(nameLower, contactId);
    return contactId;
  }

  // Check cache first (email → contact ID)
  const cached = cache.get(lower);
  if (cached !== undefined) return cached;

  // Check database (maybe inserted in a previous import run)
  const row = findEmail.get(lower) as { contact_id: number } | undefined;
  if (row) {
    cache.set(lower, row.contact_id);
    return row.contact_id;
  }

  // No existing contact for this email. Check if we can merge by display name.
  const name = displayName && displayName !== lower ? displayName : lower;
  const nameLower = name.toLowerCase().trim();

  // Only merge by name if the name looks like a real person name (not an email,
  // not too short/generic, contains a space suggesting first+last)
  const isRealName =
    !nameLower.includes("@") &&
    nameLower.includes(" ") &&
    nameLower.length >= 5;

  if (isRealName) {
    const existingId = nameToContactId.get(nameLower);
    if (existingId !== undefined) {
      // Merge: add this email to the existing contact
      insertEmail.run(lower, existingId);
      cache.set(lower, existingId);
      return existingId;
    }
  }

  // Create new inferred contact
  const result = insertContact.run(name, "inferred");
  const contactId = Number(result.lastInsertRowid);
  insertEmail.run(lower, contactId);
  cache.set(lower, contactId);
  if (isRealName) {
    nameToContactId.set(nameLower, contactId);
  }
  return contactId;
}

// ─── Hopkins Address Table ────────────────────────────────────────────────────

/**
 * Hopkins Address Table: maps lowercased display names → email addresses
 * (and vice versa) for people the user communicated with while sending
 * from a Hopkins handle.  Built in Pass 1 of the MBOX import, consumed
 * in Pass 2 to resolve "From: Name" in auto-forwarded message bodies.
 */
interface HopkinsAddressTable {
  nameToEmail: Map<string, string>;
  emailToName: Map<string, string>;
}

function buildHopkinsAddressTable(
  pairs: { name: string; email: string }[],
): HopkinsAddressTable {
  const nameToEmail = new Map<string, string>();
  const emailToName = new Map<string, string>();
  for (const { name, email } of pairs) {
    if (!email) continue;
    const lowerEmail = email.toLowerCase().trim();
    if (name) {
      const lowerName = name.toLowerCase().trim();
      if (!nameToEmail.has(lowerName)) nameToEmail.set(lowerName, lowerEmail);
      if (!emailToName.has(lowerEmail)) emailToName.set(lowerEmail, name.trim());
    }
  }
  return { nameToEmail, emailToName };
}

/** Pre-parsed record for a deferred auto-forwarded message. */
interface DeferredAutoForward {
  messageId: string;
  envelopeDate: Date | null;
  envelopeSubject: string;
  /** Pre-parsed embedded headers (null if body didn't contain recognizable headers). */
  fwd: ForwardedHeaders | null;
}

// ─── Phase 2: Import MBOX (two-pass Hopkins-aware) ───────────────────────────

async function importMbox(
  db: DB,
  userEmails: Set<string>,
  userContactId: number,
): Promise<void> {
  console.log("Phase 2: Importing MBOX messages...");
  const startTime = Date.now();
  const fileSize = statSync(MBOX_PATH).size;
  console.log(`  MBOX: ${MBOX_PATH}`);
  console.log(`  Size: ${(fileSize / 1024 / 1024).toFixed(1)} MB`);

  const hopkinsHandles = new Set(HOPKINS_HANDLES.map((h) => h.toLowerCase()));

  // ── Pass 1: Build Hopkins Address Table + collect deferred auto-forwards ──
  console.log("  Pass 1: Building Hopkins Address Table...");
  const pass1Start = Date.now();

  const hopkinsPairs: { name: string; email: string }[] = [];
  const deferredForwards: DeferredAutoForward[] = [];
  let pass1Count = 0;
  let hopkinsSentCount = 0;

  for await (const rawMsg of splitMbox(MBOX_PATH)) {
    pass1Count++;
    if (pass1Count % BATCH_SIZE === 0) {
      const elapsed = ((Date.now() - pass1Start) / 1000).toFixed(1);
      const rate = (pass1Count / ((Date.now() - pass1Start) / 1000)).toFixed(0);
      console.log(
        `  [${elapsed}s] Pass 1: ${pass1Count} scanned, ${hopkinsSentCount} Hopkins-sent, ${deferredForwards.length} auto-forwards (${rate} msg/s)`,
      );
    }

    let parsed: ParsedMail;
    try {
      parsed = await simpleParser(rawMsg, {
        skipHtmlToText: false,
        skipTextToHtml: true,
        skipImageLinks: true,
      });
    } catch {
      continue;
    }

    const fromAddrs = extractAddresses(parsed.from);
    const from = (fromAddrs[0] || "").toLowerCase().trim();
    if (!hopkinsHandles.has(from)) continue;

    // This message was sent FROM a Hopkins handle.
    // Collect all To/Cc/Bcc name+email pairs for the address table.
    const toPairs = extractNameEmailPairs(parsed.to);
    const ccPairs = extractNameEmailPairs(parsed.cc);
    const bccPairs = extractNameEmailPairs(parsed.bcc as AddressObject | AddressObject[] | undefined);
    for (const pair of [...toPairs, ...ccPairs, ...bccPairs]) {
      if (!userEmails.has(pair.email.toLowerCase())) {
        hopkinsPairs.push(pair);
      }
    }
    hopkinsSentCount++;

    // Check if this is an auto-forward: From is Hopkins handle, ALL
    // recipients are user emails (the forwarded-to-self pattern).
    const allRecipients = [...extractAddresses(parsed.to), ...extractAddresses(parsed.cc)];
    const hasExternalRecipient = allRecipients.some(
      (a) => !userEmails.has(a.toLowerCase().trim()),
    );

    if (!hasExternalRecipient) {
      const messageId =
        parsed.messageId ||
        `synth-${createHash("md5").update(rawMsg.slice(0, 4000)).digest("hex")}`;
      deferredForwards.push({
        messageId,
        envelopeDate: parsed.date ?? null,
        envelopeSubject: parsed.subject || "(no subject)",
        fwd: parseForwardedBody(parsed.text || ""),
      });
    }
  }

  const hopkinsTable = buildHopkinsAddressTable(hopkinsPairs);
  const pass1Elapsed = ((Date.now() - pass1Start) / 1000).toFixed(1);
  console.log(`  Pass 1 complete in ${pass1Elapsed}s`);
  console.log(`    Messages scanned: ${pass1Count}`);
  console.log(`    Hopkins-sent messages: ${hopkinsSentCount}`);
  console.log(`    Hopkins address table: ${hopkinsTable.nameToEmail.size} name→email entries`);
  console.log(`    Auto-forwards deferred: ${deferredForwards.length}`);
  console.log("");

  // ── Pass 2: Import all messages ──────────────────────────────────────────
  console.log("  Pass 2: Importing messages...");
  const pass2Start = Date.now();

  // Collect deferred message IDs for fast lookup
  const deferredIds = new Set(deferredForwards.map((d) => d.messageId));

  // Prepared statements
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
        subject, raw_body, source)
     VALUES (?, ?, ?, ?, ?, ?, ?, 'gmail_takeout')`,
  );

  // Contact resolution cache
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

  const existingIds = new Set<string>();
  const existingRows = db.prepare("SELECT message_id FROM messages WHERE message_id IS NOT NULL").all() as { message_id: string }[];
  for (const row of existingRows) existingIds.add(row.message_id);
  if (existingIds.size > 0) {
    console.log(`  Existing messages in DB: ${existingIds.size} (will skip)`);
  }

  /** Shared helper: resolve participants + insert a message into the DB. */
  function importSingleMessage(
    messageId: string,
    from: string,
    fromDisplayName: string | undefined,
    toAddrs: string[],
    ccAddrs: string[],
    subject: string,
    date: Date | null,
    bodyText: string,
    isFromUser: boolean,
    parsed: ParsedMail | null,
    extraParticipantNames: string[],
  ): boolean {
    const senderContactId = resolveContact(
      db,
      from,
      fromDisplayName,
      findEmail,
      insertContact,
      insertEmailStmt,
      contactCache,
      nameToContactId,
    );

    const allAddresses = [...new Set([from, ...toAddrs, ...ccAddrs].map((a) => a.toLowerCase().trim()).filter(Boolean))];
    const participantContactIds: number[] = [];

    for (const addr of allAddresses) {
      if (userEmails.has(addr)) continue;

      let displayName: string | undefined;
      if (parsed) {
        if (parsed.from) {
          const fromField = Array.isArray(parsed.from) ? parsed.from : [parsed.from];
          for (const f of fromField) {
            const match = f.value?.find(
              (v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr,
            );
            if (match?.name) { displayName = match.name; break; }
          }
        }
        if (!displayName && parsed.to) {
          const toField = Array.isArray(parsed.to) ? parsed.to : [parsed.to];
          for (const f of toField) {
            const match = f.value?.find(
              (v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr,
            );
            if (match?.name) { displayName = match.name; break; }
          }
        }
        if (!displayName && parsed.cc) {
          const ccField = Array.isArray(parsed.cc) ? parsed.cc : [parsed.cc];
          for (const f of ccField) {
            const match = f.value?.find(
              (v: { address?: string; name?: string }) => v.address?.toLowerCase() === addr,
            );
            if (match?.name) { displayName = match.name; break; }
          }
        }
      }

      const contactId = resolveContact(
        db, addr, displayName, findEmail, insertContact,
        insertEmailStmt, contactCache, nameToContactId,
      );
      participantContactIds.push(contactId);
    }

    // Resolve name-only participants (from embedded To/Cc in auto-forwards)
    for (const name of extraParticipantNames) {
      if (!name) continue;
      const contactId = resolveContact(
        db, "", name, findEmail, insertContact,
        insertEmailStmt, contactCache, nameToContactId,
      );
      if (contactId !== userContactId) {
        participantContactIds.push(contactId);
      }
    }

    // The sender is always a channel participant (unless they're the user).
    if (!isFromUser && senderContactId !== userContactId) {
      participantContactIds.push(senderContactId);
    }

    if (participantContactIds.length === 0) return false;

    const sortedIds = [...new Set(participantContactIds)].sort((a, b) => a - b);
    const channelHash = createHash("sha256")
      .update(sortedIds.join("\0"))
      .digest("hex");
    const dateStr = formatDate(date);
    if (!dateStr) return false;

    const participantNames: string[] = [];
    for (const cid of sortedIds) {
      const c = db.prepare("SELECT display_name FROM contacts WHERE id = ?").get(cid) as { display_name: string } | undefined;
      if (c) participantNames.push(c.display_name);
    }

    const channelRow = findChannel.get(channelHash) as { id: number } | undefined;
    let channelId: number;

    if (channelRow) {
      channelId = channelRow.id;
      updateChannel.run(dateStr, dateStr, isFromUser ? 1 : 0, isFromUser ? 0 : 1, channelId);
    } else {
      const result = insertChannel.run(
        channelHash, JSON.stringify(participantNames), dateStr, dateStr,
        isFromUser ? 1 : 0, isFromUser ? 0 : 1,
      );
      channelId = Number(result.lastInsertRowid);
      for (const cid of sortedIds) {
        insertParticipant.run(channelId, cid);
      }
    }

    insertMessage.run(
      channelId, messageId, dateStr, senderContactId,
      isFromUser ? 1 : 0, normalizeSubject(subject), bodyText,
    );
    existingIds.add(messageId);
    return true;
  }

  let total = 0;
  let imported = 0;
  let skipped = 0;
  let errors = 0;
  let deferredSkipped = 0;

  db.exec("BEGIN");

  // ── Pass 2a: Import all non-deferred messages ──
  for await (const rawMsg of splitMbox(MBOX_PATH)) {
    total++;

    if (total % BATCH_SIZE === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
      const elapsed = ((Date.now() - pass2Start) / 1000).toFixed(1);
      const rate = (total / ((Date.now() - pass2Start) / 1000)).toFixed(0);
      console.log(
        `  [${elapsed}s] ${total} parsed, ${imported} imported, ${skipped} skipped, ${errors} errors (${rate} msg/s)`,
      );
    }

    let parsed: ParsedMail;
    try {
      parsed = await simpleParser(rawMsg, {
        skipHtmlToText: false,
        skipTextToHtml: true,
        skipImageLinks: true,
      });
    } catch {
      errors++;
      continue;
    }

    const messageId =
      parsed.messageId ||
      `synth-${createHash("md5").update(rawMsg.slice(0, 4000)).digest("hex")}`;

    if (existingIds.has(messageId)) { skipped++; continue; }

    // Skip auto-forwarded messages — they'll be processed in Pass 2b
    if (deferredIds.has(messageId)) { deferredSkipped++; continue; }

    const fromAddrs = extractAddresses(parsed.from);
    const from = fromAddrs[0] || "";
    const to = extractAddresses(parsed.to);
    const cc = extractAddresses(parsed.cc);
    const subject = parsed.subject || "(no subject)";
    const date = parsed.date ?? null;
    const bodyText = parsed.text || "";
    const isFromUser = userEmails.has(from.toLowerCase().trim());

    let fromDisplayName: string | undefined;
    if (parsed.from) {
      const fromField = Array.isArray(parsed.from) ? parsed.from[0] : parsed.from;
      if (fromField?.value?.[0]?.name) {
        fromDisplayName = fromField.value[0].name;
      }
    }

    const ok = importSingleMessage(
      messageId, from, fromDisplayName, to, cc,
      subject, date, bodyText, isFromUser, parsed, [],
    );
    if (ok) imported++;
    else skipped++;
  }

  db.exec("COMMIT");

  const pass2aElapsed = ((Date.now() - pass2Start) / 1000).toFixed(1);
  console.log(`  Pass 2a complete in ${pass2aElapsed}s — ${imported} imported, ${skipped} skipped, ${deferredSkipped} deferred`);

  // ── Pass 2b: Process deferred auto-forwarded messages ──
  console.log(`  Pass 2b: Processing ${deferredForwards.length} auto-forwarded messages...`);
  let fwdImported = 0;
  let fwdSkipped = 0;

  db.exec("BEGIN");

  for (const deferred of deferredForwards) {
    if (existingIds.has(deferred.messageId)) { fwdSkipped++; continue; }

    const fwd = deferred.fwd;
    if (!fwd) { fwdSkipped++; continue; }

    // Try to match the sender name against the Hopkins Address Table
    const senderNameLower = fwd.fromName.toLowerCase().trim();
    let senderEmail = fwd.fromEmail;

    if (!senderEmail && senderNameLower) {
      // Name-only From: line — look up in Hopkins table
      const resolved = hopkinsTable.nameToEmail.get(senderNameLower);
      if (resolved) senderEmail = resolved;
    }

    // If we have a sender email, check if it's in the table (bidirectional)
    // If name-only and not in table, skip (no established relationship)
    if (senderEmail) {
      const knownByEmail = hopkinsTable.emailToName.has(senderEmail.toLowerCase());
      const knownByName = senderNameLower ? hopkinsTable.nameToEmail.has(senderNameLower) : false;
      if (!knownByEmail && !knownByName) { fwdSkipped++; continue; }
    } else {
      // No email, no name match → skip
      fwdSkipped++;
      continue;
    }

    const from = senderEmail;
    const fromDisplayName = fwd.fromName || hopkinsTable.emailToName.get(senderEmail.toLowerCase()) || undefined;
    const isFromUser = userEmails.has(from.toLowerCase().trim());
    const subject = fwd.subject || deferred.envelopeSubject;
    const date = fwd.date || deferred.envelopeDate;
    const bodyText = fwd.body;

    // Resolve To/Cc email addresses from the embedded headers
    const toAddrs = fwd.toEmails.filter((e) => !!e);
    const ccAddrs = fwd.ccEmails.filter((e) => !!e);

    // Name-only recipients from embedded To/Cc: resolve via Hopkins table
    const extraNames: string[] = [];
    for (const name of [...fwd.toNames, ...fwd.ccNames]) {
      if (!name) continue;
      const nameLower = name.toLowerCase().trim();
      // Check if this name resolves to an email via the Hopkins table
      const email = hopkinsTable.nameToEmail.get(nameLower);
      if (email && !userEmails.has(email)) {
        toAddrs.push(email);
      } else if (!email) {
        // Fall back to name-only contact resolution
        extraNames.push(name);
      }
    }

    const ok = importSingleMessage(
      deferred.messageId, from, fromDisplayName, toAddrs, ccAddrs,
      subject, date, bodyText, isFromUser, null, extraNames,
    );
    if (ok) fwdImported++;
    else fwdSkipped++;
  }

  db.exec("COMMIT");

  console.log(`  Pass 2b complete — ${fwdImported} imported, ${fwdSkipped} skipped`);

  imported += fwdImported;
  skipped += fwdSkipped;

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Import complete in ${elapsed}s`);
  console.log(`  Total parsed: ${total}`);
  console.log(`  Imported: ${imported} (${fwdImported} from auto-forwards)`);
  console.log(`  Skipped: ${skipped}`);
  console.log(`  Errors: ${errors}`);
  console.log("");
}

// ─── Phase 3: Diff-Strip ────────────────────────────────────────────────────

function runDiffStrip(db: DB): void {
  console.log("Phase 3: Diff-based stripping (extracting new content per message)...");
  const startTime = Date.now();

  // Get all channels
  const channels = db
    .prepare("SELECT id FROM channels")
    .all() as { id: number }[];

  console.log(`  Processing ${channels.length} channels...`);

  const updateBlob = db.prepare(
    "UPDATE messages SET blob = ? WHERE id = ?",
  );

  let processedChannels = 0;
  let processedMessages = 0;

  db.exec("BEGIN");

  for (const channel of channels) {
    // Get messages for this channel, sorted chronologically
    const messages = db
      .prepare(
        `SELECT id, raw_body, subject FROM messages
         WHERE channel_id = ? ORDER BY date ASC`,
      )
      .all(channel.id) as { id: number; raw_body: string; subject: string }[];

    if (messages.length === 0) continue;

    // Apply diff-strip to the channel
    const blobs = diffStripChannel(
      messages.map((m) => ({
        id: m.id,
        body: m.raw_body || "",
        subject: m.subject,
      })),
    );

    // Update blobs in database
    for (const [msgId, blob] of blobs) {
      updateBlob.run(blob, msgId);
      processedMessages++;
    }

    processedChannels++;
    if (processedChannels % 1000 === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      console.log(
        `  [${elapsed}s] ${processedChannels}/${channels.length} channels, ${processedMessages} messages`,
      );
    }
  }

  db.exec("COMMIT");

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Diff-strip complete in ${elapsed}s`);
  console.log(`  Channels processed: ${processedChannels}`);
  console.log(`  Messages processed: ${processedMessages}`);
  console.log("");
}

// ─── Phase 4: Prune Non-Bidirectional Channels ──────────────────────────────

function pruneChannels(db: DB): void {
  console.log("Phase 4: Pruning non-bidirectional channels...");
  const startTime = Date.now();

  // Prune channels that are not bidirectional AND where none of the
  // participants have bidirectional communication in ANY channel.
  // This implements person-level bidirectional filtering: if you've
  // exchanged messages with someone in any channel, all their channels
  // (including receive-only group threads) are retained.
  const pruneCondition = `
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
    )`;

  const nonBiCount = (
    db
      .prepare(`SELECT COUNT(*) as cnt FROM (${pruneCondition})`)
      .get() as { cnt: number }
  ).cnt;

  const totalBefore = (
    db.prepare("SELECT COUNT(*) as cnt FROM channels").get() as { cnt: number }
  ).cnt;

  // Delete in FK order: messages → channel_participants → channels
  db.exec(`DELETE FROM messages WHERE channel_id IN (${pruneCondition})`);
  db.exec(`DELETE FROM channel_participants WHERE channel_id IN (${pruneCondition})`);
  db.exec(`DELETE FROM channels WHERE id IN (${pruneCondition})`);

  const totalAfter = (
    db.prepare("SELECT COUNT(*) as cnt FROM channels").get() as { cnt: number }
  ).cnt;

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Pruned ${nonBiCount} non-bidirectional channels`);
  console.log(`  Channels: ${totalBefore} → ${totalAfter}`);
  console.log(`  Prune complete in ${elapsed}s`);
  console.log("");
}

// ─── CLI: Stats ──────────────────────────────────────────────────────────────

function showStats(db: DB): void {
  const channelCount = (
    db.prepare("SELECT COUNT(*) as c FROM channels").get() as { c: number }
  ).c;
  const msgCount = (
    db.prepare("SELECT COUNT(*) as c FROM messages").get() as { c: number }
  ).c;
  const contactCount = (
    db.prepare("SELECT COUNT(*) as c FROM contacts").get() as { c: number }
  ).c;
  const googleContacts = (
    db.prepare("SELECT COUNT(*) as c FROM contacts WHERE source = 'google_contacts'").get() as { c: number }
  ).c;
  const inferredContacts = (
    db.prepare("SELECT COUNT(*) as c FROM contacts WHERE source = 'inferred'").get() as { c: number }
  ).c;
  const userSentCount = (
    db.prepare("SELECT COUNT(*) as c FROM messages WHERE is_from_user = 1").get() as { c: number }
  ).c;

  const sourceCounts = db
    .prepare("SELECT source, COUNT(*) as c FROM messages GROUP BY source ORDER BY c DESC")
    .all() as { source: string; c: number }[];

  console.log("Outboxer Chat Timeline Database");
  console.log("─".repeat(50));
  console.log(`  Bidirectional channels: ${channelCount}`);
  console.log(`  Total messages:         ${msgCount}`);
  if (sourceCounts.length > 0) {
    for (const { source, c } of sourceCounts) {
      console.log(`    └ ${source}: ${c}`);
    }
  }
  console.log(`  User sent messages:     ${userSentCount}`);
  console.log(`  Contacts (Google):      ${googleContacts}`);
  console.log(`  Contacts (inferred):    ${inferredContacts}`);
  console.log(`  Total contacts:         ${contactCount}`);
  console.log(`  Database:               ${DB_PATH}`);
  console.log("");

  // Top 10 most active channels
  const topChannels = db
    .prepare(
      `SELECT c.id, c.participant_names, c.message_count,
              c.user_sent_count, c.user_recv_count,
              c.first_date, c.last_date
       FROM channels c
       ORDER BY c.message_count DESC
       LIMIT 10`,
    )
    .all() as {
    id: number;
    participant_names: string;
    message_count: number;
    user_sent_count: number;
    user_recv_count: number;
    first_date: string;
    last_date: string;
  }[];

  if (topChannels.length > 0) {
    console.log("Top 10 most active channels:");
    for (const ch of topChannels) {
      const names = JSON.parse(ch.participant_names) as string[];
      const period = `${ch.first_date?.slice(0, 10)} → ${ch.last_date?.slice(0, 10)}`;
      console.log(
        `  ${names.join(", ")} — ${ch.message_count} msgs (↑${ch.user_sent_count} ↓${ch.user_recv_count}) ${period}`,
      );
    }
  }
}

// ─── CLI: Timeline ───────────────────────────────────────────────────────────

function showTimeline(db: DB, query: string): void {
  const lower = query.toLowerCase().trim();

  // Search by email address or contact name
  let contactIds: number[] = [];

  // Try exact email match first
  const emailRow = db
    .prepare("SELECT contact_id FROM contact_emails WHERE email = ?")
    .get(lower) as { contact_id: number } | undefined;

  if (emailRow) {
    contactIds = [emailRow.contact_id];
  } else {
    // Search by name (fuzzy)
    const nameRows = db
      .prepare(
        "SELECT id FROM contacts WHERE LOWER(display_name) LIKE ?",
      )
      .all(`%${lower}%`) as { id: number }[];
    contactIds = nameRows.map((r) => r.id);
  }

  if (contactIds.length === 0) {
    console.log(`No contacts found matching "${query}"`);
    return;
  }

  // Show matched contacts
  for (const cid of contactIds) {
    const contact = db
      .prepare("SELECT display_name FROM contacts WHERE id = ?")
      .get(cid) as { display_name: string };
    const emails = db
      .prepare("SELECT email FROM contact_emails WHERE contact_id = ?")
      .all(cid) as { email: string }[];
    console.log(
      `Contact: ${contact.display_name} (${emails.map((e) => e.email).join(", ")})`,
    );
  }
  console.log("");

  // Find all channels involving any of these contacts
  const placeholders = contactIds.map(() => "?").join(",");
  const channels = db
    .prepare(
      `SELECT DISTINCT c.id, c.participant_names, c.first_date, c.last_date,
              c.message_count
       FROM channels c
       JOIN channel_participants cp ON cp.channel_id = c.id
       WHERE cp.contact_id IN (${placeholders})
       ORDER BY c.first_date ASC`,
    )
    .all(...contactIds) as {
    id: number;
    participant_names: string;
    first_date: string;
    last_date: string;
    message_count: number;
  }[];

  if (channels.length === 0) {
    console.log("No bidirectional channels found.");
    return;
  }

  console.log(`Found ${channels.length} channel(s):\n`);

  for (const ch of channels) {
    const names = JSON.parse(ch.participant_names) as string[];
    console.log(`═══ Channel: ${names.join(", ")} ═══`);
    console.log(
      `    ${ch.first_date?.slice(0, 10)} → ${ch.last_date?.slice(0, 10)} (${ch.message_count} messages)`,
    );
    console.log("");

    // Get all messages in this channel
    const messages = db
      .prepare(
        `SELECT m.date, m.blob, m.is_from_user, m.subject,
                c.display_name as sender_name
         FROM messages m
         JOIN contacts c ON c.id = m.sender_contact_id
         WHERE m.channel_id = ?
         ORDER BY m.date ASC`,
      )
      .all(ch.id) as {
      date: string;
      blob: string;
      is_from_user: number;
      subject: string;
      sender_name: string;
    }[];

    for (const msg of messages) {
      const arrow = msg.is_from_user ? "→" : "←";
      const time = msg.date.slice(0, 16).replace("T", " ");
      console.log(`  ${time} ${arrow} ${msg.sender_name}`);
      // Show the blob indented
      if (msg.blob) {
        for (const line of msg.blob.split("\n")) {
          console.log(`    ${line}`);
        }
      }
      console.log("");
    }
  }
}

// ─── CLI: Search ─────────────────────────────────────────────────────────────

function searchMessages(db: DB, query: string): void {
  const results = db
    .prepare(
      `SELECT m.date, m.blob, m.is_from_user, m.subject,
              ct.display_name as sender_name,
              ch.participant_names
       FROM messages_fts fts
       JOIN messages m ON m.id = fts.rowid
       JOIN contacts ct ON ct.id = m.sender_contact_id
       JOIN channels ch ON ch.id = m.channel_id
       WHERE messages_fts MATCH ?
       ORDER BY m.date DESC
       LIMIT 30`,
    )
    .all(query) as {
    date: string;
    blob: string;
    is_from_user: number;
    subject: string;
    sender_name: string;
    participant_names: string;
  }[];

  if (results.length === 0) {
    console.log("No messages found.");
    return;
  }

  console.log(`Found ${results.length} message(s) matching "${query}":\n`);

  for (const r of results) {
    const arrow = r.is_from_user ? "→" : "←";
    const time = r.date.slice(0, 16).replace("T", " ");
    const names = JSON.parse(r.participant_names) as string[];
    console.log(`  ${time} ${arrow} ${r.sender_name} [${names.join(", ")}]`);
    // Show first 3 lines of blob
    const blobLines = r.blob.split("\n").slice(0, 3);
    for (const line of blobLines) {
      console.log(`    ${line}`);
    }
    if (r.blob.split("\n").length > 3) console.log("    ...");
    console.log("");
  }
}

// ─── Source-Aware Reimport Helpers ───────────────────────────────────────────

/**
 * Delete all messages from a given source, then clean up orphaned channels
 * and channel_participants. Contacts are left intact (they're cross-source).
 */
function clearSource(db: DB, source: string): void {
  console.log(`Clearing messages from source "${source}"...`);
  const msgCount = (
    db.prepare("SELECT COUNT(*) as c FROM messages WHERE source = ?").get(source) as { c: number }
  ).c;
  console.log(`  ${msgCount} messages to remove`);

  db.exec("BEGIN");
  try {
    db.prepare("DELETE FROM messages WHERE source = ?").run(source);

    // Remove channel_participants and channels that have zero messages left
    db.exec(`
      DELETE FROM channel_participants WHERE channel_id IN (
        SELECT c.id FROM channels c
        LEFT JOIN messages m ON m.channel_id = c.id
        WHERE m.id IS NULL
      )
    `);
    db.exec(`
      DELETE FROM channels WHERE id IN (
        SELECT c.id FROM channels c
        LEFT JOIN messages m ON m.channel_id = c.id
        WHERE m.id IS NULL
      )
    `);

    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }

  console.log(`  Cleared. Remaining messages: ${(db.prepare("SELECT COUNT(*) as c FROM messages").get() as { c: number }).c}`);
  console.log(`  Remaining channels: ${(db.prepare("SELECT COUNT(*) as c FROM channels").get() as { c: number }).c}`);
  console.log("");
}

/**
 * Recompute channel aggregate stats from the actual messages table.
 * Run this after any source-specific deletion+reimport to ensure consistency.
 */
function recomputeChannelStats(db: DB): void {
  console.log("Recomputing channel statistics...");
  const startTime = Date.now();

  db.exec(`
    UPDATE channels SET
      message_count = (SELECT COUNT(*) FROM messages WHERE channel_id = channels.id),
      user_sent_count = (SELECT COUNT(*) FROM messages WHERE channel_id = channels.id AND is_from_user = 1),
      user_recv_count = (SELECT COUNT(*) FROM messages WHERE channel_id = channels.id AND is_from_user = 0),
      first_date = (SELECT MIN(date) FROM messages WHERE channel_id = channels.id),
      last_date  = (SELECT MAX(date) FROM messages WHERE channel_id = channels.id)
  `);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`  Channel stats recomputed in ${elapsed}s`);
  console.log("");
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const command = args[0] || "help";

  switch (command) {
    case "import": {
      const fresh = args.includes("--fresh");
      const freshSourceIdx = args.indexOf("--fresh-source");
      const freshSource = freshSourceIdx >= 0 ? args[freshSourceIdx + 1] : null;

      if (fresh && freshSource) {
        console.error("Cannot use --fresh and --fresh-source together");
        process.exit(1);
      }

      if (fresh && existsSync(DB_PATH)) {
        console.log(`Removing existing database: ${DB_PATH}`);
        unlinkSync(DB_PATH);
      }

      if (!existsSync(MBOX_PATH)) {
        console.error(`MBOX file not found: ${MBOX_PATH}`);
        process.exit(1);
      }

      const db = initDb(DB_PATH);
      try {
        if (freshSource) {
          clearSource(db, freshSource);
        }

        const pipelineStart = Date.now();

        // Phase 1: Contacts
        const { userEmails, userContactId } = importContacts(db);

        // Phase 2: MBOX
        await importMbox(db, userEmails, userContactId);

        // Phase 3: Diff-strip
        runDiffStrip(db);

        // Phase 4: Recompute stats + Prune
        if (freshSource) {
          recomputeChannelStats(db);
        }
        pruneChannels(db);

        // Summary
        const totalElapsed = ((Date.now() - pipelineStart) / 1000).toFixed(1);
        console.log(`═══ Pipeline complete in ${totalElapsed}s ═══`);
        showStats(db);
      } finally {
        db.close();
      }
      break;
    }

    case "stats": {
      if (!existsSync(DB_PATH)) {
        console.error(`Database not found: ${DB_PATH}`);
        process.exit(1);
      }
      const db = new DatabaseSync(DB_PATH);
      showStats(db);
      db.close();
      break;
    }

    case "timeline": {
      if (!existsSync(DB_PATH)) {
        console.error(`Database not found: ${DB_PATH}`);
        process.exit(1);
      }
      const query = args.slice(1).join(" ");
      if (!query) {
        console.error("Usage: timeline <email-or-name>");
        process.exit(1);
      }
      const db = new DatabaseSync(DB_PATH);
      showTimeline(db, query);
      db.close();
      break;
    }

    case "search": {
      if (!existsSync(DB_PATH)) {
        console.error(`Database not found: ${DB_PATH}`);
        process.exit(1);
      }
      const query = args.slice(1).join(" ");
      if (!query) {
        console.error("Usage: search <terms>");
        process.exit(1);
      }
      const db = new DatabaseSync(DB_PATH);
      searchMessages(db, query);
      db.close();
      break;
    }

    default:
      console.log("Gmail Takeout → Chat Timeline Database");
      console.log("");
      console.log("Usage:");
      console.log("  import                              Incremental import (skips existing message_ids)");
      console.log("  import --fresh                      Nuke entire database and reimport everything");
      console.log("  import --fresh-source gmail_takeout  Clear only gmail_takeout messages, reimport MBOX");
      console.log("  stats                               Show database statistics");
      console.log("  timeline <email-or-name>            Show chat timeline for a contact");
      console.log("  search <terms>                      Full-text search across messages");
      console.log("");
      console.log(`Database: ${DB_PATH}`);
      break;
  }
}

main().catch((e) => {
  console.error("Fatal error:", e);
  process.exit(1);
});
