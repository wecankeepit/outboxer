#!/usr/bin/env -S node --import tsx
/**
 * sync-google-contacts.ts — Google People API contacts sync for Outboxer.
 *
 * Syncs all Google Contacts into local tables, resolves identity links to
 * existing Outboxer people (by email/phone), and auto-merges duplicates.
 * Supports incremental sync via syncToken.
 *
 * Usage:
 *   npm run sync:contacts              # full or incremental sync
 *   npm run sync:contacts -- --relink  # re-run identity linking only
 *   npm run sync:contacts -- --daemon  # run every 15 minutes
 *
 * Auth is delegated to gog (gogcli). No per-pipeline tokens or --auth flow.
 * See src/lib/google-auth.ts.
 */

import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { google } from "googleapis";
import { createGoogleAuth, GoogleAuthError } from "./lib/google-auth.js";

const require = createRequire(import.meta.url);
const { DatabaseSync } = require("node:sqlite") as typeof import("node:sqlite");
type DB = InstanceType<typeof DatabaseSync>;

// ─── Paths & Config ─────────────────────────────────────────────────────────

const OUTBOXER_HOME = join(homedir(), ".outboxer");
const DB_PATH = join(OUTBOXER_HOME, "takeout", "gmail.db");

const PERSON_FIELDS = [
  "names",
  "emailAddresses",
  "phoneNumbers",
  "organizations",
  "addresses",
  "birthdays",
  "photos",
  "memberships",
  "metadata",
].join(",");

const PAGE_SIZE = 1000;

// ─── Database ───────────────────────────────────────────────────────────────

function openDb(): DB {
  if (!existsSync(DB_PATH)) {
    console.error(`Database not found: ${DB_PATH}`);
    process.exit(1);
  }
  const db = new DatabaseSync(DB_PATH);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec("PRAGMA foreign_keys = ON");
  return db;
}

function ensureSchema(db: DB): void {
  db.exec(`
    -- Main Google Contacts table
    CREATE TABLE IF NOT EXISTS google_contacts (
      resource_name TEXT PRIMARY KEY,
      etag          TEXT,
      display_name  TEXT NOT NULL DEFAULT '',
      given_name    TEXT DEFAULT '',
      family_name   TEXT DEFAULT '',
      nickname      TEXT DEFAULT '',
      birthday      TEXT DEFAULT '',
      photo_url     TEXT DEFAULT '',
      organizations TEXT DEFAULT '[]',   -- JSON array of {name, title, department}
      addresses     TEXT DEFAULT '[]',   -- JSON array of {formatted, type}
      update_time       TEXT DEFAULT '',     -- from Google metadata (ISO 8601)
      update_time_human TEXT DEFAULT '',     -- classified as human-initiated edit
      update_time_google TEXT DEFAULT '',    -- classified as automated Google update
      synced_at         TEXT NOT NULL DEFAULT ''
    );

    -- Email addresses for each Google Contact
    CREATE TABLE IF NOT EXISTS google_contact_emails (
      resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
      email         TEXT NOT NULL COLLATE NOCASE,
      type          TEXT DEFAULT '',
      PRIMARY KEY (resource_name, email)
    );

    -- Phone numbers for each Google Contact (normalized for matching)
    CREATE TABLE IF NOT EXISTS google_contact_phones (
      resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
      phone         TEXT NOT NULL,         -- normalized E.164 for matching
      raw_phone     TEXT DEFAULT '',       -- original display format
      type          TEXT DEFAULT '',
      PRIMARY KEY (resource_name, phone)
    );

    -- Contact group/label definitions
    CREATE TABLE IF NOT EXISTS google_contact_groups (
      resource_name TEXT PRIMARY KEY,
      name          TEXT NOT NULL,
      group_type    TEXT DEFAULT ''
    );

    -- Contact-to-group memberships
    CREATE TABLE IF NOT EXISTS google_contact_memberships (
      contact_resource_name TEXT NOT NULL REFERENCES google_contacts(resource_name) ON DELETE CASCADE,
      group_resource_name   TEXT NOT NULL,
      PRIMARY KEY (contact_resource_name, group_resource_name)
    );

    -- Sync state for contacts
    CREATE TABLE IF NOT EXISTS sync_state (
      key   TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );

    -- Index for fast email/phone lookups during linking
    CREATE INDEX IF NOT EXISTS idx_gc_emails_email ON google_contact_emails(email);
    CREATE INDEX IF NOT EXISTS idx_gc_phones_phone ON google_contact_phones(phone);
  `);
}

function getSyncState(db: DB, key: string): string | null {
  const row = db
    .prepare("SELECT value FROM sync_state WHERE key = ?")
    .get(key) as { value: string } | undefined;
  return row?.value ?? null;
}

function setSyncState(db: DB, key: string, value: string): void {
  db.prepare(
    "INSERT OR REPLACE INTO sync_state (key, value) VALUES (?, ?)",
  ).run(key, value);
}

// ─── Phone normalization ────────────────────────────────────────────────────

function normalizePhone(phone: string): string {
  // Strip everything except digits and leading +
  let digits = phone.replace(/[^\d+]/g, "");
  if (digits.startsWith("+")) {
    digits = "+" + digits.slice(1).replace(/\D/g, "");
  }
  // Normalize US numbers to E.164
  const raw = digits.replace(/^\+/, "");
  if (raw.length === 10) return `+1${raw}`;
  if (raw.length === 11 && raw.startsWith("1")) return `+${raw}`;
  // Already has country code or non-US
  if (digits.startsWith("+")) return digits;
  return `+${raw}`;
}

// ─── Process a single Person resource ───────────────────────────────────────

interface PersonResource {
  resourceName?: string;
  etag?: string;
  names?: Array<{
    displayName?: string;
    givenName?: string;
    familyName?: string;
    metadata?: { primary?: boolean };
  }>;
  nicknames?: Array<{ value?: string }>;
  emailAddresses?: Array<{
    value?: string;
    type?: string;
    metadata?: { primary?: boolean };
  }>;
  phoneNumbers?: Array<{
    value?: string;
    canonicalForm?: string;
    type?: string;
  }>;
  organizations?: Array<{
    name?: string;
    title?: string;
    department?: string;
  }>;
  addresses?: Array<{
    formattedValue?: string;
    type?: string;
  }>;
  birthdays?: Array<{
    date?: { year?: number; month?: number; day?: number };
    text?: string;
  }>;
  photos?: Array<{
    url?: string;
    default?: boolean;
    metadata?: { primary?: boolean };
  }>;
  memberships?: Array<{
    contactGroupMembership?: { contactGroupResourceName?: string };
  }>;
  metadata?: {
    sources?: Array<{
      type?: string;       // "CONTACT", "PROFILE", "DOMAIN_PROFILE", "ACCOUNT"
      updateTime?: string;
    }>;
    deleted?: boolean;
  };
}

function upsertContact(
  db: DB,
  person: PersonResource,
  stmts: {
    upsertContact: ReturnType<DB["prepare"]>;
    deleteEmails: ReturnType<DB["prepare"]>;
    deletePhones: ReturnType<DB["prepare"]>;
    deleteMemberships: ReturnType<DB["prepare"]>;
    insertEmail: ReturnType<DB["prepare"]>;
    insertPhone: ReturnType<DB["prepare"]>;
    insertMembership: ReturnType<DB["prepare"]>;
  },
): void {
  const rn = person.resourceName;
  if (!rn) return;

  // Extract primary name (or first available)
  const primaryName =
    person.names?.find((n) => n.metadata?.primary) || person.names?.[0];
  const displayName = primaryName?.displayName || "";
  const givenName = primaryName?.givenName || "";
  const familyName = primaryName?.familyName || "";
  const nickname = person.nicknames?.[0]?.value || "";

  // Birthday
  let birthday = "";
  const bd = person.birthdays?.[0];
  if (bd?.date) {
    const { year, month, day } = bd.date;
    if (year && month && day) {
      birthday = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    } else if (month && day) {
      birthday = `--${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
  } else if (bd?.text) {
    birthday = bd.text;
  }

  // Photo (non-default)
  const photo =
    person.photos?.find((p) => !p.default && p.metadata?.primary) ||
    person.photos?.find((p) => !p.default) ||
    null;
  const photoUrl = photo?.url || "";

  // Organizations
  const orgs = (person.organizations || []).map((o) => ({
    name: o.name || "",
    title: o.title || "",
    department: o.department || "",
  }));

  // Addresses
  const addrs = (person.addresses || []).map((a) => ({
    formatted: a.formattedValue || "",
    type: a.type || "",
  }));

  // Update time from metadata — prefer CONTACT source (user-edited data)
  // over PROFILE source (auto-refreshed by Google, misleadingly recent).
  let updateTime = "";
  const sources = person.metadata?.sources;
  if (sources && sources.length > 0) {
    // First try: CONTACT-type source (reflects actual user edits)
    for (const src of sources) {
      if (src.type === "CONTACT" && src.updateTime && src.updateTime > updateTime) {
        updateTime = src.updateTime;
      }
    }
    // Fallback: any source if no CONTACT source found
    if (!updateTime) {
      for (const src of sources) {
        if (src.updateTime && src.updateTime > updateTime) {
          updateTime = src.updateTime;
        }
      }
    }
  }

  const now = new Date().toISOString();

  // Upsert main contact row
  stmts.upsertContact.run(
    rn,
    person.etag || "",
    displayName,
    givenName,
    familyName,
    nickname,
    birthday,
    photoUrl,
    JSON.stringify(orgs),
    JSON.stringify(addrs),
    updateTime,
    now,
    // ON CONFLICT updates:
    person.etag || "",
    displayName,
    givenName,
    familyName,
    nickname,
    birthday,
    photoUrl,
    JSON.stringify(orgs),
    JSON.stringify(addrs),
    updateTime,
    now,
  );

  // Replace emails, phones, memberships
  stmts.deleteEmails.run(rn);
  stmts.deletePhones.run(rn);
  stmts.deleteMemberships.run(rn);

  for (const em of person.emailAddresses || []) {
    if (em.value) {
      stmts.insertEmail.run(rn, em.value.toLowerCase().trim(), em.type || "");
    }
  }

  for (const ph of person.phoneNumbers || []) {
    const raw = ph.value || "";
    const canonical = ph.canonicalForm || raw;
    if (raw) {
      const normalized = normalizePhone(canonical || raw);
      stmts.insertPhone.run(rn, normalized, raw, ph.type || "");
    }
  }

  for (const mem of person.memberships || []) {
    const groupRn = mem.contactGroupMembership?.contactGroupResourceName;
    if (groupRn) {
      stmts.insertMembership.run(rn, groupRn);
    }
  }
}

// ─── Sync contact groups ────────────────────────────────────────────────────

async function syncContactGroups(
  people: ReturnType<typeof google.people>,
  db: DB,
): Promise<void> {
  console.log("Syncing contact groups...");
  const res = await people.contactGroups.list({
    pageSize: 1000,
    groupFields: "name,groupType",
  });

  const groups = res.data.contactGroups || [];
  const upsertGroup = db.prepare(
    `INSERT OR REPLACE INTO google_contact_groups (resource_name, name, group_type)
     VALUES (?, ?, ?)`,
  );

  db.exec("BEGIN");
  for (const g of groups) {
    if (g.resourceName && g.name) {
      upsertGroup.run(g.resourceName, g.name, g.groupType || "");
    }
  }
  db.exec("COMMIT");
  console.log(`  ${groups.length} groups synced.`);
}

// ─── Full sync ──────────────────────────────────────────────────────────────

async function fullSync(
  people: ReturnType<typeof google.people>,
  db: DB,
): Promise<string | null> {
  console.log("Mode: FULL SYNC\n");

  const stmts = prepareStatements(db);
  let pageToken: string | undefined;
  let total = 0;
  let syncToken: string | null = null;

  db.exec("BEGIN");

  do {
    const res = await people.people.connections.list({
      resourceName: "people/me",
      pageSize: PAGE_SIZE,
      personFields: PERSON_FIELDS,
      requestSyncToken: true,
      pageToken,
    });

    const connections = res.data.connections || [];
    for (const person of connections) {
      upsertContact(db, person as PersonResource, stmts);
      total++;
    }

    pageToken = res.data.nextPageToken ?? undefined;
    syncToken = res.data.nextSyncToken ?? null;

    if (total % 500 === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
    }
    console.log(`  Fetched ${total} contacts so far...`);
  } while (pageToken);

  db.exec("COMMIT");
  console.log(`\nFull sync complete: ${total} contacts.`);

  return syncToken;
}

// ─── Incremental sync ───────────────────────────────────────────────────────

async function incrementalSync(
  people: ReturnType<typeof google.people>,
  db: DB,
  syncToken: string,
): Promise<string | null> {
  console.log("Mode: INCREMENTAL SYNC\n");

  const stmts = prepareStatements(db);
  const deleteContact = db.prepare(
    "DELETE FROM google_contacts WHERE resource_name = ?",
  );

  let pageToken: string | undefined;
  let total = 0;
  let deleted = 0;
  let newSyncToken: string | null = null;

  db.exec("BEGIN");

  do {
    const res = await people.people.connections.list({
      resourceName: "people/me",
      pageSize: PAGE_SIZE,
      personFields: PERSON_FIELDS,
      requestSyncToken: true,
      syncToken,
      pageToken,
    });

    const connections = res.data.connections || [];
    for (const person of connections) {
      if (person.metadata?.deleted) {
        // Contact was deleted
        if (person.resourceName) {
          deleteContact.run(person.resourceName);
          deleted++;
        }
      } else {
        upsertContact(db, person as PersonResource, stmts);
        total++;
      }
    }

    pageToken = res.data.nextPageToken ?? undefined;
    newSyncToken = res.data.nextSyncToken ?? null;
  } while (pageToken);

  db.exec("COMMIT");
  console.log(
    `Incremental sync complete: ${total} updated, ${deleted} deleted.`,
  );

  return newSyncToken;
}

// ─── Prepared statements ────────────────────────────────────────────────────

function prepareStatements(db: DB) {
  return {
    upsertContact: db.prepare(`
      INSERT INTO google_contacts
        (resource_name, etag, display_name, given_name, family_name,
         nickname, birthday, photo_url, organizations, addresses,
         update_time, synced_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(resource_name) DO UPDATE SET
        etag = ?, display_name = ?, given_name = ?, family_name = ?,
        nickname = ?, birthday = ?, photo_url = ?, organizations = ?,
        addresses = ?, update_time = ?, synced_at = ?
    `),
    deleteEmails: db.prepare(
      "DELETE FROM google_contact_emails WHERE resource_name = ?",
    ),
    deletePhones: db.prepare(
      "DELETE FROM google_contact_phones WHERE resource_name = ?",
    ),
    deleteMemberships: db.prepare(
      "DELETE FROM google_contact_memberships WHERE contact_resource_name = ?",
    ),
    insertEmail: db.prepare(
      "INSERT OR IGNORE INTO google_contact_emails (resource_name, email, type) VALUES (?, ?, ?)",
    ),
    insertPhone: db.prepare(
      "INSERT OR IGNORE INTO google_contact_phones (resource_name, phone, raw_phone, type) VALUES (?, ?, ?, ?)",
    ),
    insertMembership: db.prepare(
      "INSERT OR IGNORE INTO google_contact_memberships (contact_resource_name, group_resource_name) VALUES (?, ?)",
    ),
  };
}

// ─── Identity Linking ───────────────────────────────────────────────────────

/**
 * Link Google Contacts to existing Outboxer contacts by matching email
 * addresses and phone numbers. When a single Google Contact matches multiple
 * Outboxer contacts, merge them via contact_merges.
 */
function linkContacts(db: DB): void {
  console.log("\nLinking Google Contacts to Outboxer people...");

  // Build a map: google resource_name → Set of matched Outboxer contact IDs
  const gcToOutboxer = new Map<string, Set<number>>();

  // 1. Match by email
  const emailMatches = db
    .prepare(
      `SELECT gce.resource_name, ce.contact_id
       FROM google_contact_emails gce
       JOIN contact_emails ce ON LOWER(gce.email) = LOWER(ce.email)`,
    )
    .all() as { resource_name: string; contact_id: number }[];

  for (const row of emailMatches) {
    if (!gcToOutboxer.has(row.resource_name)) {
      gcToOutboxer.set(row.resource_name, new Set());
    }
    gcToOutboxer.get(row.resource_name)!.add(row.contact_id);
  }

  // 2. Match by phone number
  const phoneMatches = db
    .prepare(
      `SELECT gcp.resource_name, cp.contact_id
       FROM google_contact_phones gcp
       JOIN contact_phones cp ON gcp.phone = cp.phone`,
    )
    .all() as { resource_name: string; contact_id: number }[];

  for (const row of phoneMatches) {
    if (!gcToOutboxer.has(row.resource_name)) {
      gcToOutboxer.set(row.resource_name, new Set());
    }
    gcToOutboxer.get(row.resource_name)!.add(row.contact_id);
  }

  // 3. Also match phone numbers against contact display_name (for inferred phone contacts)
  const phoneNameMatches = db
    .prepare(
      `SELECT gcp.resource_name, c.id as contact_id
       FROM google_contact_phones gcp
       JOIN contacts c ON gcp.phone = c.display_name
       WHERE c.source = 'inferred'`,
    )
    .all() as { resource_name: string; contact_id: number }[];

  for (const row of phoneNameMatches) {
    if (!gcToOutboxer.has(row.resource_name)) {
      gcToOutboxer.set(row.resource_name, new Set());
    }
    gcToOutboxer.get(row.resource_name)!.add(row.contact_id);
  }

  // Also try matching against display names that look like phone numbers
  // (e.g., "+15559876543" stored as display_name)
  const allPhones = db
    .prepare("SELECT resource_name, phone FROM google_contact_phones")
    .all() as { resource_name: string; phone: string }[];

  for (const { resource_name, phone } of allPhones) {
    // Try various phone formats that might appear as display_name
    const variants = new Set<string>();
    variants.add(phone); // +15559876543
    const digits = phone.replace(/\D/g, "");
    if (digits.length === 11 && digits.startsWith("1")) {
      variants.add(`+${digits}`);
      variants.add(digits.slice(1)); // 5559876543
    }
    if (digits.length === 10) {
      variants.add(`+1${digits}`);
      variants.add(digits);
    }

    for (const variant of variants) {
      const match = db
        .prepare(
          "SELECT id FROM contacts WHERE display_name = ? AND source = 'inferred'",
        )
        .get(variant) as { id: number } | undefined;
      if (match) {
        if (!gcToOutboxer.has(resource_name)) {
          gcToOutboxer.set(resource_name, new Set());
        }
        gcToOutboxer.get(resource_name)!.add(match.id);
      }
    }
  }

  // 4. Match by display name — catches contacts created by bridge imports
  //    (e.g., gmessages sync creates inferred contacts by name only).
  //    Only match when the Google Contact already has at least one email/phone match,
  //    OR when the name is specific enough (has a space, not an email/phone).
  const allGoogleContacts = db
    .prepare("SELECT resource_name, display_name FROM google_contacts WHERE display_name != ''")
    .all() as { resource_name: string; display_name: string }[];

  for (const gc of allGoogleContacts) {
    const nameLower = gc.display_name.toLowerCase().trim();
    // Skip names that look like emails or phone numbers
    if (nameLower.includes("@") || /^\+?\d{7,}$/.test(nameLower.replace(/\D/g, ""))) continue;
    // Only match names that have a space (real person names)
    if (!nameLower.includes(" ") || nameLower.length < 4) continue;

    const nameMatches = db
      .prepare(
        `SELECT id FROM contacts
         WHERE LOWER(display_name) = ?
         AND id NOT IN (SELECT secondary_id FROM contact_merges)`,
      )
      .all(nameLower) as { id: number }[];

    for (const match of nameMatches) {
      if (!gcToOutboxer.has(gc.resource_name)) {
        gcToOutboxer.set(gc.resource_name, new Set());
      }
      gcToOutboxer.get(gc.resource_name)!.add(match.id);
    }
  }

  // 5. Process merges: when one Google Contact maps to multiple Outboxer contacts,
  //    merge them all under the first (lowest ID = oldest)
  let linkedCount = 0;
  let mergeCount = 0;

  // Resolve existing merges first
  function resolveId(id: number): number {
    const row = db
      .prepare("SELECT primary_id FROM contact_merges WHERE secondary_id = ?")
      .get(id) as { primary_id: number } | undefined;
    return row ? row.primary_id : id;
  }

  const insertMerge = db.prepare(
    "INSERT OR REPLACE INTO contact_merges (secondary_id, primary_id) VALUES (?, ?)",
  );
  const updateMergePrimary = db.prepare(
    "UPDATE contact_merges SET primary_id = ? WHERE primary_id = ?",
  );

  // Also update the display name of the primary contact to match Google's name
  const updateContactName = db.prepare(
    "UPDATE contacts SET display_name = ? WHERE id = ?",
  );

  db.exec("BEGIN");

  for (const [resourceName, outboxerIds] of gcToOutboxer) {
    if (outboxerIds.size === 0) continue;

    // Resolve all IDs through existing merges
    const resolvedIds = new Set<number>();
    for (const id of outboxerIds) {
      resolvedIds.add(resolveId(id));
    }

    // Pick the primary: lowest ID
    const sortedIds = [...resolvedIds].sort((a, b) => a - b);
    const primaryId = sortedIds[0]!;

    // Merge all others into the primary
    for (let i = 1; i < sortedIds.length; i++) {
      const secondaryId = sortedIds[i]!;
      if (secondaryId === primaryId) continue;
      // Re-point any existing merges that point to secondary
      updateMergePrimary.run(primaryId, secondaryId);
      insertMerge.run(secondaryId, primaryId);
      mergeCount++;
    }

    // Update the primary contact's display name from Google if it's better
    const gc = db
      .prepare(
        "SELECT display_name FROM google_contacts WHERE resource_name = ?",
      )
      .get(resourceName) as { display_name: string } | undefined;
    const outboxerContact = db
      .prepare("SELECT display_name, source FROM contacts WHERE id = ?")
      .get(primaryId) as { display_name: string; source: string } | undefined;

    if (gc?.display_name && outboxerContact) {
      const currentName = outboxerContact.display_name;
      const googleName = gc.display_name;
      // Prefer Google name if current is an email address, phone number, or shorter
      const isCurrentEmail = currentName.includes("@");
      const isCurrentPhone = /^\+?\d{7,}$/.test(currentName.replace(/\D/g, ""));
      if (
        isCurrentEmail ||
        isCurrentPhone ||
        (googleName.length > currentName.length && !currentName.includes(" "))
      ) {
        updateContactName.run(googleName, primaryId);
      }
    }

    linkedCount++;
  }

  db.exec("COMMIT");

  console.log(`  ${gcToOutboxer.size} Google Contacts matched to Outboxer people.`);
  console.log(`  ${linkedCount} links established.`);
  console.log(`  ${mergeCount} Outboxer contact merges performed.`);

  // 5. Update channel participant_names for merged contacts
  if (mergeCount > 0) {
    console.log("  Updating channel participant names for merged contacts...");
    refreshChannelNames(db);
  }
}

/**
 * After merges, update participant_names on channels to reflect new display names.
 */
function refreshChannelNames(db: DB): void {
  const channels = db
    .prepare("SELECT id FROM channels")
    .all() as { id: number }[];

  const getParticipants = db.prepare(
    `SELECT DISTINCT COALESCE(cm.primary_id, cp.contact_id) as cid
     FROM channel_participants cp
     LEFT JOIN contact_merges cm ON cm.secondary_id = cp.contact_id
     WHERE cp.channel_id = ?`,
  );
  const getName = db.prepare(
    "SELECT display_name FROM contacts WHERE id = ?",
  );
  const updateNames = db.prepare(
    "UPDATE channels SET participant_names = ? WHERE id = ?",
  );

  db.exec("BEGIN");
  let updated = 0;

  for (const ch of channels) {
    const participants = getParticipants.all(ch.id) as { cid: number }[];
    const names: string[] = [];
    for (const p of participants) {
      const c = getName.get(p.cid) as { display_name: string } | undefined;
      if (c) names.push(c.display_name);
    }
    updateNames.run(JSON.stringify(names), ch.id);
    updated++;

    if (updated % 500 === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
    }
  }

  db.exec("COMMIT");
  console.log(`  Updated participant names for ${updated} channels.`);
}

// ─── Timestamp classification: human vs automated ────────────────────────────

/**
 * Classifies each contact's update_time as human-initiated or automated (Google).
 *
 * Method:
 *   1. Sort all update_times chronologically on a timeline.
 *   2. Compute inter-tick distances (gaps between consecutive timestamps).
 *   3. Apply Otsu's method on log-transformed gaps to find the optimal threshold
 *      separating short (automated) gaps from long (human) gaps.
 *   4. Group consecutive contacts into "runs" where internal gaps < threshold.
 *   5. Runs of ≤ RUN_SIZE_CUTOFF contacts = human edits (someone editing a few
 *      contacts in quick succession). Larger runs = automated batch operations.
 *   6. Write update_time_human or update_time_google for each contact.
 */
function classifyUpdateTimestamps(db: DB): void {
  console.log("\nClassifying update timestamps (human vs automated)...");

  // Ensure new columns exist (idempotent migration)
  try {
    db.exec("ALTER TABLE google_contacts ADD COLUMN update_time_human TEXT DEFAULT ''");
  } catch (_) { /* column already exists */ }
  try {
    db.exec("ALTER TABLE google_contacts ADD COLUMN update_time_google TEXT DEFAULT ''");
  } catch (_) { /* column already exists */ }

  // 1. Get all timestamps, sorted
  const rows = db
    .prepare(
      `SELECT resource_name, update_time FROM google_contacts
       WHERE update_time IS NOT NULL AND update_time != ''
       ORDER BY update_time`
    )
    .all() as Array<{ resource_name: string; update_time: string }>;

  if (rows.length < 2) {
    console.log("  Not enough contacts for classification.");
    return;
  }

  // Convert to epoch ms
  const entries = rows.map((r) => ({
    resource_name: r.resource_name,
    epochMs: new Date(r.update_time).getTime(),
    update_time: r.update_time,
  }));

  // 2. Compute inter-tick gaps (seconds)
  const gaps: number[] = [];
  for (let i = 1; i < entries.length; i++) {
    gaps.push((entries[i].epochMs - entries[i - 1].epochMs) / 1000);
  }

  // 3. Otsu's method on log-transformed gaps
  const EPS = 0.001;
  const logGaps = gaps.map((g) => Math.log10(g + EPS));

  const logMin = logGaps.reduce((a, b) => Math.min(a, b), Infinity);
  const logMax = logGaps.reduce((a, b) => Math.max(a, b), -Infinity);
  const NUM_BINS = 200;
  const binWidth = (logMax - logMin) / NUM_BINS;

  const histogram = new Array(NUM_BINS).fill(0);
  const binCenters: number[] = [];
  for (let i = 0; i < NUM_BINS; i++) {
    binCenters.push(logMin + (i + 0.5) * binWidth);
  }
  for (const lg of logGaps) {
    const bin = Math.min(Math.floor((lg - logMin) / binWidth), NUM_BINS - 1);
    histogram[bin]++;
  }

  // Otsu's threshold
  const total = histogram.reduce((a: number, b: number) => a + b, 0);
  let sumAll = 0;
  for (let i = 0; i < NUM_BINS; i++) sumAll += histogram[i] * binCenters[i];

  let bestVariance = -1;
  let bestIdx = 0;
  let w0 = 0;
  let sum0 = 0;

  for (let i = 0; i < NUM_BINS - 1; i++) {
    w0 += histogram[i];
    if (w0 === 0) continue;
    const w1 = total - w0;
    if (w1 === 0) break;
    sum0 += histogram[i] * binCenters[i];
    const mean0 = sum0 / w0;
    const mean1 = (sumAll - sum0) / w1;
    const bv = w0 * w1 * (mean0 - mean1) ** 2;
    if (bv > bestVariance) {
      bestVariance = bv;
      bestIdx = i;
    }
  }

  const thresholdSeconds = Math.pow(10, binCenters[bestIdx]);
  console.log(`  Otsu threshold: ${thresholdSeconds.toFixed(1)}s (${(thresholdSeconds / 60).toFixed(1)} min)`);

  // 4. Build runs (groups of contacts connected by sub-threshold gaps)
  const RUN_SIZE_CUTOFF = 5; // runs of 1-5 contacts = human, 6+ = automated
  type Run = { startIdx: number; endIdx: number };
  const runs: Run[] = [];
  let runStart = 0;

  for (let i = 1; i < entries.length; i++) {
    const gapSec = (entries[i].epochMs - entries[i - 1].epochMs) / 1000;
    if (gapSec > thresholdSeconds) {
      runs.push({ startIdx: runStart, endIdx: i - 1 });
      runStart = i;
    }
  }
  runs.push({ startIdx: runStart, endIdx: entries.length - 1 });

  // 5. Classify and build update map
  let humanCount = 0;
  let automatedCount = 0;
  const classifications = new Map<string, "human" | "google">();

  for (const run of runs) {
    const runSize = run.endIdx - run.startIdx + 1;
    const label = runSize <= RUN_SIZE_CUTOFF ? "human" : "google";
    for (let i = run.startIdx; i <= run.endIdx; i++) {
      classifications.set(entries[i].resource_name, label);
      if (label === "human") humanCount++;
      else automatedCount++;
    }
  }

  console.log(`  Runs: ${runs.length} (cutoff: ≤${RUN_SIZE_CUTOFF} = human)`);
  console.log(`  Human: ${humanCount}, Automated: ${automatedCount}`);

  // 6. Write to database
  const stmtHuman = db.prepare(
    `UPDATE google_contacts SET update_time_human = ?, update_time_google = '' WHERE resource_name = ?`
  );
  const stmtGoogle = db.prepare(
    `UPDATE google_contacts SET update_time_human = '', update_time_google = ? WHERE resource_name = ?`
  );

  db.exec("BEGIN");
  let batchCount = 0;
  for (const entry of entries) {
    const label = classifications.get(entry.resource_name)!;
    if (label === "human") {
      stmtHuman.run(entry.update_time, entry.resource_name);
    } else {
      stmtGoogle.run(entry.update_time, entry.resource_name);
    }
    batchCount++;
    if (batchCount % 1000 === 0) {
      db.exec("COMMIT");
      db.exec("BEGIN");
    }
  }

  // Handle contacts with no update_time (both fields stay empty)
  db.exec("COMMIT");

  console.log(`  Classification written to database.`);
}

// ─── Main sync pipeline ─────────────────────────────────────────────────────

async function sync(): Promise<void> {
  console.log("Outboxer Google Contacts Sync");
  console.log("=============================\n");

  // 1. Authorize via gog
  const oauth2 = createGoogleAuth();
  const people = google.people({ version: "v1", auth: oauth2 });

  // 2. Open database and ensure schema
  const db = openDb();
  ensureSchema(db);
  console.log(`Database: ${DB_PATH}\n`);

  // 3. Sync contact groups first
  await syncContactGroups(people, db);

  // 4. Sync contacts (full or incremental)
  const existingSyncToken = getSyncState(db, "contactsSyncToken");
  let newSyncToken: string | null = null;

  if (existingSyncToken) {
    try {
      newSyncToken = await incrementalSync(people, db, existingSyncToken);
    } catch (err: any) {
      // syncToken expired — roll back the open transaction and fall back to full sync
      if (err.code === 410 || err.code === 400) {
        try { db.exec("ROLLBACK"); } catch { /* no open txn */ }
        console.log("  Sync token expired, falling back to full sync...\n");
        newSyncToken = await fullSync(people, db);
      } else {
        throw err;
      }
    }
  } else {
    newSyncToken = await fullSync(people, db);
  }

  // 5. Update sync state
  if (newSyncToken) {
    setSyncState(db, "contactsSyncToken", newSyncToken);
  }
  setSyncState(db, "contactsLastSyncAt", new Date().toISOString());

  // 6. Run identity linking
  linkContacts(db);

  // 7. Classify timestamps as human vs automated
  classifyUpdateTimestamps(db);

  // 8. Report stats
  const stats = db
    .prepare(
      `SELECT
         (SELECT COUNT(*) FROM google_contacts) as total_contacts,
         (SELECT COUNT(DISTINCT resource_name) FROM google_contact_emails) as contacts_with_email,
         (SELECT COUNT(DISTINCT resource_name) FROM google_contact_phones) as contacts_with_phone,
         (SELECT COUNT(*) FROM google_contact_groups) as total_groups`,
    )
    .get() as Record<string, number>;

  console.log("\nStats:");
  console.log(`  Total Google Contacts: ${stats.total_contacts}`);
  console.log(`  With email: ${stats.contacts_with_email}`);
  console.log(`  With phone: ${stats.contacts_with_phone}`);
  console.log(`  Groups: ${stats.total_groups}`);

  db.close();
  console.log("\nDone!");
}

// ─── Daemon mode ────────────────────────────────────────────────────────────

async function runDaemon(): Promise<void> {
  console.log("Starting Google Contacts sync daemon (every 15 minutes)...\n");

  let authFailCount = 0;
  const run = async () => {
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
        console.error("Sync error:", err);
      }
    }
    console.log(`\nNext sync at ${new Date(Date.now() + 15 * 60 * 1000).toLocaleTimeString()}\n`);
  };

  await run();
  setInterval(run, 15 * 60 * 1000);
}

// ─── CLI ────────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);

if (args.includes("--auth")) {
  console.error(
    "The --auth flag is no longer used. Contacts auth is managed by gog.\n" +
      "If gog's token is invalid, re-authorize with:\n" +
      "  gog auth add your-email@gmail.com --services gmail,contacts",
  );
  process.exit(1);
} else if (args.includes("--relink")) {
  console.log("Re-running identity linking + classification...\n");
  const db = openDb();
  ensureSchema(db);
  linkContacts(db);
  classifyUpdateTimestamps(db);
  db.close();
  console.log("\nDone!");
} else if (args.includes("--daemon")) {
  await runDaemon();
} else {
  await sync();
}
