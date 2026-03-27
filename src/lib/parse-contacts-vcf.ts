/**
 * parse-contacts-vcf.ts
 *
 * Parses Google Contacts Takeout VCF (vCard 3.0) files into a unified
 * list of contacts with all their email addresses.
 *
 * Handles:
 *  - Multi-line continuation (RFC 2425 folding)
 *  - Duplicate vCards for the same person (merged via shared email)
 *  - item1.EMAIL / item2.EMAIL prefixed properties
 *  - Multiple EMAIL fields per vCard
 */

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

// ─── Types ───────────────────────────────────────────────────────────────────

export interface ParsedContact {
  /** Display name (FN field) */
  displayName: string;
  /** All email addresses (lowercased, deduped) */
  emails: string[];
  /** Source VCF group names (CATEGORIES) */
  categories: string[];
}

// ─── VCF Parsing ─────────────────────────────────────────────────────────────

/** Unfold RFC 2425 continuation lines (lines starting with space or tab). */
function unfoldLines(raw: string): string[] {
  const lines: string[] = [];
  for (const line of raw.split(/\r?\n/)) {
    if ((line.startsWith(" ") || line.startsWith("\t")) && lines.length > 0) {
      // Continuation: append to previous line (strip leading whitespace)
      lines[lines.length - 1] += line.slice(1);
    } else {
      lines.push(line);
    }
  }
  return lines;
}

/** Parse a single vCard block into (displayName, emails, categories). */
function parseVCard(
  lines: string[],
): { displayName: string; emails: string[]; categories: string[] } | null {
  let fn = "";
  let structuredName = "";
  const emails: string[] = [];
  const categories: string[] = [];

  for (const line of lines) {
    const upper = line.toUpperCase();

    // FN: Formatted Name
    if (upper.startsWith("FN:")) {
      fn = line.slice(3).trim();
      continue;
    }

    // N: Structured Name (Last;First;Middle;Prefix;Suffix)
    if (upper.startsWith("N:") && !upper.startsWith("NOTE:") && !upper.startsWith("NICKNAME:")) {
      const parts = line.slice(2).split(";");
      const last = parts[0]?.trim() || "";
      const first = parts[1]?.trim() || "";
      if (first || last) {
        structuredName = first && last ? `${first} ${last}` : first || last;
      }
      continue;
    }

    // EMAIL (with possible item prefix and type params)
    // Formats: EMAIL:addr, EMAIL;TYPE=...:addr, item1.EMAIL;TYPE=...:addr
    const emailMatch = line.match(
      /^(?:item\d+\.)?EMAIL[^:]*:(.+)/i,
    );
    if (emailMatch) {
      const addr = emailMatch[1]!.trim().toLowerCase();
      if (addr && addr.includes("@")) {
        emails.push(addr);
      }
      continue;
    }

    // CATEGORIES
    if (upper.startsWith("CATEGORIES:")) {
      const cats = line.slice(11).split(",").map((c) => c.trim()).filter(Boolean);
      categories.push(...cats);
      continue;
    }
  }

  const displayName = fn || structuredName;
  if (!displayName && emails.length === 0) return null;

  return {
    displayName: displayName || emails[0] || "Unknown",
    emails: [...new Set(emails)],
    categories: [...new Set(categories)],
  };
}

// ─── Contact Merging (Union-Find) ────────────────────────────────────────────

class UnionFind {
  private parent: Map<number, number> = new Map();

  makeSet(x: number): void {
    if (!this.parent.has(x)) this.parent.set(x, x);
  }

  find(x: number): number {
    let root = x;
    while (this.parent.get(root) !== root) {
      root = this.parent.get(root)!;
    }
    // Path compression
    let curr = x;
    while (curr !== root) {
      const next = this.parent.get(curr)!;
      this.parent.set(curr, root);
      curr = next;
    }
    return root;
  }

  union(a: number, b: number): void {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra !== rb) this.parent.set(ra, rb);
  }

  groups(): Map<number, number[]> {
    const groups = new Map<number, number[]>();
    for (const x of this.parent.keys()) {
      const root = this.find(x);
      if (!groups.has(root)) groups.set(root, []);
      groups.get(root)!.push(x);
    }
    return groups;
  }
}

/**
 * Merge vCards that share any email address into single contacts.
 * Returns deduplicated contacts with combined email lists.
 */
function mergeContacts(
  raw: { displayName: string; emails: string[]; categories: string[] }[],
): ParsedContact[] {
  const uf = new UnionFind();
  const emailToVcardIdx = new Map<string, number>();

  // Register all vCards and build union-find based on shared emails
  for (let i = 0; i < raw.length; i++) {
    uf.makeSet(i);
    for (const email of raw[i]!.emails) {
      if (emailToVcardIdx.has(email)) {
        uf.union(i, emailToVcardIdx.get(email)!);
      } else {
        emailToVcardIdx.set(email, i);
      }
    }
  }

  // Build merged contacts from union-find groups
  const groups = uf.groups();
  const merged: ParsedContact[] = [];

  for (const indices of groups.values()) {
    const allEmails = new Set<string>();
    const allCategories = new Set<string>();
    let bestName = "";
    let bestNameLength = 0;

    for (const idx of indices) {
      const vc = raw[idx]!;
      for (const e of vc.emails) allEmails.add(e);
      for (const c of vc.categories) allCategories.add(c);
      // Pick the most detailed display name (longest non-email name)
      if (
        vc.displayName &&
        !vc.displayName.includes("@") &&
        vc.displayName.length > bestNameLength
      ) {
        bestName = vc.displayName;
        bestNameLength = vc.displayName.length;
      }
    }

    // Fallback to email-as-name if no real name found
    if (!bestName) {
      bestName =
        raw[indices[0]!]!.displayName || [...allEmails][0] || "Unknown";
    }

    if (allEmails.size > 0) {
      merged.push({
        displayName: bestName,
        emails: [...allEmails].sort(),
        categories: [...allCategories],
      });
    }
  }

  return merged.sort((a, b) => a.displayName.localeCompare(b.displayName));
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Parse a Google Contacts VCF file and return merged, deduplicated contacts.
 * Only contacts with at least one email address are included.
 */
export function parseContactsVcf(filePath: string): ParsedContact[] {
  const content = readFileSync(filePath, "utf-8");
  const unfoldedLines = unfoldLines(content);

  // Split into vCard blocks
  const rawContacts: { displayName: string; emails: string[]; categories: string[] }[] =
    [];
  let currentBlock: string[] = [];
  let inCard = false;

  for (const line of unfoldedLines) {
    if (line.toUpperCase().startsWith("BEGIN:VCARD")) {
      inCard = true;
      currentBlock = [];
      continue;
    }
    if (line.toUpperCase().startsWith("END:VCARD")) {
      if (inCard && currentBlock.length > 0) {
        const parsed = parseVCard(currentBlock);
        if (parsed) rawContacts.push(parsed);
      }
      inCard = false;
      continue;
    }
    if (inCard) {
      currentBlock.push(line);
    }
  }

  return mergeContacts(rawContacts);
}

/**
 * Recursively find all .vcf files under a directory.
 */
function findVcfFiles(dir: string): string[] {
  const results: string[] = [];
  try {
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      try {
        const st = statSync(full);
        if (st.isDirectory()) {
          results.push(...findVcfFiles(full));
        } else if (entry.toLowerCase().endsWith(".vcf")) {
          results.push(full);
        }
      } catch {
        // skip inaccessible entries
      }
    }
  } catch {
    // skip inaccessible dirs
  }
  return results;
}

/**
 * Parse ALL VCF files in a contacts takeout directory and merge them.
 * Google Takeout splits contacts across multiple group VCFs; "All Contacts"
 * is often capped at 1000, so we must read every file to get full coverage.
 */
export function parseContactsTakeout(
  contactsDir: string,
): ParsedContact[] {
  const vcfFiles = findVcfFiles(contactsDir);
  const allRaw: { displayName: string; emails: string[]; categories: string[] }[] = [];

  for (const filePath of vcfFiles) {
    const content = readFileSync(filePath, "utf-8");
    const unfoldedLines = unfoldLines(content);

    let currentBlock: string[] = [];
    let inCard = false;

    for (const line of unfoldedLines) {
      if (line.toUpperCase().startsWith("BEGIN:VCARD")) {
        inCard = true;
        currentBlock = [];
        continue;
      }
      if (line.toUpperCase().startsWith("END:VCARD")) {
        if (inCard && currentBlock.length > 0) {
          const parsed = parseVCard(currentBlock);
          if (parsed) allRaw.push(parsed);
        }
        inCard = false;
        continue;
      }
      if (inCard) {
        currentBlock.push(line);
      }
    }
  }

  return mergeContacts(allRaw);
}
