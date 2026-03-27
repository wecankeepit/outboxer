/**
 * diff-strip.ts
 *
 * Diff-based email body stripping pipeline.
 *
 * Reconceptualizes email as chat by extracting only the NEW content each
 * sender composed, using chronological diffing within a channel.
 *
 * Pipeline (per channel, messages sorted by date):
 *   1. **Beautify**: Remove ">" quote markers, decode HTML entities, strip
 *      signature delimiters, normalize line endings and whitespace.
 *   2. **Paragraph split**: Break into paragraphs (separated by blank lines).
 *   3. **Incremental diff**: Compare each paragraph against a growing corpus
 *      of all previously seen paragraphs in the channel. Only paragraphs NOT
 *      in the corpus are "new" content.
 *   4. **Build blob**: Subject line + new paragraphs → final text blob.
 *
 * Fuzzy matching: paragraphs are compared after aggressive normalization
 * (lowercase, strip non-alphanumeric, collapse whitespace) so minor
 * mutations from quoting/reformatting are still caught.
 */

import { createHash } from "node:crypto";

// ─── HTML Entity Decoding ────────────────────────────────────────────────────

const HTML_ENTITIES: Record<string, string> = {
  "&amp;": "&",
  "&lt;": "<",
  "&gt;": ">",
  "&quot;": '"',
  "&#39;": "'",
  "&apos;": "'",
  "&nbsp;": " ",
  "&ndash;": "–",
  "&mdash;": "—",
  "&hellip;": "…",
  "&laquo;": "«",
  "&raquo;": "»",
  "&copy;": "©",
  "&reg;": "®",
  "&trade;": "™",
};

function decodeHtmlEntities(text: string): string {
  // Named entities
  let result = text.replace(
    /&[a-zA-Z]+;/g,
    (match) => HTML_ENTITIES[match.toLowerCase()] ?? match,
  );
  // Numeric entities: &#123; or &#x1F;
  result = result.replace(/&#(\d+);/g, (_, num) =>
    String.fromCodePoint(parseInt(num, 10)),
  );
  result = result.replace(/&#x([0-9a-fA-F]+);/g, (_, hex) =>
    String.fromCodePoint(parseInt(hex, 16)),
  );
  return result;
}

// ─── Signature Patterns ──────────────────────────────────────────────────────

const SIGNATURE_STARTERS: RegExp[] = [
  /^-- ?$/, // Standard sig delimiter
  /^Sent from my /i,
  /^Sent from Mail for /i,
  /^Sent via /i,
  /^Get Outlook for /i,
  /^Envoyé de mon /i,
  /^Enviado desde mi /i,
  /^Sent from Yahoo Mail/i,
  /^Sent from Windows Mail/i,
];

/** Attribution / quote start lines. */
const ATTRIBUTION_PATTERNS: RegExp[] = [
  /^On .{10,} wrote:\s*$/,
  /^On .{10,}:\s*$/,
  /^-{3,}\s*Original Message\s*-{3,}/i,
  /^-{3,}\s*Forwarded message\s*-{3,}/i,
  /^[_-]{5,}\s*$/,
];

/** Email header line (part of an Outlook-style quoted header block). */
const EMAIL_HEADER_RE = /^(From|To|Cc|Bcc|Subject|Date|Sent):\s*.+/i;

// ─── Beautification ─────────────────────────────────────────────────────────

/**
 * Beautify a raw email body into clean plaintext:
 *  - Strip all leading ">" quote markers (nested too)
 *  - Decode HTML entities
 *  - Normalize line endings
 *  - Join hard-wrapped lines into natural paragraphs
 *  - Collapse excessive whitespace
 *  - Strip signature blocks and attribution lines
 */
export function beautify(body: string): string {
  if (!body) return "";

  let text = body;

  // Normalize line endings
  text = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

  // Decode HTML entities
  text = decodeHtmlEntities(text);

  // Remove HTML tags that sometimes leak into "text" parts
  text = text.replace(/<br\s*\/?>/gi, "\n");
  text = text.replace(/<\/?(p|div|blockquote|span|font|b|i|u|em|strong)[^>]*>/gi, "");
  text = text.replace(/<[^>]+>/g, ""); // catch-all for remaining tags

  // Process line by line
  const lines = text.split("\n");
  const cleaned: string[] = [];
  let sigHit = false;

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i]!;

    // Strip leading ">" quote markers (handle nested "> > >", ">>>", "> >>", etc.)
    line = line.replace(/^(?:\s*>)+\s?/, "");

    const trimmed = line.trim();

    // Stop at signature delimiters
    if (!sigHit && SIGNATURE_STARTERS.some((p) => p.test(trimmed))) {
      sigHit = true;
      continue;
    }
    if (sigHit) continue;

    // Stop at attribution lines (these start quoted blocks below)
    if (ATTRIBUTION_PATTERNS.some((p) => p.test(trimmed))) {
      break;
    }

    // Stop at email header blocks (Outlook-style quoted headers)
    if (isEmailHeaderBlockStart(lines, i)) {
      break;
    }

    cleaned.push(line);
  }

  // Join hard-wrapped lines into paragraphs
  const joined = joinHardWrapped(cleaned);

  // Collapse excessive blank lines
  return joined
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

/**
 * Detect an email header block (Outlook/Hotmail-style quoted headers).
 * 3+ consecutive header-like lines = header block.
 */
function isEmailHeaderBlockStart(lines: string[], idx: number): boolean {
  if (idx >= lines.length) return false;
  // Strip quote markers before checking
  const clean = (l: string) => l.replace(/^(?:\s*>)+\s?/, "").trim();
  const first = clean(lines[idx]!);
  if (!EMAIL_HEADER_RE.test(first)) return false;

  let headerCount = 1;
  let blankGap = 0;
  for (let i = idx + 1; i < Math.min(idx + 8, lines.length); i++) {
    const l = clean(lines[i]!);
    if (!l) {
      blankGap++;
      if (blankGap > 1) break;
      continue;
    }
    if (EMAIL_HEADER_RE.test(l)) {
      headerCount++;
      blankGap = 0;
    } else {
      break;
    }
  }
  return headerCount >= 3;
}

/**
 * Join lines that were hard-wrapped by email clients (typically at 72-78 chars).
 *
 * Heuristic: if a line doesn't end with sentence-ending punctuation and the
 * next non-empty line starts with a lowercase letter, join them with a space.
 * Blank lines are preserved as paragraph separators.
 */
function joinHardWrapped(lines: string[]): string {
  const result: string[] = [];
  let buffer = "";

  for (const line of lines) {
    const trimmed = line.trim();

    // Blank line → flush buffer, preserve paragraph break
    if (!trimmed) {
      if (buffer) {
        result.push(buffer);
        buffer = "";
      }
      result.push("");
      continue;
    }

    if (!buffer) {
      buffer = trimmed;
      continue;
    }

    // Decide whether to join with previous line
    const prevEndsClean =
      /[.!?:;)\]"'»]$/.test(buffer) || // Ends with terminal punctuation
      buffer.length < 40; // Very short line = likely intentional break

    const currStartsContinuation =
      /^[a-z]/.test(trimmed) || // Starts lowercase = continuation
      /^[,;]/.test(trimmed); // Starts with continuation punctuation

    if (!prevEndsClean && currStartsContinuation && buffer.length >= 60) {
      // Likely a hard-wrap continuation
      buffer += " " + trimmed;
    } else {
      // New line/paragraph
      result.push(buffer);
      buffer = trimmed;
    }
  }

  if (buffer) result.push(buffer);

  return result.join("\n");
}

// ─── Paragraph Diffing ──────────────────────────────────────────────────────

/**
 * Aggressively normalize a paragraph for fuzzy comparison:
 *  - lowercase
 *  - strip all non-alphanumeric characters
 *  - collapse whitespace
 */
function normalizeParagraph(paragraph: string): string {
  return paragraph
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/** Hash a normalized paragraph for corpus lookup. */
function hashParagraph(paragraph: string): string {
  const normalized = normalizeParagraph(paragraph);
  if (!normalized) return "";
  return createHash("md5").update(normalized).digest("hex");
}

/**
 * A corpus of known paragraphs for a channel, built incrementally.
 * Uses fuzzy paragraph hashing to catch minor mutations.
 */
export class ParagraphCorpus {
  private seen = new Set<string>();

  /** Check if a paragraph (or close variant) was already seen. */
  has(paragraph: string): boolean {
    const hash = hashParagraph(paragraph);
    if (!hash) return true; // empty paragraphs are always "known"
    return this.seen.has(hash);
  }

  /** Add a paragraph to the corpus. */
  add(paragraph: string): void {
    const hash = hashParagraph(paragraph);
    if (hash) this.seen.add(hash);
  }

  /** Add multiple paragraphs. */
  addAll(paragraphs: string[]): void {
    for (const p of paragraphs) this.add(p);
  }

  get size(): number {
    return this.seen.size;
  }
}

/**
 * Split text into paragraphs (separated by blank lines).
 * Trims each paragraph and drops empties.
 */
export function splitParagraphs(text: string): string[] {
  return text
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
}

// ─── Public API ──────────────────────────────────────────────────────────────

/**
 * Extract new content from an email body using the diff-based pipeline.
 *
 * @param body     Raw plain-text email body
 * @param corpus   Growing corpus of previously seen paragraphs in the channel
 * @returns        Only the new paragraphs the sender composed
 */
export function diffStripMessage(
  body: string,
  corpus: ParagraphCorpus,
): { newText: string; allParagraphs: string[] } {
  // Phase 1: Beautify
  const beautified = beautify(body);

  // Phase 2: Split into paragraphs
  const paragraphs = splitParagraphs(beautified);

  // Phase 3: Diff against corpus
  const newParagraphs = paragraphs.filter((p) => !corpus.has(p));

  // Phase 4: Add ALL paragraphs to corpus (so future messages catch them)
  corpus.addAll(paragraphs);

  return {
    newText: newParagraphs.join("\n\n"),
    allParagraphs: paragraphs,
  };
}

/**
 * Build the final message blob: subject as first line + new content.
 */
export function buildBlob(subject: string, newText: string): string {
  const sub = subject.trim();
  if (!newText) return sub;
  return `${sub}\n\n${newText}`;
}

/**
 * Process a channel's messages chronologically, applying diff-based stripping.
 *
 * @param messages  Array of { id, body, subject } sorted by date ASC
 * @returns         Map of message id → stripped blob
 */
export function diffStripChannel(
  messages: { id: number; body: string; subject: string }[],
): Map<number, string> {
  const corpus = new ParagraphCorpus();
  const blobs = new Map<number, string>();

  for (const msg of messages) {
    const { newText } = diffStripMessage(msg.body, corpus);
    blobs.set(msg.id, buildBlob(msg.subject, newText));
  }

  return blobs;
}
