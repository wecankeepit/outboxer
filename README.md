# outboxer

A personal email timeline system that reconceptualizes email as chat — text
blobs exchanged between identity-resolved contacts over channels (participant
sets) over time.

For project context and background, see
[wecankeepit.org/outboxer](https://wecankeepit.org/outboxer).

## What it does

- Imports email from Google Takeout (MBOX), Google Chat, Google Voice, and SMS
  Backup & Restore XML
- Syncs new email incrementally via the Gmail API
- Syncs Google Contacts and merges identities across email, phone, and name
- Bridges Google Messages (RCS/SMS) via a local Matrix homeserver
- Serves a chat-style web UI for browsing all messaging history by person or
  channel
- Diff-strips quoted replies so each message shows only newly composed content

## Architecture

```
src/
  chat-viewer.ts              HTTP server + embedded web UI (single-file, no framework)
  import-gmail-takeout.ts     4-phase Takeout import pipeline
  import-google-chat.ts       Google Chat/Hangouts Takeout → DB
  import-google-voice.ts      Google Voice Takeout → DB
  import-sms-backup.ts        SMS Backup & Restore XML → DB
  sync-gmail.ts               Gmail API incremental sync (daemon-capable)
  sync-google-contacts.ts     Google People API contacts sync (daemon-capable)
  sync-gmessages.ts           Matrix bridge → DB (Google Messages via mautrix)
  lib/
    diff-strip.ts             Email body diff-stripping engine
    google-auth.ts            Unified Google OAuth via gog CLI
    parse-contacts-vcf.ts     Google Contacts VCF parser with Union-Find merging
    user-config.ts            Owner email addresses (EDIT THIS FIRST)

scripts/
  chat-viewer-daemon.sh       launchd wrapper for the chat viewer
  health-check.sh             Health check for all services
  sync-contacts-daemon.sh     launchd wrapper for contacts sync
  sync-gmail-daemon.sh        launchd wrapper for Gmail sync
  sync-gmessages-daemon.sh    launchd wrapper for Google Messages sync
```

## Prerequisites

- **Node.js 22+** (required for built-in `node:sqlite`)
- **gog CLI** ([gogcli](https://github.com/pterm/gogcli)) for Google OAuth
  token management
- **Google Cloud project** with Gmail API and People API enabled, with a
  Desktop-type OAuth client
- **Google Takeout** export (for initial bootstrap import)

## Setup

1. **Clone and install:**
   ```bash
   git clone https://github.com/wecankeepit/outboxer.git
   cd outboxer
   npm install
   ```

2. **Configure your identity:**
   Edit `src/lib/user-config.ts` and replace the placeholder emails with all
   email addresses you have used. The system uses these to detect which side of
   each conversation is "you."

3. **Set up Google OAuth via gog:**
   ```bash
   # Place your OAuth client JSON at:
   #   ~/Library/Application Support/gogcli/credentials.json
   # Then authorize:
   gog auth add your-email@gmail.com --services gmail,contacts
   ```

4. **Prepare data directory:**
   ```bash
   mkdir -p ~/.outboxer/takeout/sources/contacts
   mkdir -p ~/.outboxer/takeout/sources/gmail
   ```
   Place your Google Takeout VCF files in `sources/contacts/` and the Gmail
   MBOX file in `sources/gmail/`.

5. **Run the initial import:**
   ```bash
   npm run import       # 4-phase pipeline: contacts → MBOX → diff-strip → prune
   ```

6. **Start the viewer:**
   ```bash
   npm run viewer       # http://localhost:3847
   ```

7. **Optional — enable incremental sync daemons:**
   The `scripts/` directory contains launchd wrapper scripts for macOS.
   Adjust paths and install as launchd agents for automatic syncing.

## Tech Stack

- **Runtime:** Node.js 22+ (for `node:sqlite` built-in)
- **Language:** TypeScript (ESM)
- **Database:** SQLite via `node:sqlite`
- **Email parsing:** `mailparser` (RFC 2822 / MBOX)
- **Google APIs:** `googleapis` (Gmail, People)
- **Dev tooling:** `tsx` for TypeScript execution
- **Frontend:** Vanilla HTML/CSS/JS (embedded in chat-viewer.ts — no build step)

## Legal / Copyright

This snapshot is released under
[CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/).

Knowledge lives in relationship, not ownership.
