/**
 * google-auth.ts — Unified Google OAuth via gog CLI.
 *
 * Both sync-gmail.ts and sync-google-contacts.ts use this module instead of
 * maintaining separate OAuth tokens. The gog CLI (gogcli) stores a single
 * broad-scoped refresh token for your-email@gmail.com in an encrypted
 * keyring. Outboxer exports that token at sync time and uses it with the
 * googleapis library. No per-pipeline --auth flows, no competing tokens.
 *
 * Prerequisites:
 *   - gog installed: /opt/homebrew/bin/gog
 *   - Authorized:    gog auth add your-email@gmail.com --services gmail,contacts,...
 *   - Client creds:  ~/Library/Application Support/gogcli/credentials.json
 */

import { google } from "googleapis";
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import { tmpdir } from "node:os";

const GOG_BIN = process.env.GOG_BIN || "/opt/homebrew/bin/gog";
const GOG_ACCOUNT = process.env.GOG_ACCOUNT || "your-email@gmail.com";
const GOG_CREDS_PATH =
  process.env.OUTBOXER_GOOGLE_CLIENT ||
  join(homedir(), "Library", "Application Support", "gogcli", "credentials.json");

export class GoogleAuthError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "GoogleAuthError";
  }
}

function loadClientCredentials(): { client_id: string; client_secret: string } {
  if (!existsSync(GOG_CREDS_PATH)) {
    throw new GoogleAuthError(
      `OAuth client credentials not found: ${GOG_CREDS_PATH}\n` +
        "Install gog (gogcli) or set OUTBOXER_GOOGLE_CLIENT to the Desktop app client JSON.",
    );
  }
  return JSON.parse(readFileSync(GOG_CREDS_PATH, "utf-8"));
}

function exportGogRefreshToken(): string {
  const tmpFile = join(tmpdir(), `outboxer-gog-${process.pid}-${Date.now()}.json`);
  try {
    execFileSync(GOG_BIN, ["auth", "tokens", "export", GOG_ACCOUNT, "--out", tmpFile, "--overwrite"], {
      stdio: ["ignore", "pipe", "pipe"],
      timeout: 15_000,
    });
    const data = JSON.parse(readFileSync(tmpFile, "utf-8"));
    if (!data.refresh_token) {
      throw new GoogleAuthError(
        `gog token export for ${GOG_ACCOUNT} has no refresh_token. Re-authorize with:\n` +
          `  gog auth add ${GOG_ACCOUNT} --services gmail,contacts`,
      );
    }
    return data.refresh_token as string;
  } catch (err: unknown) {
    if (err instanceof GoogleAuthError) throw err;
    throw new GoogleAuthError(
      `Failed to export gog token for ${GOG_ACCOUNT}: ${err instanceof Error ? err.message : String(err)}\n` +
        `Ensure gog is installed (${GOG_BIN}) and account is authorized:\n` +
        `  gog auth add ${GOG_ACCOUNT} --services gmail,contacts`,
    );
  } finally {
    try { unlinkSync(tmpFile); } catch { /* best effort */ }
  }
}

/**
 * Returns a ready-to-use googleapis OAuth2 client backed by gog's refresh token.
 * The library handles access-token refresh transparently.
 */
export function createGoogleAuth(): InstanceType<typeof google.auth.OAuth2> {
  const creds = loadClientCredentials();
  const refreshToken = exportGogRefreshToken();

  const oauth2 = new google.auth.OAuth2(creds.client_id, creds.client_secret);
  oauth2.setCredentials({ refresh_token: refreshToken });
  return oauth2;
}
