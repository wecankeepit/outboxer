/**
 * Centralized user identity configuration.
 * All import and sync scripts use this single list to detect
 * which email addresses belong to the Outboxer owner.
 *
 * Replace these with your own email addresses.
 */
export const SEED_EMAILS: readonly string[] = [
  "your-email@gmail.com",
  // Add all email addresses you've used over time here.
  // The importer uses these to detect which messages were sent by you
  // vs. received from others.
  //
  // Examples:
  //   "your.name@gmail.com",
  //   "your.name@work.edu",
  //   "your.name@company.com",
  //   "oldalias@yahoo.com",
];

/**
 * Institution-specific email handles used for the auto-forward pipeline.
 * Messages FROM these handles populate the institution Address Table.
 * Messages FROM these handles TO only user emails are treated as
 * auto-forwarded incoming mail whose body contains the real envelope.
 *
 * Leave empty if you don't have an auto-forward pipeline.
 */
export const HOPKINS_HANDLES: readonly string[] = [
  // "your.name@institution.edu",
];
