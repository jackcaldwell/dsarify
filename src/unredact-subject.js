/**
 * Unredact data subject references in redacted messages.
 *
 * Replaces (case-insensitive):
 *   [REDACTED NAME]  -> data subject name (John Gaskell)
 *   [REDACTED EMAIL] -> data subject email (john@freightlink.co.uk)
 *   [REDACTED]       -> in sender.name only: name; in sender.email only: email
 *
 * Use after redaction when the data subject was incorrectly redacted in
 * subject, body, or structured fields.
 */

const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redacted-filtered-messages.json",
);
const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redacted-filtered-messages-unredacted-subject.json",
);

// Must match redact-v2.js DATA_SUBJECT
const DATA_SUBJECT = {
  name: "John Gaskell",
  email: "john@freightlink.co.uk",
};

// Case-insensitive placeholder patterns (global so we replace all occurrences)
const PLACEHOLDER_NAME = /\[REDACTED\s+NAME\]/gi;
const PLACEHOLDER_EMAIL = /\[REDACTED\s+EMAIL\]/gi;

function unredactText(text) {
  if (typeof text !== "string") return text;
  return text
    .replace(PLACEHOLDER_NAME, DATA_SUBJECT.name)
    .replace(PLACEHOLDER_EMAIL, DATA_SUBJECT.email);
}

function isPlaceholderOnly(value, kind) {
  if (typeof value !== "string") return false;
  const v = value.trim();
  if (v === "[REDACTED]") return true;
  if (kind === "name" && /^\[REDACTED\s+NAME\]$/i.test(v)) return true;
  if (kind === "email" && /^\[REDACTED\s+EMAIL\]$/i.test(v)) return true;
  return false;
}

function unredactMessage(msg) {
  const out = { ...msg };

  // Subject and body: replace all [REDACTED NAME] / [REDACTED EMAIL]
  if (out.subject != null) out.subject = unredactText(out.subject);
  if (out.body != null) out.body = unredactText(out.body);

  // Sender: unredact name/email placeholders (including bare [REDACTED] in that field)
  if (out.sender) {
    out.sender = { ...out.sender };
    if (out.sender.name != null) {
      out.sender.name = isPlaceholderOnly(out.sender.name, "name")
        ? DATA_SUBJECT.name
        : unredactText(out.sender.name);
    }
    if (out.sender.email != null) {
      out.sender.email = isPlaceholderOnly(out.sender.email, "email")
        ? DATA_SUBJECT.email
        : unredactText(out.sender.email);
    }
  }

  // Recipients: replace placeholders in to/cc/bcc text (may contain multiple addresses)
  if (out.recipients) {
    out.recipients = { ...out.recipients };
    if (out.recipients.to != null) out.recipients.to = unredactText(out.recipients.to);
    if (out.recipients.cc != null) out.recipients.cc = unredactText(out.recipients.cc);
    if (out.recipients.bcc != null) out.recipients.bcc = unredactText(out.recipients.bcc);
  }

  return out;
}

function unredact() {
  const inputPath = process.env.UNREDACT_INPUT
    ? path.resolve(process.cwd(), process.env.UNREDACT_INPUT)
    : INPUT_PATH;
  const outputPath = process.env.UNREDACT_OUTPUT
    ? path.resolve(process.cwd(), process.env.UNREDACT_OUTPUT)
    : OUTPUT_PATH;

  console.log("Loading redacted messages...");
  const raw = fs.readFileSync(inputPath, "utf8");
  const data = JSON.parse(raw);

  const messages = data.messages || [];
  console.log(`Processing ${messages.length.toLocaleString()} messages...`);

  const unredactedMessages = messages.map(unredactMessage);

  const output = {
    ...data,
    metadata: {
      ...data.metadata,
      unredactSubject: {
        dataSubject: DATA_SUBJECT,
        appliedAt: new Date().toISOString(),
        description: "Unredacted case-insensitive [REDACTED NAME] / [REDACTED EMAIL] for data subject",
      },
    },
    messages: unredactedMessages,
  };

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  console.log(`\nWrote: ${outputPath}`);
  return output;
}

if (require.main === module) {
  unredact();
}

module.exports = { unredact, unredactMessage, DATA_SUBJECT };
