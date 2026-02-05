/**
 * Deduplicate messages by body (hash-based).
 * Keeps the first occurrence of each unique body; drops later duplicates.
 *
 * Body is normalized (trim + normalize line endings) before hashing so
 * "hello\r\n" and "hello\n" are treated as the same.
 */

const crypto = require("crypto");
const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "filtered-messages.json",
);
const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "filtered-messages-deduped.json",
);

function normalizeBody(body) {
  if (body == null) return "";
  return String(body)
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .trim();
}

function hashBody(body) {
  const normalized = normalizeBody(body);
  return crypto.createHash("sha256").update(normalized, "utf8").digest("hex");
}

function dedupe() {
  const inputPath = process.env.DEDUP_INPUT
    ? path.resolve(process.cwd(), process.env.DEDUP_INPUT)
    : INPUT_PATH;
  const outputPath = process.env.DEDUP_OUTPUT
    ? path.resolve(process.cwd(), process.env.DEDUP_OUTPUT)
    : OUTPUT_PATH;

  console.log("Loading messages...");
  const raw = fs.readFileSync(inputPath, "utf8");
  const data = JSON.parse(raw);

  const messages = data.messages || [];
  console.log(`Total messages: ${messages.length.toLocaleString()}`);

  const seen = new Set();
  const deduped = [];

  for (const msg of messages) {
    const key = hashBody(msg.body);
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(msg);
  }

  const removed = messages.length - deduped.length;
  console.log(`Unique (by body): ${deduped.length.toLocaleString()}`);
  console.log(`Duplicates removed: ${removed.toLocaleString()}`);

  const output = {
    ...data,
    metadata: {
      ...data.metadata,
      deduplication: {
        appliedAt: new Date().toISOString(),
        totalBefore: messages.length,
        totalAfter: deduped.length,
        duplicatesRemoved: removed,
        key: "body (SHA-256 hash, normalized)",
      },
    },
    messages: deduped,
  };

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  console.log(`\nWrote: ${outputPath}`);
  return output;
}

if (require.main === module) {
  dedupe();
}

module.exports = { dedupe, hashBody, normalizeBody };
