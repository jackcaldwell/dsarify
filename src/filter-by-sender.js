/**
 * Further filter messages by sender and body.
 *
 * Keeps only messages where:
 * - Sender name is one of: Mark Stephens, Stephanie Newsham, Jason Mayor (case-insensitive)
 * - Body contains "John" (case-insensitive)
 * - Body contains at least one keyword: performance, training, discipline,
 *   instruction, capability, development, operational (case-insensitive)
 *
 * Default input: output/filtered-messages.json
 */

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
  "filtered-messages-by-sender.json",
);

// Sender must be one of these (case-insensitive match on display name)
const ALLOWED_SENDER_NAMES = [
  "Mark Stephens",
  "Stephanie Newsham",
  "Jason Mayor",
];
const ALLOWED_SENDERS_LOWER = ALLOWED_SENDER_NAMES.map((s) =>
  s.trim().toLowerCase()
);

// Body must contain "John" (case-insensitive)
const REQUIRED_TERM = "john";

// Body must contain at least one of these (case-insensitive)
const KEYWORDS = [
  "performance",
  "training",
  "discipline",
  "instruction",
  "capability",
  "development",
  "operational",
];

function senderAllowed(msg) {
  const name = (msg.sender?.name || "").trim().toLowerCase();
  if (!name) return false;
  return ALLOWED_SENDERS_LOWER.some((allowed) => name === allowed);
}

function bodyHasJohn(body) {
  return (body || "").toLowerCase().includes(REQUIRED_TERM);
}

function bodyHasKeyword(body) {
  const lower = (body || "").toLowerCase();
  return KEYWORDS.some((kw) => lower.includes(kw));
}

function matchesFilter(msg) {
  if (!senderAllowed(msg)) return false;
  const body = msg.body || "";
  if (!bodyHasJohn(body)) return false;
  if (!bodyHasKeyword(body)) return false;
  return true;
}

function filter() {
  const inputPath = process.env.FILTER_SENDER_INPUT
    ? path.resolve(process.cwd(), process.env.FILTER_SENDER_INPUT)
    : INPUT_PATH;
  const outputPath = process.env.FILTER_SENDER_OUTPUT
    ? path.resolve(process.cwd(), process.env.FILTER_SENDER_OUTPUT)
    : OUTPUT_PATH;

  console.log("Loading messages...");
  const raw = fs.readFileSync(inputPath, "utf8");
  const data = JSON.parse(raw);

  const messages = data.messages || [];
  console.log(`Total messages: ${messages.length.toLocaleString()}`);
  console.log(`Allowed senders: ${ALLOWED_SENDER_NAMES.join(", ")}`);
  console.log(`Body must contain "John" and one of: ${KEYWORDS.join(", ")}`);

  const filtered = messages.filter(matchesFilter);
  console.log(`Matched: ${filtered.length.toLocaleString()}`);

  const output = {
    ...data,
    metadata: {
      ...data.metadata,
      filterBySender: {
        allowedSenders: ALLOWED_SENDER_NAMES,
        requiredInBody: REQUIRED_TERM,
        keywordsInBody: KEYWORDS,
        appliedAt: new Date().toISOString(),
        totalBefore: messages.length,
        totalAfter: filtered.length,
      },
    },
    messages: filtered,
  };

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2));
  console.log(`\nWrote: ${outputPath}`);
  return output;
}

if (require.main === module) {
  filter();
}

module.exports = {
  filter,
  matchesFilter,
  ALLOWED_SENDER_NAMES,
  REQUIRED_TERM,
  KEYWORDS,
};
