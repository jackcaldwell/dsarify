/**
 * Filter extracted messages to match Purview-style query.
 *
 * Query: (John) AND (performance OR training OR discipline OR instruction
 *        OR capability OR development OR operational)
 *
 * Case-insensitive. Searches subject, body, sender, and recipients.
 */

const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json",
);
const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "filtered-messages.json",
);

// Must contain "John" (case-insensitive)
const REQUIRED_TERM = "john";

// Must contain at least one of these (case-insensitive)
const OR_TERMS = [
  "performance",
  "training",
  "discipline",
  "instruction",
  "capability",
  "development",
  "operational",
];

function getSearchableText(msg) {
  const parts = [
    msg.subject || "",
    msg.body || "",
    msg.sender?.name || "",
    msg.sender?.email || "",
    msg.recipients?.to || "",
    msg.recipients?.cc || "",
    msg.recipients?.bcc || "",
  ];
  return parts.join(" ").toLowerCase();
}

function matchesFilter(msg) {
  const text = getSearchableText(msg);
  if (!text.includes(REQUIRED_TERM)) return false;
  return OR_TERMS.some((term) => text.includes(term));
}

function filter() {
  console.log("Loading extracted messages...");
  const raw = fs.readFileSync(INPUT_PATH, "utf8");
  const data = JSON.parse(raw);

  const messages = data.messages || [];
  console.log(`Total messages: ${messages.length.toLocaleString()}`);

  const filtered = messages.filter(matchesFilter);
  console.log(`Matched query: ${filtered.length.toLocaleString()}`);

  const output = {
    metadata: {
      ...data.metadata,
      filter: {
        required: REQUIRED_TERM,
        anyOf: OR_TERMS,
        description:
          "(John) AND (performance OR training OR discipline OR instruction OR capability OR development OR operational)",
        appliedAt: new Date().toISOString(),
        totalBefore: messages.length,
        totalAfter: filtered.length,
      },
    },
    messages: filtered,
  };

  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));
  console.log(`\nWrote: ${OUTPUT_PATH}`);
  return output;
}

if (require.main === module) {
  filter();
}

module.exports = { filter, matchesFilter, REQUIRED_TERM, OR_TERMS };
