require("dotenv").config({
  path: require("path").resolve(__dirname, "..", ".env"),
});

const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");

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
  "redacted-messages.json",
);
const CHECKPOINT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redaction-checkpoint.json",
);
const AUDIT_PATH = path.resolve(__dirname, "..", "output", "audit-log.json");

// Configuration
const BATCH_SIZE = 1; // Messages per API call
const CONCURRENCY = 50; // Parallel API calls
const MODEL = "gpt-4o-mini";
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const MAX_MESSAGES_TO_PROCESS = 100; // Set to a number to limit (for testing)

// Data subject - should NOT be redacted
const DATA_SUBJECT = {
  name: "John Gaskell",
  email: "john@freightlink.co.uk",
  variations: ["john gaskell", "john", "gaskell", "j. gaskell", "j gaskell"],
};

// Redaction stages - process in order
const REDACTION_STAGES = [
  {
    id: "names",
    name: "Third-Party Names",
    description:
      "Identify and redact all third-party names in headers and body",
  },
  {
    id: "companies",
    name: "Company Names",
    description: "Identify and redact all company/business names",
  },
  {
    id: "contact",
    name: "Contact Information",
    description: "Identify and redact emails, phone numbers, and addresses",
  },
];

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

/**
 * Sleep utility for retries
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Format duration in human readable format
 */
function formatDuration(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600)
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

/**
 * Check if a string is related to the data subject
 */
function isDataSubjectRelated(text) {
  if (!text) return false;
  const lower = text.toLowerCase().trim();
  return (
    DATA_SUBJECT.variations.some((v) => lower.includes(v.toLowerCase())) ||
    lower === DATA_SUBJECT.email.toLowerCase() ||
    lower.includes(DATA_SUBJECT.email.toLowerCase())
  );
}

/**
 * Escape special regex characters
 */
function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Load checkpoint if exists
 */
function loadCheckpoint() {
  if (fs.existsSync(CHECKPOINT_PATH)) {
    try {
      console.log(`  Loading checkpoint from: ${CHECKPOINT_PATH}`);
      const checkpointContent = fs.readFileSync(CHECKPOINT_PATH, "utf-8");
      console.log(
        `  Checkpoint file size: ${(checkpointContent.length / 1024 / 1024).toFixed(2)} MB`,
      );

      const checkpoint = JSON.parse(checkpointContent);

      const redactedCount = checkpoint.redactedMessages?.length || 0;
      const auditCount = checkpoint.auditLog?.length || 0;

      console.log(`  ✓ Checkpoint loaded successfully:`);
      console.log(
        `    - Processed: ${checkpoint.processedCount?.toLocaleString() || 0} messages`,
      );
      console.log(
        `    - Current stage: ${checkpoint.currentStage || "unknown"}`,
      );
      console.log(`    - Redacted messages: ${redactedCount.toLocaleString()}`);
      console.log(`    - Audit entries: ${auditCount.toLocaleString()}`);
      console.log(`    - Started: ${checkpoint.startedAt || "unknown"}`);

      // Validate checkpoint structure
      if (
        !checkpoint.redactedMessages ||
        !Array.isArray(checkpoint.redactedMessages)
      ) {
        console.log(
          `  ⚠ Warning: Checkpoint missing redactedMessages array, initializing...`,
        );
        checkpoint.redactedMessages = [];
      }
      if (!checkpoint.auditLog || !Array.isArray(checkpoint.auditLog)) {
        console.log(
          `  ⚠ Warning: Checkpoint missing auditLog array, initializing...`,
        );
        checkpoint.auditLog = [];
      }
      if (typeof checkpoint.processedCount !== "number") {
        console.log(
          `  ⚠ Warning: Checkpoint missing processedCount, initializing...`,
        );
        checkpoint.processedCount = 0;
      }
      if (!checkpoint.currentStage) {
        checkpoint.currentStage = REDACTION_STAGES[0].id;
      }

      return checkpoint;
    } catch (err) {
      console.error(`  ✗ ERROR: Could not load checkpoint: ${err.message}`);
      console.log(`  Starting fresh (checkpoint file may be corrupted)`);
      if (fs.existsSync(CHECKPOINT_PATH)) {
        const backupPath = CHECKPOINT_PATH + ".corrupted." + Date.now();
        try {
          fs.copyFileSync(CHECKPOINT_PATH, backupPath);
          console.log(`  Backed up corrupted checkpoint to: ${backupPath}`);
        } catch (backupErr) {
          console.log(
            `  Could not backup corrupted checkpoint: ${backupErr.message}`,
          );
        }
      }
    }
  } else {
    console.log(`  No checkpoint found, starting fresh`);
  }

  return {
    processedCount: 0,
    redactedMessages: [],
    auditLog: [],
    startedAt: new Date().toISOString(),
    currentStage: REDACTION_STAGES[0].id,
  };
}

/**
 * Mutex for checkpoint writes to ensure thread safety
 */
class CheckpointMutex {
  constructor() {
    this.queue = [];
    this.locked = false;
  }

  async acquire() {
    return new Promise((resolve) => {
      if (!this.locked) {
        this.locked = true;
        resolve();
      } else {
        this.queue.push(resolve);
      }
    });
  }

  release() {
    if (this.queue.length > 0) {
      const next = this.queue.shift();
      next();
    } else {
      this.locked = false;
    }
  }
}

const checkpointMutex = new CheckpointMutex();

/**
 * Build system prompt for a specific redaction stage
 */
function buildSystemPrompt(stageId) {
  const basePrompt = `You are an EXTREMELY STRICT GDPR compliance assistant for a UK Data Subject Access Request (DSAR). Your job is to identify ALL third-party personal data and sensitive information that must be redacted.

NOTE: Some pattern-based items (emails, phones, reference numbers) may have been pre-processed, but you should still identify ANY you find to ensure nothing is missed.

CRITICAL: FALSE POSITIVES ARE PREFERRED OVER FALSE NEGATIVES. When in ANY doubt, REDACT IT.

DATA SUBJECT (the person requesting their data - DO NOT REDACT):
- Name: ${DATA_SUBJECT.name}
- Email: ${DATA_SUBJECT.email}
- Also keep: "John", "Gaskell", "J. Gaskell"

CRITICAL RULES - BE EXTREMELY AGGRESSIVE WITH REDACTION:`;

  if (stageId === "names") {
    return `${basePrompt}

FOCUS: Identify ALL third-party names that must be redacted.

1. REDACT ALL THIRD-PARTY NAMES including:
   - Full names (e.g., "Sarah Smith", "Fiona Martin")
   - First names ALONE (e.g., "Hi Romey", "Thanks Megan", "Dear Jason")
   - Names in signatures (e.g., "Kind Regards, Megan")
   - Names in parentheses (e.g., "(Sean)", "(John Smith)", "(Megan)")
   - Names with titles (e.g., "Mr. Smith", "Dr. Jones")
   - ANY name that could identify a person (when in doubt, redact)

2. DO NOT REDACT:
   - "${DATA_SUBJECT.name}" or "John" or "Gaskell" (the data subject)
   - Generic team names ONLY if clearly generic (Customer Services, Logistics Team)
   - Place names ONLY if they are cities/locations (Dublin, Leeds, Preston)
   - Already redacted content (marked as [REDACTED ...])

EXAMPLES OF WHAT TO REDACT:
- "Hi Romey" → redact "Romey"
- "Fiona Martin; Yanislav Ivanov; John Gaskell" → redact "Fiona Martin" and "Yanislav Ivanov"
- "Kind Regards, Megan" → redact "Megan"
- "(Sean)" → redact "Sean"
- "(John Smith)" → redact "John Smith"

RESPONSE FORMAT (JSON):
{
  "results": [
    {
      "messageId": <number>,
      "redactions": [
        {"original": "<exact text>", "type": "name", "field": "body|from|to|cc|subject", "reason": "<why>"}
      ]
    }
  ]
}

IMPORTANT: If you identify a name in the "from", "to", or "cc" fields, include "field": "from" (or "to" or "cc") in your response so it can be properly redacted.

BE EXTREMELY THOROUGH. False positives are acceptable. False negatives are NOT. When in ANY doubt, redact it.`;
  }

  if (stageId === "companies") {
    return `${basePrompt}

FOCUS: Identify ALL company/business names that must be redacted.

1. REDACT ALL COMPANY/BUSINESS NAMES:
   - Company names (e.g., "DHL", "Walkers Transport", "Primeline", "Freightlink")
   - Company names with suffixes: "Ltd.", "Ltd", "Inc.", "Inc", "LLC", "LLP", "PLC", "Corp.", "Corporation", "Limited"
   - Examples: "Haldane Fisher Ltd.", "ABC Company Inc.", "XYZ Corporation"
   - Business entity names
   - Organization names
   - Client company names
   - Supplier company names
   - ANY text that looks like a company name (when in doubt, redact it)

2. DO NOT REDACT:
   - Generic terms like "company", "business", "organization" (standalone)
   - Place names ONLY if they are cities/locations (Dublin, Leeds, Preston)
   - Already redacted content (marked as [REDACTED ...])

EXAMPLES OF WHAT TO REDACT:
- "DHL quoted £500" → redact "DHL"
- "Walkers Transport" → redact entire company name
- "Haldane Fisher Ltd." → redact "Haldane Fisher Ltd." (has Ltd. suffix = company)
- "ABC Company Inc." → redact "ABC Company Inc." (has Inc. suffix = company)

RESPONSE FORMAT (JSON):
{
  "results": [
    {
      "messageId": <number>,
      "redactions": [
        {"original": "<exact text>", "type": "company", "field": "body|subject", "reason": "<why>"}
      ]
    }
  ]
}

BE EXTREMELY THOROUGH. False positives are acceptable. False negatives are NOT. When in ANY doubt, redact it.`;
  }

  if (stageId === "contact") {
    return `${basePrompt}

FOCUS: Identify ALL contact information that must be redacted.

1. REDACT ALL THIRD-PARTY CONTACT INFO:
   - Email addresses (except ${DATA_SUBJECT.email}) - identify ANY you see
   - Phone numbers (mobile and landline) - identify ANY you see
   - Physical addresses of individuals OR businesses - identify ANY you see

2. DO NOT REDACT:
   - "${DATA_SUBJECT.email}"
   - Already redacted content (marked as [REDACTED ...])

EXAMPLES OF WHAT TO REDACT:
- "jennifer.sharpe@example.com" → redact email
- "07527 176522" → redact phone number
- "123 Main Street, London" → redact address
- "Unit 1, GMP House, Ashbourne Industrial Estate" → redact entire address even if split across lines

RESPONSE FORMAT (JSON):
{
  "results": [
    {
      "messageId": <number>,
      "redactions": [
        {"original": "<exact text>", "type": "email|phone|address", "field": "body|from|to|cc|subject", "reason": "<why>"}
      ]
    }
  ]
}

IMPORTANT: Addresses may span multiple lines - look for complete addresses even if split by "\\n".

BE EXTREMELY THOROUGH. False positives are acceptable. False negatives are NOT. When in ANY doubt, redact it.`;
  }

  return basePrompt;
}

/**
 * Build user prompt for a specific stage
 */
function buildUserPrompt(messages, stageId) {
  const messageData = messages.map((msg) => ({
    messageId: msg.id,
    from: `${msg.sender.name || ""} <${msg.sender.email || ""}>`,
    to: msg.recipients.to || "",
    cc: msg.recipients.cc || undefined,
    subject: msg.subject || undefined,
    body: msg.body || "",
  }));

  const messageCount = messages.length;
  const messageText = messageCount === 1 ? "this message" : "these messages";

  if (stageId === "names") {
    return `Analyze ${messageText}. Identify ALL third-party names that need redaction.

CRITICAL - CHECK ALL OF THESE FIELDS:

- "from" field: 
  * Redact ALL names and emails EXCEPT "${DATA_SUBJECT.name}" and "${DATA_SUBJECT.email}"
  * Check both the name and email address
  * Examples: "Sarah Smith <sarah@example.com>" → redact both name and email

- "to" field:
  * Redact ALL names EXCEPT "${DATA_SUBJECT.name}"
  * Parse the field carefully - it may contain multiple names separated by semicolons or commas
  * Examples: "John Gaskell; Sarah Smith; Mike Jones" → redact "Sarah Smith" and "Mike Jones", keep "John Gaskell"

- "cc" field (if present):
  * Redact ALL names EXCEPT "${DATA_SUBJECT.name}"
  * Same parsing rules as "to" field

- "body": 
  * Redact ALL third-party first names, full names
  * Pay special attention to signature blocks (text after "Kind regards", "Best", "Thanks", etc.)
  * CHECK INSIDE PARENTHESES - names often appear in parentheses like "(Sean)", "(John Smith)", "(Megan)"
  * CRITICAL: Names may appear across multiple lines or be split by newlines - look for them even if they span lines
  * Examples to redact: "Jennifer", "Sarah Smith", "(Sean)", "(John Smith)"

IMPORTANT:
- Newlines in the JSON body appear as "\\n" - treat them as line breaks when identifying names
- Recipient fields (to/cc) are critical - check them carefully and redact ALL names except the data subject
- Signature areas (end of message) often contain names - check carefully
- Names in parentheses are common - always check inside ( ) brackets
- When in doubt, redact it - false positives are preferred over false negatives

${messageCount === 1 ? "Message to analyze:" : "Messages to analyze:"}
${JSON.stringify(messageData, null, 2)}`;
  }

  if (stageId === "companies") {
    return `Analyze ${messageText}. Identify ALL company/business names that need redaction.

CRITICAL - CHECK THESE FIELDS:

- "body": 
  * Redact ALL company/business names (DHL, Walkers Transport, Primeline, etc.)
  * Look for company names with suffixes: Ltd., Ltd, Inc., Inc, LLC, LLP, PLC, Corp., Corporation, Limited
  * Examples to redact: "DHL", "Walkers Transport", "Haldane Fisher Ltd.", "ABC Company Inc."

- "subject" (if present):
  * Redact ALL company/business names

IMPORTANT:
- Company names may appear anywhere in the message
- Look for common business suffixes (Ltd., Inc., Corp., etc.) as indicators
- When in doubt, redact it - false positives are preferred over false negatives

${messageCount === 1 ? "Message to analyze:" : "Messages to analyze:"}
${JSON.stringify(messageData, null, 2)}`;
  }

  if (stageId === "contact") {
    return `Analyze ${messageText}. Identify ALL contact information (emails, phone numbers, physical addresses) that need redaction.

CRITICAL - CHECK ALL OF THESE FIELDS:

- "from" field: 
  * Redact ALL emails EXCEPT "${DATA_SUBJECT.email}"
  * Check the email address

- "to" field:
  * Redact ALL emails EXCEPT "${DATA_SUBJECT.email}"
  * Parse the field carefully - it may contain multiple emails separated by semicolons or commas

- "cc" field (if present):
  * Redact ALL emails EXCEPT "${DATA_SUBJECT.email}"
  * Same parsing rules as "to" field

- "body": 
  * Redact ALL email addresses (except ${DATA_SUBJECT.email})
  * Redact ALL phone numbers
  * Redact ALL physical addresses (street addresses, postal addresses)
  * CRITICAL: Addresses may appear across multiple lines or be split by newlines - look for them even if they span lines
  * Examples to redact: "jennifer@example.com", "07527 176522", "123 Main Street"
  * Address example: "Unit 1, GMP House, Ashbourne Industrial Estate" - redact the entire address even if split across lines

IMPORTANT:
- Newlines in the JSON body appear as "\\n" - treat them as line breaks when identifying addresses
- Addresses may span multiple lines - look for complete addresses even if split by "\\n"
- When in doubt, redact it - false positives are preferred over false negatives

${messageCount === 1 ? "Message to analyze:" : "Messages to analyze:"}
${JSON.stringify(messageData, null, 2)}`;
  }

  return `Analyze ${messageText} for redaction.
${JSON.stringify(messageData, null, 2)}`;
}

/**
 * Clean and fix malformed JSON
 */
function cleanJSON(jsonString) {
  if (!jsonString) return "{}";

  // Remove any markdown code blocks
  jsonString = jsonString.replace(/```json\s*/g, "").replace(/```\s*/g, "");

  // Try to fix common JSON issues
  // Fix unescaped quotes in strings
  jsonString = jsonString.replace(
    /([{,]\s*"[^"]*"[^"]*)"([^"]*)"([^"]*":)/g,
    '$1\\"$2\\"$3',
  );

  // Try to fix unterminated strings by finding the end of the JSON object
  const openBraces = (jsonString.match(/{/g) || []).length;
  const closeBraces = (jsonString.match(/}/g) || []).length;

  if (openBraces > closeBraces) {
    // Add missing closing braces
    jsonString += "}".repeat(openBraces - closeBraces);
  }

  return jsonString;
}

/**
 * Call OpenAI API for a specific redaction stage
 */
async function callOpenAI(messages, stageId, retryCount = 0, batchId = null) {
  const batchLabel = batchId !== null ? `[Batch ${batchId}] ` : "";
  const stageLabel = `[${stageId}] `;

  try {
    console.log(
      `    ${batchLabel}${stageLabel}Calling OpenAI API for ${REDACTION_STAGES.find((s) => s.id === stageId)?.name || stageId}...`,
    );
    const apiStartTime = Date.now();

    const response = await Promise.race([
      openai.chat.completions.create({
        model: MODEL,
        messages: [
          { role: "system", content: buildSystemPrompt(stageId) },
          { role: "user", content: buildUserPrompt(messages, stageId) },
        ],
        temperature: 0.1, // Low temperature for consistent, deterministic results
        response_format: { type: "json_object" },
        max_tokens: 16000,
      }),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error("Request timeout after 5 minutes")),
          300000,
        ),
      ),
    ]);

    const apiTime = ((Date.now() - apiStartTime) / 1000).toFixed(1);
    console.log(
      `    ${batchLabel}${stageLabel}API response received (${apiTime}s)`,
    );

    const content = response.choices[0].message.content;

    if (!content) {
      throw new Error("Empty response from OpenAI");
    }

    console.log(
      `    ${batchLabel}${stageLabel}Parsing JSON response (${content.length} chars)...`,
    );

    let parsed;
    try {
      parsed = JSON.parse(content);
    } catch (parseError) {
      console.log(
        `    ${batchLabel}${stageLabel}JSON parse failed, attempting to clean...`,
      );
      const cleaned = cleanJSON(content);
      try {
        parsed = JSON.parse(cleaned);
        console.log(
          `    ${batchLabel}${stageLabel}JSON cleaned and parsed successfully`,
        );
      } catch (cleanError) {
        const preview = content.substring(0, 500);
        console.error(
          `    ${batchLabel}${stageLabel}JSON cleaning failed. Preview: ${preview}...`,
        );
        console.error(
          `    ${batchLabel}${stageLabel}Parse error: ${parseError.message}`,
        );
        throw new Error(
          `JSON parse failed: ${parseError.message}. First 500 chars: ${preview}`,
        );
      }
    }

    // Handle both array and object with results key
    const results = Array.isArray(parsed)
      ? parsed
      : parsed.results || parsed.messages || [];

    console.log(
      `    ${batchLabel}${stageLabel}Successfully parsed ${results.length} results`,
    );
    return results;
  } catch (error) {
    const errorMsg = error.message || String(error);
    console.error(`    ${batchLabel}${stageLabel}API error: ${errorMsg}`);

    if (retryCount < MAX_RETRIES) {
      const delay = RETRY_DELAY_MS * (retryCount + 1);
      console.log(
        `    ${batchLabel}${stageLabel}Retrying in ${delay / 1000}s... (attempt ${retryCount + 1}/${MAX_RETRIES})`,
      );
      await sleep(delay);
      return callOpenAI(messages, stageId, retryCount + 1, batchId);
    }

    console.error(
      `    ${batchLabel}${stageLabel}Max retries exceeded. Throwing error.`,
    );
    throw error;
  }
}

/**
 * Apply redactions from a specific stage to a message
 * Preserves original sentiment by using contextually appropriate redaction markers
 */
function applyStageRedactions(message, redactionInfo, stageId) {
  const redactedMsg = JSON.parse(JSON.stringify(message)); // Deep clone
  const appliedRedactions = [];

  if (!redactionInfo || !redactionInfo.redactions) {
    return { message: redactedMsg, appliedRedactions };
  }

  // Collect redactions for this stage
  const itemsToRedact = new Map(); // original -> { type, field, reason }

  for (const redaction of redactionInfo.redactions) {
    const original = redaction.original;
    if (!original || original.length < 2) continue;
    if (isDataSubjectRelated(original)) continue;

    const type = redaction.type || "";
    const field = redaction.field || "body";

    // Only process redactions that match this stage
    if (stageId === "names" && type === "name") {
      itemsToRedact.set(original, { type, field, reason: redaction.reason });
      // Also add first name if it's a multi-word name
      const parts = original.split(/\s+/);
      if (parts.length > 1 && parts[0].length > 2) {
        const firstName = parts[0];
        if (!isDataSubjectRelated(firstName)) {
          itemsToRedact.set(firstName, {
            type,
            field,
            reason: `First name from "${original}"`,
          });
        }
      }
    } else if (stageId === "companies" && type === "company") {
      itemsToRedact.set(original, { type, field, reason: redaction.reason });
    } else if (
      stageId === "contact" &&
      (type === "email" || type === "phone" || type === "address")
    ) {
      itemsToRedact.set(original, { type, field, reason: redaction.reason });
    }
  }

  // Apply redactions, longest first to avoid partial matches
  const sortedItems = Array.from(itemsToRedact.entries()).sort(
    (a, b) => b[0].length - a[0].length,
  );

  for (const [original, info] of sortedItems) {
    const { type, field } = info;
    let replacement;

    // Choose replacement based on type to preserve sentiment
    if (type === "name") {
      replacement = "[REDACTED NAME]";
    } else if (type === "company") {
      replacement = "[REDACTED COMPANY]";
    } else if (type === "email") {
      replacement = "[REDACTED EMAIL]";
    } else if (type === "phone") {
      replacement = "[REDACTED PHONE]";
    } else if (type === "address") {
      replacement = "[REDACTED ADDRESS]";
    } else {
      replacement = "[REDACTED]";
    }

    // Create regex for the original text
    const escaped = escapeRegex(original);
    const regex = new RegExp(escaped, "gi");

    // Apply to body
    if (redactedMsg.body && regex.test(redactedMsg.body)) {
      redactedMsg.body = redactedMsg.body.replace(regex, replacement);
      appliedRedactions.push({
        original,
        type,
        field: "body",
        replacement,
        stage: stageId,
      });
    }

    // Apply to subject
    if (redactedMsg.subject && regex.test(redactedMsg.subject)) {
      redactedMsg.subject = redactedMsg.subject.replace(regex, replacement);
      appliedRedactions.push({
        original,
        type,
        field: "subject",
        replacement,
        stage: stageId,
      });
    }

    // Apply to sender field
    if (field === "from" || field === "sender") {
      if (type === "name" && redactedMsg.sender.name) {
        if (regex.test(redactedMsg.sender.name)) {
          redactedMsg.sender.name = redactedMsg.sender.name.replace(
            regex,
            replacement,
          );
          appliedRedactions.push({
            original,
            type,
            field: "sender.name",
            replacement,
            stage: stageId,
          });
        }
      }
      if ((type === "email" || field === "from") && redactedMsg.sender.email) {
        if (
          regex.test(redactedMsg.sender.email) &&
          redactedMsg.sender.email.toLowerCase() !==
            DATA_SUBJECT.email.toLowerCase()
        ) {
          redactedMsg.sender.email = redactedMsg.sender.email.replace(
            regex,
            replacement,
          );
          appliedRedactions.push({
            original,
            type,
            field: "sender.email",
            replacement,
            stage: stageId,
          });
        }
      }
    }

    // Apply to recipient fields (to, cc, bcc)
    for (const recipientField of ["to", "cc", "bcc"]) {
      if (
        (field === recipientField || field === "to" || field === "cc") &&
        redactedMsg.recipients[recipientField]
      ) {
        const fieldValue = redactedMsg.recipients[recipientField];
        if (regex.test(fieldValue)) {
          // Split by delimiters and redact
          const parts = fieldValue
            .split(/[;,\n]/)
            .map((p) => p.trim())
            .filter((p) => p);
          const redactedParts = [];

          for (const part of parts) {
            if (isDataSubjectRelated(part)) {
              redactedParts.push(part); // Keep data subject
            } else if (regex.test(part)) {
              const redactedPart = part.replace(regex, replacement);
              redactedParts.push(
                redactedPart.trim() === replacement
                  ? replacement
                  : redactedPart,
              );
            } else {
              redactedParts.push(part);
            }
          }

          // Remove duplicates while preserving data subject
          const uniqueParts = [];
          let hasDataSubject = false;
          for (const part of redactedParts) {
            if (isDataSubjectRelated(part)) {
              if (!hasDataSubject) {
                uniqueParts.push(part);
                hasDataSubject = true;
              }
            } else {
              uniqueParts.push(part);
            }
          }

          redactedMsg.recipients[recipientField] = uniqueParts.join("; ");
          appliedRedactions.push({
            original,
            type,
            field: `recipients.${recipientField}`,
            replacement,
            stage: stageId,
          });
        }
      }
    }
  }

  return { message: redactedMsg, appliedRedactions };
}

/**
 * Process a batch of messages through all redaction stages
 * Returns results instead of modifying checkpoint directly
 */
async function processBatchSafe(batch, batchIndex) {
  const batchId = batchIndex !== undefined ? batchIndex : "?";

  try {
    console.log(
      `  [Batch ${batchId}] Processing ${batch.length} messages (IDs: ${batch.map((m) => m.id).join(",")})...`,
    );

    let currentMessages = batch.map((msg) => JSON.parse(JSON.stringify(msg))); // Deep clone
    const allAuditEntries = [];

    // Process through each stage sequentially
    for (const stage of REDACTION_STAGES) {
      console.log(`  [Batch ${batchId}] Stage: ${stage.name} (${stage.id})...`);

      // Call OpenAI for this stage
      const redactionResults = await callOpenAI(
        currentMessages,
        stage.id,
        0,
        batchId,
      );

      // Create a map of results by message ID
      const resultsMap = new Map();
      for (const result of redactionResults) {
        if (result.messageId) {
          resultsMap.set(result.messageId, result);
        }
      }

      // Apply redactions from this stage
      const stageRedactedMessages = [];
      const stageAuditEntries = [];

      for (let i = 0; i < currentMessages.length; i++) {
        const msg = currentMessages[i];
        const redactionInfo = resultsMap.get(msg.id);

        const { message: redactedMsg, appliedRedactions } =
          applyStageRedactions(msg, redactionInfo, stage.id);

        stageRedactedMessages.push(redactedMsg);
        if (appliedRedactions.length > 0) {
          stageAuditEntries.push({
            messageId: msg.id,
            stage: stage.id,
            redactions: appliedRedactions,
          });
        }
      }

      // Update for next stage
      currentMessages = stageRedactedMessages;
      allAuditEntries.push(...stageAuditEntries);

      const stageRedactionCount = stageAuditEntries.reduce(
        (sum, e) => sum + e.redactions.length,
        0,
      );
      console.log(
        `  [Batch ${batchId}] ✓ ${stage.name}: ${stageRedactionCount} redactions applied`,
      );
    }

    const totalRedactionCount = allAuditEntries.reduce(
      (sum, e) => sum + e.redactions.length,
      0,
    );
    console.log(
      `  [Batch ${batchId}] ✓ Complete: ${totalRedactionCount} total redactions applied across all stages`,
    );

    return {
      success: true,
      redactedMessages: currentMessages,
      auditEntries: allAuditEntries,
      processedCount: batch.length,
    };
  } catch (error) {
    // On error, return unredacted messages to avoid data loss
    const errorMsg = error.message || String(error);
    console.error(`  [Batch ${batchId}] ✗ ERROR: ${errorMsg}`);
    console.error(
      `  [Batch ${batchId}] Returning unredacted messages to continue processing`,
    );
    return {
      success: true, // Still mark as success to continue processing
      redactedMessages: batch, // Return original messages
      auditEntries: [],
      processedCount: batch.length,
      error: errorMsg,
    };
  }
}

/**
 * Main redaction function
 */
async function redact() {
  console.log("=".repeat(60));
  console.log("PHASE 2: AI-POWERED REDACTION (OpenAI) - MULTI-STAGE");
  console.log("=".repeat(60));
  console.log(`\n  Model: ${MODEL}`);
  console.log(`  Batch size: ${BATCH_SIZE} messages`);
  console.log(`  Stages: ${REDACTION_STAGES.map((s) => s.name).join(" → ")}`);
  console.log(`  Input: ${INPUT_PATH}`);
  console.log(`  Output: ${OUTPUT_PATH}`);

  // Check for API key
  if (!process.env.OPENAI_API_KEY) {
    console.error("\n  ERROR: OPENAI_API_KEY environment variable not set");
    console.error("  Set it with: export OPENAI_API_KEY=your-key-here");
    process.exit(1);
  }

  // Load input data
  console.log(`\n  Loading input data from: ${INPUT_PATH}`);
  const inputStartTime = Date.now();
  const data = JSON.parse(fs.readFileSync(INPUT_PATH, "utf-8"));
  const inputLoadTime = ((Date.now() - inputStartTime) / 1000).toFixed(1);
  const totalMessages = data.messages.length;
  console.log(
    `  ✓ Loaded ${totalMessages.toLocaleString()} messages in ${inputLoadTime}s`,
  );

  // Load or create checkpoint
  console.log(`\n  Checking for existing checkpoint...`);
  let checkpoint = loadCheckpoint();
  const alreadyProcessed = checkpoint.processedCount;

  if (alreadyProcessed >= totalMessages) {
    console.log(
      `\n  All messages already processed. Delete checkpoint to reprocess.`,
    );
    return;
  }

  let messagesToProcess = data.messages.slice(alreadyProcessed);

  // Apply message limit if configured (for testing)
  if (MAX_MESSAGES_TO_PROCESS && MAX_MESSAGES_TO_PROCESS > 0) {
    const originalCount = messagesToProcess.length;
    messagesToProcess = messagesToProcess.slice(0, MAX_MESSAGES_TO_PROCESS);
    console.log(
      `  ⚠ TEST MODE: Limiting to ${MAX_MESSAGES_TO_PROCESS} messages (${originalCount.toLocaleString()} available)`,
    );
  }

  console.log(
    `  Messages to process: ${messagesToProcess.length.toLocaleString()}`,
  );

  if (messagesToProcess.length === 0) {
    console.log(`\n  ✓ No messages to process. All done!`);
    return;
  }

  const startTime = Date.now();

  // Split messages into batches
  console.log(`\n  Creating batches (size: ${BATCH_SIZE})...`);
  const batches = [];
  for (let i = 0; i < messagesToProcess.length; i += BATCH_SIZE) {
    batches.push({
      sequence: batches.length,
      messages: messagesToProcess.slice(i, i + BATCH_SIZE),
    });
  }
  console.log(`  ✓ Created ${batches.length.toLocaleString()} batches`);

  console.log(
    `  Total batches: ${batches.length} (${BATCH_SIZE} msgs/batch, ${CONCURRENCY} concurrent workers)`,
  );
  console.log(
    `\n  Starting worker pool with ${CONCURRENCY} concurrent workers...`,
  );
  console.log(`  Checkpoints will be written immediately as batches complete`);
  console.log("");

  // Track progress
  let totalProcessed = alreadyProcessed;
  let totalBatchesCompleted = 0;
  let lastProgressUpdate = Date.now();

  // Worker pool: process batches with constant concurrency
  const processWithWorkerPool = async () => {
    const workers = [];
    let batchIndex = 0;

    // Create worker pool
    for (let workerId = 0; workerId < CONCURRENCY; workerId++) {
      const worker = async () => {
        while (batchIndex < batches.length) {
          const batch = batches[batchIndex++];
          if (!batch) break;

          const batchId = batch.sequence;
          try {
            // Process the batch through all stages
            const result = await processBatchSafe(batch.messages, batchId);

            // Write to checkpoint immediately (with mutex)
            await checkpointMutex.acquire();
            try {
              // Add results to checkpoint (mutex ensures only one writer at a time)
              if (result.success) {
                for (const msg of result.redactedMessages) {
                  checkpoint.redactedMessages.push(msg);
                }
                for (const audit of result.auditEntries) {
                  checkpoint.auditLog.push(audit);
                }
                checkpoint.processedCount += result.processedCount;
                totalProcessed = checkpoint.processedCount;
              }

              // Save checkpoint atomically
              fs.writeFileSync(
                CHECKPOINT_PATH,
                JSON.stringify(checkpoint, null, 2),
              );
            } finally {
              checkpointMutex.release();
            }

            // Track completion
            totalBatchesCompleted++;

            // Update progress display
            const now = Date.now();
            if (now - lastProgressUpdate > 1000) {
              // Update every second
              const elapsed = (now - startTime) / 1000;
              const rate = totalProcessed / elapsed;
              const remaining = totalMessages - totalProcessed;
              const eta = remaining / rate;
              const percent = ((totalProcessed / totalMessages) * 100).toFixed(
                1,
              );

              process.stdout.write(
                `\r  Progress: ${totalProcessed.toLocaleString()}/${totalMessages.toLocaleString()} (${percent}%) | ` +
                  `Rate: ${rate.toFixed(1)}/sec | ` +
                  `ETA: ${formatDuration(eta)} | ` +
                  `Batches: ${totalBatchesCompleted}/${batches.length}    `,
              );
              lastProgressUpdate = now;
            }

            // Log completion
            const redactionCount = result.auditEntries.reduce(
              (sum, e) => sum + e.redactions.length,
              0,
            );
            console.log(
              `\n  [Batch ${batchId}] ✓ Complete (${redactionCount} redactions, checkpoint saved)`,
            );
          } catch (error) {
            console.error(`\n  [Batch ${batchId}] ✗ ERROR: ${error.message}`);
            // Still track as completed to continue processing
            totalBatchesCompleted++;

            // Write unredacted messages to checkpoint on error
            await checkpointMutex.acquire();
            try {
              for (const msg of batch.messages) {
                checkpoint.redactedMessages.push(msg);
              }
              checkpoint.processedCount += batch.messages.length;
              totalProcessed = checkpoint.processedCount;
              fs.writeFileSync(
                CHECKPOINT_PATH,
                JSON.stringify(checkpoint, null, 2),
              );
            } finally {
              checkpointMutex.release();
            }
          }
        }
      };

      workers.push(worker());
    }

    // Wait for all workers to complete
    await Promise.all(workers);
  };

  try {
    await processWithWorkerPool();

    console.log("\n");

    // Reload checkpoint one final time to ensure we have the latest
    await checkpointMutex.acquire();
    let finalCheckpoint;
    try {
      if (fs.existsSync(CHECKPOINT_PATH)) {
        const content = fs.readFileSync(CHECKPOINT_PATH, "utf-8");
        finalCheckpoint = JSON.parse(content);
      }
    } finally {
      checkpointMutex.release();
    }

    if (finalCheckpoint) {
      checkpoint = finalCheckpoint;
    }

    // Build final output
    const output = {
      metadata: {
        ...data.metadata,
        redactedAt: new Date().toISOString(),
        dataSubject: DATA_SUBJECT,
        model: MODEL,
        stages: REDACTION_STAGES.map((s) => s.id),
        messagesWithRedactions: checkpoint.auditLog.length,
      },
      messages: checkpoint.redactedMessages,
    };

    // Write final outputs
    console.log(`  Writing final output files...`);
    fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));

    const auditOutput = {
      generatedAt: new Date().toISOString(),
      dataSubject: DATA_SUBJECT,
      model: MODEL,
      stages: REDACTION_STAGES.map((s) => s.id),
      totalMessages: totalMessages,
      messagesWithRedactions: checkpoint.auditLog.length,
      totalRedactions: checkpoint.auditLog.reduce(
        (sum, entry) => sum + entry.redactions.length,
        0,
      ),
      details: checkpoint.auditLog,
    };
    fs.writeFileSync(AUDIT_PATH, JSON.stringify(auditOutput, null, 2));

    // Clean up checkpoint on success
    if (fs.existsSync(CHECKPOINT_PATH)) {
      fs.unlinkSync(CHECKPOINT_PATH);
    }

    const elapsed = (Date.now() - startTime) / 1000;
    console.log("=".repeat(60));
    console.log("REDACTION COMPLETE");
    console.log(`  Time: ${formatDuration(elapsed)}`);
    console.log(`  Batches processed: ${totalBatchesCompleted}`);
    console.log(
      `  Messages with redactions: ${checkpoint.auditLog.length.toLocaleString()}`,
    );
    console.log(
      `  Total redactions: ${auditOutput.totalRedactions.toLocaleString()}`,
    );
    console.log("=".repeat(60));

    return output;
  } catch (error) {
    console.error(`\n\n  ERROR: ${error.message}`);
    console.error(
      `  Checkpoint saved at ${checkpoint.processedCount} messages.`,
    );
    console.error(`  Run again to resume from checkpoint.`);
    await checkpointMutex.acquire();
    try {
      fs.writeFileSync(CHECKPOINT_PATH, JSON.stringify(checkpoint, null, 2));
    } finally {
      checkpointMutex.release();
    }
    throw error;
  }
}

if (require.main === module) {
  redact().catch((err) => {
    console.error("Redaction failed:", err.message);
    process.exit(1);
  });
}

module.exports = { redact };
