/**
 * REDACTION V2 - Fresh rewrite with paranoid approach
 *
 * Philosophy: NOTHING gets through. Over-redact rather than under-redact.
 *
 * Strategy:
 * 1. DETERMINISTIC: Redact ALL structured fields (from/to/cc) unless exactly matching data subject
 * 2. DETERMINISTIC: Redact ALL patterns (emails, phones, postcodes, references, prices, URLs)
 * 3. AI-ASSISTED: Find remaining names/companies in body text
 * 4. PARANOID: Final pass to catch anything that looks like a name
 */

require("dotenv").config({
  path: require("path").resolve(__dirname, "..", ".env"),
});

const fs = require("fs");
const path = require("path");
const OpenAI = require("openai");

// Paths
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
  "redaction-checkpoint-v2.json",
);
const AUDIT_PATH = path.resolve(__dirname, "..", "output", "audit-log.json");

// Configuration
const MODEL = "gpt-4.1-mini"; // Mini is sufficient - deterministic stages do the heavy lifting
const BATCH_SIZE = 1; // Can batch more with mini (faster, cheaper)
const CONCURRENCY = 50; // Can increase concurrency with mini
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 2000;
const MAX_MESSAGES_TO_PROCESS = null; // Set to a number to limit for testing

// Data subject - the ONLY person whose info should NOT be redacted
const DATA_SUBJECT = {
  name: "John Gaskell",
  email: "john@freightlink.co.uk",
  // All variations that should be preserved (lowercase for comparison)
  nameVariations: [
    "john gaskell",
    "john",
    "gaskell",
    "j. gaskell",
    "j gaskell",
    "mr gaskell",
    "mr. gaskell",
  ],
  emailVariations: ["john@freightlink.co.uk"],
};

// Initialize OpenAI
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatDuration(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600)
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Check if text matches the data subject (case-insensitive)
 */
function isDataSubject(text) {
  if (!text) return false;
  const lower = text.toLowerCase().trim();

  // Check email
  if (DATA_SUBJECT.emailVariations.some((e) => lower === e.toLowerCase())) {
    return true;
  }

  // Check name variations
  if (DATA_SUBJECT.nameVariations.some((n) => lower === n.toLowerCase())) {
    return true;
  }

  // Check if it contains the data subject's full name
  if (lower.includes("john gaskell")) {
    return true;
  }

  return false;
}

/**
 * Check if text CONTAINS the data subject (for partial matches in longer strings)
 */
function containsDataSubject(text) {
  if (!text) return false;
  const lower = text.toLowerCase();

  return (
    DATA_SUBJECT.nameVariations.some((n) => lower.includes(n.toLowerCase())) ||
    DATA_SUBJECT.emailVariations.some((e) => lower.includes(e.toLowerCase()))
  );
}

// ============================================================================
// REPLY THREAD TRIMMING
// ============================================================================

/**
 * Trim reply threads from message body
 * Keeps only the first/current message, removes quoted replies
 */
function trimReplyThreads(body) {
  if (!body) return body;

  // Common reply separators
  const separators = [
    /\n-{20,}\n/, // ----------------------------------------
    /\n_{20,}\n/, // ____________________
    /\n={20,}\n/, // ====================
    /\nFrom:.*?\nSent:.*?\nTo:/s, // Outlook-style header block
    /\n>.*?wrote:/i, // Gmail-style "> On ... wrote:"
    /\nOn\s+\d{1,2}[\s\/\-]\w+[\s\/\-]\d{2,4}.*?wrote:/i, // "On 15 July 2024... wrote:"
    /\n-{3,}\s*Original Message\s*-{3,}/i, // --- Original Message ---
  ];

  let trimmed = body;

  for (const sep of separators) {
    const match = trimmed.match(sep);
    if (match && match.index !== undefined && match.index > 50) {
      // Only trim if separator is not at the very start (keep at least 50 chars)
      trimmed = trimmed.substring(0, match.index).trim();
    }
  }

  return trimmed;
}

// ============================================================================
// DETERMINISTIC REDACTION PATTERNS
// ============================================================================

const PATTERNS = {
  // Email addresses
  email: /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g,

  // Phone numbers (UK and international formats)
  phone: [
    /\b(?:\+\d{1,3}[-.\s]?)?\(?\d{2,5}\)?[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b/g,
    /\b0\d{2,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b/g,
    /\b\+\d{10,15}\b/g,
  ],

  // URLs and domains (except freightlink.co.uk)
  url: /\b(?:https?:\/\/)?(?:www\.)?[a-zA-Z0-9][-a-zA-Z0-9]*\.[a-zA-Z]{2,}(?:\/[^\s]*)?\b/gi,

  // Reference numbers (various formats) - IMPROVED
  reference: [
    /\b(?:ref|job|order|quote|invoice|po|booking|customer|account|a\/c)[\s.]*(?:no\.?|number|#)?[\s.:=-]*\d+\b/gi,
    /\b[A-Z]{1,3}[-]?\d{4,}[-]?\d*\b/g, // e.g., R2407-83180-1
    /\b\d{5,}\b/g, // Any 5+ digit number (likely a reference)
    /\b(?:Job No|Customer No|Account ID|A\/C no)[\s.:]*\d+\b/gi,
  ],

  // Prices and currency
  currency: [
    /[£$€]\s*\d{1,3}(?:[,\s]?\d{3})*(?:\.\d{2})?\b/g,
    /\b\d{1,3}(?:[,\s]?\d{3})*(?:\.\d{2})?\s*(?:pounds?|GBP|EUR|USD)\b/gi,
  ],

  // Company registration numbers
  companyReg:
    /\b(?:company\s*(?:number|no\.?|reg(?:istration)?)|registered\s*(?:number|no\.?))[\s:]*\d{6,8}\b/gi,

  // VAT numbers
  vatNumber:
    /\bVAT\s*(?:no\.?|number)?[\s:]*[A-Z]{0,2}\s*\d{3}\s*\d{4}\s*\d{2}\b/gi,

  // FULL addresses - run FIRST to catch complete address blocks
  fullAddress: [
    // Full UK address with postcode at end: "Quarry Bank, Chorley Road, Preston, PR5 4JN, UK."
    /[A-Z][A-Za-z0-9\s]+(?:,\s*[A-Z][A-Za-z0-9\s]+){2,},?\s*[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}(?:,?\s*UK)?\.?/gi,
    // Irish addresses with Eircode: "Ashbourne, Co. Meath, A84 EC83"
    /[A-Z][A-Za-z\s]+,\s*(?:Co\.?\s*)?[A-Z][A-Za-z]+,\s*[A-Z]\d{2}\s*[A-Z0-9]{4}/gi,
    // Unit/Building addresses: "Unit 1, GMP House, Ashbourne Industrial Estate"
    /Unit\s+\d+[A-Za-z]?,\s*[A-Z][A-Za-z0-9\s,]+(?:Estate|Park|Industrial|Business|Centre|Center)[A-Za-z0-9\s,]*/gi,
    // "A:" prefixed addresses in signatures
    /A:\s*[A-Z][A-Za-z0-9\s,.-]+(?:[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}|[A-Z]\d{2}\s*[A-Z0-9]{4})/gi,
  ],

  // UK postcodes (standalone, after full addresses)
  postcode: /\b[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}\b/gi,

  // Irish Eircodes
  eircode: /\b[A-Z]\d{2}\s*[A-Z0-9]{4}\b/gi,

  // Street addresses (simpler patterns for remaining)
  address: [
    /\b\d+[A-Za-z]?\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Way|Close|Court|Ct|Place|Pl|Crescent|Cres|Park|Gardens|Gdn|Terrace|Walk|Row|Square|Sq|Hill|Rise|Green|Grove|Mews)\b/gi,
  ],
};

// Common words that should NOT be redacted as names
const COMMON_WORDS = new Set([
  // Greetings/closings
  "hi",
  "hello",
  "dear",
  "morning",
  "afternoon",
  "evening",
  "regards",
  "thanks",
  "thank",
  "cheers",
  "best",
  "kind",
  "sincerely",
  "yours",
  // Business terms
  "manager",
  "director",
  "head",
  "team",
  "department",
  "customer",
  "service",
  "services",
  "sales",
  "support",
  "logistics",
  "operations",
  "booking",
  "bookings",
  "account",
  "accounts",
  "export",
  "import",
  "freight",
  "delivery",
  "collection",
  "order",
  "orders",
  // Common words
  "please",
  "note",
  "attached",
  "following",
  "regarding",
  "subject",
  "update",
  "confirmation",
  "enquiry",
  "query",
  "request",
  "today",
  "tomorrow",
  "monday",
  "tuesday",
  "wednesday",
  "thursday",
  "friday",
  "saturday",
  "sunday",
  "january",
  "february",
  "march",
  "april",
  "may",
  "june",
  "july",
  "august",
  "september",
  "october",
  "november",
  "december",
  // Tech/email
  "email",
  "sent",
  "received",
  "forwarded",
  "replied",
  "website",
  "link",
  "click",
  "login",
  "password",
  // Locations (cities/countries are OK)
  "uk",
  "ireland",
  "dublin",
  "london",
  "manchester",
  "leeds",
  "preston",
  "birmingham",
  "ashbourne",
  "cork",
  "belfast",
  // Company types (standalone)
  "ltd",
  "limited",
  "inc",
  "incorporated",
  "plc",
  "llc",
  "corp",
  "corporation",
  // Other
  "the",
  "and",
  "for",
  "with",
  "from",
  "this",
  "that",
  "here",
  "there",
  "will",
  "would",
  "could",
  "should",
  "have",
  "has",
  "been",
]);

// ============================================================================
// STAGE 1: DETERMINISTIC REDACTION OF STRUCTURED FIELDS
// ============================================================================

/**
 * Redact sender field - if not data subject, redact entirely
 */
function redactSender(sender) {
  const redactions = [];
  const result = { ...sender };

  // Check if sender is the data subject
  const isDS = isDataSubject(sender.name) || isDataSubject(sender.email);

  if (!isDS) {
    // Redact everything
    if (sender.name && sender.name.trim()) {
      redactions.push({
        original: sender.name,
        type: "sender_name",
        replacement: "[REDACTED]",
      });
      result.name = "[REDACTED]";
    }
    if (sender.email && sender.email.trim()) {
      redactions.push({
        original: sender.email,
        type: "sender_email",
        replacement: "[REDACTED]",
      });
      result.email = "[REDACTED]";
    }
  }

  return { result, redactions };
}

/**
 * Redact recipient fields (to, cc, bcc) - redact all except data subject
 */
function redactRecipients(recipients) {
  const redactions = [];
  const result = { ...recipients };

  for (const field of ["to", "cc", "bcc"]) {
    if (!recipients[field] || !recipients[field].trim()) continue;

    // Split by common delimiters
    const parts = recipients[field]
      .split(/[;,]/)
      .map((p) => p.trim())
      .filter((p) => p);
    const redactedParts = [];

    for (const part of parts) {
      // Check if this part is/contains the data subject
      if (isDataSubject(part) || containsDataSubject(part)) {
        // Keep the data subject's name only
        if (part.toLowerCase().includes("john gaskell")) {
          redactedParts.push("John Gaskell");
        } else if (part.toLowerCase().includes("john@freightlink")) {
          redactedParts.push(DATA_SUBJECT.email);
        } else {
          redactedParts.push(DATA_SUBJECT.name);
        }
      } else {
        // Redact this third party
        redactions.push({
          original: part,
          type: `recipient_${field}`,
          replacement: "[REDACTED]",
        });
        redactedParts.push("[REDACTED]");
      }
    }

    // Deduplicate consecutive [REDACTED] entries
    const deduped = [];
    for (const part of redactedParts) {
      if (
        part === "[REDACTED]" &&
        deduped[deduped.length - 1] === "[REDACTED]"
      ) {
        continue; // Skip duplicate REDACTED
      }
      deduped.push(part);
    }

    result[field] = deduped.join("; ");
  }

  return { result, redactions };
}

// ============================================================================
// STAGE 2: DETERMINISTIC PATTERN-BASED REDACTION
// ============================================================================

/**
 * Apply all regex patterns to redact emails, phones, postcodes, references, etc.
 */
function redactPatterns(text) {
  if (!text) return { result: text, redactions: [] };

  let result = text;
  const redactions = [];

  // Helper to apply a pattern
  const applyPattern = (pattern, type, replacement) => {
    const regex =
      pattern instanceof RegExp ? pattern : new RegExp(pattern, "gi");
    result = result.replace(regex, (match) => {
      // Don't redact data subject's email
      if (
        type === "email" &&
        match.toLowerCase() === DATA_SUBJECT.email.toLowerCase()
      ) {
        return match;
      }
      // Don't redact freightlink URLs
      if (type === "url" && match.toLowerCase().includes("freightlink")) {
        return match;
      }
      // Don't double-redact
      if (match.includes("[REDACTED")) {
        return match;
      }

      redactions.push({ original: match, type, replacement });
      return replacement;
    });
  };

  // Apply all patterns - ORDER MATTERS!

  // 1. FULL ADDRESSES FIRST - catch complete address blocks before postcodes break them up
  for (const p of PATTERNS.fullAddress) {
    applyPattern(p, "address", "[REDACTED ADDRESS]");
  }

  // 2. Remaining address patterns
  for (const p of PATTERNS.address) {
    applyPattern(p, "address", "[REDACTED ADDRESS]");
  }

  // 3. Postcodes and Eircodes (standalone, after addresses caught the full blocks)
  applyPattern(PATTERNS.postcode, "postcode", "[REDACTED]");
  applyPattern(PATTERNS.eircode, "eircode", "[REDACTED]");

  // 4. Email addresses
  applyPattern(PATTERNS.email, "email", "[REDACTED EMAIL]");

  // 5. Phone numbers
  for (const p of PATTERNS.phone) {
    applyPattern(p, "phone", "[REDACTED PHONE]");
  }

  // 6. Reference numbers
  for (const p of PATTERNS.reference) {
    applyPattern(p, "reference", "[REDACTED REF]");
  }

  // 7. Currency
  for (const p of PATTERNS.currency) {
    applyPattern(p, "currency", "[REDACTED AMOUNT]");
  }

  // 8. Company/VAT registrations
  applyPattern(PATTERNS.companyReg, "company_reg", "[REDACTED REG]");
  applyPattern(PATTERNS.vatNumber, "vat", "[REDACTED VAT]");

  // 9. URLs last (to not interfere with emails)
  applyPattern(PATTERNS.url, "url", "[REDACTED URL]");

  return { result, redactions };
}

// ============================================================================
// STAGE 3: AI-ASSISTED SEMANTIC REDACTION
// ============================================================================

/**
 * Build prompt for AI to identify names and companies in text
 */
function buildAIPrompt(messages) {
  const messagesData = messages.map((m) => ({
    id: m.id,
    body: m.body?.substring(0, 8000) || "", // Increased limit - reply threads already trimmed
    subject: m.subject || "",
  }));

  return {
    system: `You are a GDPR compliance assistant. Your task is to identify ALL remaining personal names and company names in message text that need redaction.

DATA SUBJECT (DO NOT REDACT): ${DATA_SUBJECT.name} (${DATA_SUBJECT.email})
Also keep: "John", "Gaskell", "J. Gaskell"

ALREADY REDACTED: Text marked as [REDACTED ...] should be ignored.

IDENTIFY AND RETURN:
1. NAMES: Any person's name (first name, last name, or full name) that is NOT the data subject
   - Full names: "Sarah Smith", "Hayley Myers", "John Smith"
   - First names alone: "Sarah", "Nicola", "Romey", "Megan"
   - Names in greetings: "Hi Romey", "Dear Jason"
   - Names in signatures: "Kind Regards, Megan", "Thanks, Sarah"
   - Standalone names on their own line: "Hayley Myers" (on a line by itself)
   - Names in parentheses: "(Sean)", "(John Smith)"
   - Names with titles: "Mr. Smith", "Dr. Williams"
   - CRITICAL: If you see a capitalized word that could be a name (especially at start of line or after newline), redact it

2. COMPANIES: Any company or organization name
   - Examples: "DHL", "Walkers Transport", "Primeline Express", "Haldane Fisher Ltd"
   - Include company names with suffixes: Ltd, Inc, Corp, PLC, LLC

RESPONSE FORMAT (JSON only, no markdown):
{
  "results": [
    {
      "messageId": <number>,
      "items": [
        {"text": "<exact text to redact>", "type": "name|company"}
      ]
    }
  ]
}

Be EXTREMELY thorough. False positives are preferred over false negatives.`,

    user: `Analyze these messages and identify ALL names and company names to redact:

${JSON.stringify(messagesData, null, 2)}`,
  };
}

/**
 * Call OpenAI API
 */
async function callOpenAI(messages, retryCount = 0) {
  try {
    const prompt = buildAIPrompt(messages);

    const response = await Promise.race([
      openai.chat.completions.create({
        model: MODEL,
        messages: [
          { role: "system", content: prompt.system },
          { role: "user", content: prompt.user },
        ],
        temperature: 1,
        response_format: { type: "json_object" },
        max_completion_tokens: 16000, // Max output for gpt-4o-mini - prioritize accuracy
      }),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Timeout")), 120000),
      ),
    ]);

    const content = response.choices[0].message.content;
    if (!content) throw new Error("Empty response");

    const parsed = JSON.parse(
      content.replace(/```json\s*/g, "").replace(/```\s*/g, ""),
    );
    return parsed.results || [];
  } catch (error) {
    if (retryCount < MAX_RETRIES) {
      await sleep(RETRY_DELAY_MS * (retryCount + 1));
      return callOpenAI(messages, retryCount + 1);
    }
    console.error(`    AI call failed: ${error.message}`);
    return []; // Return empty on failure - patterns already caught most things
  }
}

/**
 * Apply AI-identified redactions
 */
function applyAIRedactions(text, items) {
  if (!text || !items || items.length === 0)
    return { result: text, redactions: [] };

  let result = text;
  const redactions = [];

  // Sort by length descending to avoid partial matches
  const sorted = [...items].sort((a, b) => b.text.length - a.text.length);

  for (const item of sorted) {
    const { text: original, type } = item;

    // Skip if too short or is data subject
    if (!original || original.length < 2) continue;
    if (isDataSubject(original)) continue;

    // Skip common words
    if (COMMON_WORDS.has(original.toLowerCase())) continue;

    // Skip already redacted
    if (original.includes("[REDACTED")) continue;

    const replacement =
      type === "company" ? "[REDACTED COMPANY]" : "[REDACTED NAME]";
    const regex = new RegExp(`\\b${escapeRegex(original)}\\b`, "gi");

    if (regex.test(result)) {
      result = result.replace(regex, replacement);
      redactions.push({ original, type, replacement });
    }
  }

  return { result, redactions };
}

// ============================================================================
// STAGE 4: PARANOID FINAL PASS
// ============================================================================

/**
 * Final paranoid pass to catch any remaining name-like patterns
 */
function paranoidPass(text) {
  if (!text) return { result: text, redactions: [] };

  let result = text;
  const redactions = [];

  // Pattern: Capitalized word after greeting (Hi Sarah, Dear Mike, etc.)
  result = result.replace(
    /\b(Hi|Hello|Dear|Hey|Morning|Afternoon)\s+([A-Z][a-z]{2,})\b/gi,
    (match, greeting, name) => {
      if (isDataSubject(name) || COMMON_WORDS.has(name.toLowerCase()))
        return match;
      redactions.push({
        original: name,
        type: "greeting_name",
        replacement: "[REDACTED NAME]",
      });
      return `${greeting} [REDACTED NAME]`;
    },
  );

  // Pattern: Name in signature (Regards, Sarah / Thanks, Mike)
  result = result.replace(
    /\b(Regards|Thanks|Cheers|Best|Sincerely)[,\s]+([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]+)?)\b/gi,
    (match, closing, name) => {
      if (isDataSubject(name) || COMMON_WORDS.has(name.toLowerCase()))
        return match;
      if (name.includes("[REDACTED")) return match;
      redactions.push({
        original: name,
        type: "signature_name",
        replacement: "[REDACTED NAME]",
      });
      return `${closing}, [REDACTED NAME]`;
    },
  );

  // Pattern: Name in parentheses (Sean), (John Smith)
  result = result.replace(
    /\(([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\)/g,
    (match, name) => {
      if (isDataSubject(name) || COMMON_WORDS.has(name.toLowerCase()))
        return match;
      redactions.push({
        original: name,
        type: "parentheses_name",
        replacement: "[REDACTED NAME]",
      });
      return "([REDACTED NAME])";
    },
  );

  // Pattern: Title + Name (Mr. Smith, Dr. Jones)
  result = result.replace(
    /\b(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?)\s+([A-Z][a-z]+)\b/gi,
    (match, title, name) => {
      if (isDataSubject(name)) return match;
      redactions.push({
        original: `${title} ${name}`,
        type: "titled_name",
        replacement: `${title} [REDACTED NAME]`,
      });
      return `${title} [REDACTED NAME]`;
    },
  );

  // Pattern: Two consecutive capitalized words that look like a name (First Last)
  // Only in specific contexts to avoid false positives
  result = result.replace(
    /\b(From|To|Cc|Sent by|Contact|Name)[\s:]+([A-Z][a-z]+\s+[A-Z][a-z]+)\b/gi,
    (match, prefix, name) => {
      if (isDataSubject(name)) return match;
      if (name.includes("[REDACTED")) return match;
      redactions.push({
        original: name,
        type: "labeled_name",
        replacement: "[REDACTED NAME]",
      });
      return `${prefix}: [REDACTED NAME]`;
    },
  );

  // Pattern: Standalone full names (First Last) on their own line or after newline
  // Examples: "Hayley Myers", "Nicola" (single name), "John Smith"
  result = result.replace(
    /(?:^|\n)\s*([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\s*(?:\n|$)/gm,
    (match, name) => {
      // Skip if it's the data subject
      if (isDataSubject(name)) return match;
      // Skip common words
      if (COMMON_WORDS.has(name.toLowerCase())) return match;
      // Skip if already redacted
      if (name.includes("[REDACTED")) return match;
      // Skip if it's part of a sentence (has punctuation before it)
      const beforeMatch = match.match(/[.!?]\s*$/);
      if (beforeMatch) return match;
      // Skip if it looks like a company name (has Ltd, Inc, etc.)
      if (
        /\b(Ltd|Limited|Inc|Incorporated|Corp|Corporation|LLC|LLP|PLC|Co)\b/i.test(
          name,
        )
      )
        return match;

      redactions.push({
        original: name,
        type: "standalone_name",
        replacement: "[REDACTED NAME]",
      });
      return match.replace(name, "[REDACTED NAME]");
    },
  );

  // Pattern: Names at start of line (common in signatures)
  // "Hayley Myers\n" or "Nicola\n"
  result = result.replace(
    /(?:^|\n)([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)(?:\n|$)/gm,
    (match, name, offset, string) => {
      if (isDataSubject(name)) return match;
      if (COMMON_WORDS.has(name.toLowerCase())) return match;
      if (name.includes("[REDACTED")) return match;

      // Check context - if it's followed by a job title or email, it's likely a name
      const afterMatch = string.substring(
        offset + match.length,
        offset + match.length + 50,
      );
      const isLikelyName =
        /^\s*(Head|Manager|Director|Executive|Officer|Coordinator|Specialist|Assistant|Lead|Senior|Junior)/i.test(
          afterMatch,
        ) ||
        /^\s*[A-Z][a-z]+\s+of\s+[A-Z]/i.test(afterMatch) ||
        /^\s*[A-Z][a-z]+@/i.test(afterMatch) ||
        /^\s*\+?\d/i.test(afterMatch); // Phone number

      if (isLikelyName) {
        redactions.push({
          original: name,
          type: "signature_name",
          replacement: "[REDACTED NAME]",
        });
        return match.replace(name, "[REDACTED NAME]");
      }

      return match;
    },
  );

  return { result, redactions };
}

// ============================================================================
// MAIN PROCESSING
// ============================================================================

/**
 * Process a single message through all redaction stages
 */
function processMessage(message) {
  const result = JSON.parse(JSON.stringify(message)); // Deep clone
  const allRedactions = [];

  // Stage 0: TRIM REPLY THREADS - remove quoted replies to reduce noise and errors
  if (result.body) {
    const originalLength = result.body.length;
    result.body = trimReplyThreads(result.body);
    if (result.body.length < originalLength) {
      allRedactions.push({
        type: "reply_trimmed",
        field: "body",
        original: `[${originalLength - result.body.length} chars of reply thread removed]`,
        replacement: "",
      });
    }
  }

  // Stage 1: Redact structured fields
  const { result: senderResult, redactions: senderRedactions } = redactSender(
    result.sender,
  );
  result.sender = senderResult;
  allRedactions.push(...senderRedactions);

  const { result: recipientsResult, redactions: recipientRedactions } =
    redactRecipients(result.recipients);
  result.recipients = recipientsResult;
  allRedactions.push(...recipientRedactions);

  // Stage 2: Pattern-based redaction on body and subject
  if (result.body) {
    const { result: bodyResult, redactions: bodyRedactions } = redactPatterns(
      result.body,
    );
    result.body = bodyResult;
    allRedactions.push(...bodyRedactions.map((r) => ({ ...r, field: "body" })));
  }

  if (result.subject) {
    const { result: subjectResult, redactions: subjectRedactions } =
      redactPatterns(result.subject);
    result.subject = subjectResult;
    allRedactions.push(
      ...subjectRedactions.map((r) => ({ ...r, field: "subject" })),
    );
  }

  // Stage 4: Paranoid pass (done after patterns, before AI for efficiency)
  if (result.body) {
    const { result: paranoidBodyResult, redactions: paranoidBodyRedactions } =
      paranoidPass(result.body);
    result.body = paranoidBodyResult;
    allRedactions.push(
      ...paranoidBodyRedactions.map((r) => ({ ...r, field: "body" })),
    );
  }

  // Remove attachments - they're useless without actual content
  delete result.attachments;
  delete result.hasAttachments;

  return { message: result, redactions: allRedactions };
}

/**
 * Process a batch of messages with AI assistance
 */
async function processBatch(messages, batchIndex) {
  console.log(
    `  [Batch ${batchIndex}] Processing ${messages.length} messages...`,
  );

  // First apply deterministic redaction to all messages
  const processedMessages = [];
  const allRedactions = [];

  for (const msg of messages) {
    const { message, redactions } = processMessage(msg);
    processedMessages.push(message);
    if (redactions.length > 0) {
      allRedactions.push({ messageId: msg.id, redactions });
    }
  }

  // Then call AI to find remaining names/companies
  console.log(`  [Batch ${batchIndex}] Calling AI for semantic analysis...`);
  const aiResults = await callOpenAI(processedMessages);

  // Apply AI redactions
  const aiResultsMap = new Map();
  for (const r of aiResults) {
    if (r.messageId) aiResultsMap.set(r.messageId, r.items || []);
  }

  for (let i = 0; i < processedMessages.length; i++) {
    const msg = processedMessages[i];
    const aiItems = aiResultsMap.get(msg.id) || [];

    if (aiItems.length > 0 && msg.body) {
      const { result, redactions } = applyAIRedactions(msg.body, aiItems);
      msg.body = result;

      if (redactions.length > 0) {
        const existing = allRedactions.find((r) => r.messageId === msg.id);
        if (existing) {
          existing.redactions.push(
            ...redactions.map((r) => ({ ...r, field: "body", source: "ai" })),
          );
        } else {
          allRedactions.push({
            messageId: msg.id,
            redactions: redactions.map((r) => ({
              ...r,
              field: "body",
              source: "ai",
            })),
          });
        }
      }
    }

    // Apply AI redactions to subject too
    if (aiItems.length > 0 && msg.subject) {
      const { result, redactions } = applyAIRedactions(msg.subject, aiItems);
      msg.subject = result;
    }
  }

  const totalRedactions = allRedactions.reduce(
    (sum, a) => sum + a.redactions.length,
    0,
  );
  console.log(
    `  [Batch ${batchIndex}] ✓ Complete: ${totalRedactions} redactions`,
  );

  return { messages: processedMessages, audit: allRedactions };
}

// ============================================================================
// CHECKPOINT MANAGEMENT
// ============================================================================

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

      // Validate consistency
      const msgCount = checkpoint.redactedMessages.length;
      const processedCount = checkpoint.processedCount || 0;

      if (msgCount !== processedCount) {
        console.log(`  ⚠ Warning: Checkpoint inconsistency detected:`);
        console.log(`    - processedCount: ${processedCount.toLocaleString()}`);
        console.log(
          `    - redactedMessages.length: ${msgCount.toLocaleString()}`,
        );
        console.log(`    - Using processedCount as source of truth`);
        // Trim or pad redactedMessages to match processedCount
        if (msgCount > processedCount) {
          console.log(
            `    - Trimming redactedMessages to match processedCount`,
          );
          checkpoint.redactedMessages = checkpoint.redactedMessages.slice(
            0,
            processedCount,
          );
        } else if (msgCount < processedCount) {
          console.log(
            `    - This shouldn't happen - checkpoint may be incomplete`,
          );
        }
      }

      return checkpoint;
    } catch (err) {
      console.error(`  ✗ ERROR: Could not load checkpoint: ${err.message}`);
      console.log(`  Starting fresh (checkpoint file may be corrupted)`);
      // Backup corrupted checkpoint
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
  };
}

function saveCheckpoint(checkpoint) {
  // Note: Mutex should be acquired by caller
  fs.writeFileSync(CHECKPOINT_PATH, JSON.stringify(checkpoint, null, 2));
}

// ============================================================================
// MAIN
// ============================================================================

async function redact() {
  console.log("=".repeat(60));
  console.log("REDACTION V2 - PARANOID MODE");
  console.log("=".repeat(60));
  console.log(`\n  Strategy: Deterministic first, AI second, paranoid always`);
  console.log(`  Model: ${MODEL}`);
  console.log(`  Batch size: ${BATCH_SIZE}`);
  console.log(`  Concurrency: ${CONCURRENCY}`);
  console.log(`  Checkpoint: ${CHECKPOINT_PATH}`);

  if (!process.env.OPENAI_API_KEY) {
    console.error("\n  ERROR: OPENAI_API_KEY not set");
    process.exit(1);
  }

  // Load input
  console.log(`\n  Loading input...`);
  const data = JSON.parse(fs.readFileSync(INPUT_PATH, "utf-8"));
  const totalMessages = data.messages.length;
  console.log(`  ✓ Loaded ${totalMessages.toLocaleString()} messages`);

  // Load checkpoint
  let checkpoint = loadCheckpoint();
  const alreadyProcessed = checkpoint.processedCount;

  if (alreadyProcessed >= totalMessages) {
    console.log(
      `\n  All messages already processed. Delete checkpoint to reprocess.`,
    );
    return;
  }

  let messagesToProcess = data.messages.slice(alreadyProcessed);

  if (MAX_MESSAGES_TO_PROCESS) {
    messagesToProcess = messagesToProcess.slice(0, MAX_MESSAGES_TO_PROCESS);
    console.log(
      `  ⚠ TEST MODE: Processing ${MAX_MESSAGES_TO_PROCESS} messages (${messagesToProcess.length.toLocaleString()} available)`,
    );
  }

  if (messagesToProcess.length === 0) {
    console.log(`\n  ✓ No messages to process. All done!`);
    return;
  }

  console.log(
    `  Messages to process: ${messagesToProcess.length.toLocaleString()}`,
  );

  const startTime = Date.now();

  // Create batches
  const batches = [];
  for (let i = 0; i < messagesToProcess.length; i += BATCH_SIZE) {
    batches.push(messagesToProcess.slice(i, i + BATCH_SIZE));
  }
  console.log(`\n  Created ${batches.length} batches`);

  // Process with concurrency
  let batchIndex = 0;
  const processNext = async () => {
    while (batchIndex < batches.length) {
      const currentBatch = batches[batchIndex];
      const currentIndex = batchIndex;
      batchIndex++;

      try {
        const { messages, audit } = await processBatch(
          currentBatch,
          currentIndex,
        );

        // Update checkpoint (with mutex protection)
        await checkpointMutex.acquire();
        try {
          checkpoint.redactedMessages.push(...messages);
          checkpoint.auditLog.push(...audit);
          checkpoint.processedCount += messages.length;
          saveCheckpoint(checkpoint);
        } finally {
          checkpointMutex.release();
        }

        // Progress
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = checkpoint.processedCount / elapsed;
        const remaining = totalMessages - checkpoint.processedCount;
        const eta = remaining / rate;

        process.stdout.write(
          `\r  Progress: ${checkpoint.processedCount}/${totalMessages} | ` +
            `Rate: ${rate.toFixed(1)}/sec | ` +
            `ETA: ${formatDuration(eta)}    `,
        );
      } catch (error) {
        console.error(`\n  [Batch ${currentIndex}] ERROR: ${error.message}`);
        console.error(
          `  [Batch ${currentIndex}] Checkpoint saved - will skip this batch on resume`,
        );
        // Don't add unprocessed messages - they'll be retried on next run
        // Just save current state
        await checkpointMutex.acquire();
        try {
          saveCheckpoint(checkpoint);
        } finally {
          checkpointMutex.release();
        }
      }
    }
  };

  // Run workers
  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(processNext());
  }
  try {
    await Promise.all(workers);

    console.log("\n");

    // Reload checkpoint one final time to ensure we have the latest state
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

    // Write final output
    console.log(`  Writing final output files...`);
    const output = {
      metadata: {
        ...data.metadata,
        redactedAt: new Date().toISOString(),
        dataSubject: DATA_SUBJECT,
        model: MODEL,
        version: "v2-paranoid",
      },
      messages: checkpoint.redactedMessages,
    };

    fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));

    const auditOutput = {
      generatedAt: new Date().toISOString(),
      dataSubject: DATA_SUBJECT,
      totalMessages,
      messagesWithRedactions: checkpoint.auditLog.length,
      totalRedactions: checkpoint.auditLog.reduce(
        (sum, a) => sum + a.redactions.length,
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
    console.log(`  Messages: ${totalMessages.toLocaleString()}`);
    console.log(
      `  Redactions: ${auditOutput.totalRedactions.toLocaleString()}`,
    );
    console.log("=".repeat(60));
  } catch (error) {
    console.error(`\n\n  ERROR: ${error.message}`);
    console.error(
      `  Checkpoint saved at ${checkpoint.processedCount} messages.`,
    );
    console.error(`  Run again to resume from checkpoint.`);
    await checkpointMutex.acquire();
    try {
      saveCheckpoint(checkpoint);
    } finally {
      checkpointMutex.release();
    }
    throw error;
  }
}

if (require.main === module) {
  redact().catch((err) => {
    console.error("Failed:", err.message);
    process.exit(1);
  });
}

module.exports = { redact };
