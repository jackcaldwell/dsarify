const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json"
);
const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redacted-messages.json"
);
const AUDIT_PATH = path.resolve(__dirname, "..", "output", "audit-log.json");

// Data subject identifiers - these should NOT be redacted
const DATA_SUBJECT = {
  names: ["john gaskell", "john", "gaskell", "j. gaskell", "j gaskell"],
  emails: ["john@freightlink.co.uk"],
};

/**
 * Build a set of third-party identifiers from the messages
 */
function buildThirdPartyList(messages) {
  const thirdPartyNames = new Set();
  const thirdPartyEmails = new Set();

  for (const msg of messages) {
    // Collect sender info
    if (msg.sender.name) {
      const name = msg.sender.name.toLowerCase().trim();
      if (!isDataSubject(name, msg.sender.email)) {
        thirdPartyNames.add(msg.sender.name.trim());
      }
    }
    if (msg.sender.email) {
      const email = msg.sender.email.toLowerCase().trim();
      if (!DATA_SUBJECT.emails.includes(email)) {
        thirdPartyEmails.add(msg.sender.email.trim());
      }
    }

    // Collect recipient info (parse comma-separated lists)
    for (const field of ["to", "cc", "bcc"]) {
      const recipientStr = msg.recipients[field];
      if (recipientStr) {
        // Recipients can be comma or semicolon separated
        const recipients = recipientStr
          .split(/[;,]/)
          .map((r) => r.trim())
          .filter(Boolean);
        for (const recipient of recipients) {
          const lower = recipient.toLowerCase();
          if (!isDataSubject(lower, "")) {
            thirdPartyNames.add(recipient);
          }
        }
      }
    }
  }

  return {
    names: Array.from(thirdPartyNames).sort(),
    emails: Array.from(thirdPartyEmails).sort(),
  };
}

/**
 * Check if a name/email belongs to the data subject
 */
function isDataSubject(name, email) {
  const lowerName = name.toLowerCase().trim();
  const lowerEmail = email.toLowerCase().trim();

  // Check email match
  if (lowerEmail && DATA_SUBJECT.emails.includes(lowerEmail)) {
    return true;
  }

  // Check name match
  for (const dsName of DATA_SUBJECT.names) {
    if (lowerName === dsName || lowerName.includes(dsName)) {
      return true;
    }
  }

  return false;
}

/**
 * Escape special regex characters
 */
function escapeRegex(str) {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/**
 * Redact third-party information from text
 */
function redactText(text, thirdParties, auditLog, messageId) {
  if (!text) return text;

  let redactedText = text;
  const redactions = [];

  // Redact email addresses (using regex pattern)
  const emailPattern = /\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/g;
  const emails = text.match(emailPattern) || [];

  for (const email of emails) {
    const lowerEmail = email.toLowerCase();
    if (!DATA_SUBJECT.emails.includes(lowerEmail)) {
      const pattern = new RegExp(escapeRegex(email), "gi");
      if (pattern.test(redactedText)) {
        redactedText = redactedText.replace(pattern, "[REDACTED EMAIL]");
        redactions.push({
          type: "email",
          original: email,
          replacement: "[REDACTED EMAIL]",
        });
      }
    }
  }

  // Redact third-party names (longer names first to avoid partial matches)
  const sortedNames = [...thirdParties.names].sort(
    (a, b) => b.length - a.length
  );

  for (const name of sortedNames) {
    // Skip very short names (3 chars or less) to avoid false positives
    if (name.length <= 3) continue;

    // Skip if it's a data subject name
    if (isDataSubject(name, "")) continue;

    const pattern = new RegExp("\\b" + escapeRegex(name) + "\\b", "gi");
    if (pattern.test(redactedText)) {
      const beforeRedact = redactedText;
      redactedText = redactedText.replace(pattern, "[REDACTED NAME]");
      if (beforeRedact !== redactedText) {
        redactions.push({
          type: "name",
          original: name,
          replacement: "[REDACTED NAME]",
        });
      }
    }
  }

  // Log redactions for this message
  if (redactions.length > 0) {
    auditLog.push({
      messageId: messageId,
      redactions: redactions,
    });
  }

  return redactedText;
}

/**
 * Redact a single message
 */
function redactMessage(msg, thirdParties, auditLog) {
  const redactedMsg = { ...msg };

  // Redact body
  redactedMsg.body = redactText(msg.body, thirdParties, auditLog, msg.id);

  // Redact subject
  redactedMsg.subject = redactText(msg.subject, thirdParties, auditLog, msg.id);

  // Redact sender (only if not data subject)
  if (!isDataSubject(msg.sender.name || "", msg.sender.email || "")) {
    redactedMsg.sender = {
      name: "[REDACTED NAME]",
      email: "[REDACTED EMAIL]",
    };
  }

  // Redact recipients
  redactedMsg.recipients = {
    to: redactRecipientField(msg.recipients.to, thirdParties),
    cc: redactRecipientField(msg.recipients.cc, thirdParties),
    bcc: redactRecipientField(msg.recipients.bcc, thirdParties),
  };

  return redactedMsg;
}

/**
 * Redact a recipient field (comma-separated list)
 */
function redactRecipientField(recipientStr, thirdParties) {
  if (!recipientStr) return recipientStr;

  const recipients = recipientStr
    .split(/[;,]/)
    .map((r) => r.trim())
    .filter(Boolean);
  const redactedRecipients = recipients.map((recipient) => {
    if (isDataSubject(recipient, "")) {
      return recipient;
    }
    return "[REDACTED NAME]";
  });

  return redactedRecipients.join("; ");
}

/**
 * Main redaction function
 */
function redact() {
  console.log("=".repeat(60));
  console.log("PHASE 2: REDACTING THIRD-PARTY DATA");
  console.log("=".repeat(60));
  console.log(`\nInput: ${INPUT_PATH}`);
  console.log(`Output: ${OUTPUT_PATH}`);
  console.log(`Audit Log: ${AUDIT_PATH}\n`);

  // Load extracted messages
  const data = JSON.parse(fs.readFileSync(INPUT_PATH, "utf-8"));
  console.log(`Loaded ${data.messages.length} messages`);

  // Build third-party list
  console.log("\nBuilding third-party identifier list...");
  const thirdParties = buildThirdPartyList(data.messages);
  console.log(`  Found ${thirdParties.names.length} unique third-party names`);
  console.log(
    `  Found ${thirdParties.emails.length} unique third-party emails`
  );

  // Redact messages
  console.log("\nRedacting messages...");
  const auditLog = [];
  const redactedMessages = data.messages.map((msg, index) => {
    if ((index + 1) % 5000 === 0) {
      console.log(`  Processed ${index + 1} messages...`);
    }
    return redactMessage(msg, thirdParties, auditLog);
  });

  // Build output
  const output = {
    metadata: {
      ...data.metadata,
      redactedAt: new Date().toISOString(),
      dataSubject: {
        name: "John Gaskell",
        email: "john@freightlink.co.uk",
      },
      thirdPartiesRedacted: {
        uniqueNames: thirdParties.names.length,
        uniqueEmails: thirdParties.emails.length,
      },
      messagesWithRedactions: auditLog.length,
    },
    messages: redactedMessages,
  };

  // Write redacted messages
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));

  // Write audit log
  const auditOutput = {
    generatedAt: new Date().toISOString(),
    dataSubject: {
      name: "John Gaskell",
      email: "john@freightlink.co.uk",
    },
    thirdPartiesIdentified: thirdParties,
    totalRedactions: auditLog.reduce(
      (sum, entry) => sum + entry.redactions.length,
      0
    ),
    messagesAffected: auditLog.length,
    details: auditLog,
  };
  fs.writeFileSync(AUDIT_PATH, JSON.stringify(auditOutput, null, 2));

  console.log(`\n✓ Redacted ${auditLog.length} messages with third-party data`);
  console.log(`✓ Total redactions applied: ${auditOutput.totalRedactions}`);
  console.log(`✓ Output written to ${OUTPUT_PATH}`);
  console.log(`✓ Audit log written to ${AUDIT_PATH}`);
  console.log("=".repeat(60));

  return output;
}

// Run if called directly
if (require.main === module) {
  redact();
}

module.exports = { redact };
