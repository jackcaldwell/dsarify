const { PSTFile, PSTFolder, PSTMessage } = require("pst-extractor");
const { convert } = require("html-to-text");
const path = require("path");
const fs = require("fs");

const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json"
);

// PST export directories (from MS Purview exports under pst/)
const PST_ROOT = path.resolve(__dirname, "..", "pst");
const PST_EXPORT_DIRS = ["Exchange", "Exchange 2", "Exchange 3"];

/**
 * Recursively find all .pst files under a directory
 */
function findPstFiles(dir, baseDir = dir) {
  const results = [];
  if (!fs.existsSync(dir)) return results;

  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const ent of entries) {
    const fullPath = path.join(dir, ent.name);
    if (ent.isDirectory()) {
      results.push(...findPstFiles(fullPath, baseDir));
    } else if (ent.isFile() && ent.name.toLowerCase().endsWith(".pst")) {
      results.push(fullPath);
    }
  }
  return results;
}

/**
 * Collect all PST file paths from the configured export directories
 */
function getPstFilesToProcess() {
  const pstPaths = [];
  for (const exportDir of PST_EXPORT_DIRS) {
    const dirPath = path.join(PST_ROOT, exportDir);
    const found = findPstFiles(dirPath);
    pstPaths.push(...found);
  }
  return pstPaths;
}

/**
 * Trim reply threads from message body.
 * Keeps only the first/current message, removes quoted replies.
 * Same logic as redact-v2.js so extracted data is consistent.
 */
function trimReplyThreads(body) {
  if (!body) return body;

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
      trimmed = trimmed.substring(0, match.index).trim();
    }
  }
  return trimmed;
}

/**
 * Convert HTML body to plain text
 */
function htmlToPlainText(html) {
  if (!html) return "";
  return convert(html, {
    wordwrap: false,
    selectors: [
      { selector: "a", options: { ignoreHref: true } },
      { selector: "img", format: "skip" },
    ],
  }).trim();
}

/**
 * Extract attachment information from a message
 */
function extractAttachments(email) {
  const attachments = [];
  if (email.hasAttachments && email.numberOfAttachments > 0) {
    for (let i = 0; i < email.numberOfAttachments; i++) {
      try {
        const attachment = email.getAttachment(i);
        if (attachment) {
          attachments.push({
            filename:
              attachment.longFilename ||
              attachment.filename ||
              `attachment_${i}`,
            size: attachment.size || 0,
            mimeType: attachment.mimeTag || "unknown",
          });
        }
      } catch (err) {
        attachments.push({
          filename: `attachment_${i}`,
          size: 0,
          mimeType: "unknown",
          error: "Could not read attachment",
        });
      }
    }
  }
  return attachments;
}

/**
 * Extract message data into a structured object
 */
function extractMessageData(email, index, sourceFile) {
  let body = email.body || "";
  let bodySource = "plain";

  if (!body && email.bodyHTML) {
    body = htmlToPlainText(email.bodyHTML);
    bodySource = "html";
  }

  if (!body && email.conversationTopic) {
    body = email.conversationTopic;
    bodySource = "topic";
  }

  body = trimReplyThreads(body);

  return {
    id: index,
    sourceFile: sourceFile,
    sender: {
      name: email.senderName || "",
      email: email.senderEmailAddress || "",
    },
    recipients: {
      to: email.displayTo || "",
      cc: email.displayCC || "",
      bcc: email.displayBCC || "",
    },
    subject: email.subject || "",
    body: body,
    bodySource: bodySource,
    date: email.clientSubmitTime ? email.clientSubmitTime.toISOString() : null,
    messageClass: email.messageClass || "",
    conversationTopic: email.conversationTopic || "",
    attachments: extractAttachments(email),
    hasAttachments: email.hasAttachments || false,
  };
}

/**
 * Recursively process folders and extract all messages
 * Now with error handling for corrupted PST sections
 */
function processFolder(
  folder,
  messages,
  sourceFile,
  globalIndex,
  progress,
  errors = []
) {
  const folderName = folder.displayName || "Root";

  // Process subfolders with error handling
  if (folder.hasSubfolders) {
    try {
      const subfolders = folder.getSubFolders();
      for (const subfolder of subfolders) {
        processFolder(
          subfolder,
          messages,
          sourceFile,
          globalIndex,
          progress,
          errors
        );
      }
    } catch (err) {
      errors.push({
        folder: folderName,
        type: "subfolders",
        error: err.message,
      });
    }
  }

  // Process messages with error handling
  if (folder.contentCount > 0) {
    try {
      let email = folder.getNextChild();
      while (email != null) {
        if (email instanceof PSTMessage) {
          globalIndex.value++;
          const msgData = extractMessageData(
            email,
            globalIndex.value,
            sourceFile
          );
          msgData.folder = folderName;
          messages.push(msgData);

          // Progress update every 1000 messages
          if (messages.length % 1000 === 0) {
            const elapsed = (Date.now() - progress.startTime) / 1000;
            const rate = messages.length / elapsed;
            console.log(
              `  [${sourceFile}] Extracted ${messages.length.toLocaleString()} messages (${rate.toFixed(
                0
              )}/sec)`
            );
          }
        }
        email = folder.getNextChild();
      }
    } catch (err) {
      errors.push({
        folder: folderName,
        type: "content",
        expectedItems: folder.contentCount,
        error: err.message,
      });
      console.log(
        `  WARNING: Skipped ${folder.contentCount} items in "${folderName}" (corrupted index)`
      );
    }
  }

  return errors;
}

/**
 * Process a single PST file
 */
function processPstFile(pstPath, messages, globalIndex, progress) {
  const filename = path.basename(pstPath);
  console.log(`\n  Processing: ${filename}`);

  try {
    const pstFile = new PSTFile(pstPath);
    const storeName = pstFile.getMessageStore().displayName;
    console.log(`  Store: ${storeName}`);

    const beforeCount = messages.length;
    const errors = [];

    processFolder(
      pstFile.getRootFolder(),
      messages,
      filename,
      globalIndex,
      progress,
      errors
    );

    const addedCount = messages.length - beforeCount;

    // Calculate skipped items from errors
    const skippedItems = errors
      .filter((e) => e.expectedItems)
      .reduce((sum, e) => sum + e.expectedItems, 0);

    console.log(`  -> Added ${addedCount.toLocaleString()} messages`);
    if (skippedItems > 0) {
      console.log(
        `  -> Skipped ${skippedItems.toLocaleString()} items (corrupted folders)`
      );
    }

    return {
      storeName,
      messageCount: addedCount,
      skippedItems,
      errors: errors.length > 0 ? errors : undefined,
    };
  } catch (error) {
    console.error(`  ERROR processing ${filename}: ${error.message}`);
    return { storeName: "ERROR", messageCount: 0, error: error.message };
  }
}

/**
 * Main parse function
 */
function parse() {
  console.log("=".repeat(60));
  console.log("PHASE 1: PARSING PST FILES");
  console.log("=".repeat(60));

  const pstPaths = getPstFilesToProcess();

  console.log(`\nScanning: ${PST_ROOT}`);
  console.log(`  Export dirs: ${PST_EXPORT_DIRS.join(", ")}`);
  console.log(`\nFound ${pstPaths.length} PST files to process:`);
  pstPaths.forEach((p) => console.log(`  - ${path.relative(PST_ROOT, p)}`));
  console.log(`\nOutput: ${OUTPUT_PATH}`);

  const messages = [];
  const globalIndex = { value: 0 };
  const progress = { startTime: Date.now() };
  const fileSummaries = [];

  for (const pstPath of pstPaths) {
    const filename = path.basename(pstPath);
    const summary = processPstFile(pstPath, messages, globalIndex, progress);
    fileSummaries.push({ file: filename, path: pstPath, ...summary });
  }

  // Build metadata
  const output = {
    metadata: {
      sourceFiles: fileSummaries,
      extractedAt: new Date().toISOString(),
      totalMessages: messages.length,
    },
    messages: messages,
  };

  // Write to JSON file
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(output, null, 2));

  const elapsed = ((Date.now() - progress.startTime) / 1000).toFixed(1);
  console.log(`\n${"=".repeat(60)}`);
  console.log(`PARSING COMPLETE`);
  console.log(`  Total messages: ${messages.length.toLocaleString()}`);
  console.log(`  Time: ${elapsed}s`);
  console.log(`  Output: ${OUTPUT_PATH}`);
  console.log("=".repeat(60));

  return output;
}

if (require.main === module) {
  parse();
}

module.exports = { parse };
