const { PSTFile, PSTFolder, PSTMessage } = require("pst-extractor");
const { convert } = require("html-to-text");
const path = require("path");
const fs = require("fs");

const OUTPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json",
);

// PST files to process
const PST_FILES = [
  // "john_all_business_1.pst",
  // "john_all_business_2.pst",
  // "john_received_emails.pst",
  // "john_sent_teams.pst",
  "john@freightlink.co.uk.001.pst",
];

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
  errors = [],
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
          errors,
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
            sourceFile,
          );
          msgData.folder = folderName;
          messages.push(msgData);

          // Progress update every 1000 messages
          if (messages.length % 1000 === 0) {
            const elapsed = (Date.now() - progress.startTime) / 1000;
            const rate = messages.length / elapsed;
            console.log(
              `  [${sourceFile}] Extracted ${messages.length.toLocaleString()} messages (${rate.toFixed(
                0,
              )}/sec)`,
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
        `  WARNING: Skipped ${folder.contentCount} items in "${folderName}" (corrupted index)`,
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
      errors,
    );

    const addedCount = messages.length - beforeCount;

    // Calculate skipped items from errors
    const skippedItems = errors
      .filter((e) => e.expectedItems)
      .reduce((sum, e) => sum + e.expectedItems, 0);

    console.log(`  -> Added ${addedCount.toLocaleString()} messages`);
    if (skippedItems > 0) {
      console.log(
        `  -> Skipped ${skippedItems.toLocaleString()} items (corrupted folders)`,
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

  const rootDir = path.resolve(__dirname, "..");
  const availableFiles = PST_FILES.filter((f) =>
    fs.existsSync(path.join(rootDir, f)),
  );

  console.log(`\nFound ${availableFiles.length} PST files to process:`);
  availableFiles.forEach((f) => console.log(`  - ${f}`));
  console.log(`\nOutput: ${OUTPUT_PATH}`);

  const messages = [];
  const globalIndex = { value: 0 };
  const progress = { startTime: Date.now() };
  const fileSummaries = [];

  for (const pstFile of availableFiles) {
    const pstPath = path.join(rootDir, pstFile);
    const summary = processPstFile(pstPath, messages, globalIndex, progress);
    fileSummaries.push({ file: pstFile, ...summary });
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
