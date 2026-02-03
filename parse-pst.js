const { PSTFile, PSTFolder, PSTMessage } = require("pst-extractor");
const path = require("path");

const pstFilePath = path.resolve(__dirname, "data.pst");

console.log("Opening PST file:", pstFilePath);
console.log("---");

const pstFile = new PSTFile(pstFilePath);
console.log("PST Store Name:", pstFile.getMessageStore().displayName);
console.log("---\n");

let messageCount = 0;
const maxMessages = 100; // Limit to first 5 messages for demo

/**
 * Recursively process folders and extract messages
 * @param {PSTFolder} folder
 * @param {number} depth
 */
function processFolder(folder, depth = 0) {
  if (messageCount >= maxMessages) return;

  const indent = "  ".repeat(depth);

  // Print folder name (skip root which has no display name)
  if (depth > 0) {
    console.log(
      `${indent}📁 Folder: ${folder.displayName} (${folder.contentCount} items)`
    );
  }

  // Process subfolders first
  if (folder.hasSubfolders) {
    const subfolders = folder.getSubFolders();
    for (const subfolder of subfolders) {
      if (messageCount >= maxMessages) break;
      processFolder(subfolder, depth + 1);
    }
  }

  // Process emails in this folder
  if (folder.contentCount > 0) {
    let email = folder.getNextChild();
    while (email != null && messageCount < maxMessages) {
      if (email instanceof PSTMessage) {
        messageCount++;
        console.log(`\n${"=".repeat(60)}`);
        console.log(`MESSAGE #${messageCount}`);
        console.log(`${"=".repeat(60)}`);

        // Sender information
        console.log("\n📤 SENDER:");
        console.log(`   Name: ${email.senderName || "(none)"}`);
        console.log(`   Email: ${email.senderEmailAddress || "(none)"}`);

        // Recipient information
        console.log("\n📥 RECIPIENTS:");
        console.log(`   To: ${email.displayTo || "(none)"}`);
        console.log(`   CC: ${email.displayCC || "(none)"}`);
        console.log(`   BCC: ${email.displayBCC || "(none)"}`);

        // Subject
        console.log(`\n📋 SUBJECT: ${email.subject || "(no subject)"}`);

        // Message class (useful for eDiscovery to know what type)
        console.log(`📌 TYPE: ${email.messageClass || "(unknown)"}`);

        // Date
        console.log(`📅 DATE: ${email.clientSubmitTime || "(no date)"}`);

        // Body - try multiple sources (plain text, HTML, conversation topic)
        let body = email.body || "";
        let bodySource = "plain text";

        // If plain text is empty, try HTML body
        if (!body && email.bodyHTML) {
          body = email.bodyHTML;
          bodySource = "HTML";
        }

        // For Teams messages, content might be in conversation topic
        if (!body && email.conversationTopic) {
          body = `[Conversation Topic]: ${email.conversationTopic}`;
          bodySource = "conversation topic";
        }

        const bodyPreview =
          body.length > 500
            ? body.substring(0, 500) + "...\n   [truncated]"
            : body;
        console.log(
          `\n📝 BODY (${bodySource}):\n${bodyPreview || "(empty body)"}`
        );

        // Attachments info
        if (email.hasAttachments) {
          console.log(
            `\n📎 ATTACHMENTS: Yes (${email.numberOfAttachments} attachment(s))`
          );
        }
      }
      email = folder.getNextChild();
    }
  }
}

try {
  processFolder(pstFile.getRootFolder());
  console.log(`\n\n${"=".repeat(60)}`);
  console.log(`Parsed ${messageCount} message(s) from PST file.`);
  console.log(`${"=".repeat(60)}`);
} catch (error) {
  console.error("Error parsing PST file:", error.message);
  process.exit(1);
}
