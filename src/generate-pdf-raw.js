/**
 * Generate PDFs from raw (unredacted) extracted messages.
 * Same format as generate-pdf.js but uses output/extracted-messages.json
 * and outputs as dsar-raw-{source}.pdf
 */

const PDFDocument = require("pdfkit");
const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json"
);
const OUTPUT_DIR = path.resolve(__dirname, "..", "output");

// Page dimensions and margins
const PAGE_MARGIN = 50;
const CONTENT_WIDTH = 595.28 - PAGE_MARGIN * 2; // A4 width minus margins

const DATA_SUBJECT = {
  name: "John Gaskell",
  email: "john@freightlink.co.uk",
};

function formatDate(dateStr) {
  if (!dateStr) return "N/A";
  try {
    const date = new Date(dateStr);
    return date.toLocaleString("en-GB", {
      dateStyle: "medium",
      timeStyle: "short",
    });
  } catch {
    return dateStr;
  }
}

function normalizeWhitespace(text) {
  if (!text) return "";
  text = text.replace(/\n{2,}/g, "\n");
  text = text.replace(/[ \t]{2,}/g, " ");
  text = text.replace(/[ \t]+$/gm, "");
  text = text.replace(/^[ \t]+/gm, (match, offset, string) => {
    const line = string.substring(offset);
    if (/^[ \t]+[•\-\*\+]/.test(line)) return " ";
    return "";
  });
  text = text.replace(/^\s*$/gm, "");
  text = text.replace(/\n{2,}/g, "\n");
  return text.trim();
}

function truncateText(text, maxLength = 5000) {
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return (
    text.substring(0, maxLength) + "\n\n[... Content truncated for display ...]"
  );
}

function createSafeFilename(sourceFile) {
  const baseName = sourceFile
    .replace(/\.pst$/i, "")
    .replace(/[^a-zA-Z0-9_-]/g, "_");
  return `dsar-raw-${baseName}.pdf`;
}

/** Safe filename for messageClass (e.g. IPM.Note -> IPM_Note) */
function createSafeFilenameForMessageClass(messageClass) {
  const baseName = (messageClass || "Unknown")
    .replace(/\./g, "_")
    .replace(/[^a-zA-Z0-9_-]/g, "_");
  return `dsar-raw-${baseName}.pdf`;
}

function addCoverPage(doc, groupLabel, groupValue, messages, metadata) {
  doc.fontSize(24).font("Helvetica-Bold").text("DATA SUBJECT ACCESS REQUEST", {
    align: "center",
  });

  doc.moveDown(0.5);
  doc.fontSize(18).font("Helvetica").text("Response Document (Raw / Unredacted)", {
    align: "center",
  });

  doc.moveDown(2);

  doc.fontSize(12).font("Helvetica-Bold").text("Data Subject:");
  doc.font("Helvetica");
  doc.text(DATA_SUBJECT.name);
  doc.text(DATA_SUBJECT.email);

  doc.moveDown(1);

  doc.font("Helvetica-Bold").text(`${groupLabel}:`);
  doc.font("Helvetica");
  doc.text(groupValue);
  doc.text(`Messages: ${messages.length.toLocaleString()}`);

  doc.moveDown(1);

  doc.font("Helvetica-Bold").text("Document Information:");
  doc.font("Helvetica");
  doc.text(`Generated: ${formatDate(new Date().toISOString())}`);

  doc.moveDown(2);

  doc.font("Helvetica-Bold").text("Notice:");
  doc.font("Helvetica").fontSize(10);
  const noticeHeight = doc.page.height - doc.y - PAGE_MARGIN;
  doc.text(
    "This document is a raw export of extracted messages and has NOT been redacted. " +
      "It is for internal or pre-redaction use only. Do not disclose without appropriate redaction. " +
      "For the redacted DSAR response, use the dsar-{source}.pdf files.",
    { align: "justify", width: CONTENT_WIDTH, height: Math.max(100, noticeHeight) }
  );
}

function addMessage(doc, msg, index, total) {
  if (doc.y > 700) {
    doc.addPage();
  }

  doc.fontSize(10).font("Helvetica-Bold");
  doc.fillColor("#333333");
  doc.text(`Message ${index} of ${total.toLocaleString()}`, {
    continued: true,
  });
  doc.fillColor("#666666").font("Helvetica").text(`  ID: ${msg.id}`);

  doc.moveDown(0.3);

  doc.strokeColor("#cccccc").lineWidth(0.5);
  doc
    .moveTo(PAGE_MARGIN, doc.y)
    .lineTo(PAGE_MARGIN + CONTENT_WIDTH, doc.y)
    .stroke();
  doc.moveDown(0.3);

  doc.fillColor("#000000").fontSize(9);

  doc.font("Helvetica-Bold").text("Date: ", { continued: true });
  doc.font("Helvetica").text(formatDate(msg.date));

  doc.font("Helvetica-Bold").text("From: ", { continued: true });
  doc
    .font("Helvetica")
    .text(`${msg.sender?.name || "?"} <${msg.sender?.email || "?"}>`);

  doc.font("Helvetica-Bold").text("To: ", { continued: true });
  doc.font("Helvetica").text(msg.recipients?.to || "N/A");

  if (msg.recipients?.cc) {
    doc.font("Helvetica-Bold").text("CC: ", { continued: true });
    doc.font("Helvetica").text(msg.recipients.cc);
  }

  if (msg.subject) {
    doc.font("Helvetica-Bold").text("Subject: ", { continued: true });
    doc.font("Helvetica").text(msg.subject);
  }

  doc.font("Helvetica-Bold").text("Type: ", { continued: true });
  doc.font("Helvetica").text(msg.messageClass || "Unknown");

  doc.moveDown(0.5);

  doc.font("Helvetica-Bold").text("Content:");
  doc.font("Helvetica").fontSize(9);

  let body = msg.body || "(No content)";
  body = normalizeWhitespace(body);
  body = truncateText(body);
  doc.fillColor("#000000");

  const lineHeight = doc.currentLineHeight(true) + 2;
  const pageBottom = doc.page.height - 50;
  const lines = body.split("\n");

  for (const line of lines) {
    if (doc.y + lineHeight > pageBottom) {
      doc.addPage();
    }
    const availableHeight = Math.max(lineHeight, pageBottom - doc.y);
    doc.text(line || " ", {
      width: CONTENT_WIDTH,
      height: availableHeight,
      align: "left",
      lineGap: 2,
    });
  }

  if (msg.hasAttachments && msg.attachments?.length > 0) {
    doc.moveDown(0.5);
    doc.font("Helvetica-Bold").text("Attachments:");
    doc.font("Helvetica");
    for (const att of msg.attachments) {
      doc.text(`  • ${att.filename} (${att.mimeType})`);
    }
  }

  doc.moveDown(1);

  doc.strokeColor("#eeeeee").lineWidth(1);
  doc
    .moveTo(PAGE_MARGIN, doc.y)
    .lineTo(PAGE_MARGIN + CONTENT_WIDTH, doc.y)
    .stroke();
  doc.moveDown(0.5);
}

function generatePdfForGroup(outputFilename, groupLabel, groupValue, messages, metadata) {
  return new Promise((resolve, reject) => {
    const outputPath = path.join(OUTPUT_DIR, outputFilename);

    const doc = new PDFDocument({
      size: "A4",
      margins: {
        top: PAGE_MARGIN,
        bottom: PAGE_MARGIN,
        left: PAGE_MARGIN,
        right: PAGE_MARGIN,
      },
      bufferPages: true,
      info: {
        Title: `DSAR Raw - ${DATA_SUBJECT.name} - ${groupValue}`,
        Author: "Freightlink",
        Subject: "Data Subject Access Request - Raw Export",
        Keywords: "DSAR, GDPR, UK GDPR, Data Protection, Raw",
      },
    });

    const stream = fs.createWriteStream(outputPath);
    doc.pipe(stream);

    addCoverPage(doc, groupLabel, groupValue, messages, metadata);

    const total = messages.length;
    if (total > 0) {
      doc.addPage();
      messages.forEach((msg, index) => {
        addMessage(doc, msg, index + 1, total);
      });
    }

    doc.end();

    stream.on("finish", () => resolve(outputPath));
    stream.on("error", reject);
  });
}

async function generatePdfRaw() {
  console.log("=".repeat(60));
  console.log("GENERATING PDFs FROM RAW EXTRACTED MESSAGES");
  console.log("=".repeat(60));
  console.log(`\nInput: ${INPUT_PATH}`);
  console.log(`Output directory: ${OUTPUT_DIR}\n`);

  if (!fs.existsSync(INPUT_PATH)) {
    console.error(`ERROR: Input file not found: ${INPUT_PATH}`);
    console.error("Run the parse step first to create extracted-messages.json");
    process.exit(1);
  }

  const data = JSON.parse(fs.readFileSync(INPUT_PATH, "utf-8"));
  console.log(
    `Loaded ${data.messages.length.toLocaleString()} raw messages`
  );

  // Group by messageClass
  const messagesByClass = new Map();
  for (const msg of data.messages) {
    const messageClass = msg.messageClass || "Unknown";
    if (!messagesByClass.has(messageClass)) {
      messagesByClass.set(messageClass, []);
    }
    messagesByClass.get(messageClass).push(msg);
  }

  // Sort each group by date (oldest first)
  for (const [messageClass, messages] of messagesByClass) {
    messages.sort((a, b) => {
      const dateA = a.date ? new Date(a.date).getTime() : 0;
      const dateB = b.date ? new Date(b.date).getTime() : 0;
      return dateA - dateB;
    });
  }

  console.log(`\nFound ${messagesByClass.size} message classes (sorted by date):`);
  for (const [messageClass, msgs] of messagesByClass) {
    console.log(`  - ${messageClass}: ${msgs.length.toLocaleString()} messages`);
  }

  console.log("\nGenerating PDFs...");
  const generatedFiles = [];

  for (const [messageClass, messages] of messagesByClass) {
    const outputFilename = createSafeFilenameForMessageClass(messageClass);
    console.log(
      `  Processing: ${messageClass} (${messages.length.toLocaleString()} messages)...`
    );
    const outputPath = await generatePdfForGroup(
      outputFilename,
      "Message class",
      messageClass,
      messages,
      data.metadata
    );
    generatedFiles.push(outputPath);
    console.log(`    ✓ Created: ${path.basename(outputPath)}`);
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log("PDF GENERATION COMPLETE (RAW)");
  console.log(`  Generated ${generatedFiles.length} PDF files:`);
  for (const file of generatedFiles) {
    const stats = fs.statSync(file);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(1);
    console.log(`    - ${path.basename(file)} (${sizeMB} MB)`);
  }
  console.log("=".repeat(60));

  return generatedFiles;
}

if (require.main === module) {
  generatePdfRaw().catch((err) => {
    console.error("Error generating PDF:", err);
    process.exit(1);
  });
}

module.exports = { generatePdfRaw };
