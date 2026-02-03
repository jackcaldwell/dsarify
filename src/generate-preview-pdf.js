/**
 * Generate preview PDFs from the redaction checkpoint - split by source file
 * Useful for reviewing partial results while redaction is still running
 */

const PDFDocument = require("pdfkit");
const fs = require("fs");
const path = require("path");

const CHECKPOINT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redaction-checkpoint.json"
);
const EXTRACTED_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "extracted-messages.json"
);
const OUTPUT_DIR = path.resolve(__dirname, "..", "output");

const PAGE_MARGIN = 50;
const CONTENT_WIDTH = 595.28 - PAGE_MARGIN * 2;

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

/**
 * Normalize whitespace - aggressively reduce excessive newlines and spaces
 */
function normalizeWhitespace(text) {
  if (!text) return "";
  
  // Replace 2+ consecutive newlines with max 1 newline (much more aggressive)
  text = text.replace(/\n{2,}/g, "\n");
  
  // Replace multiple spaces/tabs with single space
  text = text.replace(/[ \t]{2,}/g, " ");
  
  // Remove trailing whitespace from each line
  text = text.replace(/[ \t]+$/gm, "");
  
  // Remove leading whitespace from each line (except intentional indentation like "  • ")
  text = text.replace(/^[ \t]+/gm, (match, offset, string) => {
    const line = string.substring(offset);
    // Keep one space for bullet points
    if (/^[ \t]+[•\-\*\+]/.test(line)) {
      return " ";
    }
    return "";
  });
  
  // Remove lines that are only whitespace (empty lines)
  text = text.replace(/^\s*$/gm, "");
  
  // Replace any remaining 2+ newlines with single newline (catch any we missed)
  text = text.replace(/\n{2,}/g, "\n");
  
  // Clean up any remaining excessive whitespace
  text = text.trim();
  
  return text;
}

function truncateText(text, maxLength = 3000) {
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + "\n\n[... Content truncated ...]";
}

function createSafeFilename(sourceFile) {
  const baseName = sourceFile
    .replace(/\.pst$/i, "")
    .replace(/[^a-zA-Z0-9_-]/g, "_");
  return `preview-${baseName}.pdf`;
}

function addCoverPage(doc, sourceFile, messages, totalForSource) {
  doc.fontSize(24).font("Helvetica-Bold").text("DSAR PREVIEW", {
    align: "center",
  });

  doc.moveDown(0.5);
  doc
    .fontSize(14)
    .font("Helvetica")
    .fillColor("red")
    .text("DRAFT - NOT FINAL", {
      align: "center",
    });
  doc.fillColor("black");

  doc.moveDown(2);

  doc.fontSize(12).font("Helvetica-Bold").text("Data Subject:");
  doc.font("Helvetica");
  doc.text(DATA_SUBJECT.name);
  doc.text(DATA_SUBJECT.email);

  doc.moveDown(1);

  doc.font("Helvetica-Bold").text("Source File:");
  doc.font("Helvetica");
  doc.text(sourceFile);

  doc.moveDown(1);

  doc.font("Helvetica-Bold").text("Progress:");
  doc.font("Helvetica");

  const percent = totalForSource
    ? ((messages.length / totalForSource) * 100).toFixed(1)
    : "?";

  doc.text(
    `Processed: ${messages.length.toLocaleString()} of ${(
      totalForSource || "?"
    ).toLocaleString()} (${percent}%)`
  );
  doc.text(`Generated: ${new Date().toLocaleString("en-GB")}`);

  doc.moveDown(2);

  doc
    .font("Helvetica-Bold")
    .fillColor("red")
    .text("NOTICE:", { continued: false });
  doc.font("Helvetica").fontSize(10).fillColor("black");
  doc.text(
    "This is a PREVIEW document generated from partial redaction results. " +
      "The redaction process is still in progress. This document should be used " +
      "for internal review only and should NOT be sent to the data subject.",
    { align: "justify" }
  );
}

function addMessage(doc, msg, index, total) {
  if (doc.y > 700) {
    doc.addPage();
  }

  doc.fontSize(10).font("Helvetica-Bold").fillColor("#333333");
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

  doc.moveDown(0.5);

  doc.font("Helvetica-Bold").text("Content:");
  doc.font("Helvetica").fontSize(9);
  let body = msg.body || "(No content)";
  body = normalizeWhitespace(body); // Clean up excessive whitespace
  body = truncateText(body);
  doc.fillColor("#000000");
  doc.text(body, { width: CONTENT_WIDTH, align: "left", lineGap: 2 });

  doc.moveDown(1);
  doc.strokeColor("#eeeeee").lineWidth(1);
  doc
    .moveTo(PAGE_MARGIN, doc.y)
    .lineTo(PAGE_MARGIN + CONTENT_WIDTH, doc.y)
    .stroke();
  doc.moveDown(0.5);
}

function addPageNumbers(doc, sourceFile) {
  const range = doc.bufferedPageRange();
  for (let i = range.start; i < range.start + range.count; i++) {
    doc.switchToPage(i);
    doc.fontSize(8).fillColor("#999999");
    doc.text(
      `Page ${i + 1} of ${
        range.count
      } | PREVIEW - ${sourceFile} | Confidential`,
      PAGE_MARGIN,
      doc.page.height - 30,
      { align: "center", width: CONTENT_WIDTH }
    );
  }
}

function generatePdfForSource(sourceFile, messages, totalForSource) {
  return new Promise((resolve, reject) => {
    const outputPath = path.join(OUTPUT_DIR, createSafeFilename(sourceFile));

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
        Title: `DSAR Preview - ${DATA_SUBJECT.name} - ${sourceFile}`,
        Author: "Freightlink",
        Subject: "Data Subject Access Request Preview",
      },
    });

    const stream = fs.createWriteStream(outputPath);
    doc.pipe(stream);

    addCoverPage(doc, sourceFile, messages, totalForSource);

    doc.addPage();
    const total = messages.length;
    messages.forEach((msg, index) => {
      addMessage(doc, msg, index + 1, total);
    });

    addPageNumbers(doc, sourceFile);

    doc.end();

    stream.on("finish", () => resolve(outputPath));
    stream.on("error", reject);
  });
}

async function generatePreviewPdf() {
  console.log("=".repeat(60));
  console.log("GENERATING PREVIEW PDFs BY SOURCE FILE");
  console.log("=".repeat(60));

  if (!fs.existsSync(CHECKPOINT_PATH)) {
    console.error("\nERROR: No checkpoint file found at:");
    console.error(CHECKPOINT_PATH);
    console.error("\nRun the redaction first to create a checkpoint.");
    process.exit(1);
  }

  console.log(`\nReading checkpoint: ${CHECKPOINT_PATH}`);
  const checkpoint = JSON.parse(fs.readFileSync(CHECKPOINT_PATH, "utf-8"));

  // Try to load extracted metadata for totals per source
  let totalsBySource = new Map();
  if (fs.existsSync(EXTRACTED_PATH)) {
    try {
      console.log("Reading extracted messages for totals...");
      const extracted = JSON.parse(fs.readFileSync(EXTRACTED_PATH, "utf-8"));
      for (const msg of extracted.messages) {
        const source = msg.sourceFile || "unknown";
        totalsBySource.set(source, (totalsBySource.get(source) || 0) + 1);
      }
    } catch (e) {
      console.log("  Could not load extracted messages for totals");
    }
  }

  const messages = checkpoint.redactedMessages || [];
  console.log(
    `Found ${messages.length.toLocaleString()} redacted messages in checkpoint`
  );

  if (messages.length === 0) {
    console.error("\nERROR: No messages in checkpoint yet.");
    process.exit(1);
  }

  // Group messages by source file
  const messagesBySource = new Map();
  for (const msg of messages) {
    const source = msg.sourceFile || "unknown";
    if (!messagesBySource.has(source)) {
      messagesBySource.set(source, []);
    }
    messagesBySource.get(source).push(msg);
  }

  console.log(`\nFound ${messagesBySource.size} source files:`);
  for (const [source, msgs] of messagesBySource) {
    const total = totalsBySource.get(source) || "?";
    const percent =
      total !== "?" ? ((msgs.length / total) * 100).toFixed(1) : "?";
    console.log(
      `  - ${source}: ${msgs.length.toLocaleString()} of ${total.toLocaleString()} (${percent}%)`
    );
  }

  // Generate PDF for each source file
  console.log("\nGenerating preview PDFs...");
  const generatedFiles = [];

  for (const [source, msgs] of messagesBySource) {
    console.log(`  Processing: ${source}...`);
    const totalForSource = totalsBySource.get(source);
    const outputPath = await generatePdfForSource(source, msgs, totalForSource);
    generatedFiles.push(outputPath);
    console.log(`    ✓ Created: ${path.basename(outputPath)}`);
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log("PREVIEW GENERATION COMPLETE");
  console.log(`  Generated ${generatedFiles.length} preview PDF files:`);
  for (const file of generatedFiles) {
    const stats = fs.statSync(file);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(1);
    console.log(`    - ${path.basename(file)} (${sizeMB} MB)`);
  }
  console.log("=".repeat(60));

  return generatedFiles;
}

if (require.main === module) {
  generatePreviewPdf().catch((err) => {
    console.error("Error generating preview PDF:", err.message);
    process.exit(1);
  });
}

module.exports = { generatePreviewPdf };
