const PDFDocument = require("pdfkit");
const fs = require("fs");
const path = require("path");

const INPUT_PATH = path.resolve(
  __dirname,
  "..",
  "output",
  "redacted-messages.json"
);
const OUTPUT_DIR = path.resolve(__dirname, "..", "output");

// Page dimensions and margins
const PAGE_MARGIN = 50;
const CONTENT_WIDTH = 595.28 - PAGE_MARGIN * 2; // A4 width minus margins

const DATA_SUBJECT = {
  name: "John Gaskell",
  email: "john@freightlink.co.uk",
};

/**
 * Format a date string for display
 */
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

/**
 * Truncate text to fit within a reasonable length
 */
function truncateText(text, maxLength = 5000) {
  if (!text) return "";
  if (text.length <= maxLength) return text;
  return (
    text.substring(0, maxLength) + "\n\n[... Content truncated for display ...]"
  );
}

/**
 * Create a safe filename from source file name
 */
function createSafeFilename(sourceFile) {
  // Remove .pst extension and replace unsafe characters
  const baseName = sourceFile
    .replace(/\.pst$/i, "")
    .replace(/[^a-zA-Z0-9_-]/g, "_");
  return `dsar-${baseName}.pdf`;
}

/**
 * Add the cover page for a specific source file
 */
function addCoverPage(doc, sourceFile, messages, metadata) {
  doc.fontSize(24).font("Helvetica-Bold").text("DATA SUBJECT ACCESS REQUEST", {
    align: "center",
  });

  doc.moveDown(0.5);
  doc.fontSize(18).font("Helvetica").text("Response Document", {
    align: "center",
  });

  doc.moveDown(2);

  // Data subject info
  doc.fontSize(12).font("Helvetica-Bold").text("Data Subject:");
  doc.font("Helvetica");
  doc.text(DATA_SUBJECT.name);
  doc.text(DATA_SUBJECT.email);

  doc.moveDown(1);

  // Source file info
  doc.font("Helvetica-Bold").text("Source File:");
  doc.font("Helvetica");
  doc.text(sourceFile);
  doc.text(`Messages in this file: ${messages.length.toLocaleString()}`);

  doc.moveDown(1);

  // Document info
  doc.font("Helvetica-Bold").text("Document Information:");
  doc.font("Helvetica");
  doc.text(`Generated: ${formatDate(new Date().toISOString())}`);

  doc.moveDown(2);

  // Legal notice (constrain to current page so PDFKit never adds a page here)
  doc.font("Helvetica-Bold").text("Notice:");
  doc.font("Helvetica").fontSize(10);
  const noticeHeight = doc.page.height - doc.y - PAGE_MARGIN;
  doc.text(
    "This document has been prepared in response to a Data Subject Access Request under the UK General Data Protection Regulation (UK GDPR) and the Data Protection Act 2018. " +
      "Third-party personal data has been redacted to protect the privacy rights of other individuals. " +
      "Redacted content is marked as [REDACTED ...]. " +
      "An audit log of all redactions is available for compliance verification.",
    { align: "justify", width: CONTENT_WIDTH, height: Math.max(100, noticeHeight) }
  );
}

/**
 * Add a message to the PDF
 */
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
  body = normalizeWhitespace(body); // Clean up excessive whitespace
  body = truncateText(body);
  doc.fillColor("#000000");

  // Render body line-by-line so we control page breaks. This prevents PDFKit's
  // flowing text from ever adding a new page (which can create blank pages when
  // the remaining content is empty).
  const lineHeight = doc.currentLineHeight(true) + 2;
  const pageBottom = doc.page.height - 50;
  const lines = body.split("\n");

  for (const line of lines) {
    // If this line would go past the bottom, start a new page first
    if (doc.y + lineHeight > pageBottom) {
      doc.addPage();
    }
    // Limit height so a long (wrapping) line never triggers PDFKit to add a page
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

// Page numbers / header removed - was appearing on blank pages and isn't essential

/**
 * Generate a single PDF for a source file
 */
function generatePdfForSource(sourceFile, messages, metadata) {
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
        Title: `DSAR Response - ${DATA_SUBJECT.name} - ${sourceFile}`,
        Author: "Freightlink",
        Subject: "Data Subject Access Request Response",
        Keywords: "DSAR, GDPR, UK GDPR, Data Protection",
      },
    });

    const stream = fs.createWriteStream(outputPath);
    doc.pipe(stream);

    // Cover page
    addCoverPage(doc, sourceFile, messages, metadata);

    // Messages (only add content page if there are messages)
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

/**
 * Main PDF generation function - generates multiple PDFs by source file
 */
async function generatePdf() {
  console.log("=".repeat(60));
  console.log("PHASE 3: GENERATING PDFs BY SOURCE FILE");
  console.log("=".repeat(60));
  console.log(`\nInput: ${INPUT_PATH}`);
  console.log(`Output directory: ${OUTPUT_DIR}\n`);

  // Load redacted messages
  const data = JSON.parse(fs.readFileSync(INPUT_PATH, "utf-8"));
  console.log(
    `Loaded ${data.messages.length.toLocaleString()} redacted messages`
  );

  // Group messages by source file
  const messagesBySource = new Map();
  for (const msg of data.messages) {
    const source = msg.sourceFile || "unknown";
    if (!messagesBySource.has(source)) {
      messagesBySource.set(source, []);
    }
    messagesBySource.get(source).push(msg);
  }

  console.log(`\nFound ${messagesBySource.size} source files:`);
  for (const [source, msgs] of messagesBySource) {
    console.log(`  - ${source}: ${msgs.length.toLocaleString()} messages`);
  }

  // Generate PDF for each source file
  console.log("\nGenerating PDFs...");
  const generatedFiles = [];

  for (const [source, messages] of messagesBySource) {
    console.log(
      `  Processing: ${source} (${messages.length.toLocaleString()} messages)...`
    );
    const outputPath = await generatePdfForSource(
      source,
      messages,
      data.metadata
    );
    generatedFiles.push(outputPath);
    console.log(`    ✓ Created: ${path.basename(outputPath)}`);
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log("PDF GENERATION COMPLETE");
  console.log(`  Generated ${generatedFiles.length} PDF files:`);
  for (const file of generatedFiles) {
    const stats = fs.statSync(file);
    const sizeMB = (stats.size / (1024 * 1024)).toFixed(1);
    console.log(`    - ${path.basename(file)} (${sizeMB} MB)`);
  }
  console.log("=".repeat(60));

  return generatedFiles;
}

// Run if called directly
if (require.main === module) {
  generatePdf().catch((err) => {
    console.error("Error generating PDF:", err);
    process.exit(1);
  });
}

module.exports = { generatePdf };
