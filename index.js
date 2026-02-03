require("dotenv").config();

const { parse } = require("./src/parse");
const { redact } = require("./src/redact-v2");
const { generatePdf } = require("./src/generate-pdf");
const fs = require("fs");
const path = require("path");

/**
 * DSAR PST Processing Pipeline
 *
 * Processes PST files for a Data Subject Access Request:
 * 1. Parse - Extract all messages from multiple PST files to JSON
 * 2. Redact - Use OpenAI to identify and redact third-party personal data
 * 3. Generate PDF - Create the response document
 *
 * Features:
 * - Multi-PST file support
 * - AI-powered redaction with GPT-4o-mini
 * - Checkpoint/resume capability
 * - Progress tracking with ETA
 */
async function main() {
  console.log("\n");
  console.log("╔════════════════════════════════════════════════════════════╗");
  console.log("║         DSAR PST PROCESSING PIPELINE                       ║");
  console.log("║         UK GDPR / Data Protection Act 2018                 ║");
  console.log("║         AI-Powered Redaction                               ║");
  console.log("╚════════════════════════════════════════════════════════════╝");
  console.log("\n");

  const startTime = Date.now();

  // Check for OpenAI API key
  if (!process.env.OPENAI_API_KEY) {
    console.error("ERROR: OPENAI_API_KEY environment variable not set");
    console.error("\nTo set it:");
    console.error("  export OPENAI_API_KEY=your-api-key-here");
    console.error("\nOr create a .env file with:");
    console.error("  OPENAI_API_KEY=your-api-key-here");
    process.exit(1);
  }

  // Ensure output directory exists
  const outputDir = path.resolve(__dirname, "output");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  try {
    // Phase 1: Parse (skip if extracted-messages.json already exists and is recent)
    const extractedPath = path.join(outputDir, "extracted-messages.json");
    if (fs.existsSync(extractedPath)) {
      const stats = fs.statSync(extractedPath);
      const ageHours = (Date.now() - stats.mtimeMs) / (1000 * 60 * 60);
      if (ageHours < 24) {
        console.log(
          "Phase 1: SKIP (extracted-messages.json exists and is recent)"
        );
        console.log(`         Delete it to force re-extraction.\n`);
      } else {
        parse();
      }
    } else {
      parse();
    }
    console.log("\n");

    // Phase 2: Redact with AI
    await redact();
    console.log("\n");

    // Phase 3: Generate PDF
    await generatePdf();
    console.log("\n");

    // Summary
    const elapsed = (Date.now() - startTime) / 1000;
    const minutes = Math.floor(elapsed / 60);
    const seconds = Math.round(elapsed % 60);

    console.log(
      "╔════════════════════════════════════════════════════════════╗"
    );
    console.log(
      "║                    PIPELINE COMPLETE                       ║"
    );
    console.log(
      "╚════════════════════════════════════════════════════════════╝"
    );
    console.log(`\nTotal time: ${minutes}m ${seconds}s`);
    console.log("\nOutput files:");
    console.log("  output/extracted-messages.json  - Raw extracted messages");
    console.log(
      "  output/redacted-messages.json   - Messages with AI redactions"
    );
    console.log("  output/audit-log.json           - Redaction audit trail");
    console.log(
      "  output/dsar-response-john-gaskell.pdf - Final DSAR response"
    );
    console.log("\n");
  } catch (error) {
    console.error("\nPipeline failed:", error.message);
    console.error(
      "\nIf this was during redaction, run again to resume from checkpoint."
    );
    process.exit(1);
  }
}

main();
