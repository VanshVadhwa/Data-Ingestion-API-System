const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

// Priority levels
const PRIORITY_LEVELS = {
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

// In-memory stores
const ingestions = new Map(); // ingestion_id -> { ingestion_id, priority, batches, createdAt }
const batches = new Map(); // batch_id -> { batch_id, ingestion_id, ids, status, createdAt }

// Priority queue for batches: array of batch objects
let batchQueue = [];

// Processing state
let isProcessing = false;
let processingTimeout = null; // To keep track of the 5s delay timeout

// Helper to simulate external API call with delay
function simulateExternalApiCall(id) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve({ id, data: 'processed' });
    }, 500); // 500ms delay to simulate external API
  });
}

// Helper to compare priority and createdAt for sorting
function compareBatches(a, b) {
  if (PRIORITY_LEVELS[b.priority] !== PRIORITY_LEVELS[a.priority]) {
    return PRIORITY_LEVELS[b.priority] - PRIORITY_LEVELS[a.priority];
  }
  return a.createdAt - b.createdAt;
}

// Enqueue batches from ingestion request
function enqueueBatches(ingestion) {
  ingestion.batches.forEach((batch) => {
    batchQueue.push({
      batch_id: batch.batch_id,
      ingestion_id: ingestion.ingestion_id,
      ids: batch.ids,
      status: 'yet_to_start',
      priority: ingestion.priority,
      createdAt: ingestion.createdAt,
    });
  });
  // Sort queue by priority and createdAt
  batchQueue.sort(compareBatches);
}

// Process batches asynchronously with rate limit 1 batch per 5 seconds
async function processBatches() {
  if (isProcessing) return;
  isProcessing = true;

  while (batchQueue.length > 0) {
    const batch = batchQueue.shift();

    // Guard against batch_id not found in batches map
    if (!batches.has(batch.batch_id)) {
      // eslint-disable-next-line no-console
      console.error(`Batch ID ${batch.batch_id} from queue not found in batches map. Skipping.`);
      continue; // Skip this batch if its details are missing
    }

    const currentBatchData = batches.get(batch.batch_id);
    currentBatchData.status = 'triggered';

    // Process each id in batch (max 3 ids per batch)
    const results = [];
    for (const id of batch.ids) {
      // Simulate external API call
      // eslint-disable-next-line no-await-in-loop
      const result = await simulateExternalApiCall(id);
      results.push(result);
    }

    // Mark batch as completed
    currentBatchData.status = 'completed';

    // Wait 5 seconds before processing next batch (or finishing) to respect rate limit
    // eslint-disable-next-line no-await-in-loop
    await new Promise((resolve) => {
      processingTimeout = setTimeout(resolve, 5000);
    });
    processingTimeout = null; // Clear timeout reference after it has resolved
  }

  isProcessing = false;
  if (processingTimeout) { // Should not happen if logic is correct, but as a safeguard
    clearTimeout(processingTimeout);
    processingTimeout = null;
  }
}

// POST /ingest endpoint
app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;

  // Validate input
  if (
    !Array.isArray(ids) ||
    ids.some((id) => !Number.isInteger(id) || id < 1 || id > 1000000007) ||
    !['HIGH', 'MEDIUM', 'LOW'].includes(priority)
  ) {
    return res.status(400).json({ error: 'Invalid input' });
  }

  const ingestion_id = uuidv4();
  const createdAt = Date.now();

  // Split ids into batches of max 3 ids
  const batchesArr = [];
  for (let i = 0; i < ids.length; i += 3) {
    const batch_ids = ids.slice(i, i + 3);
    const batch_id = uuidv4();
    batchesArr.push({
      batch_id,
      ids: batch_ids,
      status: 'yet_to_start',
    });
    batches.set(batch_id, {
      batch_id,
      ingestion_id,
      ids: batch_ids,
      status: 'yet_to_start',
      createdAt,
    });
  }

  // Store ingestion
  ingestions.set(ingestion_id, {
    ingestion_id,
    priority,
    batches: batchesArr,
    createdAt,
  });

  // Enqueue batches
  enqueueBatches(ingestions.get(ingestion_id));

  // Start processing if not already started
  processBatches();

  return res.json({ ingestion_id });
});

// GET /status/:ingestion_id endpoint
app.get('/status/:ingestion_id', (req, res) => {
  const { ingestion_id } = req.params;
  const ingestion = ingestions.get(ingestion_id);
  if (!ingestion) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }

  // Determine overall status
  const batchStatuses = ingestion.batches.map((batch) => batches.get(batch.batch_id)?.status || 'yet_to_start');

  let overallStatus = 'yet_to_start';
  if (batchStatuses.every((status) => status === 'completed')) {
    overallStatus = 'completed';
  } else if (batchStatuses.some((status) => status === 'triggered')) {
    overallStatus = 'triggered';
  }

  // Prepare batches info
  const batchesInfo = ingestion.batches.map((batch) => ({
    batch_id: batch.batch_id,
    ids: batch.ids,
    status: batches.get(batch.batch_id)?.status || 'yet_to_start',
  }));

  return res.json({
    ingestion_id,
    status: overallStatus,
    batches: batchesInfo,
  });
});

let serverInstance;

function startServer(port = PORT) {
  return new Promise((resolve) => {
    serverInstance = app.listen(port, () => {
      // eslint-disable-next-line no-console
      console.log(`Data Ingestion API System running on port ${port}`);
      resolve(serverInstance);
    });
  });
}

function stopServer() {
  return new Promise((resolve, reject) => {
    if (processingTimeout) { // Clear any pending processing delay timeout
      clearTimeout(processingTimeout);
      processingTimeout = null;
    }
    if (serverInstance) {
      serverInstance.close((err) => {
        if (err) {
          // eslint-disable-next-line no-console
          console.error("Error stopping server:", err);
          return reject(err);
        }
        // eslint-disable-next-line no-console
        // console.log('Server stopped.'); // Keep console clean for tests
        serverInstance = null;
        resolve();
      });
    } else {
      resolve(); // No server instance to stop
    }
  });
}

function resetState() {
  ingestions.clear();
  batches.clear();
  batchQueue.length = 0;
  isProcessing = false;
  if (processingTimeout) {
    clearTimeout(processingTimeout);
    processingTimeout = null;
  }
  // console.log("Application state reset for testing.");
}

// Start the server only if this file is run directly (e.g., `node src/app.js`)
// and not when imported as a module.
if (require.main === module) {
  startServer();
}

module.exports = { app, startServer, stopServer, resetState, PRIORITY_LEVELS };
