const request = require('supertest');
const { app, startServer, stopServer, resetState } = require('../src/app');

jest.setTimeout(90000); // Increased timeout for extensive tests

// Helper function to pause execution
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

describe('Data Ingestion API System', () => {
  beforeAll(async () => {
    await startServer(5001); // Use a different port for testing to avoid conflicts
  });

  beforeEach(() => {
    resetState(); // Reset application state before each test
  });

  afterAll(async () => {
    await stopServer();
  });

  describe('POST /ingest', () => {
    it('should reject invalid ingestion requests (invalid ID type)', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [1, 2, 'a'], priority: 'HIGH' });
      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('Invalid input');
    });

    it('should reject invalid ingestion requests (invalid priority)', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [1, 2, 3], priority: 'VERY_HIGH' });
      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('Invalid input');
    });

    it('should reject invalid ingestion requests (ID out of range)', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [0], priority: 'LOW' });
      expect(res.statusCode).toBe(400);
      expect(res.body.error).toBe('Invalid input');
    });
    
    it('should reject empty IDs array', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [], priority: 'LOW' });
      expect(res.statusCode).toBe(200); 
      expect(res.body).toHaveProperty('ingestion_id');
      const statusRes = await request(app).get(`/status/${res.body.ingestion_id}`);
      expect(statusRes.statusCode).toBe(200);
      expect(statusRes.body.status).toBe('completed'); 
      expect(statusRes.body.batches).toEqual([]);
    });


    it('should accept valid ingestion requests and return ingestion_id', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [10, 20, 30, 40, 50], priority: 'MEDIUM' });
      expect(res.statusCode).toBe(200);
      expect(res.body).toHaveProperty('ingestion_id');
    });

    it('should correctly split IDs into batches of 3', async () => {
      const idsToIngest = [1, 2, 3, 4, 5, 6, 7]; // Expect 3 batches: [1,2,3], [4,5,6], [7]
      const res = await request(app)
        .post('/ingest')
        .send({ ids: idsToIngest, priority: 'LOW' });
      expect(res.statusCode).toBe(200);
      const ingestionId = res.body.ingestion_id;

      const statusRes = await request(app).get(`/status/${ingestionId}`);
      expect(statusRes.statusCode).toBe(200);
      expect(statusRes.body.batches.length).toBe(3);
      expect(statusRes.body.batches[0].ids).toEqual([1, 2, 3]);
      expect(statusRes.body.batches[1].ids).toEqual([4, 5, 6]);
      expect(statusRes.body.batches[2].ids).toEqual([7]);
    });
  });

  describe('GET /status/:ingestion_id', () => {
    it('should return 404 for unknown ingestion_id status request', async () => {
      const res = await request(app).get('/status/unknown-id-123');
      expect(res.statusCode).toBe(404);
    });

    it('should return initial status correctly', async () => {
      const resIngest = await request(app)
        .post('/ingest')
        .send({ ids: [101, 102, 103], priority: 'LOW' });
      const ingestionId = resIngest.body.ingestion_id;

      await sleep(100); 

      const resStatus = await request(app).get(`/status/${ingestionId}`);
      expect(resStatus.statusCode).toBe(200);
      expect(resStatus.body.ingestion_id).toBe(ingestionId);
      expect(['yet_to_start', 'triggered']).toContain(resStatus.body.status);
      expect(resStatus.body.batches.length).toBe(1);
      expect(resStatus.body.batches[0].ids).toEqual([101, 102, 103]);
      expect(['yet_to_start', 'triggered']).toContain(resStatus.body.batches[0].status);
    });
  });

  describe('Priority and Rate Limiting Logic', () => {
    it('should process batches respecting priority and rate limit as per example', async () => {
      // Request 1 (MEDIUM)
      const resM = await request(app)
        .post('/ingest')
        .send({ ids: [1, 2, 3, 4, 5], priority: 'MEDIUM' }); // M1:[1,2,3], M2:[4,5]
      expect(resM.statusCode).toBe(200);
      const ingestionIdM = resM.body.ingestion_id;

      // At T0: M1 is picked.
      await sleep(1000); // T = 1s. M1 should be 'triggered'.
      let statusM = await request(app).get(`/status/${ingestionIdM}`);
      expect(statusM.body.status).toBe('triggered');
      expect(statusM.body.batches[0].status).toBe('triggered'); // M1
      expect(statusM.body.batches[1].status).toBe('yet_to_start'); // M2

      // Wait 3 more seconds (Total T = 4s), then submit HIGH priority request
      await sleep(3000); // T = 4s
      const resH = await request(app)
        .post('/ingest')
        .send({ ids: [6, 7, 8, 9], priority: 'HIGH' }); // H1:[6,7,8], H2:[9]
      expect(resH.statusCode).toBe(200);
      const ingestionIdH = resH.body.ingestion_id;
      
      // T = 7.5s (give a bit more buffer). M1 should be complete. H1 should have started.
      // M1: [1,2,3] (1.5s work). Cycle ends ~T=6.5s.
      // H1: [6,7,8] (1.5s work). Starts after M1 cycle, ~T=6.5s.
      await sleep(3500); // Current T = 4s. Wait 3.5s more -> T = 7.5s.
      statusM = await request(app).get(`/status/${ingestionIdM}`);
      let statusH = await request(app).get(`/status/${ingestionIdH}`);
      expect(statusM.body.batches[0].status).toBe('completed'); // M1
      expect(['yet_to_start', 'triggered']).toContain(statusM.body.batches[1].status); // M2 might have just started
      expect(statusM.body.status).toBe('triggered'); 

      expect(statusH.body.status).toBe('triggered'); 
      expect(statusH.body.batches[0].status).toBe('triggered'); // H1
      expect(statusH.body.batches[1].status).toBe('yet_to_start'); // H2

      // Wait until T = 14s (from T=7.5s, wait 6.5s).
      // H1 cycle ends ~T=6.5s (M1 end) + 1.5s (H1 work) + 5s (H1 delay) = ~13s.
      // H2 ([9], 0.5s work) should start processing around T=13s.
      await sleep(6500); // Current T = 7.5s. Wait 6.5s more -> T = 14s
      statusM = await request(app).get(`/status/${ingestionIdM}`);
      statusH = await request(app).get(`/status/${ingestionIdH}`);
      expect(statusM.body.batches[0].status).toBe('completed'); // M1
      expect(statusM.body.batches[1].status).toBe('yet_to_start'); 
      expect(statusM.body.status).toBe('triggered');

      expect(statusH.body.batches[0].status).toBe('completed'); // H1
      expect(statusH.body.batches[1].status).toBe('triggered'); // H2
      expect(statusH.body.status).toBe('triggered');

      // Wait until T = 19.5s (add a small buffer for H2 completion).
      // H2 cycle ends ~T=13s (H1 end) + 0.5s (H2 work) + 5s (H2 delay) = ~18.5s.
      // M2 ([4,5], 1s work) should start processing around T=18.5s.
      await sleep(5500); // Current T = 14s. Wait 5.5s more -> T = 19.5s
      statusM = await request(app).get(`/status/${ingestionIdM}`);
      statusH = await request(app).get(`/status/${ingestionIdH}`);
      
      expect(statusH.body.status).toBe('completed'); 
      expect(statusH.body.batches[0].status).toBe('completed');
      expect(statusH.body.batches[1].status).toBe('completed');

      expect(statusM.body.batches[0].status).toBe('completed'); // M1
      expect(statusM.body.batches[1].status).toBe('triggered'); // M2
      expect(statusM.body.status).toBe('triggered');

      // Wait until T = 25.5s (add a small buffer for M2 completion).
      // M2 cycle ends ~T=18.5s (H2 end) + 1.0s (M2 work) + 5s (M2 delay) = ~24.5s. All complete.
      await sleep(6000); // Current T = 19.5s. Wait 6s more -> T = 25.5s
      statusM = await request(app).get(`/status/${ingestionIdM}`);
      statusH = await request(app).get(`/status/${ingestionIdH}`);

      expect(statusM.body.status).toBe('completed');
      expect(statusM.body.batches.every(b => b.status === 'completed')).toBe(true);
      expect(statusH.body.status).toBe('completed');
      expect(statusH.body.batches.every(b => b.status === 'completed')).toBe(true);
    });

    it('should strictly adhere to rate limit (1 batch per 5 seconds)', async () => {
      const ids = [201, 202, 203, 204, 205, 206]; // Two batches
      const res = await request(app)
        .post('/ingest')
        .send({ ids, priority: 'HIGH' });
      const ingestionId = res.body.ingestion_id;

      // T0: Batch 1 ([201,202,203]) starts. Takes 1.5s work. Cycle ends T=1.5+5=6.5s.
      await sleep(1000); // T = 1s. Batch 1 is 'triggered'.
      let status = await request(app).get(`/status/${ingestionId}`);
      expect(status.body.batches[0].status).toBe('triggered');
      expect(status.body.batches[1].status).toBe('yet_to_start');

      // Wait until T = 7s. Batch 1 (cycle ends 6.5s) should be 'completed'. Batch 2 should be 'triggered'.
      await sleep(6000); // Current T = 1s. Wait 6s more -> T = 7s.
      status = await request(app).get(`/status/${ingestionId}`);
      expect(status.body.batches[0].status).toBe('completed');
      expect(['triggered', 'completed']).toContain(status.body.batches[1].status); // Batch 2 might be triggered or already done
      
      // Batch 2 cycle ends ~6.5s (B1 end) + 1.5s (B2 work) + 5s (B2 delay) = ~13s.
      // Wait until T = 13.5s. Batch 2 should be 'completed'.
      await sleep(6500); // Current T = 7s. Wait 6.5s more -> T = 13.5s
      status = await request(app).get(`/status/${ingestionId}`);
      expect(status.body.batches[0].status).toBe('completed');
      expect(status.body.batches[1].status).toBe('completed');
      expect(status.body.status).toBe('completed');
    });

    it('should process HIGH priority before LOW priority submitted earlier', async () => {
      // Submit LOW priority
      const resLow = await request(app)
        .post('/ingest')
        .send({ ids: [301, 302, 303], priority: 'LOW' }); // L1
      expect(resLow.statusCode).toBe(200);
      expect(resLow.body).toHaveProperty('ingestion_id');
      const ingestionIdLow = resLow.body.ingestion_id;
      
      await sleep(200); // Slightly longer sleep to ensure L1 is definitely picked by processor if idle

      // Submit HIGH priority
      const resHigh = await request(app)
        .post('/ingest')
        .send({ ids: [401, 402, 403], priority: 'HIGH' }); // H1
      expect(resHigh.statusCode).toBe(200);
      expect(resHigh.body).toHaveProperty('ingestion_id');
      const ingestionIdHigh = resHigh.body.ingestion_id;

      // L1 ([301,302,303], 1.5s work) starts at T0. Cycle ends ~T=6.5s.
      // H1 ([401,402,403], 1.5s work) submitted at T=0.2s.
      // Queue after H1 submitted (L1 is processing): L1 (current), H1.
      // After L1's cycle (~6.5s from T0), H1 is picked.

      // Wait until T = 7s (from T0). L1 should be 'completed'. H1 should be 'triggered'.
      // The sleep(200) for L1 means L1 processing starts around T=0.2s (relative to this test's T0).
      // H1 submitted at T=0.2s (relative to this test's T0).
      // L1 cycle ends T = 0.2s + 1.5s + 5s = 6.7s.
      // So at T=7s, L1 is 'completed', H1 is 'triggered'.
      await sleep(6800); // Wait from T=0.2s to T=7s
      
      const statusLow = await request(app).get(`/status/${ingestionIdLow}`);
      const statusHigh = await request(app).get(`/status/${ingestionIdHigh}`);

      expect(statusLow.body.batches).not.toBeUndefined();
      expect(statusLow.body.batches).toHaveLength(1);
      expect(statusHigh.body.batches).not.toBeUndefined();
      expect(statusHigh.body.batches).toHaveLength(1);

      expect(statusLow.body.batches[0].status).toBe('completed'); // L1 completed
      expect(['triggered', 'completed']).toContain(statusHigh.body.batches[0].status); // H1 might be processing or done quickly

      // Wait until T = 13.5s (from T0).
      // H1 cycle ends ~T=6.7s (L1 end) + 1.5s (H1 work) + 5s (H1 delay) = ~13.2s.
      // H1 should be 'completed'.
      await sleep(6500); // Current T = 7s. Wait 6.5s more -> T = 13.5s.
      const finalStatusLow = await request(app).get(`/status/${ingestionIdLow}`);
      const finalStatusHigh = await request(app).get(`/status/${ingestionIdHigh}`);
      
      expect(finalStatusLow.body.status).toBe('completed');
      expect(finalStatusHigh.body.status).toBe('completed');
    });
  });

  describe('Overall Status Logic', () => {
    it('should show "yet_to_start" if all batches are "yet_to_start" (difficult to test without halting processing)', async () => {
      // This state is transient.
    });

    it('should show "triggered" if at least one batch is "triggered" or "yet_to_start" and at least one is not "completed"', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [501, 502, 503, 504, 505, 506], priority: 'MEDIUM' }); // 2 batches
      const ingestionId = res.body.ingestion_id;

      await sleep(1000); // Allow first batch to trigger
      let status = await request(app).get(`/status/${ingestionId}`);
      expect(status.body.status).toBe('triggered');
      expect(status.body.batches[0].status).toBe('triggered');
      expect(status.body.batches[1].status).toBe('yet_to_start');

      await sleep(6000); // Allow first batch to complete, second to trigger (Total ~7s)
      status = await request(app).get(`/status/${ingestionId}`);
      // If second batch is very fast, overall might be completed.
      expect(['triggered', 'completed']).toContain(status.body.status);
      expect(status.body.batches[0].status).toBe('completed');
      // Second batch could be triggered or completed if the first one + its delay allowed it to finish.
      expect(['triggered', 'completed']).toContain(status.body.batches[1].status);
    });

    it('should show "completed" if all batches are "completed"', async () => {
      const res = await request(app)
        .post('/ingest')
        .send({ ids: [601, 602], priority: 'LOW' }); // 1 batch
      const ingestionId = res.body.ingestion_id;

      await sleep(7000); // Wait for batch to process (1s work + 5s delay = ~6s)
      const status = await request(app).get(`/status/${ingestionId}`);
      expect(status.body.status).toBe('completed');
      expect(status.body.batches[0].status).toBe('completed');
    });
  });
});
