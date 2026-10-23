/**
 * ==============================================================================
 * Tepache Fermentation Cloud Ingestion Engine & Live Monitor
 * Platform : Node.js / Express / MongoDB Atlas / Render
 * File     : backend/server.js
 * ==============================================================================
 */

require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 5000;

// ============================================================================
// 1. MONGODB ATLAS CONNECTION
// ============================================================================
const MONGODB_URI = process.env.MONGODB_URI || 
  "mongodb+srv://vinodht24cse_db_user:4UJdIy9KAA8fnptd@cluster0.eogvzbi.mongodb.net/tepache_db?retryWrites=true&w=majority&appName=Cluster0";

console.log('[DATABASE] Connecting to MongoDB Atlas...');

mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 5000,
  maxPoolSize: 20,
  minPoolSize: 5,
  socketTimeoutMS: 20000,
})
.then(() => {
  console.log('[DATABASE] Connected to MongoDB Atlas (tepache_db) successfully!');
})
.catch((err) => {
  console.error('[DATABASE ERROR] Connection failed:', err.message);
});

// ============================================================================
// 2. MONGOOSE SCHEMA & IDEMPOTENT DEDUPLICATION
// ============================================================================
const SensorReadingSchema = new mongoose.Schema({
  record_id: { type: Number, required: true, index: true },
  batch_id:  { type: String, required: true, default: 'TEP001', index: true },
  timestamp: { type: Date, required: true },
  elapsed_seconds: { type: Number, default: 0 },
  temperature_c:   { type: Number, required: true },
  ph_estimate:     { type: Number, required: true },
  ph_voltage:      { type: Number, default: 0.0 },
  turbidity_voltage: { type: Number, default: 0.0 },
  turbidity_raw:   { type: Number, default: 0 },
  turbidity_ntu:   { type: Number, default: -1 },
  clock_quality:   { type: String, default: 'RTC_VERIFIED' },
  received_at:     { type: Date, default: Date.now },
}, { timestamps: true });

// CRITICAL COMPOUND UNIQUE INDEX: Zero duplicate records on network retries
SensorReadingSchema.index({ batch_id: 1, record_id: 1 }, { unique: true });
SensorReadingSchema.index({ batch_id: 1, timestamp: -1 });
SensorReadingSchema.index({ record_id: -1 });

const SensorReading = mongoose.model('SensorReading', SensorReadingSchema);

// Batch Summary Model
const BatchSummarySchema = new mongoose.Schema({
  batch_id:        { type: String, required: true, unique: true },
  start_time:      { type: Date, default: Date.now },
  last_record_id:  { type: Number, default: 0 },
  total_records:   { type: Number, default: 0 },
  last_reading:    { type: Object, default: {} },
  updated_at:      { type: Date, default: Date.now }
});

const BatchSummary = mongoose.model('BatchSummary', BatchSummarySchema);

// ============================================================================
// 3. MIDDLEWARE
// ============================================================================
app.use(cors({ maxAge: 86400 }));
app.use(express.json({ limit: '2mb' }));

// ============================================================================
// 4. API ROUTES
// ============================================================================

/**
 * GET /health
 * Lightweight reachability check for the ESP32 offline-first engine
 */
app.get('/health', (req, res) => {
  const isDbReady = mongoose.connection.readyState === 1;
  res.status(isDbReady ? 200 : 503).json({
    status: isDbReady ? 'ok' : 'degraded',
    uptime: Math.floor(process.uptime()),
    database: isDbReady ? 'connected' : 'disconnected',
    timestamp: new Date().toISOString()
  });
});

/**
 * POST /api/sensor-data
 * Primary ingestion endpoint supporting both:
 * 1. Slow Chunked Batch: { "batch_id": "TEP001", "records": [ ... ] }
 * 2. Single Record Object
 */
app.post('/api/sensor-data', async (req, res) => {
  try {
    const payload = req.body;
    let records = [];
    let batchId = payload.batch_id || 'TEP001';

    if (Array.isArray(payload.records)) {
      records = payload.records;
    } else if (payload.record_id !== undefined || payload.temperature_c !== undefined) {
      records = [payload];
      batchId = payload.batch_id || batchId;
    } else {
      return res.status(400).json({ success: false, error: 'Invalid payload format' });
    }

    if (records.length === 0) {
      return res.status(200).json({ success: true, count: 0, synced_up_to: 0 });
    }

    let highestId = 0;
    const bulkOps = [];

    for (const item of records) {
      const recId = Number(item.record_id || 0);
      const bId = String(item.batch_id || batchId).trim();
      const date = item.timestamp ? new Date(item.timestamp) : new Date();

      if (recId > highestId) highestId = recId;

      bulkOps.push({
        updateOne: {
          filter: { batch_id: bId, record_id: recId },
          update: {
            $set: {
              batch_id: bId,
              record_id: recId,
              timestamp: isNaN(date.getTime()) ? new Date() : date,
              elapsed_seconds: Number(item.elapsed_seconds || 0),
              temperature_c: Number(item.temperature_c || 0),
              ph_estimate: Number(item.ph_estimate || 0),
              ph_voltage: Number(item.ph_voltage || 0),
              turbidity_voltage: Number(item.turbidity_voltage || 0),
              turbidity_raw: Number(item.turbidity_raw || 0),
              turbidity_ntu: Number(item.turbidity_ntu !== undefined ? item.turbidity_ntu : -1),
              clock_quality: String(item.clock_quality || 'RTC_VERIFIED').trim(),
              received_at: new Date()
            }
          },
          upsert: true
        }
      });
    }

    // Execute bulk write atomically
    const result = await SensorReading.bulkWrite(bulkOps, { ordered: false });

    // Update Batch Summary
    const latestItem = records[records.length - 1];
    await BatchSummary.findOneAndUpdate(
      { batch_id: batchId },
      {
        $max: { last_record_id: highestId },
        $inc: { total_records: result.upsertedCount || 0 },
        $set: {
          last_reading: {
            record_id: highestId,
            temperature_c: latestItem.temperature_c,
            ph_estimate: latestItem.ph_estimate,
            turbidity_voltage: latestItem.turbidity_voltage,
            turbidity_ntu: latestItem.turbidity_ntu,
            clock_quality: latestItem.clock_quality,
            timestamp: new Date()
          },
          updated_at: new Date()
        },
        $setOnInsert: { start_time: new Date() }
      },
      { upsert: true }
    );

    console.log(`[INGEST] Ingested ${records.length} records for '${batchId}'. Synced up to ID: #${highestId} (New: ${result.upsertedCount}, Updated: ${result.matchedCount})`);

    // Acknowledge back to ESP32
    return res.status(200).json({
      success: true,
      count: records.length,
      synced_up_to: highestId,
      message: 'Chunk stored successfully'
    });

  } catch (err) {
    console.error('[INGEST ERROR]', err.message);
    return res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * GET /api/sensor-data
 * Retrieve records with optional batch filtering and limit
 */
app.get('/api/sensor-data', async (req, res) => {
  try {
    const { batch_id, limit = 50 } = req.query;
    const filter = batch_id ? { batch_id: String(batch_id).trim() } : {};
    const isAll = limit === 'all' || limit === '0' || Number(limit) === 0;

    let query = SensorReading.find(filter)
      .sort({ record_id: -1 })
      .select('-__v');

    if (!isAll) {
      const limitNum = Math.min(Math.max(Number(limit) || 25, 1), 50000);
      query = query.limit(limitNum);
    }

    const records = await query.lean();
    res.set('Cache-Control', 'no-cache');
    res.json({ success: true, count: records.length, records });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * GET /api/latest
 * Get single most recent reading
 */
app.get('/api/latest', async (req, res) => {
  try {
    const reading = await SensorReading.findOne().sort({ record_id: -1 }).lean();
    if (!reading) return res.status(404).json({ success: false, message: 'No data' });
    res.json({ success: true, reading });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * GET /api/export-csv
 * Direct streaming CSV download of all logged telemetry readings
 */
app.get('/api/export-csv', async (req, res) => {
  try {
    const { batch_id } = req.query;
    const filter = batch_id ? { batch_id: String(batch_id).trim() } : {};

    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', 'attachment; filename="tepache_telemetry.csv"');

    res.write('record_id,batch_id,timestamp_utc,elapsed_seconds,temperature_c,ph_estimate,ph_voltage,turbidity_voltage,turbidity_raw,turbidity_ntu,clock_quality\n');

    const cursor = SensorReading.find(filter).sort({ record_id: 1 }).cursor();
    for (let doc = await cursor.next(); doc != null; doc = await cursor.next()) {
      const line = `${doc.record_id},${doc.batch_id},${doc.timestamp ? doc.timestamp.toISOString() : ''},${doc.elapsed_seconds || 0},${Number(doc.temperature_c || 0).toFixed(2)},${Number(doc.ph_estimate || 0).toFixed(2)},${Number(doc.ph_voltage || 0).toFixed(3)},${Number(doc.turbidity_voltage || 0).toFixed(3)},${doc.turbidity_raw || 0},${doc.turbidity_ntu !== undefined ? Number(doc.turbidity_ntu).toFixed(1) : '-1.0'},${doc.clock_quality || 'RTC_VERIFIED'}\n`;
      res.write(line);
    }
    res.end();
  } catch (err) {
    console.error('[CSV EXPORT ERROR]', err.message);
    res.status(500).send(`Export failed: ${err.message}`);
  }
});

/**
 * POST /api/clear-data & DELETE /api/sensor-data
 * Empties all records from MongoDB Atlas collection and resets batch summary
 */
const handleClearData = async (req, res) => {
  try {
    const readingResult = await SensorReading.deleteMany({});
    const batchResult = await BatchSummary.deleteMany({});
    console.log(`[DATABASE RESET] Deleted ${readingResult.deletedCount} readings and ${batchResult.deletedCount} batch summaries.`);
    res.json({
      success: true,
      message: 'MongoDB database cleared successfully',
      deleted_readings: readingResult.deletedCount,
      deleted_batches: batchResult.deletedCount
    });
  } catch (err) {
    console.error('[DATABASE RESET ERROR]', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
};

app.post('/api/clear-data', handleClearData);
app.delete('/api/sensor-data', handleClearData);

// ============================================================================
// 4.1 MACHINE LEARNING INFERENCE & ANALYTICS INTEGRATION
// ============================================================================
const ML_SERVICE_URL = process.env.ML_SERVICE_URL || 'http://127.0.0.1:8000';
const fs = require('fs');
const path = require('path');

const proxyToML = async (req, res, targetPath, method = 'GET', body = null) => {
  try {
    const url = `${ML_SERVICE_URL}${targetPath}`;
    const options = {
      method,
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' }
    };
    if (body && (method === 'POST' || method === 'PUT')) {
      options.body = JSON.stringify(body);
    }

    const response = await fetch(url, options);
    const data = await response.json();
    return res.status(response.status).json(data);
  } catch (err) {
    // Graceful offline fallback
    if (targetPath.startsWith('/api/ml/metrics')) {
      try {
        const metricsFile = path.resolve(__dirname, '../model/saved_models/metrics.json');
        if (fs.existsSync(metricsFile)) {
          const fileData = JSON.parse(fs.readFileSync(metricsFile, 'utf8'));
          return res.json({ success: true, metrics: fileData, source: 'cached_disk_artifact' });
        }
      } catch (fErr) {
        // Continue to offline error
      }
    }

    return res.status(503).json({
      success: false,
      service: 'tepache-ml-service',
      status: 'offline',
      error: `Could not connect to Python ML service at ${ML_SERVICE_URL}: ${err.message}`,
      recommendation: 'Start the Python ML service using: python -m uvicorn model.ml_api:app --port 8000'
    });
  }
};

app.get('/api/ml/status', (req, res) => {
  proxyToML(req, res, '/api/ml/status', 'GET');
});

app.get('/api/ml/metrics', (req, res) => {
  proxyToML(req, res, '/api/ml/metrics', 'GET');
});

app.post('/api/ml/predict', async (req, res) => {
  let payload = req.body;

  // Auto-populate with latest MongoDB reading if request body is empty
  if (!payload || !payload.current_reading) {
    try {
      const latest = await SensorReading.findOne().sort({ record_id: -1 }).lean();
      if (latest) {
        payload = {
          current_reading: {
            record_id: latest.record_id,
            batch_id: latest.batch_id,
            timestamp: latest.timestamp,
            elapsed_seconds: latest.elapsed_seconds,
            temperature_c: latest.temperature_c,
            ph_estimate: latest.ph_estimate,
            ph_voltage: latest.ph_voltage,
            turbidity_voltage: latest.turbidity_voltage,
            turbidity_raw: latest.turbidity_raw,
            turbidity_ntu: latest.turbidity_ntu
          },
          forecast_horizon_minutes: (req.query && req.query.horizon) ? Number(req.query.horizon) : 15
        };
      }
    } catch (e) {
      console.warn('[ML PREDICT] Could not fetch latest MongoDB reading:', e.message);
    }
  }

  proxyToML(req, res, '/api/ml/predict', 'POST', payload);
});

app.get('/api/ml/anomalies', (req, res) => {
  const queryStr = req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : '';
  proxyToML(req, res, `/api/ml/anomalies${queryStr}`, 'GET');
});

app.get('/api/ml/readiness', async (req, res) => {
  let queryStr = req.url.includes('?') ? req.url.substring(req.url.indexOf('?')) : '';

  // Auto-fill query params from latest MongoDB record if no params provided
  if (!queryStr || queryStr === '?') {
    try {
      const latest = await SensorReading.findOne().sort({ record_id: -1 }).lean();
      if (latest) {
        queryStr = `?temp_c=${latest.temperature_c}&ph_estimate=${latest.ph_estimate}&turbidity_NTU=${latest.turbidity_ntu >= 0 ? latest.turbidity_ntu : 100}&elapsed_seconds=${latest.elapsed_seconds || 0}`;
      }
    } catch (e) {
      // ignore
    }
  }

  proxyToML(req, res, `/api/ml/readiness${queryStr}`, 'GET');
});

// ============================================================================
// 5. EMBEDDED REAL-TIME WEB DASHBOARD (GET /)
// ============================================================================
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Tepache Fermentation Live Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #090d16;
      --card: rgba(18, 26, 43, 0.85);
      --border: rgba(255, 255, 255, 0.08);
      --text: #f8fafc;
      --muted: #94a3b8;
      --gold: #f59e0b;
      --rose: #f43f5e;
      --cyan: #06b6d4;
      --green: #10b981;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: var(--bg);
      color: var(--text);
      font-family: 'Plus Jakarta Sans', sans-serif;
      padding: 24px;
      min-height: 100vh;
    }
    .container { max-width: 1200px; margin: 0 auto; }
    header {
      display: flex; justify-content: space-between; align-items: center;
      margin-bottom: 24px; flex-wrap: wrap; gap: 12px;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .brand h1 { font-size: 22px; font-weight: 800; }
    .status-pill {
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.25);
      padding: 6px 14px; border-radius: 99px;
      color: var(--green); font-size: 13px; font-weight: 600;
    }
    .pulse-dot {
      width: 8px; height: 8px; background: #10b981; border-radius: 50%; display: inline-block;
      animation: pulse 1.4s infinite;
    }
    @keyframes pulse {
      0% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
      70% { transform: scale(1.1); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
      100% { transform: scale(0.9); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
    .grid {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px; margin-bottom: 24px;
    }
    .card {
      background: var(--card); border: 1px solid var(--border);
      border-radius: 16px; padding: 20px;
    }
    .card-label { font-size: 12px; font-weight: 700; color: var(--muted); text-transform: uppercase; margin-bottom: 6px; }
    .card-val { font-size: 32px; font-weight: 800; margin-bottom: 4px; }
    .card-sub { font-size: 13px; color: var(--muted); }
    .chart-box {
      background: var(--card); border: 1px solid var(--border);
      border-radius: 16px; padding: 20px; margin-bottom: 24px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th { text-align: left; padding: 10px 14px; color: var(--muted); border-bottom: 1px solid var(--border); }
    td { padding: 10px 14px; border-bottom: 1px solid rgba(255,255,255,0.04); }
    .mono { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <span style="font-size: 28px;">🍍</span>
        <div>
          <h1>Tepache Fermentation Cloud Monitor</h1>
          <p style="font-size: 12px; color: var(--muted);">ESP32 Dev Module • MongoDB Atlas • Render</p>
        </div>
      </div>
      <div style="display:flex; align-items:center; gap: 10px;">
        <a href="/api/export-csv" class="status-pill" style="text-decoration:none; background: rgba(59,130,246,0.15); color: #60a5fa; border-color: rgba(59,130,246,0.35);">⬇ Export CSV</a>
        <button onclick="clearDatabase()" class="status-pill" style="cursor:pointer; background: rgba(244,63,94,0.15); color: #f43f5e; border-color: rgba(244,63,94,0.35);">🗑 Empty Database</button>
        <div class="status-pill" style="display:flex; align-items:center; gap:8px;"><span class="pulse-dot"></span> Live Real-time (1s)</div>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <div class="card-label">Acidity (pH)</div>
        <div class="card-val" style="color: var(--rose);" id="phVal">--</div>
        <div class="card-sub" id="phSub">Voltage: -- V</div>
      </div>
      <div class="card">
        <div class="card-label">Broth Temperature</div>
        <div class="card-val" style="color: var(--gold);" id="tempVal">--</div>
        <div class="card-sub">Sensor: DS18B20 (GPIO 4)</div>
      </div>
      <div class="card">
        <div class="card-label">Turbidity</div>
        <div class="card-val" style="color: var(--cyan);" id="turbVal">--</div>
        <div class="card-sub" id="turbSub">Optical Sensor (GPIO 34)</div>
      </div>
      <div class="card">
        <div class="card-label">Batch Progression</div>
        <div class="card-val" style="color: var(--green);" id="batchRec">#0</div>
        <div class="card-sub" id="batchSub">Batch: TEP001 • Ingested: 0</div>
      </div>
    </div>

    <div class="chart-box">
      <h2 style="font-size: 16px; margin-bottom: 16px;">Telemetry Time Series</h2>
      <div style="height: 280px;"><canvas id="chart"></canvas></div>
    </div>

    <div class="card" style="overflow-x: auto;">
      <h2 style="font-size: 16px; margin-bottom: 14px;">Recent Telemetry Ingests</h2>
      <table>
        <thead>
          <tr>
            <th># ID</th>
            <th>Batch</th>
            <th>Timestamp (UTC)</th>
            <th>Elapsed</th>
            <th>Temp (°C)</th>
            <th>pH</th>
            <th>Turbidity (V / NTU)</th>
            <th>Clock Quality</th>
          </tr>
        </thead>
        <tbody id="tbody">
          <tr><td colspan="7" style="text-align: center; color: var(--muted);">Waiting for sensor data...</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    let chart;
    function initChart() {
      const ctx = document.getElementById('chart').getContext('2d');
      chart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: [],
          datasets: [
            { label: 'pH', data: [], borderColor: '#f43f5e', yAxisID: 'yPh', tension: 0.3, borderWidth: 2 },
            { label: 'Temp (°C)', data: [], borderColor: '#f59e0b', yAxisID: 'yTemp', tension: 0.3, borderWidth: 2 },
            { label: 'Turbidity (V)', data: [], borderColor: '#06b6d4', yAxisID: 'yTurb', tension: 0.3, borderWidth: 2 }
          ]
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          animation: false,
          scales: {
            x: { ticks: { color: '#64748b' } },
            yPh: { type: 'linear', position: 'left', min: 2, max: 8, ticks: { color: '#f43f5e' } },
            yTemp: { type: 'linear', position: 'right', min: 15, max: 40, ticks: { color: '#f59e0b' } },
            yTurb: { type: 'linear', position: 'right', min: 0, max: 3.5, grid: { drawOnChartArea: false }, ticks: { color: '#06b6d4' } }
          },
          plugins: { legend: { labels: { color: '#94a3b8' } } }
        }
      });
    }

    let lastRecordId = null;
    let isFetching = false;

    async function loadData() {
      try {
        const res = await fetch('/api/sensor-data?limit=25&_t=' + Date.now(), { cache: 'no-store' });
        const data = await res.json();
        if (!data.success || !data.records.length) return;

        const latest = data.records[0];
        if (lastRecordId === latest.record_id) return;
        lastRecordId = latest.record_id;

        document.getElementById('phVal').innerText = latest.ph_estimate.toFixed(2);
        document.getElementById('phSub').innerText = 'Voltage: ' + latest.ph_voltage.toFixed(3) + ' V';
        document.getElementById('tempVal').innerText = latest.temperature_c.toFixed(2) + ' °C';
        document.getElementById('turbVal').innerText = latest.turbidity_voltage.toFixed(3) + ' V';
        document.getElementById('batchRec').innerText = '#' + latest.record_id;
        document.getElementById('batchSub').innerText = 'Batch: ' + latest.batch_id + ' • Ingested: ' + data.records.length;

        const tbody = document.getElementById('tbody');
        tbody.innerHTML = '';
        data.records.slice(0, 10).forEach(r => {
          const sec = r.elapsed_seconds || 0;
          const h = String(Math.floor(sec / 3600)).padStart(2, '0');
          const m = String(Math.floor((sec % 3600) / 60)).padStart(2, '0');
          const s = String(sec % 60).padStart(2, '0');
          const turbNtuText = (r.turbidity_ntu !== undefined && r.turbidity_ntu >= 0) ? (' (' + r.turbidity_ntu.toFixed(0) + ' NTU)') : '';
          const qualColor = r.clock_quality === 'UNCERTAIN_BOOT_CLOCK' ? '#f43f5e' : '#10b981';
          const row = document.createElement('tr');
          row.innerHTML = \`
            <td class="mono">#\${r.record_id}</td>
            <td><strong>\${r.batch_id}</strong></td>
            <td>\${new Date(r.timestamp).toISOString()}</td>
            <td>\${h}:\${m}:\${s}</td>
            <td>\${r.temperature_c.toFixed(2)} °C</td>
            <td>\${r.ph_estimate.toFixed(2)}</td>
            <td>\${r.turbidity_voltage.toFixed(3)} V\${turbNtuText}</td>
            <td><span class="mono" style="font-size:11px; color: \${qualColor};">\${r.clock_quality || 'RTC_VERIFIED'}</span></td>
          \`;
          tbody.appendChild(row);
        });

        const rev = [...data.records].reverse();
        chart.data.labels = rev.map(r => new Date(r.timestamp).toLocaleTimeString());
        chart.data.datasets[0].data = rev.map(r => r.ph_estimate);
        chart.data.datasets[1].data = rev.map(r => r.temperature_c);
        chart.data.datasets[2].data = rev.map(r => r.turbidity_voltage);
        chart.update('none');

      } catch (e) {
        console.error('Fetch error:', e);
      }
    }

    async function streamLoop() {
      if (isFetching) return;
      isFetching = true;
      try {
        await loadData();
      } finally {
        isFetching = false;
        setTimeout(streamLoop, 1000); // 1-second refresh
      }
    }

    async function clearDatabase() {
      const confirmReset = confirm("⚠️ Are you sure you want to empty the MongoDB database?\\n\\nAll recorded telemetry will be permanently deleted from the cloud. This action cannot be undone.");
      if (!confirmReset) return;

      try {
        const res = await fetch('/api/clear-data', { method: 'POST' });
        const data = await res.json();
        if (data.success) {
          alert("✅ Database emptied! (" + data.deleted_readings + " records deleted).");
          location.reload();
        } else {
          alert("Error clearing database: " + data.error);
        }
      } catch (e) {
        alert("Failed to communicate with server: " + e.message);
      }
    }

    window.addEventListener('DOMContentLoaded', () => {
      initChart();
      streamLoop();
    });
  </script>
</body>
</html>
  `);
});

// ============================================================================
// 6. SERVER START
// ============================================================================
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[SERVER] Ingestion Engine online on port ${PORT}`);
  console.log(`[SERVER] Live URL      : https://tapache-2-o.onrender.com`);
  console.log(`[SERVER] Health check  : https://tapache-2-o.onrender.com/health`);
  console.log(`[SERVER] Ingestion URL : https://tapache-2-o.onrender.com/api/sensor-data`);
  console.log(`[SERVER] Live Dashboard: https://tapache-2-o.onrender.com/`);
});
