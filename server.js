/*
 * AGRISOLVE Backend - server.js
 * Express + WebSocket server
 * Real-time sensor updates via WebSocket
 * AI plant analysis via Anthropic Claude vision
 */

require('dotenv').config();
const express   = require('express');
const http      = require('http');
const httpLib   = require('http');
const https     = require('https');
const { WebSocketServer, WebSocket } = require('ws');
const cors      = require('cors');
const path      = require('path');
const Anthropic = require('@anthropic-ai/sdk');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: '/ws/sensors' });
const PORT   = process.env.PORT || 3000;

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── STATE ────────────────────────────────────────────────────────────────────
let latestSensor   = { temperature: null, soil_moisture: null, soil_status: null, timestamp: null };
let latestAnalysis = null;
let sensorHistory  = [];
let cameraIP       = null;

// Device online tracking
const deviceStatus = {
  main_esp:   { online: false, lastSeen: null },
  camera_esp: { online: false, lastSeen: null },
};

// Connected WebSocket clients (browser dashboards)
const dashboardClients = new Set();

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  console.log('[WS] New connection from', req.socket.remoteAddress);
  let deviceType = 'dashboard'; // default until identified

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      // ── Device identification ────────────────────────────────────────────
      if (msg.type === 'identify') {
        deviceType = msg.device; // 'main_esp' or 'camera_esp'
        deviceStatus[deviceType].online  = true;
        deviceStatus[deviceType].lastSeen = new Date().toISOString();

        if (msg.device === 'camera_esp') {
          // Save camera IP from connection for later use
          cameraIP = req.socket.remoteAddress.replace('::ffff:', '');
        }

        console.log(`[WS] Device identified: ${deviceType}`);
        broadcastStatus();
        return;
      }

      // ── Sensor data from main ESP ─────────────────────────────────────────
      if (msg.type === 'sensor_data' && msg.device === 'main_esp') {
        latestSensor = {
          temperature:   msg.temperature,
          soil_moisture: msg.soil_moisture,
          soil_status:   msg.soil_status,
          timestamp:     new Date().toISOString(),
        };
        deviceStatus.main_esp.online   = true;
        deviceStatus.main_esp.lastSeen = latestSensor.timestamp;

        sensorHistory.unshift(latestSensor);
        if (sensorHistory.length > 50) sensorHistory.pop();

        // Push to all dashboard browsers instantly
        broadcastToDashboards({
          type:   'sensor_update',
          sensor: latestSensor,
          status: deviceStatus,
        });
      }

      // ── Dashboard client identifies itself ────────────────────────────────
      if (msg.type === 'dashboard_hello') {
        deviceType = 'dashboard';
        dashboardClients.add(ws);
        // Send current state immediately on connect
        ws.send(JSON.stringify({
          type:     'init',
          sensor:   latestSensor,
          analysis: latestAnalysis,
          history:  sensorHistory,
          status:   deviceStatus,
          camera_ip: cameraIP,
        }));
      }

    } catch (e) {
      console.error('[WS] Parse error:', e.message);
    }
  });

  ws.on('close', () => {
    dashboardClients.delete(ws);
    if (deviceType !== 'dashboard') {
      deviceStatus[deviceType].online = false;
      console.log(`[WS] Device disconnected: ${deviceType}`);
      broadcastStatus();
    }
  });

  // Heartbeat to detect dead connections
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

// Ping all clients every 30s to detect disconnects
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) { ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

function broadcastToDashboards(data) {
  const msg = JSON.stringify(data);
  dashboardClients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  });
}

function broadcastStatus() {
  broadcastToDashboards({ type: 'status_update', status: deviceStatus });
}

// ─── REST: /api/latest ────────────────────────────────────────────────────────
app.get('/api/latest', (req, res) => {
  res.json({ sensor: latestSensor, analysis: latestAnalysis, status: deviceStatus, camera_ip: cameraIP });
});

// ─── REST: /api/history ───────────────────────────────────────────────────────
app.get('/api/history', (req, res) => {
  res.json({ history: sensorHistory });
});

// ─── REST: /api/analyse ───────────────────────────────────────────────────────
// Dashboard POSTs { camera_ip } → backend fetches photo → Claude analyses
app.post('/api/analyse', async (req, res) => {
  const camIP = req.body.camera_ip || cameraIP;
  if (!camIP) return res.status(400).json({ error: 'Camera IP not known yet. Make sure Camera ESP is connected.' });

  console.log(`[Analyse] Fetching image from http://${camIP}/capture`);

  let imageBase64;
  try {
    imageBase64 = await fetchImageAsBase64(`http://${camIP}/capture`);
  } catch (err) {
    return res.status(502).json({ error: `Cannot reach camera at ${camIP}: ${err.message}` });
  }

  console.log('[Analyse] Image received, sending to Claude vision...');
  try {
    const response = await anthropic.messages.create({
      model: 'claude-opus-4-6',
      max_tokens: 1024,
      messages: [{
        role: 'user',
        content: [
          {
            type: 'image',
            source: { type: 'base64', media_type: 'image/jpeg', data: imageBase64 },
          },
          {
            type: 'text',
            text: `You are an expert agricultural plant pathologist. Analyse this plant image carefully.
Respond ONLY with a valid JSON object, no markdown, no extra text:
{
  "health_status": "Healthy | Diseased | Stressed | Unknown",
  "disease_name": "specific disease name or None if healthy",
  "cause": "what is causing this condition (pathogen, pest, environmental, nutrient deficiency, etc.)",
  "recommendation": "immediate actionable advice for the farmer",
  "treatment": "specific treatment steps if diseased, or No treatment needed if healthy",
  "confidence": "High | Medium | Low"
}`,
          },
        ],
      }],
    });

    const raw = response.content[0].text.trim();
    let analysis;
    try {
      analysis = JSON.parse(raw);
    } catch {
      const m = raw.match(/\{[\s\S]*\}/);
      analysis = m ? JSON.parse(m[0]) : { health_status: 'Unknown', disease_name: 'Parse error', cause: raw, recommendation: '', treatment: '', confidence: 'Low' };
    }
    analysis.timestamp = new Date().toISOString();
    latestAnalysis = analysis;

    // Push analysis result to all dashboards instantly
    broadcastToDashboards({ type: 'analysis_result', analysis });

    console.log(`[Analyse] ${analysis.health_status} — ${analysis.disease_name}`);
    res.json({ ok: true, analysis });

  } catch (err) {
    console.error('[Analyse] AI error:', err.message);
    res.status(500).json({ error: 'AI analysis failed: ' + err.message });
  }
});

// ─── REST: /api/flash ─────────────────────────────────────────────────────────
app.post('/api/flash', async (req, res) => {
  const { state, camera_ip } = req.body;
  const camIP = camera_ip || cameraIP;
  if (!camIP) return res.status(400).json({ error: 'Camera IP unknown' });

  const url = `http://${camIP}/flash/${state === 'on' ? 'on' : 'off'}`;
  try {
    await fetchUrl(url);
    res.json({ ok: true, flash: state });
  } catch (err) {
    res.status(502).json({ error: `Flash command failed: ${err.message}` });
  }
});

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function fetchImageAsBase64(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : httpLib;
    const req = client.get(url, { timeout: 10000 }, (resp) => {
      if (resp.statusCode !== 200) return reject(new Error(`Status ${resp.statusCode}`));
      const chunks = [];
      resp.on('data', c => chunks.push(c));
      resp.on('end', () => resolve(Buffer.concat(chunks).toString('base64')));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timed out')); });
  });
}

function fetchUrl(url) {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https') ? https : httpLib;
    const req = client.get(url, { timeout: 5000 }, (resp) => {
      resp.resume();
      resp.on('end', resolve);
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timed out')); });
  });
}

// ─── CATCH ALL → serve index.html ─────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─────────────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n🌱 AGRISOLVE Backend running on port ${PORT}`);
  console.log(`   Dashboard: http://localhost:${PORT}\n`);
});
