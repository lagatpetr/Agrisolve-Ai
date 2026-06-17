/*
 * AGRISOLVE Backend - server.js
 * AI vision analysis now uses Google Gemini (free tier) instead of Anthropic.
 */

require('dotenv').config();
const express   = require('express');
const http      = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const cors      = require('cors');
const path      = require('path');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: '/ws/sensors' });
const PORT   = process.env.PORT || 3000;

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL    = 'gemini-2.5-flash';
const GEMINI_URL      = `https://generativelanguage.googleapis.com/v1beta/models/${GEMINI_MODEL}:generateContent?key=${GEMINI_API_KEY}`;

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ─── STATE ────────────────────────────────────────────────────────────────────
let latestSensor   = { temperature: null, soil_moisture: null, soil_status: null, timestamp: null };
let latestAnalysis = null;
let sensorHistory  = [];

const deviceStatus = {
  main_esp:   { online: false, lastSeen: null },
  camera_esp: { online: false, lastSeen: null },
};

const dashboardClients = new Set();

let cameraWS       = null;
let cameraIP       = null;

let pendingCapture = null;

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  console.log('[WS] New connection from', req.socket.remoteAddress);
  let deviceType = 'dashboard';

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'identify') {
        if (msg.device !== 'main_esp' && msg.device !== 'camera_esp') {
          console.warn('[WS] Unknown identify device:', msg.device);
          return;
        }
        deviceType = msg.device;
        deviceStatus[deviceType].online   = true;
        deviceStatus[deviceType].lastSeen = new Date().toISOString();

        if (msg.device === 'camera_esp') {
          cameraWS = ws;
          cameraIP = msg.ip || 'unknown';
          console.log(`[WS] Camera ESP registered (local IP: ${cameraIP})`);
        }

        console.log(`[WS] Device identified: ${deviceType}`);
        broadcastStatus();
        ws.send(JSON.stringify({ type: 'ack', message: 'Registered' }));
        return;
      }

      if (msg.type === 'image_data' && msg.data) {
        console.log(`[WS] Image received from camera (${msg.data.length} chars)`);

        broadcastToDashboards({ type: 'captured_image', imageBase64: msg.data });

        if (pendingCapture) {
          pendingCapture.resolve(msg.data);
          pendingCapture = null;
        }
        return;
      }

      if (msg.type === 'capture_error') {
        console.error('[WS] Camera error:', msg.message);
        broadcastToDashboards({ type: 'analysis_error', error: 'Camera: ' + msg.message });
        if (pendingCapture) {
          pendingCapture.reject(new Error(msg.message));
          pendingCapture = null;
        }
        return;
      }

      if (msg.type === 'sensor_data' && msg.device === 'main_esp') {
        latestSensor = {
          temperature:   msg.temperature,
          soil_moisture: msg.soil_moisture,
          soil_status:   msg.soil_status,
          water_detected: msg.water_detected,
          timestamp:     new Date().toISOString(),
        };
        deviceStatus.main_esp.online   = true;
        deviceStatus.main_esp.lastSeen = latestSensor.timestamp;

        sensorHistory.unshift(latestSensor);
        if (sensorHistory.length > 50) sensorHistory.pop();

        broadcastToDashboards({
          type:   'sensor_update',
          sensor: latestSensor,
          status: deviceStatus,
        });
      }

      if (msg.type === 'dashboard_hello') {
        deviceType = 'dashboard';
        dashboardClients.add(ws);
        ws.send(JSON.stringify({
          type:      'init',
          sensor:    latestSensor,
          analysis:  latestAnalysis,
          history:   sensorHistory,
          status:    deviceStatus,
          camera_ip: cameraIP,
        }));
      }

    } catch (e) {
      console.error('[WS] Parse error:', e.message);
    }
  });

  ws.on('close', () => {
    dashboardClients.delete(ws);
    if (deviceType === 'camera_esp') {
      cameraWS = null;
      deviceStatus.camera_esp.online = false;
      console.log('[WS] Camera ESP disconnected');
      broadcastStatus();
    } else if (deviceType === 'main_esp') {
      deviceStatus.main_esp.online = false;
      console.log('[WS] Main ESP disconnected');
      broadcastStatus();
    }
  });

  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
});

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
  res.json({
    sensor:    latestSensor,
    analysis:  latestAnalysis,
    status:    deviceStatus,
    camera_ip: cameraIP,
  });
});

// ─── REST: /api/history ───────────────────────────────────────────────────────
app.get('/api/history', (req, res) => {
  res.json({ history: sensorHistory });
});

// ─── REST: /api/analyse ───────────────────────────────────────────────────────
app.post('/api/analyse', async (req, res) => {
  if (!cameraWS || cameraWS.readyState !== WebSocket.OPEN) {
    return res.status(400).json({
      error: 'Camera ESP32 not connected. Power it on and check Serial Monitor for [WS] Connected.'
    });
  }

  if (!GEMINI_API_KEY) {
    return res.status(500).json({
      error: 'GEMINI_API_KEY is not set on the server. Add it in Render → Environment.'
    });
  }

  console.log('[Analyse] Sending capture_now to camera via WebSocket...');

  try {
    cameraWS.send(JSON.stringify({ type: 'capture_now' }));

    const imageBase64 = await new Promise((resolve, reject) => {
      pendingCapture = { resolve, reject };
      setTimeout(() => {
        if (pendingCapture) {
          pendingCapture = null;
          reject(new Error('Camera timed out — no image received in 20s'));
        }
      }, 20000);
    });

    console.log('[Analyse] Image received — sending to Gemini vision...');

    const prompt = `You are an expert agricultural plant pathologist. Analyse this plant image carefully.
Respond ONLY with a valid JSON object, no markdown, no extra text:
{
  "health_status": "Healthy | Diseased | Stressed | Unknown",
  "disease_name": "specific disease name or None if healthy",
  "cause": "what is causing this condition",
  "recommendation": "immediate actionable advice for the farmer",
  "treatment": "specific treatment steps if diseased, or No treatment needed if healthy",
  "confidence": "High | Medium | Low"
}`;

    const geminiRes = await fetch(GEMINI_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        contents: [{
          parts: [
            { text: prompt },
            {
              inline_data: {
                mime_type: 'image/jpeg',
                data: imageBase64,
              },
            },
          ],
        }],
        generationConfig: {
          temperature: 0.2,
        },
      }),
    });

    const geminiData = await geminiRes.json();

    if (!geminiRes.ok) {
      const errMsg = geminiData?.error?.message || `Gemini request failed (${geminiRes.status})`;
      throw new Error(errMsg);
    }

    const raw = geminiData?.candidates?.[0]?.content?.parts?.[0]?.text?.trim() || '';

    let analysis;
    try {
      analysis = JSON.parse(raw);
    } catch {
      const m = raw.match(/\{[\s\S]*\}/);
      analysis = m ? JSON.parse(m[0]) : {
        health_status: 'Unknown', disease_name: 'Parse error',
        cause: raw, recommendation: '', treatment: '', confidence: 'Low'
      };
    }
    analysis.timestamp    = new Date().toISOString();
    analysis.imageBase64  = imageBase64;
    latestAnalysis        = analysis;

    broadcastToDashboards({ type: 'analysis_result', analysis });

    console.log(`[Analyse] ${analysis.health_status} — ${analysis.disease_name}`);
    res.json({ ok: true, analysis });

  } catch (err) {
    console.error('[Analyse] Error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ─── REST: /api/flash ─────────────────────────────────────────────────────────
app.post('/api/flash', (req, res) => {
  const { state } = req.body;
  if (!cameraWS || cameraWS.readyState !== WebSocket.OPEN) {
    return res.status(400).json({ error: 'Camera ESP32 not connected' });
  }
  cameraWS.send(JSON.stringify({ type: 'flash', state: state === 'on' ? 'on' : 'off' }));
  res.json({ ok: true, flash: state });
});

// ─── CATCH ALL ────────────────────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

server.listen(PORT, () => {
  console.log(`\n🌱 AGRISOLVE Backend running on port ${PORT}`);
  console.log(`   Dashboard: http://localhost:${PORT}\n`);
});