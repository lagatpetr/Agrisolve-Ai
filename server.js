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

const GROQ_API_KEY = process.env.GROQ_API_KEY;
const GROQ_MODEL    = 'meta-llama/llama-4-scout-17b-16e-instruct'; // ✅ FIXED

app.use(cors());
app.use(express.json({ limit: '20mb' }));
app.use(express.static(path.join(__dirname, 'public')));

let latestSensor   = { temperature: null, soil_moisture: null, soil_status: null, timestamp: null };
let latestAnalysis = null;
let sensorHistory  = [];
let cameraWS       = null;
let cameraIP       = null;
let pendingCapture = null;
let streaming      = false;

const deviceStatus = {
  main_esp:   { online: false, lastSeen: null },
  camera_esp: { online: false, lastSeen: null },
};
const dashboardClients = new Set();

wss.on('connection', (ws, req) => {
  console.log('[WS] New connection from', req.socket.remoteAddress);
  let deviceType = 'dashboard';

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw.toString());

      if (msg.type === 'identify') {
        deviceType = msg.device;
        deviceStatus[deviceType].online   = true;
        deviceStatus[deviceType].lastSeen = new Date().toISOString();
        if (msg.device === 'camera_esp') {
          cameraWS = ws;
          cameraIP = msg.ip || 'unknown';
          console.log(`[WS] Camera ESP registered (IP: ${cameraIP})`);
        }
        broadcastStatus();
        ws.send(JSON.stringify({ type: 'ack', message: 'Registered' }));
        return;
      }

      if (msg.type === 'image_data' && msg.data) {
        console.log(`[WS] Still image received (${msg.data.length} chars)`);
        broadcastToDashboards({ type: 'captured_image', imageBase64: msg.data });
        if (pendingCapture) {
          pendingCapture.resolve(msg.data);
          pendingCapture = null;
        }
        return;
      }

      if (msg.type === 'stream_frame' && msg.data) {
        if (streaming) {
          broadcastToDashboards({ type: 'stream_frame', imageBase64: msg.data });
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
          pump:          msg.pump,
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
        return;
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
        return;
      }

      if (msg.type === 'start_stream') {
        streaming = true;
        console.log('[WS] Live stream started');
        if (cameraWS && cameraWS.readyState === WebSocket.OPEN)
          cameraWS.send(JSON.stringify({ type: 'start_stream' }));
        return;
      }

      if (msg.type === 'stop_stream') {
        streaming = false;
        console.log('[WS] Live stream stopped');
        if (cameraWS && cameraWS.readyState === WebSocket.OPEN)
          cameraWS.send(JSON.stringify({ type: 'stop_stream' }));
        return;
      }

    } catch (e) {
      console.error('[WS] Parse error:', e.message);
    }
  });

  ws.on('close', () => {
    dashboardClients.delete(ws);
    if (deviceType === 'camera_esp') {
      cameraWS = null; streaming = false;
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
    ws.isAlive = false; ws.ping();
  });
}, 30000);

function broadcastToDashboards(data) {
  const msg = JSON.stringify(data);
  dashboardClients.forEach((c) => {
    if (c.readyState === WebSocket.OPEN) c.send(msg);
  });
}

function broadcastStatus() {
  broadcastToDashboards({ type: 'status_update', status: deviceStatus });
}

app.get('/api/latest', (req, res) => {
  res.json({ sensor: latestSensor, analysis: latestAnalysis, status: deviceStatus, camera_ip: cameraIP });
});

app.get('/api/history', (req, res) => {
  res.json({ history: sensorHistory });
});

app.post('/api/analyse', async (req, res) => {
  if (!cameraWS || cameraWS.readyState !== WebSocket.OPEN)
    return res.status(400).json({ error: 'Camera ESP32 not connected.' });

  if (!GROQ_API_KEY)
    return res.status(500).json({ error: 'GROQ_API_KEY not set in environment variables' });

  if (streaming) cameraWS.send(JSON.stringify({ type: 'stop_stream' }));

  console.log('[Analyse] Sending capture_now to camera...');

  try {
    cameraWS.send(JSON.stringify({ type: 'capture_now' }));

    const imageBase64 = await new Promise((resolve, reject) => {
      pendingCapture = { resolve, reject };
      setTimeout(() => {
        if (pendingCapture) {
          pendingCapture = null;
          reject(new Error('Camera timed out — no image in 20s'));
        }
      }, 20000);
    });

    console.log('[Analyse] Image received — sending to Groq AI...');

    const response = await fetch('https://api.groq.com/openai/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${GROQ_API_KEY}`,
      },
      body: JSON.stringify({
        model: GROQ_MODEL,
        messages: [{
          role: 'user',
          content: [
            {
              type: 'text',
              text: `You are an expert agricultural plant pathologist. Analyse this plant image carefully.
Respond ONLY with a valid JSON object, no markdown, no extra text:
{
  "health_status": "Healthy | Diseased | Stressed | Unknown",
  "disease_name": "specific disease name or None if healthy",
  "cause": "what is causing this condition",
  "recommendation": "immediate actionable advice for the farmer",
  "treatment": "specific treatment steps if diseased, or No treatment needed if healthy",
  "confidence": "High | Medium | Low"
}`,
            },
            {
              type: 'image_url',
              image_url: { url: `data:image/jpeg;base64,${imageBase64}` },
            },
          ],
        }],
        response_format: { type: 'json_object' },
        temperature: 0.3,
        max_completion_tokens: 1024,
      }),
    });

    const groqData = await response.json();

    if (!response.ok)
      throw new Error(groqData?.error?.message || `Groq API error ${response.status}`);

    const raw = groqData?.choices?.[0]?.message?.content?.trim() || '';
    let analysis;
    try {
      analysis = JSON.parse(raw);
    } catch {
      const m = raw.match(/\{[\s\S]*\}/);
      analysis = m ? JSON.parse(m[0]) : {
        health_status: 'Unknown',
        disease_name:  'Parse error',
        cause:         raw,
        recommendation: '',
        treatment:     '',
        confidence:    'Low',
      };
    }

    analysis.timestamp   = new Date().toISOString();
    analysis.imageBase64 = imageBase64;
    latestAnalysis       = analysis;

    broadcastToDashboards({ type: 'analysis_result', analysis });

    if (streaming && cameraWS && cameraWS.readyState === WebSocket.OPEN)
      cameraWS.send(JSON.stringify({ type: 'start_stream' }));

    console.log(`[Analyse] ${analysis.health_status} — ${analysis.disease_name}`);
    res.json({ ok: true, analysis });

  } catch (err) {
    console.error('[Analyse] Error:', err.message);
    if (streaming && cameraWS && cameraWS.readyState === WebSocket.OPEN)
      cameraWS.send(JSON.stringify({ type: 'start_stream' }));
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/flash', (req, res) => {
  const { state } = req.body;
  if (!cameraWS || cameraWS.readyState !== WebSocket.OPEN)
    return res.status(400).json({ error: 'Camera ESP32 not connected' });
  cameraWS.send(JSON.stringify({ type: 'flash', state: state === 'on' ? 'on' : 'off' }));
  res.json({ ok: true, flash: state });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

server.listen(PORT, () => {
  console.log(`\n🌱 AGRISOLVE Backend running on port ${PORT}`);
  console.log(`   Dashboard: http://localhost:${PORT}\n`);
});