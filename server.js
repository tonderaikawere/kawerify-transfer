import express from 'express';
import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';
import cors from 'cors';
import multer from 'multer';
import path from 'path';
import { fileURLToPath } from 'url';
import fs from 'fs';
import { networkInterfaces } from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = process.env.PORT || 5000;
const app = express();
const server = createServer(app);

// Disable server timeouts so multi-terabyte (1TB+) streams never time out
server.timeout = 0;
server.keepAliveTimeout = 0;
server.requestTimeout = 0;

app.use(cors());
app.use(express.json({ limit: '50mb' }));

const UPLOADS_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    cb(null, UPLOADS_DIR);
  },
  filename: (_req, file, cb) => {
    const fileId = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    cb(null, `${fileId}-${file.originalname}`);
  }
});

// Unlimited file size (supports 1TB+ files streamed directly to disk)
const upload = multer({
  storage,
  limits: { fileSize: Infinity }
});

// Map: fileId -> { id, name, size, type, path, remainingDownloads, createdAt }
const activeTransfers = new Map();

function getLocalIPs() {
  const nets = networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name] || []) {
      if (net.family === 'IPv4' && !net.internal) {
        addresses.push(net.address);
      }
    }
  }
  return addresses;
}

app.get('/api/ips', (_req, res) => {
  res.json({ ips: getLocalIPs(), port: PORT });
});

app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  const filename = req.file.filename;
  const fileId = filename.substring(0, filename.indexOf('-', filename.indexOf('-') + 1));
  const targetCount = Math.max(1, parseInt(req.query.targets || '1', 10));

  const fileData = {
    id: fileId,
    name: req.file.originalname,
    size: req.file.size,
    type: req.file.mimetype || 'application/octet-stream',
    path: req.file.path,
    remainingDownloads: targetCount,
    createdAt: Date.now()
  };

  activeTransfers.set(fileId, fileData);

  res.json({
    success: true,
    fileId,
    downloadUrl: `/api/download/${fileId}`,
    name: fileData.name,
    size: fileData.size
  });
});

app.get('/api/download/:fileId', (req, res) => {
  const { fileId } = req.params;
  const fileData = activeTransfers.get(fileId);

  if (!fileData || !fs.existsSync(fileData.path)) {
    activeTransfers.delete(fileId);
    return res.status(404).send('File not found or transfer expired');
  }

  res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(fileData.name)}"`);
  res.setHeader('Content-Type', fileData.type);
  res.setHeader('Content-Length', fileData.size);

  // Use 1MB highWaterMark chunks for maximum LAN throughput on multi-gigabyte / 1TB files
  const fileStream = fs.createReadStream(fileData.path, { highWaterMark: 1024 * 1024 });
  fileStream.pipe(res);

  res.on('finish', () => {
    fileData.remainingDownloads -= 1;
    if (fileData.remainingDownloads <= 0) {
      setTimeout(() => {
        try {
          if (fs.existsSync(fileData.path)) {
            fs.unlinkSync(fileData.path);
          }
        } catch (err) {
          console.error(`Error deleting file ${fileData.name}:`, err);
        }
        activeTransfers.delete(fileId);
      }, 10000);
    }
  });
});

app.use(express.static(path.join(__dirname, 'client', 'dist')));

app.get('*', (_req, res) => {
  const indexHtmlPath = path.join(__dirname, 'client', 'dist', 'index.html');
  if (fs.existsSync(indexHtmlPath)) {
    res.sendFile(indexHtmlPath);
  } else {
    res.status(200).send('Kawerify Transfer Server is running. Frontend is not built yet.');
  }
});

const wss = new WebSocketServer({ server });
const peers = new Map();

const ADJECTIVES = ['North', 'Cobalt', 'Amber', 'Cedar', 'Lunar', 'Sol', 'Vertex', 'Quartz', 'Metro', 'Atlas', 'Prime'];
const NOUNS = ['Laptop', 'Workstation', 'Studio', 'Node', 'Terminal', 'Device'];

function generateRandomName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const noun = NOUNS[Math.floor(Math.random() * NOUNS.length)];
  const num = Math.floor(Math.random() * 90) + 10;
  return `${adj} ${noun}-${num}`;
}

wss.on('connection', (ws) => {
  const peerId = `peer-${Math.random().toString(36).substring(2, 9)}`;
  const peerName = generateRandomName();

  const peerState = {
    id: peerId,
    name: peerName,
    avatar: 1,
    deviceType: 'Laptop / Desktop',
    joinedAt: Date.now()
  };

  peers.set(peerId, { ws, state: peerState });

  ws.send(JSON.stringify({
    type: 'welcome',
    peer: peerState
  }));

  broadcastPeers();

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      switch (data.type) {
        case 'rename':
          if (data.name && data.name.trim().length > 0) {
            const current = peers.get(peerId);
            if (current) {
              current.state.name = data.name.trim().substring(0, 24);
              if (data.deviceType) current.state.deviceType = data.deviceType;
              broadcastPeers();
            }
          }
          break;

        case 'transfer-init': {
          // Support single targetId OR array of targetIds (for sharing to 5+ devices at once)
          const targets = Array.isArray(data.targetIds)
            ? data.targetIds
            : data.targetId
              ? [data.targetId]
              : [];

          for (const tid of targets) {
            if (peers.has(tid)) {
              const target = peers.get(tid);
              target.ws.send(JSON.stringify({
                type: 'transfer-offer',
                senderId: peerId,
                senderName: peerState.name,
                fileId: data.fileId,
                fileName: data.fileName,
                fileSize: data.fileSize,
                fileType: data.fileType
              }));
            }
          }
          break;
        }

        case 'transfer-accept':
          if (data.senderId && peers.has(data.senderId)) {
            const sender = peers.get(data.senderId);
            sender.ws.send(JSON.stringify({
              type: 'transfer-accepted',
              receiverId: peerId,
              receiverName: peerState.name,
              fileId: data.fileId
            }));
          }
          break;

        case 'transfer-cancel':
          if (data.targetId && peers.has(data.targetId)) {
            const target = peers.get(data.targetId);
            target.ws.send(JSON.stringify({
              type: 'transfer-canceled',
              peerId: peerId,
              fileId: data.fileId
            }));
          }
          break;

        case 'webrtc-signal':
          if (data.targetId && peers.has(data.targetId)) {
            const target = peers.get(data.targetId);
            target.ws.send(JSON.stringify({
              ...data,
              senderId: peerId,
              senderName: peerState.name
            }));
          }
          break;

        default:
          break;
      }
    } catch (err) {
      console.error('Error handling websocket message:', err);
    }
  });

  ws.on('close', () => {
    peers.delete(peerId);
    broadcastPeers();
  });
});

function broadcastPeers() {
  const peerList = Array.from(peers.values()).map(p => p.state);
  const payload = JSON.stringify({
    type: 'peers-update',
    peers: peerList
  });

  for (const client of peers.values()) {
    if (client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(payload);
    }
  }
}

server.listen(PORT, '0.0.0.0', () => {
  const ips = getLocalIPs();
  console.log('==================================================');
  console.log('🚀 KAWERIFY TRANSFER SERVER IS RUNNING OFFLINE');
  console.log(`📡 Port: ${PORT}`);
  console.log('📌 Connect to this server from any device on Wi-Fi:');
  ips.forEach(ip => {
    console.log(`   👉 http://${ip}:${PORT}`);
  });
  console.log('==================================================');
});

// Build version: 127
