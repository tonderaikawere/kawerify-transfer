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

// Enable CORS for frontend development
app.use(cors());
app.use(express.json());

// Ensure uploads directory exists
const UPLOADS_DIR = path.join(__dirname, 'uploads');
if (!fs.existsSync(UPLOADS_DIR)) {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
}

// Config Multer for local disk storage
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, UPLOADS_DIR);
  },
  filename: (req, file, cb) => {
    // Save file with a safe timestamp prefix + original name to avoid collision
    const fileId = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    cb(null, `${fileId}-${file.originalname}`);
  }
});

const upload = multer({
  storage,
  limits: { fileSize: 100 * 1024 * 1024 * 1024 } // 100 GB limit
});

// Map to track active file metadata: fileId -> { name, size, type, path }
const activeTransfers = new Map();

// Helper to get local IPv4 addresses
function getLocalIPs() {
  const nets = networkInterfaces();
  const addresses = [];
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        addresses.push(net.address);
      }
    }
  }
  return addresses;
}

// Endpoint to list local IPs
app.get('/api/ips', (req, res) => {
  res.json({ ips: getLocalIPs(), port: PORT });
});

// File upload endpoint
app.post('/api/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  // extract actual fileId from saved filename
  const filename = req.file.filename;
  const fileId = filename.substring(0, filename.indexOf('-', filename.indexOf('-') + 1));

  const fileData = {
    id: fileId,
    name: req.file.originalname,
    size: req.file.size,
    type: req.file.mimetype || 'application/octet-stream',
    path: req.file.path
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

// File download endpoint with auto-cleanup after transfer
app.get('/api/download/:fileId', (req, res) => {
  const { fileId } = req.params;
  const fileData = activeTransfers.get(fileId);

  if (!fileData) {
    return res.status(404).send('File not found or transfer expired');
  }

  if (!fs.existsSync(fileData.path)) {
    activeTransfers.delete(fileId);
    return res.status(404).send('File does not exist on disk');
  }

  res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(fileData.name)}"`);
  res.setHeader('Content-Type', fileData.type);
  res.setHeader('Content-Length', fileData.size);

  const fileStream = fs.createReadStream(fileData.path);
  fileStream.pipe(res);

  // Clean up disk and map once transfer is complete or interrupted
  res.on('finish', () => {
    setTimeout(() => {
      try {
        if (fs.existsSync(fileData.path)) {
          fs.unlinkSync(fileData.path);
        }
      } catch (err) {
        console.error(`Error deleting file ${fileData.name}:`, err);
      }
      activeTransfers.delete(fileId);
    }, 5000); // 5 second buffer to make sure client received bytes
  });

  res.on('close', () => {
    // If connection was closed early, don't delete yet unless fully finished
    // Res will finish if complete. If connection interrupted, we keep it temporarily
  });
});

// Serve frontend build output
app.use(express.static(path.join(__dirname, 'client', 'dist')));

// SPA route fallback
app.get('*', (req, res, next) => {
  const indexHtmlPath = path.join(__dirname, 'client', 'dist', 'index.html');
  if (fs.existsSync(indexHtmlPath)) {
    res.sendFile(indexHtmlPath);
  } else {
    res.status(200).send('Kawerify Transfer Server is running. Frontend is not built yet.');
  }
});

// Setup WebSocket server
const wss = new WebSocketServer({ server });

// Map of peer ID -> client state
const peers = new Map();

// Random Name Generator
const ADJECTIVES = ['Swift', 'Silent', 'Hyper', 'Turbo', 'Ninja', 'Cosmic', 'Solar', 'Quantum', 'Nebula', 'Echo', 'Frost', 'Wild'];
const ANIMALS = ['Cheetah', 'Falcon', 'Panther', 'Shark', 'Eagle', 'Wolf', 'Leopard', 'Hawk', 'Badger', 'Tiger', 'Viper', 'Lynx'];

function generateRandomName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const animal = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
  const num = Math.floor(Math.random() * 900) + 100;
  return `${adj} ${animal} ${num}`;
}

wss.on('connection', (ws) => {
  const peerId = `peer-${Math.random().toString(36).substring(2, 9)}`;
  const peerName = generateRandomName();
  const avatarSeed = Math.floor(Math.random() * 8) + 1; // 8 avatar variations

  const peerState = {
    id: peerId,
    name: peerName,
    avatar: avatarSeed,
    joinedAt: Date.now()
  };

  peers.set(peerId, { ws, state: peerState });

  // Welcome message with local peer details
  ws.send(JSON.stringify({
    type: 'welcome',
    peer: peerState
  }));

  // Broadcast peer updates to everyone
  broadcastPeers();

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      switch (data.type) {
        case 'rename':
          if (data.name && data.name.trim().length > 0) {
            const current = peers.get(peerId);
            if (current) {
              current.state.name = data.name.trim().substring(0, 20);
              broadcastPeers();
            }
          }
          break;

        case 'transfer-init':
          // Forward file transfer offer to target peer
          if (data.targetId && peers.has(data.targetId)) {
            const target = peers.get(data.targetId);
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
          break;

        case 'transfer-accept':
          // Forward accept to sender
          if (data.senderId && peers.has(data.senderId)) {
            const sender = peers.get(data.senderId);
            sender.ws.send(JSON.stringify({
              type: 'transfer-accepted',
              receiverId: peerId,
              fileId: data.fileId
            }));
          }
          break;

        case 'transfer-cancel':
          // Forward cancel to peer
          if (data.targetId && peers.has(data.targetId)) {
            const target = peers.get(data.targetId);
            target.ws.send(JSON.stringify({
              type: 'transfer-canceled',
              peerId: peerId,
              fileId: data.fileId
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

// Start Server
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

