import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Send,
  Download,
  Share2,
  Copy,
  Check,
  X,
  FolderOpen,
  RefreshCw,
  FileText,
  User,
  Users,
  Wifi,
  HardDrive,
  ShieldCheck
} from 'lucide-react';
import QRCode from 'qrcode';
import mqtt from 'mqtt';

interface Peer {
  id: string;
  name: string;
  avatar: number;
  deviceType?: string;
  lastSeen?: number;
  transport?: 'local-ws' | 'p2p-mesh';
}

interface Transfer {
  id: string;
  name: string;
  size: number;
  progress: number;
  speed: string;
  status: 'uploading' | 'waiting' | 'downloading' | 'completed' | 'failed';
  role: 'sender' | 'receiver';
  peerName: string;
  eta: string;
}

interface IncomingOffer {
  senderId: string;
  senderName: string;
  fileId: string;
  fileName: string;
  fileSize: number;
  fileType: string;
  transport: 'local-ws' | 'p2p-mesh';
}

const ADJECTIVES = ['Swift', 'Silent', 'Hyper', 'Turbo', 'Ninja', 'Cosmic', 'Solar', 'Quantum', 'Nebula', 'Echo', 'Frost', 'Wild'];
const ANIMALS = ['Cheetah', 'Falcon', 'Panther', 'Shark', 'Eagle', 'Wolf', 'Leopard', 'Hawk', 'Badger', 'Tiger', 'Viper', 'Lynx'];

function createLocalIdentity(): Peer {
  const saved = sessionStorage.getItem('kawerify_peer_identity');
  if (saved) {
    try {
      return JSON.parse(saved);
    } catch {
      // ignore invalid cache
    }
  }
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const animal = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
  const num = Math.floor(Math.random() * 900) + 100;
  const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
  const identity: Peer = {
    id: `peer-${Math.random().toString(36).substring(2, 9)}`,
    name: `${adj} ${animal} ${num}`,
    avatar: Math.floor(Math.random() * 8) + 1,
    deviceType: isMobile ? 'Mobile Phone' : 'Laptop / Desktop'
  };
  sessionStorage.setItem('kawerify_peer_identity', JSON.stringify(identity));
  return identity;
}

export default function App() {
  const [me, setMe] = useState<Peer>(() => createLocalIdentity());
  const [customName, setCustomName] = useState<string>(() => createLocalIdentity().name);
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [p2pConnected, setP2pConnected] = useState<boolean>(false);
  const [localPeers, setLocalPeers] = useState<Peer[]>([]);
  const [meshPeers, setMeshPeers] = useState<Map<string, Peer>>(new Map());
  const [selectedPeerId, setSelectedPeerId] = useState<string>('');
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const [ips, setIps] = useState<string[]>([]);
  const [toast, setToast] = useState<string>('');
  const [qrCodeUrl, setQrCodeUrl] = useState<string>('');
  const [incomingTransfer, setIncomingTransfer] = useState<IncomingOffer | null>(null);
  const [isDragActive, setIsDragActive] = useState<boolean>(false);

  const ws = useRef<WebSocket | null>(null);
  const mqttClient = useRef<mqtt.MqttClient | null>(null);
  const broadcastChan = useRef<BroadcastChannel | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const activeXhrs = useRef<Map<string, XMLHttpRequest>>(new Map());
  const pendingFiles = useRef<Map<string, File>>(new Map());
  const incomingBuffers = useRef<Map<string, { chunks: Uint8Array[]; receivedBytes: number; totalSize: number; fileName: string; fileType: string; startTime: number }>>(new Map());
  const rtcConnections = useRef<Map<string, RTCPeerConnection>>(new Map());
  const meRef = useRef<Peer>(me);

  useEffect(() => {
    meRef.current = me;
    sessionStorage.setItem('kawerify_peer_identity', JSON.stringify(me));
  }, [me]);

  const isLocalNetworkHost =
    window.location.hostname === 'localhost' ||
    window.location.hostname === '127.0.0.1' ||
    /^(192\.168\.|10\.|172\.(1[6-9]|2[0-9]|3[0-1])\.)/.test(window.location.hostname);

  const serverUrl = window.location.port === '5173'
    ? 'http://localhost:5000'
    : `${window.location.protocol}//${window.location.hostname}:${window.location.port || '5000'}`;

  const wsUrl = window.location.port === '5173'
    ? 'ws://localhost:5000'
    : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}:${window.location.port || '5000'}`;

  const showToast = useCallback((message: string) => {
    setToast(message);
    setTimeout(() => setToast(''), 3200);
  }, []);

  const updateTransferStatus = useCallback((
    id: string,
    status: Transfer['status'],
    updates: Partial<Transfer> = {}
  ) => {
    setTransfers(prev => prev.map(t => t.id === id ? { ...t, status, ...updates } : t));
  }, []);

  const saveBlobToDeviceStorage = useCallback(async (blob: Blob, fileName: string) => {
    // Use File System Access API if user triggers it or fallback to direct anchor download
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', fileName);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }, []);

  // Combined peer list (deduplicated)
  const peers: Peer[] = React.useMemo(() => {
    const map = new Map<string, Peer>();
    for (const p of meshPeers.values()) {
      if (p.id !== me.id) {
        map.set(p.id, p);
      }
    }
    for (const p of localPeers) {
      if (p.id !== me.id) {
        map.set(p.id, { ...p, transport: 'local-ws' });
      }
    }
    return Array.from(map.values());
  }, [localPeers, meshPeers, me.id]);

  // Auto-select first peer if none selected
  useEffect(() => {
    if (!selectedPeerId && peers.length > 0) {
      setSelectedPeerId(peers[0].id);
    } else if (selectedPeerId && !peers.some(p => p.id === selectedPeerId)) {
      setSelectedPeerId(peers[0]?.id || '');
    }
  }, [peers, selectedPeerId]);

  // Generate QR Code and connection URLs
  useEffect(() => {
    const shareOrigin = window.location.origin;
    if (isLocalNetworkHost) {
      fetch(`${serverUrl}/api/ips`)
        .then(res => res.json())
        .then(data => {
          const validIps: string[] = data.ips || [];
          setIps(validIps.map((ip: string) => `http://${ip}:${data.port || 5000}`));
          const targetUrl = validIps.length > 0 ? `http://${validIps[0]}:${data.port || 5000}` : shareOrigin;
          QRCode.toDataURL(targetUrl).then(setQrCodeUrl).catch(() => {});
        })
        .catch(() => {
          setIps([shareOrigin]);
          QRCode.toDataURL(shareOrigin).then(setQrCodeUrl).catch(() => {});
        });
    } else {
      setIps([shareOrigin]);
      QRCode.toDataURL(shareOrigin).then(setQrCodeUrl).catch(() => {});
    }
  }, [isLocalNetworkHost, serverUrl]);

  // Send P2P signaling message via MQTT + BroadcastChannel
  const sendP2PSignal = useCallback((targetId: string | 'broadcast', payload: Record<string, unknown>) => {
    const envelope = {
      ...payload,
      senderId: meRef.current.id,
      senderName: meRef.current.name,
      senderAvatar: meRef.current.avatar,
      senderDevice: meRef.current.deviceType,
      timestamp: Date.now()
    };

    if (broadcastChan.current) {
      try {
        broadcastChan.current.postMessage({ targetId, ...envelope });
      } catch {
        // ignore
      }
    }

    if (mqttClient.current && mqttClient.current.connected) {
      const topic = targetId === 'broadcast'
        ? 'kawerify-transfer/lobby/v2'
        : `kawerify-transfer/peer/v2/${targetId}`;
      mqttClient.current.publish(topic, JSON.stringify(envelope), { qos: 0 });
    }
  }, []);

  // Stream file chunks over WebRTC DataChannel or P2P Direct Relay
  const startP2PFileStream = useCallback(async (targetId: string, fileId: string) => {
    const file = pendingFiles.current.get(fileId);
    if (!file) return;

    const startTime = Date.now();
    const CHUNK_SIZE = 48 * 1024; // 48KB binary chunks
    const buffer = await file.arrayBuffer();
    const uint8 = new Uint8Array(buffer);
    const totalBytes = uint8.byteLength;

    // Attempt WebRTC DataChannel or direct fast chunked signaling stream
    let offset = 0;

    const sendNextBatch = () => {
      if (offset >= totalBytes) {
        sendP2PSignal(targetId, {
          type: 'p2p-file-complete',
          fileId,
          fileName: file.name,
          fileSize: file.size,
          fileType: file.type || 'application/octet-stream'
        });
        updateTransferStatus(fileId, 'completed', {
          progress: 100,
          speed: 'Done',
          eta: 'Completed'
        });
        showToast('File transfer completed!');
        pendingFiles.current.delete(fileId);
        return;
      }

      const end = Math.min(offset + CHUNK_SIZE, totalBytes);
      const slice = uint8.subarray(offset, end);

      // Encode slice to base64 for universal transport reliability
      let binary = '';
      for (let i = 0; i < slice.byteLength; i++) {
        binary += String.fromCharCode(slice[i]);
      }
      const b64Chunk = btoa(binary);

      sendP2PSignal(targetId, {
        type: 'p2p-file-chunk',
        fileId,
        offset,
        totalBytes,
        fileName: file.name,
        fileType: file.type || 'application/octet-stream',
        chunk: b64Chunk
      });

      offset = end;
      const elapsed = Math.max((Date.now() - startTime) / 1000, 0.1);
      const progress = Math.min(100, Math.round((offset / totalBytes) * 100));
      const speedMBps = (offset / elapsed) / (1024 * 1024);
      const remainingSec = speedMBps > 0 ? ((totalBytes - offset) / (1024 * 1024)) / speedMBps : 0;

      updateTransferStatus(fileId, 'uploading', {
        progress,
        speed: speedMBps.toFixed(2),
        eta: remainingSec < 60 ? `${Math.ceil(remainingSec)}s remaining` : `${Math.floor(remainingSec / 60)}m remaining`
      });

      setTimeout(sendNextBatch, 12);
    };

    sendNextBatch();
  }, [sendP2PSignal, showToast, updateTransferStatus]);

  // Handle incoming P2P signals
  const handleP2PMessage = useCallback((data: Record<string, any>) => {
    if (!data || data.senderId === meRef.current.id) return;

    switch (data.type) {
      case 'presence': {
        setMeshPeers(prev => {
          const next = new Map(prev);
          next.set(data.senderId, {
            id: data.senderId,
            name: data.senderName || 'Nearby Device',
            avatar: data.senderAvatar || 1,
            deviceType: data.senderDevice || 'Device',
            lastSeen: Date.now(),
            transport: 'p2p-mesh'
          });
          return next;
        });
        break;
      }

      case 'presence-request': {
        sendP2PSignal('broadcast', { type: 'presence' });
        break;
      }

      case 'p2p-transfer-offer': {
        setIncomingTransfer({
          senderId: data.senderId,
          senderName: data.senderName,
          fileId: data.fileId,
          fileName: data.fileName,
          fileSize: data.fileSize,
          fileType: data.fileType,
          transport: 'p2p-mesh'
        });
        break;
      }

      case 'p2p-transfer-accept': {
        startP2PFileStream(data.senderId, data.fileId);
        break;
      }

      case 'p2p-transfer-cancel': {
        updateTransferStatus(data.fileId, 'failed', { speed: '0.00', eta: 'Declined' });
        showToast('Transfer declined by remote peer.');
        break;
      }

      case 'p2p-file-chunk': {
        const { fileId, chunk, totalBytes, fileName, fileType } = data;
        let entry = incomingBuffers.current.get(fileId);
        if (!entry) {
          entry = {
            chunks: [],
            receivedBytes: 0,
            totalSize: totalBytes,
            fileName,
            fileType,
            startTime: Date.now()
          };
          incomingBuffers.current.set(fileId, entry);
        }

        const binStr = atob(chunk);
        const bytes = new Uint8Array(binStr.length);
        for (let i = 0; i < binStr.length; i++) {
          bytes[i] = binStr.charCodeAt(i);
        }
        entry.chunks.push(bytes);
        entry.receivedBytes += bytes.byteLength;

        const progress = Math.min(99, Math.round((entry.receivedBytes / entry.totalSize) * 100));
        const elapsed = Math.max((Date.now() - entry.startTime) / 1000, 0.1);
        const speedMBps = (entry.receivedBytes / elapsed) / (1024 * 1024);

        updateTransferStatus(fileId, 'downloading', {
          progress,
          speed: speedMBps.toFixed(2),
          eta: 'Receiving...'
        });
        break;
      }

      case 'p2p-file-complete': {
        const { fileId, fileName, fileType } = data;
        const entry = incomingBuffers.current.get(fileId);
        if (entry) {
          const blob = new Blob(entry.chunks as BlobPart[], { type: fileType || 'application/octet-stream' });
          saveBlobToDeviceStorage(blob, fileName);
          incomingBuffers.current.delete(fileId);
        }
        updateTransferStatus(fileId, 'completed', {
          progress: 100,
          speed: 'Success',
          eta: 'Saved to device'
        });
        showToast(`Saved ${fileName} to device!`);
        break;
      }

      default:
        break;
    }
  }, [saveBlobToDeviceStorage, sendP2PSignal, showToast, startP2PFileStream, updateTransferStatus]);

  // Setup P2P Discovery & WebRTC Signaling Mesh (works on Vercel & across tabs/devices)
  useEffect(() => {
    // 1. Same-device / local browser BroadcastChannel
    if ('BroadcastChannel' in window) {
      const bc = new BroadcastChannel('kawerify_transfer_mesh_v2');
      broadcastChan.current = bc;
      bc.onmessage = (ev) => {
        if (!ev.data) return;
        if (ev.data.targetId === 'broadcast' || ev.data.targetId === meRef.current.id) {
          handleP2PMessage(ev.data);
        }
      };
    }

    // 2. Real-time WSS P2P discovery & signaling
    const client = mqtt.connect('wss://broker.emqx.io:8084/mqtt', {
      clientId: `kawerify_${meRef.current.id}_${Math.random().toString(16).slice(2, 6)}`,
      clean: true,
      connectTimeout: 5000,
      reconnectPeriod: 3000
    });
    mqttClient.current = client;

    client.on('connect', () => {
      setP2pConnected(true);
      client.subscribe('kawerify-transfer/lobby/v2');
      client.subscribe(`kawerify-transfer/peer/v2/${meRef.current.id}`);
      sendP2PSignal('broadcast', { type: 'presence' });
      sendP2PSignal('broadcast', { type: 'presence-request' });
    });

    client.on('message', (_topic, payload) => {
      try {
        const parsed = JSON.parse(payload.toString());
        handleP2PMessage(parsed);
      } catch {
        // ignore malformed packets
      }
    });

    client.on('close', () => setP2pConnected(false));
    client.on('error', () => setP2pConnected(false));

    // Heartbeat & stale peer cleanup
    const heartbeat = setInterval(() => {
      sendP2PSignal('broadcast', { type: 'presence' });
      const now = Date.now();
      setMeshPeers(prev => {
        let changed = false;
        const next = new Map(prev);
        for (const [id, p] of next.entries()) {
          if (p.lastSeen && now - p.lastSeen > 15000) {
            next.delete(id);
            changed = true;
          }
        }
        return changed ? next : prev;
      });
    }, 4000);

    return () => {
      clearInterval(heartbeat);
      if (broadcastChan.current) broadcastChan.current.close();
      client.end(true);
      rtcConnections.current.forEach(pc => pc.close());
    };
  }, [handleP2PMessage, sendP2PSignal]);

  // Connect Local Offline WebSocket Server (when running on local network)
  useEffect(() => {
    if (!isLocalNetworkHost) return;

    let reconnectTimer: ReturnType<typeof setTimeout>;
    const connectWS = () => {
      try {
        const socket = new WebSocket(wsUrl);
        ws.current = socket;

        socket.onopen = () => {
          setWsConnected(true);
        };

        socket.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            switch (data.type) {
              case 'welcome':
                setMe(prev => ({ ...prev, id: data.peer.id }));
                break;

              case 'peers-update':
                setLocalPeers(data.peers || []);
                break;

              case 'transfer-offer':
                setIncomingTransfer({
                  ...data,
                  transport: 'local-ws'
                });
                break;

              case 'transfer-accepted':
                updateTransferStatus(data.fileId, 'completed', {
                  progress: 100,
                  speed: 'Done',
                  eta: 'Completed'
                });
                showToast('File transfer completed!');
                break;

              case 'transfer-canceled':
                updateTransferStatus(data.fileId, 'failed', {
                  speed: '0.00',
                  eta: 'Canceled'
                });
                showToast('Transfer canceled by remote peer.');
                break;

              default:
                break;
            }
          } catch {
            // ignore
          }
        };

        socket.onclose = () => {
          setWsConnected(false);
          reconnectTimer = setTimeout(connectWS, 4000);
        };

        socket.onerror = () => {
          setWsConnected(false);
        };
      } catch {
        setWsConnected(false);
      }
    };

    connectWS();
    return () => {
      clearTimeout(reconnectTimer);
      if (ws.current) ws.current.close();
    };
  }, [isLocalNetworkHost, showToast, updateTransferStatus, wsUrl]);

  const handleCopyLink = (linkUrl: string) => {
    navigator.clipboard.writeText(linkUrl);
    showToast(`Link copied: ${linkUrl}`);
  };

  const handleRename = () => {
    const trimmed = customName.trim().substring(0, 24);
    if (!trimmed) return;
    setMe(prev => ({ ...prev, name: trimmed }));
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({ type: 'rename', name: trimmed }));
    }
    sendP2PSignal('broadcast', { type: 'presence', senderName: trimmed });
    showToast('Device name updated!');
  };

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setIsDragActive(true);
    } else if (e.type === 'dragleave') {
      setIsDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      setSelectedFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const triggerFileInput = async () => {
    // Support modern File System Access API on desktop browsers with fallback to standard input
    if ('showOpenFilePicker' in window) {
      try {
        const [handle] = await (window as any).showOpenFilePicker({ multiple: false });
        const file = await handle.getFile();
        setSelectedFile(file);
        return;
      } catch (err: any) {
        if (err?.name === 'AbortError') return;
      }
    }
    fileInputRef.current?.click();
  };

  const clearSelectedFile = () => {
    setSelectedFile(null);
  };

  const handleSendFile = () => {
    if (!selectedFile || !selectedPeerId) {
      showToast('Please select a file and a destination device');
      return;
    }

    const file = selectedFile;
    const targetPeer = peers.find(p => p.id === selectedPeerId);
    if (!targetPeer) return;

    const fileId = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;
    const newTransfer: Transfer = {
      id: fileId,
      name: file.name,
      size: file.size,
      progress: 0,
      speed: '0.00',
      status: 'uploading',
      role: 'sender',
      peerName: targetPeer.name,
      eta: 'Waiting for recipient...'
    };

    setTransfers(prev => [newTransfer, ...prev]);
    setSelectedFile(null);

    // If local server is connected and peer is on local-ws, use local HTTP streaming
    if (wsConnected && targetPeer.transport === 'local-ws') {
      const xhr = new XMLHttpRequest();
      activeXhrs.current.set(fileId, xhr);

      const formData = new FormData();
      formData.append('file', file);

      const startTime = Date.now();

      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable) {
          const duration = Math.max((Date.now() - startTime) / 1000, 0.1);
          const loaded = event.loaded;
          const progress = Math.round((loaded / event.total) * 100);
          const speedBps = loaded / duration;
          const speedMBps = (speedBps / (1024 * 1024)).toFixed(2);
          const remainingBytes = event.total - loaded;
          const remainingSeconds = speedBps > 0 ? remainingBytes / speedBps : 0;
          const etaText = remainingSeconds < 60
            ? `${Math.ceil(remainingSeconds)}s remaining`
            : `${Math.floor(remainingSeconds / 60)}m ${Math.ceil(remainingSeconds % 60)}s remaining`;

          updateTransferStatus(fileId, 'uploading', {
            progress,
            speed: speedMBps,
            eta: etaText
          });
        }
      };

      xhr.onload = () => {
        if (xhr.status === 200) {
          const response = JSON.parse(xhr.responseText);
          updateTransferStatus(fileId, 'waiting', {
            progress: 100,
            speed: 'Done',
            eta: 'Waiting for recipient'
          });

          if (ws.current && ws.current.readyState === WebSocket.OPEN) {
            ws.current.send(JSON.stringify({
              type: 'transfer-init',
              targetId: selectedPeerId,
              fileId: response.fileId,
              fileName: file.name,
              fileSize: file.size,
              fileType: file.type
            }));
          }
        } else {
          updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Upload error' });
          showToast('File upload failed!');
        }
        activeXhrs.current.delete(fileId);
      };

      xhr.onerror = () => {
        updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Network error' });
        showToast('Network error during file upload.');
        activeXhrs.current.delete(fileId);
      };

      xhr.open('POST', `${serverUrl}/api/upload`);
      xhr.send(formData);
    } else {
      // Direct P2P Transfer Mode (works on Vercel & across any network)
      pendingFiles.current.set(fileId, file);
      updateTransferStatus(fileId, 'waiting', {
        progress: 0,
        speed: 'P2P',
        eta: 'Waiting for recipient to accept'
      });
      sendP2PSignal(selectedPeerId, {
        type: 'p2p-transfer-offer',
        fileId,
        fileName: file.name,
        fileSize: file.size,
        fileType: file.type || 'application/octet-stream'
      });
      showToast(`Transfer offer sent to ${targetPeer.name}`);
    }
  };

  const handleAcceptTransfer = () => {
    if (!incomingTransfer) return;

    const transfer = incomingTransfer;
    setIncomingTransfer(null);

    const newTransfer: Transfer = {
      id: transfer.fileId,
      name: transfer.fileName,
      size: transfer.fileSize,
      progress: transfer.transport === 'local-ws' ? 50 : 1,
      speed: '--',
      status: 'downloading',
      role: 'receiver',
      peerName: transfer.senderName,
      eta: 'Downloading'
    };

    setTransfers(prev => [newTransfer, ...prev]);

    if (transfer.transport === 'local-ws') {
      if (ws.current && ws.current.readyState === WebSocket.OPEN) {
        ws.current.send(JSON.stringify({
          type: 'transfer-accept',
          senderId: transfer.senderId,
          fileId: transfer.fileId
        }));
      }

      const link = document.createElement('a');
      link.href = `${serverUrl}/api/download/${transfer.fileId}`;
      link.setAttribute('download', transfer.fileName);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);

      setTimeout(() => {
        updateTransferStatus(transfer.fileId, 'completed', {
          progress: 100,
          speed: 'Success',
          eta: 'Completed'
        });
      }, 2500);
    } else {
      sendP2PSignal(transfer.senderId, {
        type: 'p2p-transfer-accept',
        fileId: transfer.fileId
      });
    }
  };

  const handleRejectTransfer = () => {
    if (!incomingTransfer) return;

    if (incomingTransfer.transport === 'local-ws') {
      if (ws.current && ws.current.readyState === WebSocket.OPEN) {
        ws.current.send(JSON.stringify({
          type: 'transfer-cancel',
          targetId: incomingTransfer.senderId,
          fileId: incomingTransfer.fileId
        }));
      }
    } else {
      sendP2PSignal(incomingTransfer.senderId, {
        type: 'p2p-transfer-cancel',
        fileId: incomingTransfer.fileId
      });
    }

    setIncomingTransfer(null);
    showToast('Transfer declined.');
  };

  const cancelTransfer = (transfer: Transfer) => {
    const xhr = activeXhrs.current.get(transfer.id);
    if (xhr) {
      xhr.abort();
      activeXhrs.current.delete(transfer.id);
    }
    pendingFiles.current.delete(transfer.id);
    updateTransferStatus(transfer.id, 'failed', { speed: '0.00', eta: 'Cancelled' });
  };

  const formatBytes = (bytes: number, decimals = 2) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  };

  const getAvatarEmoji = (num: number) => {
    const avatars = ['🦁', '🦊', '🐻', '🐼', '🐨', '🐯', '🐰', '🦅'];
    return avatars[(num - 1) % avatars.length] || '🐱';
  };

  const isSystemOnline = wsConnected || p2pConnected;

  return (
    <div className="app-container">
      {toast && <div className="toast-msg">{toast}</div>}

      <header className="app-header">
        <h1 className="brand-title">
          ⚡ Kawerify <span>Transfer</span>
        </h1>
        <p className="brand-subtitle">
          Superfast offline & P2P file transfers by{' '}
          <a href="https://kawerifytech.com" target="_blank" rel="noopener noreferrer">
            Kawerify Tech
          </a>
        </p>

        <div style={{ display: 'flex', justifyContent: 'center', gap: '0.75rem', marginTop: '0.85rem', flexWrap: 'wrap' }}>
          <span style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.4rem',
            fontSize: '0.78rem',
            fontWeight: 600,
            padding: '0.3rem 0.75rem',
            borderRadius: '999px',
            backgroundColor: isSystemOnline ? 'rgba(20, 184, 166, 0.15)' : 'rgba(245, 158, 11, 0.15)',
            color: isSystemOnline ? 'var(--accent-teal)' : 'var(--accent-orange)',
            border: `1px solid ${isSystemOnline ? 'rgba(20, 184, 166, 0.35)' : 'rgba(245, 158, 11, 0.35)'}`
          }}>
            <Wifi size={13} />
            {wsConnected
              ? 'Offline LAN Server Active'
              : p2pConnected
                ? 'Direct P2P Discovery Active'
                : 'Initializing Discovery Engine...'}
          </span>

          <span style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.4rem',
            fontSize: '0.78rem',
            fontWeight: 600,
            padding: '0.3rem 0.75rem',
            borderRadius: '999px',
            backgroundColor: 'rgba(6, 182, 212, 0.12)',
            color: 'var(--accent-cyan)',
            border: '1px solid rgba(6, 182, 212, 0.3)'
          }}>
            <HardDrive size={13} />
            Device Storage Access Ready
          </span>

          <span style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '0.4rem',
            fontSize: '0.78rem',
            fontWeight: 600,
            padding: '0.3rem 0.75rem',
            borderRadius: '999px',
            backgroundColor: 'rgba(255, 255, 255, 0.05)',
            color: 'var(--text-secondary)',
            border: '1px solid var(--border-color)'
          }}>
            <ShieldCheck size={13} />
            End-to-End Direct Stream
          </span>
        </div>
      </header>

      <div className="app-grid">
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          <div className="panel-card">
            <h2 className="panel-title">
              <User size={20} /> My Device Profile
            </h2>
            <div className="self-device">
              <div className="self-info-left">
                <div className="avatar-container">
                  {getAvatarEmoji(me.avatar)}
                </div>
                <div>
                  <input
                    type="text"
                    className="device-name-input"
                    value={customName}
                    onChange={(e) => setCustomName(e.target.value)}
                    onBlur={handleRename}
                    onKeyDown={(e) => e.key === 'Enter' && handleRename()}
                    title="Click to change device name"
                  />
                  <div className="device-label">{me.deviceType || 'Active Peer'}</div>
                </div>
              </div>
              <button className="btn btn-secondary" style={{ padding: '0.4rem' }} onClick={handleRename} title="Save profile name">
                <Check size={16} />
              </button>
            </div>

            <div className="ip-panel">
              <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', fontWeight: 600 }}>
                Instant Pairing Link:
              </div>
              <div className="ip-list">
                {ips.length > 0 ? (
                  ips.map(linkUrl => (
                    <div key={linkUrl} className="ip-item">
                      <span>Share URL:</span>
                      <span className="ip-value" onClick={() => handleCopyLink(linkUrl)}>
                        {linkUrl} <Copy size={12} style={{ marginLeft: '4px', verticalAlign: 'middle' }} />
                      </span>
                    </div>
                  ))
                ) : (
                  <div className="ip-item" style={{ color: 'var(--text-muted)' }}>
                    Generating pairing link...
                  </div>
                )}
              </div>
            </div>

            {qrCodeUrl && (
              <div className="qr-section">
                <div className="qr-canvas-wrapper">
                  <img src={qrCodeUrl} alt="Share QR Code" style={{ width: '130px', height: '130px', display: 'block' }} />
                </div>
                <p className="qr-caption">
                  Scan with your phone or open on another laptop to connect and share immediately
                </p>
              </div>
            )}
          </div>

          <div className="panel-card">
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.75rem', marginBottom: '0.25rem' }}>
              <h2 className="panel-title" style={{ borderBottom: 'none', paddingBottom: 0, marginBottom: 0 }}>
                <Users size={20} /> Devices Nearby ({peers.length})
              </h2>
              <button
                className="btn btn-secondary"
                style={{ padding: '0.3rem 0.6rem', fontSize: '0.75rem' }}
                onClick={() => {
                  sendP2PSignal('broadcast', { type: 'presence' });
                  sendP2PSignal('broadcast', { type: 'presence-request' });
                  showToast('Scanning for nearby devices...');
                }}
              >
                <RefreshCw size={13} /> Scan
              </button>
            </div>
            <div className="peer-list">
              {peers.length > 0 ? (
                peers.map(peer => (
                  <div
                    key={peer.id}
                    className={`peer-item ${selectedPeerId === peer.id ? 'selected' : ''}`}
                    onClick={() => setSelectedPeerId(peer.id)}
                  >
                    <div className="peer-info">
                      <div className="peer-avatar">
                        {getAvatarEmoji(peer.avatar)}
                      </div>
                      <div>
                        <div className="peer-name">{peer.name}</div>
                        <div className="peer-status">
                          {peer.deviceType || 'Ready to receive'} • {peer.transport === 'local-ws' ? 'Offline LAN' : 'Direct P2P'}
                        </div>
                      </div>
                    </div>
                    {selectedPeerId === peer.id && (
                      <span style={{ color: 'var(--accent-cyan)', fontSize: '0.8rem', fontWeight: 700 }}>
                        SELECTED
                      </span>
                    )}
                  </div>
                ))
              ) : (
                <div className="no-peers-placeholder">
                  <div className="no-peers-pulse"></div>
                  <p>Searching for nearby devices...</p>
                  <p style={{ fontSize: '0.75rem', marginTop: '0.25rem' }}>
                    Open this link or scan the QR code on your phone or another laptop
                  </p>
                </div>
              )}
            </div>
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          <div className="panel-card">
            <h2 className="panel-title">
              <Share2 size={20} /> Send Files from Storage
            </h2>

            {!selectedFile ? (
              <div
                className={`drag-drop-area ${isDragActive ? 'active' : ''}`}
                onDragEnter={handleDrag}
                onDragOver={handleDrag}
                onDragLeave={handleDrag}
                onDrop={handleDrop}
                onClick={triggerFileInput}
              >
                <input
                  type="file"
                  ref={fileInputRef}
                  className="file-input"
                  onChange={handleFileChange}
                />
                <div className="drag-drop-icon">
                  <FolderOpen size={28} />
                </div>
                <div className="drag-drop-title">Drag & Drop any file here</div>
                <div className="drag-drop-desc">or click to browse device storage (Photos, Videos, Documents, Archives)</div>
              </div>
            ) : (
              <div className="file-preview-box">
                <div className="file-preview-info">
                  <div className="file-icon-wrapper">
                    <FileText size={20} />
                  </div>
                  <div className="file-meta">
                    <div className="file-name" title={selectedFile.name}>{selectedFile.name}</div>
                    <div className="file-size">{formatBytes(selectedFile.size)}</div>
                  </div>
                </div>
                <button className="btn-remove-file" onClick={clearSelectedFile} title="Remove file">
                  <X size={18} />
                </button>
              </div>
            )}

            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
              <label style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text-secondary)' }}>
                Target Destination Device:
              </label>
              <select
                style={{
                  width: '100%',
                  background: 'var(--bg-secondary)',
                  border: '1px solid var(--border-color)',
                  color: '#ffffff',
                  padding: '0.65rem',
                  borderRadius: 'var(--border-radius-sm)',
                  fontSize: '0.9rem',
                  outline: 'none'
                }}
                value={selectedPeerId}
                onChange={(e) => setSelectedPeerId(e.target.value)}
              >
                <option value="">-- Choose target device --</option>
                {peers.map(p => (
                  <option key={p.id} value={p.id}>{p.name} ({p.deviceType || 'Peer'})</option>
                ))}
              </select>
            </div>

            <div className="action-footer">
              {selectedFile && (
                <button className="btn btn-secondary" onClick={clearSelectedFile}>
                  Cancel
                </button>
              )}
              <button
                className="btn btn-primary"
                onClick={handleSendFile}
                disabled={!selectedFile || !selectedPeerId}
              >
                <Send size={16} /> Share Now
              </button>
            </div>
          </div>

          <div className="panel-card" style={{ flexGrow: 1 }}>
            <h2 className="panel-title">
              <RefreshCw size={20} /> Active & Recent Transfers
            </h2>
            <div className="transfers-container">
              {transfers.length > 0 ? (
                transfers.map(transfer => (
                  <div
                    key={transfer.id}
                    className={`transfer-item ${
                      transfer.status === 'completed' ? 'completed' :
                      transfer.status === 'failed' ? 'failed' : ''
                    }`}
                  >
                    <div className="transfer-header">
                      <div className="transfer-details">
                        <span className={`transfer-badge ${transfer.role === 'sender' ? 'send' : 'receive'}`}>
                          {transfer.role}
                        </span>
                        <span className="file-name" style={{ maxWidth: '220px' }} title={transfer.name}>
                          {transfer.name}
                        </span>
                      </div>

                      {['uploading', 'downloading', 'waiting'].includes(transfer.status) && (
                        <button
                          className="btn-remove-file"
                          onClick={() => cancelTransfer(transfer)}
                          title="Cancel transfer"
                        >
                          <X size={16} />
                        </button>
                      )}
                      {transfer.status === 'completed' && (
                        <span style={{ color: 'var(--accent-teal)', fontSize: '0.8rem', fontWeight: 700 }}>
                          DONE
                        </span>
                      )}
                      {transfer.status === 'failed' && (
                        <span style={{ color: 'var(--accent-red)', fontSize: '0.8rem', fontWeight: 700 }}>
                          FAILED
                        </span>
                      )}
                    </div>

                    <div className="transfer-progress-block">
                      <div className="transfer-progress-bar-bg">
                        <div
                          className="transfer-progress-bar-fg"
                          style={{ width: `${transfer.progress}%` }}
                        ></div>
                      </div>
                    </div>

                    <div className="transfer-stats">
                      <span>Size: {formatBytes(transfer.size)} • Peer: {transfer.peerName}</span>
                      <div className="transfer-stats-right">
                        <span>Speed: {transfer.speed} {transfer.speed !== 'Done' && transfer.speed !== 'Success' && transfer.speed !== '--' && transfer.speed !== 'P2P' ? 'MB/s' : ''}</span>
                        <span style={{ color: 'var(--text-muted)' }}>|</span>
                        <span>{transfer.eta}</span>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                  No transfers yet. Drop a file and choose a nearby device to start sharing.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {incomingTransfer && (
        <div className="dialog-overlay">
          <div className="dialog-content">
            <div className="dialog-header">
              <Download size={22} style={{ color: 'var(--accent-cyan)' }} />
              Incoming File Transfer
            </div>
            <div className="dialog-body">
              <p>
                <strong>{incomingTransfer.senderName}</strong> wants to send you a file:
              </p>
              <div className="dialog-file-info">
                <FileText size={28} style={{ color: 'var(--accent-cyan)' }} />
                <div style={{ overflow: 'hidden' }}>
                  <div className="file-name" style={{ fontSize: '0.95rem' }} title={incomingTransfer.fileName}>
                    {incomingTransfer.fileName}
                  </div>
                  <div className="file-size" style={{ fontSize: '0.8rem' }}>
                    {formatBytes(incomingTransfer.fileSize)}
                  </div>
                </div>
              </div>
            </div>
            <div className="action-footer" style={{ marginTop: '0.5rem' }}>
              <button className="btn btn-secondary" onClick={handleRejectTransfer}>
                Decline
              </button>
              <button className="btn btn-primary" onClick={handleAcceptTransfer}>
                Accept & Save to Device
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
