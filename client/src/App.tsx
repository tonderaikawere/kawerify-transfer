import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Send,
  Download,
  Copy,
  Check,
  X,
  FolderOpen,
  RefreshCw,
  FileText,
  Laptop,
  Smartphone,
  Radio,
  Sun,
  Moon,
  ArrowUpRight
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

const ADJECTIVES = ['North', 'Cobalt', 'Amber', 'Cedar', 'Lunar', 'Sol', 'Vertex', 'studio', 'Quartz', 'Metro', 'Atlas', 'Prime'];
const NOUNS = ['MacBook', 'ThinkPad', 'Workstation', 'Phone', 'Studio', 'Node', 'Terminal', 'Surface', 'Pixel', 'Device'];

function createLocalIdentity(): Peer {
  const saved = sessionStorage.getItem('kawerify_peer_identity');
  if (saved) {
    try {
      return JSON.parse(saved);
    } catch {
      // ignore invalid cache
    }
  }
  const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const noun = isMobile ? 'Mobile' : NOUNS[Math.floor(Math.random() * 3)];
  const num = Math.floor(Math.random() * 90) + 10;
  const identity: Peer = {
    id: `peer-${Math.random().toString(36).substring(2, 9)}`,
    name: `${adj} ${noun}-${num}`,
    avatar: isMobile ? 2 : 1,
    deviceType: isMobile ? 'Mobile Phone' : 'Laptop / Desktop'
  };
  sessionStorage.setItem('kawerify_peer_identity', JSON.stringify(identity));
  return identity;
}

export default function App() {
  const [theme, setTheme] = useState<'dark' | 'light'>(() => {
    const saved = localStorage.getItem('kawerify_theme');
    return saved === 'light' ? 'light' : 'dark';
  });
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
  const [copiedUrl, setCopiedUrl] = useState<string>('');
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
  const meRef = useRef<Peer>(me);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('kawerify_theme', theme);
  }, [theme]);

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
    setTimeout(() => setToast(''), 3000);
  }, []);

  const updateTransferStatus = useCallback((
    id: string,
    status: Transfer['status'],
    updates: Partial<Transfer> = {}
  ) => {
    setTransfers(prev => prev.map(t => t.id === id ? { ...t, status, ...updates } : t));
  }, []);

  const saveBlobToDeviceStorage = useCallback(async (blob: Blob, fileName: string) => {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', fileName);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setTimeout(() => URL.revokeObjectURL(url), 10000);
  }, []);

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

  useEffect(() => {
    if (!selectedPeerId && peers.length > 0) {
      setSelectedPeerId(peers[0].id);
    } else if (selectedPeerId && !peers.some(p => p.id === selectedPeerId)) {
      setSelectedPeerId(peers[0]?.id || '');
    }
  }, [peers, selectedPeerId]);

  useEffect(() => {
    const shareOrigin = window.location.origin;
    if (isLocalNetworkHost) {
      fetch(`${serverUrl}/api/ips`)
        .then(res => res.json())
        .then(data => {
          const validIps: string[] = data.ips || [];
          setIps(validIps.map((ip: string) => `http://${ip}:${data.port || 5000}`));
          const targetUrl = validIps.length > 0 ? `http://${validIps[0]}:${data.port || 5000}` : shareOrigin;
          QRCode.toDataURL(targetUrl, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
        })
        .catch(() => {
          setIps([shareOrigin]);
          QRCode.toDataURL(shareOrigin, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
        });
    } else {
      setIps([shareOrigin]);
      QRCode.toDataURL(shareOrigin, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
    }
  }, [isLocalNetworkHost, serverUrl]);

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

  const startP2PFileStream = useCallback(async (targetId: string, fileId: string) => {
    const file = pendingFiles.current.get(fileId);
    if (!file) return;

    const startTime = Date.now();
    const CHUNK_SIZE = 48 * 1024;
    const buffer = await file.arrayBuffer();
    const uint8 = new Uint8Array(buffer);
    const totalBytes = uint8.byteLength;
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
        showToast('Transfer completed');
        pendingFiles.current.delete(fileId);
        return;
      }

      const end = Math.min(offset + CHUNK_SIZE, totalBytes);
      const slice = uint8.subarray(offset, end);

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
        eta: remainingSec < 60 ? `${Math.ceil(remainingSec)}s left` : `${Math.floor(remainingSec / 60)}m left`
      });

      setTimeout(sendNextBatch, 12);
    };

    sendNextBatch();
  }, [sendP2PSignal, showToast, updateTransferStatus]);

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
            deviceType: data.senderDevice || 'Laptop / Desktop',
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
        showToast('Transfer declined by peer');
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
          speed: 'Done',
          eta: 'Saved'
        });
        showToast(`Saved ${fileName}`);
        break;
      }

      default:
        break;
    }
  }, [saveBlobToDeviceStorage, sendP2PSignal, showToast, startP2PFileStream, updateTransferStatus]);

  useEffect(() => {
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
    };
  }, [handleP2PMessage, sendP2PSignal]);

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
                setIncomingTransfer({ ...data, transport: 'local-ws' });
                break;
              case 'transfer-accepted':
                updateTransferStatus(data.fileId, 'completed', {
                  progress: 100,
                  speed: 'Done',
                  eta: 'Completed'
                });
                showToast('Transfer completed');
                break;
              case 'transfer-canceled':
                updateTransferStatus(data.fileId, 'failed', {
                  speed: '0.00',
                  eta: 'Canceled'
                });
                showToast('Transfer canceled');
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
    setCopiedUrl(linkUrl);
    setTimeout(() => setCopiedUrl(''), 2000);
  };

  const handleRename = () => {
    const trimmed = customName.trim().substring(0, 24);
    if (!trimmed || trimmed === me.name) return;
    setMe(prev => ({ ...prev, name: trimmed }));
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({ type: 'rename', name: trimmed }));
    }
    sendP2PSignal('broadcast', { type: 'presence', senderName: trimmed });
    showToast('Device name saved');
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

  const handleSendFile = () => {
    if (!selectedFile || !selectedPeerId) {
      showToast('Select a file and a target device first');
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
      eta: 'Connecting...'
    };

    setTransfers(prev => [newTransfer, ...prev]);
    setSelectedFile(null);

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
            ? `${Math.ceil(remainingSeconds)}s left`
            : `${Math.floor(remainingSeconds / 60)}m left`;

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
            eta: 'Awaiting acceptance'
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
          updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Failed' });
          showToast('Upload failed');
        }
        activeXhrs.current.delete(fileId);
      };

      xhr.onerror = () => {
        updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Network error' });
        activeXhrs.current.delete(fileId);
      };

      xhr.open('POST', `${serverUrl}/api/upload`);
      xhr.send(formData);
    } else {
      pendingFiles.current.set(fileId, file);
      updateTransferStatus(fileId, 'waiting', {
        progress: 0,
        speed: 'P2P',
        eta: 'Awaiting acceptance'
      });
      sendP2PSignal(selectedPeerId, {
        type: 'p2p-transfer-offer',
        fileId,
        fileName: file.name,
        fileSize: file.size,
        fileType: file.type || 'application/octet-stream'
      });
      showToast(`Sent request to ${targetPeer.name}`);
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
          speed: 'Done',
          eta: 'Saved'
        });
      }, 2000);
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

  const formatBytes = (bytes: number, decimals = 1) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  };

  const getFileExtension = (name: string) => {
    const parts = name.split('.');
    if (parts.length > 1) {
      return parts[parts.length - 1].substring(0, 4).toUpperCase();
    }
    return 'FILE';
  };

  const renderDeviceIcon = (deviceType?: string, size = 18) => {
    if (deviceType && /mobile|phone|android|iphone/i.test(deviceType)) {
      return <Smartphone size={size} />;
    }
    return <Laptop size={size} />;
  };

  const isSystemOnline = wsConnected || p2pConnected;
  const activeTargetPeer = peers.find(p => p.id === selectedPeerId);

  return (
    <div className="shell">
      {toast && <div className="toast">{toast}</div>}

      {/* Top Navigation */}
      <header className="topbar">
        <div className="topbar-inner">
          <a href="/" className="brand">
            <div className="brand-mark">K</div>
            <div className="brand-text">
              <span className="brand-name">Kawerify Transfer</span>
              <span className="brand-byline">Direct Local & P2P File Sharing</span>
            </div>
          </a>

          <div className="topbar-actions">
            <div className="status-pill">
              <span className={`status-dot ${isSystemOnline ? 'online' : ''}`} />
              <span>
                {wsConnected
                  ? 'Local Network Ready'
                  : p2pConnected
                    ? 'Direct P2P Ready'
                    : 'Connecting...'}
              </span>
            </div>

            <button
              className="icon-btn"
              onClick={() => setTheme(prev => (prev === 'dark' ? 'light' : 'dark'))}
              title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
              aria-label="Toggle color theme"
            >
              {theme === 'dark' ? <Sun size={16} /> : <Moon size={16} />}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="workspace">
        {/* Left Column: This Device & Nearby Peers */}
        <div className="column">
          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">This Device</span>
              </div>
              <span className="card-subtitle mono">{me.id}</span>
            </div>

            <div className="card-body">
              <div className="identity-box">
                <div className="identity-left">
                  <div className="device-badge-icon">
                    {renderDeviceIcon(me.deviceType, 20)}
                  </div>
                  <div className="identity-meta">
                    <div className="identity-label">Visible Name</div>
                    <div className="identity-input-row">
                      <input
                        type="text"
                        className="identity-input"
                        value={customName}
                        onChange={(e) => setCustomName(e.target.value)}
                        onBlur={handleRename}
                        onKeyDown={(e) => e.key === 'Enter' && handleRename()}
                        aria-label="Device name"
                      />
                    </div>
                  </div>
                </div>
                {customName.trim() !== me.name && (
                  <button className="btn btn-secondary btn-sm" onClick={handleRename}>
                    Save
                  </button>
                )}
              </div>

              <div className="pairing-section">
                {ips.map((linkUrl) => (
                  <div key={linkUrl} className="url-copy-box">
                    <span className="url-text mono">{linkUrl}</span>
                    <button
                      className="btn btn-secondary btn-sm"
                      onClick={() => handleCopyLink(linkUrl)}
                    >
                      {copiedUrl === linkUrl ? (
                        <>
                          <Check size={13} /> Copied
                        </>
                      ) : (
                        <>
                          <Copy size={13} /> Copy
                        </>
                      )}
                    </button>
                  </div>
                ))}

                {qrCodeUrl && (
                  <div className="qr-panel">
                    <div className="qr-frame">
                      <img src={qrCodeUrl} alt="Pair via QR code" />
                    </div>
                    <div className="qr-info">
                      <h4>Pair another device</h4>
                      <p>
                        Scan this code with your phone camera or open the URL above on another computer to connect immediately.
                      </p>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </section>

          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">Nearby Devices</span>
                <span className="transport-tag mono">{peers.length}</span>
              </div>
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => {
                  sendP2PSignal('broadcast', { type: 'presence' });
                  sendP2PSignal('broadcast', { type: 'presence-request' });
                  showToast('Refreshing nearby devices...');
                }}
              >
                <RefreshCw size={13} /> Refresh
              </button>
            </div>

            <div className="card-body">
              {peers.length > 0 ? (
                <div className="peers-grid">
                  {peers.map((peer) => {
                    const isSelected = selectedPeerId === peer.id;
                    return (
                      <div
                        key={peer.id}
                        className={`peer-card ${isSelected ? 'selected' : ''}`}
                        onClick={() => setSelectedPeerId(peer.id)}
                      >
                        <div className="peer-card-left">
                          <div className="peer-icon-box">
                            {renderDeviceIcon(peer.deviceType, 18)}
                          </div>
                          <div style={{ minWidth: 0 }}>
                            <div className="peer-card-name">{peer.name}</div>
                            <div className="peer-card-meta">
                              <span>{peer.deviceType || 'Computer'}</span>
                              <span>•</span>
                              <span>{peer.transport === 'local-ws' ? 'LAN' : 'Direct P2P'}</span>
                            </div>
                          </div>
                        </div>
                        <span className="transport-tag">
                          {isSelected ? 'Target' : 'Select'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <div className="empty-radar">
                  <div className="radar-circles">
                    <Radio size={20} />
                  </div>
                  <div style={{ fontSize: '0.88rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                    Waiting for other devices
                  </div>
                  <p style={{ fontSize: '0.78rem', color: 'var(--text-secondary)', maxWidth: '250px' }}>
                    Open this page on your phone or another laptop to appear here automatically.
                  </p>
                </div>
              )}
            </div>
          </section>
        </div>

        {/* Right Column: File Selection & Transfers */}
        <div className="column">
          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">Send File</span>
              </div>
              <span className="card-subtitle">No file size limits</span>
            </div>

            <div className="card-body">
              <input
                type="file"
                ref={fileInputRef}
                style={{ display: 'none' }}
                onChange={handleFileChange}
              />

              {!selectedFile ? (
                <div
                  className={`dropzone ${isDragActive ? 'active' : ''}`}
                  onDragEnter={handleDrag}
                  onDragOver={handleDrag}
                  onDragLeave={handleDrag}
                  onDrop={handleDrop}
                  onClick={triggerFileInput}
                >
                  <div className="dropzone-icon">
                    <FolderOpen size={20} />
                  </div>
                  <div className="dropzone-title">Choose a file or drag it here</div>
                  <div className="dropzone-hint">
                    Supports photos, videos, archives, and documents directly from device storage
                  </div>
                </div>
              ) : (
                <div className="selected-file-card">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', minWidth: 0 }}>
                    <div className="file-ext-badge mono">
                      {getFileExtension(selectedFile.name)}
                    </div>
                    <div style={{ minWidth: 0 }}>
                      <div className="transfer-filename" title={selectedFile.name}>
                        {selectedFile.name}
                      </div>
                      <div className="card-subtitle mono" style={{ marginTop: '0.1rem' }}>
                        {formatBytes(selectedFile.size)}
                      </div>
                    </div>
                  </div>
                  <button
                    className="icon-btn"
                    onClick={() => setSelectedFile(null)}
                    title="Remove file"
                  >
                    <X size={15} />
                  </button>
                </div>
              )}

              <div className="send-bar">
                <div className="target-indicator">
                  {activeTargetPeer ? (
                    <>
                      Sending to <strong>{activeTargetPeer.name}</strong>
                    </>
                  ) : (
                    <span>Select a recipient under Nearby Devices</span>
                  )}
                </div>

                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  {selectedFile && (
                    <button className="btn btn-secondary" onClick={triggerFileInput}>
                      Change File
                    </button>
                  )}
                  <button
                    className="btn btn-primary"
                    onClick={handleSendFile}
                    disabled={!selectedFile || !selectedPeerId}
                  >
                    <Send size={15} /> Send File
                  </button>
                </div>
              </div>
            </div>
          </section>

          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">Transfers</span>
                {transfers.length > 0 && (
                  <span className="transport-tag mono">{transfers.length}</span>
                )}
              </div>
              {transfers.some(t => t.status === 'completed' || t.status === 'failed') && (
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => setTransfers(prev => prev.filter(t => t.status !== 'completed' && t.status !== 'failed'))}
                >
                  Clear finished
                </button>
              )}
            </div>

            <div className="card-body">
              {transfers.length > 0 ? (
                <div className="transfer-list">
                  {transfers.map((transfer) => (
                    <div
                      key={transfer.id}
                      className={`transfer-row ${
                        transfer.status === 'completed' ? 'completed' :
                        transfer.status === 'failed' ? 'failed' : ''
                      }`}
                    >
                      <div className="transfer-row-top">
                        <div className="transfer-title-wrap">
                          <span className={`role-pill ${transfer.role}`}>
                            {transfer.role === 'sender' ? 'Out' : 'In'}
                          </span>
                          <span className="transfer-filename" title={transfer.name}>
                            {transfer.name}
                          </span>
                        </div>

                        {['uploading', 'downloading', 'waiting'].includes(transfer.status) ? (
                          <button
                            className="icon-btn"
                            style={{ width: 26, height: 26 }}
                            onClick={() => cancelTransfer(transfer)}
                            title="Cancel transfer"
                          >
                            <X size={14} />
                          </button>
                        ) : (
                          <span className="mono" style={{
                            fontSize: '0.73rem',
                            fontWeight: 600,
                            color: transfer.status === 'completed' ? 'var(--status-online)' : 'var(--status-danger)'
                          }}>
                            {transfer.status === 'completed' ? 'Completed' : 'Canceled'}
                          </span>
                        )}
                      </div>

                      <div className="progress-track">
                        <div
                          className="progress-fill"
                          style={{ width: `${transfer.progress}%` }}
                        />
                      </div>

                      <div className="transfer-meta-row mono">
                        <span>{formatBytes(transfer.size)} • {transfer.peerName}</span>
                        <span>
                          {transfer.speed !== 'Done' && transfer.speed !== '--' && transfer.speed !== 'P2P'
                            ? `${transfer.speed} MB/s • `
                            : ''}
                          {transfer.eta}
                        </span>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-tertiary)', fontSize: '0.84rem' }}>
                  Active and completed file transfers will appear here.
                </div>
              )}
            </div>
          </section>
        </div>
      </main>

      {/* Incoming Transfer Modal */}
      {incomingTransfer && (
        <div className="modal-backdrop">
          <div className="modal-card">
            <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
              <div className="device-badge-icon" style={{ width: 34, height: 34 }}>
                <Download size={18} />
              </div>
              <div>
                <div style={{ fontSize: '0.95rem', fontWeight: 600 }}>Incoming File</div>
                <div className="card-subtitle">From {incomingTransfer.senderName}</div>
              </div>
            </div>

            <div className="selected-file-card">
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', minWidth: 0 }}>
                <div className="file-ext-badge mono">
                  <FileText size={16} />
                </div>
                <div style={{ minWidth: 0 }}>
                  <div className="transfer-filename" title={incomingTransfer.fileName}>
                    {incomingTransfer.fileName}
                  </div>
                  <div className="card-subtitle mono">
                    {formatBytes(incomingTransfer.fileSize)}
                  </div>
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem' }}>
              <button className="btn btn-secondary" onClick={handleRejectTransfer}>
                Decline
              </button>
              <button className="btn btn-primary" onClick={handleAcceptTransfer}>
                Accept & Save
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Footer */}
      <footer className="footer">
        Built by{' '}
        <a href="https://kawerifytech.com" target="_blank" rel="noopener noreferrer">
          Kawerify Tech <ArrowUpRight size={12} style={{ verticalAlign: 'text-top' }} />
        </a>{' '}
        • Free & Open Source under 8 Licenses
      </footer>
    </div>
  );
}
