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
  ArrowUpRight,
  Users,
  Hash
} from 'lucide-react';
import QRCode from 'qrcode';
import mqtt from 'mqtt';

interface Peer {
  id: string;
  name: string;
  avatar: number;
  deviceType?: string;
  roomCode?: string;
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

interface IncomingStreamState {
  chunks: Uint8Array[];
  writableStream?: any;
  receivedBytes: number;
  totalSize: number;
  fileName: string;
  fileType: string;
  startTime: number;
}

const ADJECTIVES = ['North', 'Cobalt', 'Amber', 'Cedar', 'Lunar', 'Sol', 'Vertex', 'Quartz', 'Metro', 'Atlas', 'Prime'];
const NOUNS = ['MacBook', 'ThinkPad', 'Workstation', 'Laptop', 'Studio', 'Desktop'];

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
  const noun = isMobile ? 'Phone' : NOUNS[Math.floor(Math.random() * NOUNS.length)];
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
  const [roomCode, setRoomCode] = useState<string>(() => {
    const params = new URLSearchParams(window.location.search);
    return (params.get('room') || 'GLOBAL').toUpperCase();
  });
  const [roomInput, setRoomInput] = useState<string>(() => {
    const params = new URLSearchParams(window.location.search);
    return (params.get('room') || 'GLOBAL').toUpperCase();
  });

  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [p2pConnected, setP2pConnected] = useState<boolean>(false);
  const [localPeers, setLocalPeers] = useState<Peer[]>([]);
  const [meshPeers, setMeshPeers] = useState<Map<string, Peer>>(new Map());
  const [selectedPeerIds, setSelectedPeerIds] = useState<string[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const [ips, setIps] = useState<string[]>([]);
  const [copiedUrl, setCopiedUrl] = useState<string>('');
  const [toast, setToast] = useState<string>('');
  const [qrCodeUrl, setQrCodeUrl] = useState<string>('');
  const [incomingQueue, setIncomingQueue] = useState<IncomingOffer[]>([]);
  const [isDragActive, setIsDragActive] = useState<boolean>(false);

  const ws = useRef<WebSocket | null>(null);
  const mqttClient = useRef<mqtt.MqttClient | null>(null);
  const broadcastChan = useRef<BroadcastChannel | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const activeXhrs = useRef<Map<string, XMLHttpRequest>>(new Map());
  const canceledTransfers = useRef<Set<string>>(new Set());
  const pendingFiles = useRef<Map<string, File>>(new Map());
  const incomingBuffers = useRef<Map<string, IncomingStreamState>>(new Map());
  const meRef = useRef<Peer>(me);
  const roomRef = useRef<string>(roomCode);

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('kawerify_theme', theme);
  }, [theme]);

  useEffect(() => {
    meRef.current = me;
    sessionStorage.setItem('kawerify_peer_identity', JSON.stringify(me));
  }, [me]);

  useEffect(() => {
    roomRef.current = roomCode;
  }, [roomCode]);

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

  const saveBlobToDeviceStorage = useCallback((blob: Blob, fileName: string) => {
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', fileName);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setTimeout(() => URL.revokeObjectURL(url), 15000);
  }, []);

  const peers: Peer[] = React.useMemo(() => {
    const map = new Map<string, Peer>();
    for (const p of meshPeers.values()) {
      if (p.id !== me.id && (!p.roomCode || p.roomCode === roomCode || roomCode === 'GLOBAL')) {
        map.set(p.id, p);
      }
    }
    for (const p of localPeers) {
      if (p.id !== me.id) {
        map.set(p.id, { ...p, transport: 'local-ws' });
      }
    }
    return Array.from(map.values());
  }, [localPeers, meshPeers, me.id, roomCode]);

  // Keep selectedPeerIds valid and auto-select first discovered peer if none selected
  useEffect(() => {
    setSelectedPeerIds(prev => {
      const valid = prev.filter(id => peers.some(p => p.id === id));
      if (valid.length === 0 && peers.length > 0) {
        return [peers[0].id];
      }
      return valid;
    });
  }, [peers]);

  useEffect(() => {
    const baseOrigin = window.location.origin;
    const shareUrlWithRoom = roomCode && roomCode !== 'GLOBAL'
      ? `${baseOrigin}/?room=${encodeURIComponent(roomCode)}`
      : baseOrigin;

    if (isLocalNetworkHost) {
      fetch(`${serverUrl}/api/ips`)
        .then(res => res.json())
        .then(data => {
          const validIps: string[] = data.ips || [];
          const urls = validIps.map((ip: string) => `http://${ip}:${data.port || 5000}`);
          setIps(urls.length > 0 ? urls : [shareUrlWithRoom]);
          const targetUrl = urls.length > 0 ? urls[0] : shareUrlWithRoom;
          QRCode.toDataURL(targetUrl, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
        })
        .catch(() => {
          setIps([shareUrlWithRoom]);
          QRCode.toDataURL(shareUrlWithRoom, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
        });
    } else {
      setIps([shareUrlWithRoom]);
      QRCode.toDataURL(shareUrlWithRoom, { margin: 1, width: 180 }).then(setQrCodeUrl).catch(() => {});
    }
  }, [isLocalNetworkHost, serverUrl, roomCode]);

  const sendP2PSignal = useCallback((targetId: string | 'broadcast', payload: Record<string, unknown>) => {
    const envelope = {
      ...payload,
      senderId: meRef.current.id,
      senderName: meRef.current.name,
      senderAvatar: meRef.current.avatar,
      senderDevice: meRef.current.deviceType,
      roomCode: roomRef.current,
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

  /**
   * 1TB+ Capable Zero-RAM File Streamer:
   * Uses lazy `file.slice(offset, end).arrayBuffer()` so only 64KB is ever loaded into memory at once.
   */
  const startP2PFileStream = useCallback(async (targetId: string, fileId: string) => {
    const file = pendingFiles.current.get(fileId);
    if (!file) return;

    const startTime = Date.now();
    const CHUNK_SIZE = 64 * 1024; // 64KB lazy disk slice
    const totalBytes = file.size;
    let offset = 0;

    const pumpNextChunk = async () => {
      if (canceledTransfers.current.has(fileId)) {
        canceledTransfers.current.delete(fileId);
        return;
      }

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
        showToast(`Delivered ${file.name}`);
        return;
      }

      const end = Math.min(offset + CHUNK_SIZE, totalBytes);
      try {
        // Read ONLY this 64KB slice from disk — keeps RAM flat (<1MB) even on a 1TB file
        const chunkBuffer = await file.slice(offset, end).arrayBuffer();
        const slice = new Uint8Array(chunkBuffer);

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
        const progress = totalBytes > 0 ? Math.min(100, Math.round((offset / totalBytes) * 100)) : 100;
        const speedMBps = (offset / elapsed) / (1024 * 1024);
        const remainingSec = speedMBps > 0 ? ((totalBytes - offset) / (1024 * 1024)) / speedMBps : 0;

        updateTransferStatus(fileId, 'uploading', {
          progress,
          speed: speedMBps.toFixed(2),
          eta: remainingSec < 60
            ? `${Math.ceil(remainingSec)}s left`
            : remainingSec < 3600
              ? `${Math.floor(remainingSec / 60)}m left`
              : `${(remainingSec / 3600).toFixed(1)}h left`
        });

        setTimeout(pumpNextChunk, 8);
      } catch {
        updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Read error' });
      }
    };

    pumpNextChunk();
  }, [sendP2PSignal, showToast, updateTransferStatus]);

  const handleP2PMessage = useCallback(async (data: Record<string, any>) => {
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
            roomCode: data.roomCode || 'GLOBAL',
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
        setIncomingQueue(prev => {
          if (prev.some(item => item.fileId === data.fileId)) return prev;
          return [
            ...prev,
            {
              senderId: data.senderId,
              senderName: data.senderName,
              fileId: data.fileId,
              fileName: data.fileName,
              fileSize: data.fileSize,
              fileType: data.fileType,
              transport: 'p2p-mesh'
            }
          ];
        });
        break;
      }

      case 'p2p-transfer-accept': {
        startP2PFileStream(data.senderId, data.fileId);
        break;
      }

      case 'p2p-transfer-cancel': {
        canceledTransfers.current.add(data.fileId);
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

        // If receiver opened a direct-to-disk FileSystemWritableFileStream (for huge / 1TB files), write straight to disk!
        if (entry.writableStream) {
          try {
            await entry.writableStream.write(bytes);
          } catch {
            // fallback if stream closed
          }
        } else {
          entry.chunks.push(bytes);
        }

        entry.receivedBytes += bytes.byteLength;

        const progress = entry.totalSize > 0
          ? Math.min(99, Math.round((entry.receivedBytes / entry.totalSize) * 100))
          : 99;
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
          if (entry.writableStream) {
            try {
              await entry.writableStream.close();
            } catch {
              // ignore
            }
          } else {
            const blob = new Blob(entry.chunks as BlobPart[], { type: fileType || 'application/octet-stream' });
            saveBlobToDeviceStorage(blob, fileName);
          }
          incomingBuffers.current.delete(fileId);
        }
        updateTransferStatus(fileId, 'completed', {
          progress: 100,
          speed: 'Done',
          eta: 'Saved to disk'
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
          socket.send(JSON.stringify({
            type: 'rename',
            name: meRef.current.name,
            deviceType: meRef.current.deviceType
          }));
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
                setIncomingQueue(prev => [
                  ...prev,
                  { ...data, transport: 'local-ws' }
                ]);
                break;
              case 'transfer-accepted':
                updateTransferStatus(`${data.fileId}-${data.receiverId}`, 'completed', {
                  progress: 100,
                  speed: 'Done',
                  eta: 'Completed'
                });
                updateTransferStatus(data.fileId, 'completed', {
                  progress: 100,
                  speed: 'Done',
                  eta: 'Completed'
                });
                showToast(`Accepted by ${data.receiverName || 'peer'}`);
                break;
              case 'transfer-canceled':
                updateTransferStatus(`${data.fileId}-${data.peerId}`, 'failed', {
                  speed: '0.00',
                  eta: 'Canceled'
                });
                updateTransferStatus(data.fileId, 'failed', {
                  speed: '0.00',
                  eta: 'Canceled'
                });
                showToast('Transfer declined by peer');
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
      ws.current.send(JSON.stringify({
        type: 'rename',
        name: trimmed,
        deviceType: me.deviceType
      }));
    }
    sendP2PSignal('broadcast', { type: 'presence', senderName: trimmed });
    showToast('Device name saved');
  };

  const handleJoinRoom = () => {
    const clean = (roomInput.trim() || 'GLOBAL').toUpperCase().substring(0, 16);
    setRoomInput(clean);
    setRoomCode(clean);
    roomRef.current = clean;
    sendP2PSignal('broadcast', { type: 'presence', roomCode: clean });
    sendP2PSignal('broadcast', { type: 'presence-request' });
    showToast(`Joined pairing room: ${clean}`);
  };

  const togglePeerSelection = (peerId: string) => {
    setSelectedPeerIds(prev =>
      prev.includes(peerId)
        ? prev.filter(id => id !== peerId)
        : [...prev, peerId]
    );
  };

  const toggleSelectAllPeers = () => {
    if (selectedPeerIds.length === peers.length) {
      setSelectedPeerIds([]);
    } else {
      setSelectedPeerIds(peers.map(p => p.id));
    }
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

  /**
   * Broadcast file simultaneously to 1, 5, 10, or more selected devices!
   */
  const handleSendFile = () => {
    if (!selectedFile || selectedPeerIds.length === 0) {
      showToast('Select a file and at least one destination device');
      return;
    }

    const file = selectedFile;
    const targetPeers = peers.filter(p => selectedPeerIds.includes(p.id));
    if (targetPeers.length === 0) return;

    const localTargetPeers = targetPeers.filter(p => wsConnected && p.transport === 'local-ws');
    const p2pTargetPeers = targetPeers.filter(p => !(wsConnected && p.transport === 'local-ws'));

    // 1. Handle Local Offline LAN Broadcast (1 upload -> streamed simultaneously to N local receivers)
    if (localTargetPeers.length > 0) {
      const batchId = `${Date.now()}-${Math.random().toString(36).substring(2, 7)}`;
      const peerNames = localTargetPeers.length === 1
        ? localTargetPeers[0].name
        : `${localTargetPeers.length} devices (${localTargetPeers.map(p => p.name).join(', ')})`;

      const newTransfer: Transfer = {
        id: batchId,
        name: file.name,
        size: file.size,
        progress: 0,
        speed: '0.00',
        status: 'uploading',
        role: 'sender',
        peerName: peerNames,
        eta: 'Streaming to LAN...'
      };

      setTransfers(prev => [newTransfer, ...prev]);

      const xhr = new XMLHttpRequest();
      activeXhrs.current.set(batchId, xhr);

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
            : remainingSeconds < 3600
              ? `${Math.floor(remainingSeconds / 60)}m left`
              : `${(remainingSeconds / 3600).toFixed(1)}h left`;

          updateTransferStatus(batchId, 'uploading', {
            progress,
            speed: speedMBps,
            eta: etaText
          });
        }
      };

      xhr.onload = () => {
        if (xhr.status === 200) {
          const response = JSON.parse(xhr.responseText);
          // Remap transfer ID to server's fileId
          setTransfers(prev => prev.map(t => t.id === batchId ? {
            ...t,
            id: response.fileId,
            progress: 100,
            status: 'waiting',
            speed: 'Done',
            eta: `Sent to ${localTargetPeers.length} device(s)`
          } : t));

          if (ws.current && ws.current.readyState === WebSocket.OPEN) {
            ws.current.send(JSON.stringify({
              type: 'transfer-init',
              targetIds: localTargetPeers.map(p => p.id),
              fileId: response.fileId,
              fileName: file.name,
              fileSize: file.size,
              fileType: file.type
            }));
          }
        } else {
          updateTransferStatus(batchId, 'failed', { speed: '0.00', eta: 'Failed' });
          showToast('Upload failed');
        }
        activeXhrs.current.delete(batchId);
      };

      xhr.onerror = () => {
        updateTransferStatus(batchId, 'failed', { speed: '0.00', eta: 'Network error' });
        activeXhrs.current.delete(batchId);
      };

      xhr.open('POST', `${serverUrl}/api/upload?targets=${localTargetPeers.length}`);
      xhr.send(formData);
    }

    // 2. Handle Direct P2P Multi-Device Broadcast (streams concurrently to 5+ devices with zero-RAM slicing)
    if (p2pTargetPeers.length > 0) {
      const newRows: Transfer[] = [];
      for (const peer of p2pTargetPeers) {
        const fileId = `${Date.now()}-${Math.random().toString(36).substring(2, 8)}-${peer.id.slice(-3)}`;
        pendingFiles.current.set(fileId, file);
        newRows.push({
          id: fileId,
          name: file.name,
          size: file.size,
          progress: 0,
          speed: 'P2P',
          status: 'waiting',
          role: 'sender',
          peerName: peer.name,
          eta: 'Awaiting acceptance'
        });

        sendP2PSignal(peer.id, {
          type: 'p2p-transfer-offer',
          fileId,
          fileName: file.name,
          fileSize: file.size,
          fileType: file.type || 'application/octet-stream'
        });
      }
      setTransfers(prev => [...newRows, ...prev]);
    }

    showToast(`Sharing "${file.name}" with ${targetPeers.length} device(s)`);
    setSelectedFile(null);
  };

  const incomingTransfer = incomingQueue[0] || null;

  const handleAcceptTransfer = async () => {
    if (!incomingTransfer) return;

    const transfer = incomingTransfer;
    setIncomingQueue(prev => prev.slice(1));

    const newTransfer: Transfer = {
      id: transfer.fileId,
      name: transfer.fileName,
      size: transfer.fileSize,
      progress: transfer.transport === 'local-ws' ? 50 : 1,
      speed: '--',
      status: 'downloading',
      role: 'receiver',
      peerName: transfer.senderName,
      eta: 'Streaming to disk...'
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
      // For huge files (or when File System Access API is available), offer direct-to-disk streaming so 1TB files use 0MB RAM
      let writableStream: any = undefined;
      if ('showSaveFilePicker' in window && transfer.fileSize > 100 * 1024 * 1024) {
        try {
          const handle = await (window as any).showSaveFilePicker({
            suggestedName: transfer.fileName
          });
          writableStream = await handle.createWritable();
        } catch {
          // fallback to standard download
        }
      }

      incomingBuffers.current.set(transfer.fileId, {
        chunks: [],
        writableStream,
        receivedBytes: 0,
        totalSize: transfer.fileSize,
        fileName: transfer.fileName,
        fileType: transfer.fileType,
        startTime: Date.now()
      });

      sendP2PSignal(transfer.senderId, {
        type: 'p2p-transfer-accept',
        fileId: transfer.fileId
      });
    }
  };

  const handleRejectTransfer = () => {
    if (!incomingTransfer) return;
    const transfer = incomingTransfer;
    setIncomingQueue(prev => prev.slice(1));

    if (transfer.transport === 'local-ws') {
      if (ws.current && ws.current.readyState === WebSocket.OPEN) {
        ws.current.send(JSON.stringify({
          type: 'transfer-cancel',
          targetId: transfer.senderId,
          fileId: transfer.fileId
        }));
      }
    } else {
      sendP2PSignal(transfer.senderId, {
        type: 'p2p-transfer-cancel',
        fileId: transfer.fileId
      });
    }
  };

  const cancelTransfer = (transfer: Transfer) => {
    canceledTransfers.current.add(transfer.id);
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
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
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
  const selectedPeersList = peers.filter(p => selectedPeerIds.includes(p.id));

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
              <span className="brand-byline">Multi-Device Local & P2P File Sharing</span>
            </div>
          </a>

          <div className="topbar-actions">
            <div className="status-pill">
              <span className={`status-dot ${isSystemOnline ? 'online' : ''}`} />
              <span>
                {wsConnected
                  ? 'Offline LAN Ready'
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

      {/* Main Responsive Workspace */}
      <main className="workspace">
        {/* Left Column: This Device & Multi-Device Radar */}
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
                    <div className="identity-label">Device Name</div>
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

              {/* Laptop-to-Laptop Direct Room Code (Zero QR Scanning Needed) */}
              <div className="room-bar">
                <div className="room-bar-header">
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem' }}>
                    <Hash size={13} /> Laptop-to-Laptop Room Code
                  </span>
                  <span className="mono" style={{ color: 'var(--accent-primary)' }}>
                    Active: {roomCode}
                  </span>
                </div>
                <div className="room-input-group">
                  <input
                    type="text"
                    className="room-input mono"
                    value={roomInput}
                    placeholder="GLOBAL or custom code"
                    onChange={(e) => setRoomInput(e.target.value.toUpperCase())}
                    onKeyDown={(e) => e.key === 'Enter' && handleJoinRoom()}
                  />
                  <button className="btn btn-secondary btn-sm" onClick={handleJoinRoom}>
                    Join Room
                  </button>
                </div>
                <div style={{ fontSize: '0.72rem', color: 'var(--text-tertiary)' }}>
                  Laptops with the same Room Code discover each other automatically without scanning.
                </div>
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
                          <Copy size={13} /> Copy Link
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
                      <h4>Phone or Tablet QR</h4>
                      <p>
                        Scan with a phone camera, or simply open the link on another laptop to connect without scanning.
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

              <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                {peers.length > 0 && (
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={toggleSelectAllPeers}
                  >
                    <Users size={13} />
                    {selectedPeerIds.length === peers.length ? 'Deselect All' : `Select All (${peers.length})`}
                  </button>
                )}
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => {
                    sendP2PSignal('broadcast', { type: 'presence' });
                    sendP2PSignal('broadcast', { type: 'presence-request' });
                    showToast('Scanning for nearby devices...');
                  }}
                  title="Refresh device list"
                >
                  <RefreshCw size={13} />
                </button>
              </div>
            </div>

            <div className="card-body">
              {peers.length > 0 ? (
                <>
                  <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginBottom: '0.65rem' }}>
                    Tap one or multiple devices (5+ supported) to broadcast the same file simultaneously:
                  </div>
                  <div className="peers-grid">
                    {peers.map((peer) => {
                      const isSelected = selectedPeerIds.includes(peer.id);
                      return (
                        <div
                          key={peer.id}
                          className={`peer-card ${isSelected ? 'selected' : ''}`}
                          onClick={() => togglePeerSelection(peer.id)}
                        >
                          <div className="peer-card-left">
                            <div className="peer-checkbox">
                              <Check size={12} strokeWidth={3} />
                            </div>
                            <div className="peer-icon-box">
                              {renderDeviceIcon(peer.deviceType, 18)}
                            </div>
                            <div style={{ minWidth: 0 }}>
                              <div className="peer-card-name">{peer.name}</div>
                              <div className="peer-card-meta">
                                <span>{peer.deviceType || 'Laptop / Desktop'}</span>
                                <span>•</span>
                                <span>{peer.transport === 'local-ws' ? 'Offline LAN' : 'Direct P2P'}</span>
                              </div>
                            </div>
                          </div>
                          <span className="transport-tag">
                            {isSelected ? 'Selected' : 'Select'}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </>
              ) : (
                <div className="empty-radar">
                  <div className="radar-circles">
                    <Radio size={20} />
                  </div>
                  <div style={{ fontSize: '0.88rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                    Searching for nearby devices
                  </div>
                  <p style={{ fontSize: '0.78rem', color: 'var(--text-secondary)', maxWidth: '270px' }}>
                    Open this app on other laptops or phones (using the same Room Code or Wi-Fi) to share to 1 or 5+ devices at once.
                  </p>
                </div>
              )}
            </div>
          </section>
        </div>

        {/* Right Column: File Selection & Multi-Stream Transfers */}
        <div className="column">
          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">Send File</span>
              </div>
              <span className="card-subtitle mono">Supports 1TB+ Zero-RAM Stream</span>
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
                    Stream any file size (from documents to 1TB+ video archives) directly from device storage to multiple devices at once
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
                        {formatBytes(selectedFile.size)} • Ready for chunked disk stream
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

              {/* Selected Recipients Multi-Device Bar */}
              <div className="recipients-bar">
                <div className="recipients-header">
                  <span>
                    Target Recipients ({selectedPeersList.length} selected)
                  </span>
                  {peers.length > 1 && (
                    <button
                      className="btn btn-secondary btn-sm"
                      style={{ padding: '0.2rem 0.5rem', fontSize: '0.72rem' }}
                      onClick={toggleSelectAllPeers}
                    >
                      {selectedPeerIds.length === peers.length ? 'Clear' : `Select All (${peers.length})`}
                    </button>
                  )}
                </div>

                {selectedPeersList.length > 0 ? (
                  <div className="recipient-chips">
                    {selectedPeersList.map(peer => (
                      <span key={peer.id} className="recipient-chip">
                        {renderDeviceIcon(peer.deviceType, 12)}
                        <span>{peer.name}</span>
                        <button
                          onClick={() => togglePeerSelection(peer.id)}
                          title={`Remove ${peer.name}`}
                        >
                          <X size={12} />
                        </button>
                      </span>
                    ))}
                  </div>
                ) : (
                  <div style={{ fontSize: '0.78rem', color: 'var(--text-tertiary)' }}>
                    Click one or more devices under Nearby Devices to select recipients.
                  </div>
                )}
              </div>

              <div className="send-bar">
                <div style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
                  {selectedPeersList.length > 1
                    ? `Simultaneous broadcast to ${selectedPeersList.length} devices`
                    : selectedPeersList.length === 1
                      ? `Direct stream to ${selectedPeersList[0].name}`
                      : 'Waiting for recipient selection'}
                </div>

                <div className="send-actions">
                  {selectedFile && (
                    <button className="btn btn-secondary" onClick={triggerFileInput}>
                      Change File
                    </button>
                  )}
                  <button
                    className="btn btn-primary"
                    onClick={handleSendFile}
                    disabled={!selectedFile || selectedPeersList.length === 0}
                  >
                    <Send size={15} />
                    {selectedPeersList.length > 1
                      ? `Share to ${selectedPeersList.length} Devices`
                      : 'Send File'}
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
                            style={{ width: 28, height: 28 }}
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
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '0.6rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                <div className="device-badge-icon" style={{ width: 34, height: 34 }}>
                  <Download size={18} />
                </div>
                <div>
                  <div style={{ fontSize: '0.95rem', fontWeight: 600 }}>Incoming File</div>
                  <div className="card-subtitle">From {incomingTransfer.senderName}</div>
                </div>
              </div>
              {incomingQueue.length > 1 && (
                <span className="transport-tag mono">+{incomingQueue.length - 1} more</span>
              )}
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
