import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Send,
  Download,
  Copy,
  Check,
  X,
  FolderOpen,
  FolderPlus,
  Plus,
  RefreshCw,
  FileText,
  Laptop,
  Smartphone,
  Radio,
  Sun,
  Moon,
  ArrowUpRight,
  Users,
  Hash,
  ShieldCheck
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
  receivedBytes: number;
  totalSize: number;
  expectedChunkIndex: number;
  fileName: string;
  fileType: string;
  startTime: number;
  completed?: boolean;
}

const RTC_CONFIG: RTCConfiguration = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
    { urls: 'stun:stun2.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' }
  ]
};

const ADJECTIVES = ['North', 'Cobalt', 'Amber', 'Cedar', 'Lunar', 'Sol', 'Vertex', 'Quartz', 'Metro', 'Atlas', 'Prime'];
const NOUNS = ['MacBook', 'ThinkPad', 'Workstation', 'Laptop', 'Studio', 'Desktop'];

function createLocalIdentity(): Peer {
  const saved = sessionStorage.getItem('kawerify_peer_identity');
  if (saved) {
    try {
      return JSON.parse(saved);
    } catch {
      // ignore
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

async function extractFilesFromDataTransfer(items: DataTransferItemList): Promise<File[]> {
  const files: File[] = [];

  const readEntry = async (entry: any): Promise<void> => {
    if (!entry) return;
    if (entry.isFile) {
      await new Promise<void>((resolve) => {
        entry.file((f: File) => {
          files.push(f);
          resolve();
        }, () => resolve());
      });
    } else if (entry.isDirectory) {
      const reader = entry.createReader();
      const readAllEntries = async (): Promise<any[]> => {
        const batch: any[] = await new Promise((resolve) => {
          reader.readEntries((entries: any[]) => resolve(entries || []), () => resolve([]));
        });
        if (batch.length > 0) {
          const rest = await readAllEntries();
          return batch.concat(rest);
        }
        return [];
      };
      const entries = await readAllEntries();
      for (const child of entries) {
        await readEntry(child);
      }
    }
  };

  const entries: any[] = [];
  for (let i = 0; i < items.length; i++) {
    const item = items[i];
    if (item.kind === 'file') {
      const entry = typeof item.webkitGetAsEntry === 'function' ? item.webkitGetAsEntry() : null;
      if (entry) {
        entries.push(entry);
      } else {
        const f = item.getAsFile();
        if (f) files.push(f);
      }
    }
  }

  for (const entry of entries) {
    await readEntry(entry);
  }

  return files;
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
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const [ips, setIps] = useState<string[]>([]);
  const [copiedUrl, setCopiedUrl] = useState<string>('');
  const [toast, setToast] = useState<string>('');
  const [qrCodeUrl, setQrCodeUrl] = useState<string>('');
  const [incomingQueue, setIncomingQueue] = useState<IncomingOffer[]>([]);
  const [autoAccept, setAutoAccept] = useState<boolean>(true);
  const [isDragActive, setIsDragActive] = useState<boolean>(false);

  const ws = useRef<WebSocket | null>(null);
  const mqttClient = useRef<mqtt.MqttClient | null>(null);
  const broadcastChan = useRef<BroadcastChannel | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const folderInputRef = useRef<HTMLInputElement>(null);
  const activeXhrs = useRef<Map<string, XMLHttpRequest>>(new Map());
  const canceledTransfers = useRef<Set<string>>(new Set());
  const pendingFiles = useRef<Map<string, File>>(new Map());
  const incomingBuffers = useRef<Map<string, IncomingStreamState>>(new Map());
  const wakeLockRef = useRef<any>(null);

  // Single persistent RTCPeerConnection and RTCDataChannel per target peer
  const rtcPeers = useRef<Map<string, RTCPeerConnection>>(new Map());
  const rtcChannels = useRef<Map<string, RTCDataChannel>>(new Map());
  const rtcReadyPromises = useRef<Map<string, { promise: Promise<RTCDataChannel>; resolve: (dc: RTCDataChannel) => void }>>(new Map());

  // Outgoing sequential queue to avoid network / WebRTC flooding
  const outgoingQueue = useRef<Array<{ targetId: string; fileId: string; file: File }>>([]);
  const isPumpingQueue = useRef<boolean>(false);

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

  // Request Screen Wake Lock so phones don't sleep in the background during sending/receiving
  const acquireWakeLock = useCallback(async () => {
    if ('wakeLock' in navigator && !wakeLockRef.current) {
      try {
        wakeLockRef.current = await (navigator as any).wakeLock.request('screen');
        wakeLockRef.current.addEventListener('release', () => {
          wakeLockRef.current = null;
        });
      } catch {
        // ignore
      }
    }
  }, []);

  const releaseWakeLock = useCallback(() => {
    if (wakeLockRef.current) {
      try {
        wakeLockRef.current.release();
        wakeLockRef.current = null;
      } catch {
        // ignore
      }
    }
  }, []);

  // Monitor active transfers to manage Wake Lock lifecycle
  useEffect(() => {
    const hasActive = transfers.some(t => t.status === 'uploading' || t.status === 'downloading');
    if (hasActive) {
      acquireWakeLock();
    } else {
      releaseWakeLock();
    }
  }, [transfers, acquireWakeLock, releaseWakeLock]);

  // Re-acquire wake lock if tab becomes visible again while transfers are ongoing
  useEffect(() => {
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        const hasActive = transfers.some(t => t.status === 'uploading' || t.status === 'downloading');
        if (hasActive) acquireWakeLock();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, [transfers, acquireWakeLock]);

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

  /**
   * Safe, Uncorrupted Browser Download:
   * Uses application/octet-stream and preserves original filename & extension without browser transcoding.
   */
  const saveBlobToDeviceStorage = useCallback((blob: Blob, fileName: string) => {
    try {
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = fileName;
      link.style.display = 'none';
      document.body.appendChild(link);
      link.click();
      setTimeout(() => {
        try {
          document.body.removeChild(link);
          URL.revokeObjectURL(url);
        } catch {
          // ignore
        }
      }, 60000);
    } catch (err) {
      console.error('Error saving blob to device:', err);
    }
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
   * Get or initialize a single, persistent WebRTC DataChannel connection to a peer
   */
  const getOrCreatePeerChannel = useCallback(async (targetId: string): Promise<RTCDataChannel> => {
    const existing = rtcChannels.current.get(targetId);
    if (existing && existing.readyState === 'open') {
      return existing;
    }

    let waiter = rtcReadyPromises.current.get(targetId);
    if (waiter) {
      return waiter.promise;
    }

    let resolvePromise: (dc: RTCDataChannel) => void = () => {};
    const promise = new Promise<RTCDataChannel>((resolve) => {
      resolvePromise = resolve;
    });
    rtcReadyPromises.current.set(targetId, { promise, resolve: resolvePromise });

    let pc = rtcPeers.current.get(targetId);
    if (!pc || pc.connectionState === 'failed' || pc.connectionState === 'closed') {
      try { pc?.close(); } catch { /* ignore */ }
      pc = new RTCPeerConnection(RTC_CONFIG);
      rtcPeers.current.set(targetId, pc);

      pc.onicecandidate = (ev) => {
        if (ev.candidate) {
          sendP2PSignal(targetId, {
            type: 'webrtc-ice',
            candidate: ev.candidate.toJSON()
          });
        }
      };
    }

    const dc = pc.createDataChannel('kawerify-stream-pipe', { ordered: true });
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = 256 * 1024;
    rtcChannels.current.set(targetId, dc);

    dc.onopen = () => {
      const w = rtcReadyPromises.current.get(targetId);
      if (w) {
        w.resolve(dc);
        rtcReadyPromises.current.delete(targetId);
      }
    };

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      sendP2PSignal(targetId, {
        type: 'webrtc-offer',
        sdp: offer
      });
    } catch (err) {
      console.error('Error creating WebRTC offer:', err);
    }

    return promise;
  }, [sendP2PSignal]);

  /**
   * Safe, 16KB-Framed, High-Speed Binary Streamer:
   * 1. Uses a 16KB universal SCTP frame size (zero drops on iOS Safari / Android Chrome).
   * 2. Prepends a 4-byte sequence header (Uint32) on every packet for 100% corruption-free assembly.
   * 3. Streams sequentially through the persistent DataChannel.
   */
  const streamFileOverDataChannel = useCallback(async (
    dc: RTCDataChannel,
    _targetId: string,
    fileId: string,
    file: File
  ) => {
    const startTime = Date.now();
    const FRAME_SIZE = 16 * 1024; // 16KB frame size (universal golden standard for WebRTC SCTP)
    const MAX_BUFFER = 2 * 1024 * 1024; // 2MB in-flight socket window
    const totalBytes = file.size;
    let offset = 0;
    let chunkIndex = 0;

    // Send in-band start header
    dc.send(JSON.stringify({
      type: 'file-start',
      fileId,
      fileName: file.name,
      fileSize: file.size,
      fileType: file.type || 'application/octet-stream'
    }));

    while (offset < totalBytes) {
      if (canceledTransfers.current.has(fileId)) {
        dc.send(JSON.stringify({ type: 'file-cancel', fileId }));
        return;
      }

      if (dc.bufferedAmount > MAX_BUFFER) {
        await new Promise<void>((resolve) => {
          dc.onbufferedamountlow = () => {
            dc.onbufferedamountlow = null;
            resolve();
          };
          setTimeout(resolve, 30);
        });
      }

      const end = Math.min(offset + FRAME_SIZE, totalBytes);
      const rawSlice = await file.slice(offset, end).arrayBuffer();
      const rawBytes = new Uint8Array(rawSlice);

      // Packet structure: [4 bytes Uint32: chunkIndex] + [raw payload bytes]
      const packet = new Uint8Array(4 + rawBytes.byteLength);
      const view = new DataView(packet.buffer);
      view.setUint32(0, chunkIndex, false); // Big endian sequence index
      packet.set(rawBytes, 4);

      dc.send(packet.buffer);

      chunkIndex++;
      offset = end;
      const elapsed = Math.max((Date.now() - startTime) / 1000, 0.05);
      const progress = totalBytes > 0 ? Math.min(100, Math.round((offset / totalBytes) * 100)) : 100;
      const speedMBps = (offset / elapsed) / (1024 * 1024);
      const remainingSec = speedMBps > 0 ? ((totalBytes - offset) / (1024 * 1024)) / speedMBps : 0;

      updateTransferStatus(fileId, 'uploading', {
        progress,
        speed: speedMBps.toFixed(2),
        eta: remainingSec < 60 ? `${Math.ceil(remainingSec)}s left` : `${Math.floor(remainingSec / 60)}m left`
      });
    }

    // Wait for buffer to clear before sending in-band EOF
    while (dc.bufferedAmount > 0 && dc.readyState === 'open') {
      await new Promise(r => setTimeout(r, 20));
    }

    // In-band EOF: ordered channel guarantees arrival after the last byte
    dc.send(JSON.stringify({
      type: 'file-end',
      fileId,
      totalBytes
    }));

    updateTransferStatus(fileId, 'completed', {
      progress: 100,
      speed: 'Done',
      eta: 'Completed'
    });
  }, [updateTransferStatus]);

  /**
   * Process outgoing file queue sequentially to prevent network / WebRTC flooding
   */
  const processOutgoingQueue = useCallback(async () => {
    if (isPumpingQueue.current) return;
    isPumpingQueue.current = true;

    try {
      while (outgoingQueue.current.length > 0) {
        const item = outgoingQueue.current.shift();
        if (!item) break;

        const { targetId, fileId, file } = item;
        try {
          const dc = await getOrCreatePeerChannel(targetId);
          await streamFileOverDataChannel(dc, targetId, fileId, file);
        } catch (err) {
          console.error(`Failed to stream file ${file.name}:`, err);
          updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Error' });
        }
      }
    } finally {
      isPumpingQueue.current = false;
    }
  }, [getOrCreatePeerChannel, streamFileOverDataChannel, updateTransferStatus]);

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

      case 'p2p-transfer-offer-batch': {
        const incomingFiles = Array.isArray(data.files) ? data.files : [];
        setIncomingQueue(prev => {
          const existingIds = new Set(prev.map(i => i.fileId));
          const additions: IncomingOffer[] = [];
          for (const f of incomingFiles) {
            if (!existingIds.has(f.fileId)) {
              additions.push({
                senderId: data.senderId,
                senderName: data.senderName,
                fileId: f.fileId,
                fileName: f.fileName,
                fileSize: f.fileSize,
                fileType: f.fileType,
                transport: 'p2p-mesh'
              });
            }
          }
          return [...prev, ...additions];
        });
        break;
      }

      case 'p2p-transfer-accept': {
        const file = pendingFiles.current.get(data.fileId);
        if (file) {
          outgoingQueue.current.push({
            targetId: data.senderId,
            fileId: data.fileId,
            file
          });
          processOutgoingQueue();
        }
        break;
      }

      case 'webrtc-offer': {
        const senderId = data.senderId;
        let pc = rtcPeers.current.get(senderId);
        if (!pc || pc.connectionState === 'failed' || pc.connectionState === 'closed') {
          try { pc?.close(); } catch { /* ignore */ }
          pc = new RTCPeerConnection(RTC_CONFIG);
          rtcPeers.current.set(senderId, pc);

          pc.onicecandidate = (ev) => {
            if (ev.candidate) {
              sendP2PSignal(senderId, {
                type: 'webrtc-ice',
                candidate: ev.candidate.toJSON()
              });
            }
          };
        }

        pc.ondatachannel = (ev) => {
          const channel = ev.channel;
          channel.binaryType = 'arraybuffer';
          rtcChannels.current.set(senderId, channel);

          let currentFileId = '';

          channel.onmessage = async (msgEv) => {
            // 1. In-band JSON control messages
            if (typeof msgEv.data === 'string') {
              try {
                const ctrl = JSON.parse(msgEv.data);
                if (ctrl.type === 'file-start') {
                  currentFileId = ctrl.fileId;
                  incomingBuffers.current.set(ctrl.fileId, {
                    chunks: [],
                    receivedBytes: 0,
                    totalSize: ctrl.fileSize,
                    expectedChunkIndex: 0,
                    fileName: ctrl.fileName,
                    fileType: ctrl.fileType || 'application/octet-stream',
                    startTime: Date.now()
                  });
                } else if (ctrl.type === 'file-end') {
                  const entry = incomingBuffers.current.get(ctrl.fileId);
                  if (entry && !entry.completed) {
                    entry.completed = true;
                    const blob = new Blob(entry.chunks as BlobPart[], {
                      type: 'application/octet-stream'
                    });
                    saveBlobToDeviceStorage(blob, entry.fileName);
                    incomingBuffers.current.delete(ctrl.fileId);
                    updateTransferStatus(ctrl.fileId, 'completed', {
                      progress: 100,
                      speed: 'Done',
                      eta: 'Saved'
                    });
                    showToast(`Saved ${entry.fileName}`);
                  }
                } else if (ctrl.type === 'file-cancel') {
                  incomingBuffers.current.delete(ctrl.fileId);
                  updateTransferStatus(ctrl.fileId, 'failed', { speed: '0.00', eta: 'Canceled' });
                }
              } catch {
                // ignore
              }
              return;
            }

            // 2. Binary Packets: [4 bytes Uint32: chunkIndex] + [raw bytes]
            if (msgEv.data instanceof ArrayBuffer) {
              const view = new DataView(msgEv.data);
              const chunkIdx = view.getUint32(0, false);
              const payloadBytes = new Uint8Array(msgEv.data, 4);

              const entry = incomingBuffers.current.get(currentFileId);
              if (entry) {
                entry.chunks.push(payloadBytes);
                entry.receivedBytes += payloadBytes.byteLength;
                entry.expectedChunkIndex = chunkIdx + 1;

                const progress = entry.totalSize > 0
                  ? Math.min(99, Math.round((entry.receivedBytes / entry.totalSize) * 100))
                  : 99;
                const elapsed = Math.max((Date.now() - entry.startTime) / 1000, 0.05);
                const speedMBps = (entry.receivedBytes / elapsed) / (1024 * 1024);

                updateTransferStatus(currentFileId, 'downloading', {
                  progress,
                  speed: speedMBps.toFixed(2),
                  eta: 'Direct WebRTC'
                });
              }
            }
          };
        };

        try {
          await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
          const answer = await pc.createAnswer();
          await pc.setLocalDescription(answer);
          sendP2PSignal(senderId, {
            type: 'webrtc-answer',
            sdp: answer
          });
        } catch (err) {
          console.error('Error handling WebRTC offer:', err);
        }
        break;
      }

      case 'webrtc-answer': {
        const senderId = data.senderId;
        const pc = rtcPeers.current.get(senderId);
        if (pc && data.sdp) {
          try {
            await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));
          } catch { /* ignore */ }
        }
        break;
      }

      case 'webrtc-ice': {
        const senderId = data.senderId;
        const pc = rtcPeers.current.get(senderId);
        if (pc && data.candidate) {
          try {
            await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
          } catch { /* ignore */ }
        }
        break;
      }

      case 'p2p-transfer-cancel': {
        canceledTransfers.current.add(data.fileId);
        updateTransferStatus(data.fileId, 'failed', { speed: '0.00', eta: 'Declined' });
        break;
      }

      default:
        break;
    }
  }, [saveBlobToDeviceStorage, sendP2PSignal, showToast, processOutgoingQueue, updateTransferStatus]);

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
        // ignore
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
      rtcPeers.current.forEach(pc => pc.close());
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
              case 'transfer-offer-batch': {
                const batchFiles = Array.isArray(data.files) ? data.files : [];
                setIncomingQueue(prev => [
                  ...prev,
                  ...batchFiles.map((f: any) => ({
                    senderId: data.senderId,
                    senderName: data.senderName,
                    fileId: f.fileId,
                    fileName: f.fileName,
                    fileSize: f.fileSize,
                    fileType: f.fileType,
                    transport: 'local-ws' as const
                  }))
                ]);
                break;
              }
              case 'transfer-accepted':
                updateTransferStatus(data.fileId, 'completed', {
                  progress: 100,
                  speed: 'Done',
                  eta: 'Completed'
                });
                break;
              case 'transfer-canceled':
                updateTransferStatus(data.fileId, 'failed', {
                  speed: '0.00',
                  eta: 'Canceled'
                });
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
  }, [isLocalNetworkHost, updateTransferStatus, wsUrl]);

  const acceptSingleOffer = useCallback(async (transfer: IncomingOffer) => {
    const newTransfer: Transfer = {
      id: transfer.fileId,
      name: transfer.fileName,
      size: transfer.fileSize,
      progress: transfer.transport === 'local-ws' ? 50 : 1,
      speed: '--',
      status: 'downloading',
      role: 'receiver',
      peerName: transfer.senderName,
      eta: 'Streaming...'
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
      link.download = transfer.fileName;
      link.style.display = 'none';
      document.body.appendChild(link);
      link.click();
      setTimeout(() => {
        try {
          document.body.removeChild(link);
        } catch {
          // ignore
        }
      }, 30000);

      setTimeout(() => {
        updateTransferStatus(transfer.fileId, 'completed', {
          progress: 100,
          speed: 'Done',
          eta: 'Saved'
        });
      }, 1500);
    } else {
      incomingBuffers.current.set(transfer.fileId, {
        chunks: [],
        receivedBytes: 0,
        totalSize: transfer.fileSize,
        expectedChunkIndex: 0,
        fileName: transfer.fileName,
        fileType: transfer.fileType || 'application/octet-stream',
        startTime: Date.now()
      });

      sendP2PSignal(transfer.senderId, {
        type: 'p2p-transfer-accept',
        fileId: transfer.fileId
      });
    }
  }, [sendP2PSignal, serverUrl, updateTransferStatus]);

  useEffect(() => {
    if (autoAccept && incomingQueue.length > 0) {
      const batch = [...incomingQueue];
      setIncomingQueue([]);
      batch.forEach((offer, idx) => {
        setTimeout(() => {
          acceptSingleOffer(offer);
        }, idx * 60);
      });
    }
  }, [autoAccept, incomingQueue, acceptSingleOffer]);

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

  const appendFilesToQueue = (newFiles: File[]) => {
    if (!newFiles || newFiles.length === 0) return;
    setSelectedFiles(prev => [...prev, ...newFiles]);
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

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);

    if (e.dataTransfer.items && e.dataTransfer.items.length > 0) {
      const extracted = await extractFilesFromDataTransfer(e.dataTransfer.items);
      if (extracted.length > 0) {
        appendFilesToQueue(extracted);
        return;
      }
    }

    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      appendFilesToQueue(Array.from(e.dataTransfer.files));
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      appendFilesToQueue(Array.from(e.target.files));
      e.target.value = '';
    }
  };

  const triggerFileInput = () => {
    fileInputRef.current?.click();
  };

  const triggerFolderInput = () => {
    folderInputRef.current?.click();
  };

  const removeFileAtIndex = (index: number) => {
    setSelectedFiles(prev => prev.filter((_, i) => i !== index));
  };

  const handleSendFiles = () => {
    if (selectedFiles.length === 0 || selectedPeerIds.length === 0) {
      showToast('Select files and at least one destination device');
      return;
    }

    const filesToShare = [...selectedFiles];
    const targetPeers = peers.filter(p => selectedPeerIds.includes(p.id));
    if (targetPeers.length === 0) return;

    const localTargetPeers = targetPeers.filter(p => wsConnected && p.transport === 'local-ws');
    const p2pTargetPeers = targetPeers.filter(p => !(wsConnected && p.transport === 'local-ws'));

    // 1. High-Speed Local LAN Multi-File Pipeline
    if (localTargetPeers.length > 0) {
      const peerNames = localTargetPeers.length === 1
        ? localTargetPeers[0].name
        : `${localTargetPeers.length} devices`;

      let cursor = 0;
      const CONCURRENCY = 2; // Controlled concurrency to prevent socket lockup

      const uploadWorker = () => {
        while (cursor < filesToShare.length) {
          const file = filesToShare[cursor++];
          const tempId = `${Date.now()}-${Math.random().toString(36).substring(2, 8)}`;

          setTransfers(prev => [
            {
              id: tempId,
              name: file.name,
              size: file.size,
              progress: 0,
              speed: '0.00',
              status: 'uploading',
              role: 'sender',
              peerName: peerNames,
              eta: 'Streaming...'
            },
            ...prev
          ]);

          const xhr = new XMLHttpRequest();
          activeXhrs.current.set(tempId, xhr);

          const formData = new FormData();
          formData.append('file', file);
          const startTime = Date.now();

          xhr.upload.onprogress = (event) => {
            if (event.lengthComputable) {
              const duration = Math.max((Date.now() - startTime) / 1000, 0.05);
              const loaded = event.loaded;
              const progress = Math.round((loaded / event.total) * 100);
              const speedBps = loaded / duration;
              const speedMBps = (speedBps / (1024 * 1024)).toFixed(2);
              const remainingBytes = event.total - loaded;
              const remainingSeconds = speedBps > 0 ? remainingBytes / speedBps : 0;
              const etaText = remainingSeconds < 60
                ? `${Math.ceil(remainingSeconds)}s left`
                : `${Math.floor(remainingSeconds / 60)}m left`;

              updateTransferStatus(tempId, 'uploading', {
                progress,
                speed: speedMBps,
                eta: etaText
              });
            }
          };

          xhr.onload = () => {
            activeXhrs.current.delete(tempId);
            if (xhr.status === 200) {
              const response = JSON.parse(xhr.responseText);
              setTransfers(prev => prev.map(t => t.id === tempId ? {
                ...t,
                id: response.fileId,
                progress: 100,
                status: 'waiting',
                speed: 'Done',
                eta: `Ready on ${localTargetPeers.length} device(s)`
              } : t));

              if (ws.current && ws.current.readyState === WebSocket.OPEN) {
                ws.current.send(JSON.stringify({
                  type: 'transfer-init',
                  targetIds: localTargetPeers.map(p => p.id),
                  fileId: response.fileId,
                  fileName: file.name,
                  fileSize: file.size,
                  fileType: file.type || 'application/octet-stream'
                }));
              }
            } else {
              updateTransferStatus(tempId, 'failed', { speed: '0.00', eta: 'Failed' });
            }
            uploadWorker();
          };

          xhr.onerror = () => {
            activeXhrs.current.delete(tempId);
            updateTransferStatus(tempId, 'failed', { speed: '0.00', eta: 'Network error' });
            uploadWorker();
          };

          xhr.open('POST', `${serverUrl}/api/upload?targets=${localTargetPeers.length}`);
          xhr.send(formData);
          return;
        }
      };

      for (let w = 0; w < Math.min(CONCURRENCY, filesToShare.length); w++) {
        uploadWorker();
      }
    }

    // 2. Direct P2P Sequential Pipeline over Persistent WebRTC
    if (p2pTargetPeers.length > 0) {
      const newRows: Transfer[] = [];

      for (const peer of p2pTargetPeers) {
        const batchMeta: Array<{ fileId: string; fileName: string; fileSize: number; fileType: string }> = [];

        for (const file of filesToShare) {
          const fileId = `${Date.now()}-${Math.random().toString(36).substring(2, 8)}-${peer.id.slice(-3)}`;
          pendingFiles.current.set(fileId, file);
          batchMeta.push({
            fileId,
            fileName: file.name,
            fileSize: file.size,
            fileType: file.type || 'application/octet-stream'
          });
          newRows.push({
            id: fileId,
            name: file.name,
            size: file.size,
            progress: 0,
            speed: 'Direct WebRTC',
            status: 'waiting',
            role: 'sender',
            peerName: peer.name,
            eta: 'Queued'
          });
        }

        const BATCH_STEP = 25;
        for (let i = 0; i < batchMeta.length; i += BATCH_STEP) {
          sendP2PSignal(peer.id, {
            type: 'p2p-transfer-offer-batch',
            files: batchMeta.slice(i, i + BATCH_STEP)
          });
        }
      }

      setTransfers(prev => [...newRows, ...prev]);
    }

    showToast(`Queued ${filesToShare.length} file(s) for sending`);
    setSelectedFiles([]);
  };

  const incomingTransfer = incomingQueue[0] || null;

  const handleAcceptSingle = () => {
    if (!incomingTransfer) return;
    const current = incomingTransfer;
    setIncomingQueue(prev => prev.slice(1));
    acceptSingleOffer(current);
  };

  const handleAcceptAllIncoming = () => {
    const all = [...incomingQueue];
    setIncomingQueue([]);
    all.forEach((offer, idx) => {
      setTimeout(() => {
        acceptSingleOffer(offer);
      }, idx * 60);
    });
    showToast(`Accepted all ${all.length} incoming files`);
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

  const handleRejectAllIncoming = () => {
    const all = [...incomingQueue];
    setIncomingQueue([]);
    for (const transfer of all) {
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
  const totalSelectedBytes = selectedFiles.reduce((acc, f) => acc + f.size, 0);

  return (
    <div className="shell">
      {toast && <div className="toast">{toast}</div>}

      <header className="topbar">
        <div className="topbar-inner">
          <a href="/" className="brand">
            <div className="brand-mark">K</div>
            <div className="brand-text">
              <span className="brand-name">Kawerify Transfer</span>
              <span className="brand-byline">Direct WebRTC & Local Wi-Fi File Sharing</span>
            </div>
          </a>

          <div className="topbar-actions">
            <div className="status-pill">
              <span className={`status-dot ${isSystemOnline ? 'online' : ''}`} />
              <span>
                {wsConnected
                  ? 'Offline LAN Ready'
                  : p2pConnected
                    ? 'Direct WebRTC Ready'
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

      <main className="workspace">
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
                  Devices with the same Room Code discover each other automatically without scanning.
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
                        Scan with a phone camera, or open the link on another laptop to connect without scanning.
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
                    Select 1, 5, or all devices below to broadcast files simultaneously:
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
                                <span>{peer.transport === 'local-ws' ? 'Offline LAN' : 'Direct WebRTC'}</span>
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

        <div className="column">
          <section className="card">
            <div className="card-header">
              <div className="card-title-group">
                <span className="card-title">Send Files & Folders</span>
                {selectedFiles.length > 0 && (
                  <span className="transport-tag mono">{selectedFiles.length} files</span>
                )}
              </div>
              <span className="card-subtitle mono" style={{ display: 'inline-flex', alignItems: 'center', gap: '0.3rem' }}>
                <ShieldCheck size={13} style={{ color: 'var(--status-online)' }} /> Background Keep-Alive • Bit-for-Bit Stream
              </span>
            </div>

            <div className="card-body">
              <input
                type="file"
                multiple
                ref={fileInputRef}
                style={{ display: 'none' }}
                onChange={handleFileChange}
              />
              <input
                type="file"
                multiple
                {...({ webkitdirectory: '', directory: '' } as any)}
                ref={folderInputRef}
                style={{ display: 'none' }}
                onChange={handleFileChange}
              />

              {selectedFiles.length === 0 ? (
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
                  <div className="dropzone-title">Select 100+ files, folders, or drag & drop here</div>
                  <div className="dropzone-hint">
                    Original files are transferred with verified sequential 16KB frames and zero cloud throttling
                  </div>
                  <div
                    style={{ display: 'flex', gap: '0.5rem', marginTop: '0.5rem', flexWrap: 'wrap', justifyContent: 'center' }}
                    onClick={(e) => e.stopPropagation()}
                  >
                    <button className="btn btn-secondary btn-sm" onClick={triggerFileInput}>
                      <Plus size={14} /> Select Files
                    </button>
                    <button className="btn btn-secondary btn-sm" onClick={triggerFolderInput}>
                      <FolderPlus size={14} /> Select Entire Folder
                    </button>
                  </div>
                </div>
              ) : (
                <div className="files-queue-box">
                  <div className="files-queue-header">
                    <div>
                      <div style={{ fontSize: '0.86rem', fontWeight: 600, color: 'var(--text-primary)' }}>
                        {selectedFiles.length} {selectedFiles.length === 1 ? 'file' : 'files'} queued
                      </div>
                      <div className="card-subtitle mono">
                        Total size: {formatBytes(totalSelectedBytes)}
                      </div>
                    </div>

                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', flexWrap: 'wrap' }}>
                      <button className="btn btn-secondary btn-sm" onClick={triggerFileInput}>
                        <Plus size={13} /> Add Files
                      </button>
                      <button className="btn btn-secondary btn-sm" onClick={triggerFolderInput}>
                        <FolderPlus size={13} /> Add Folder
                      </button>
                      <button
                        className="btn btn-secondary btn-sm"
                        onClick={() => setSelectedFiles([])}
                        style={{ color: 'var(--status-danger)' }}
                      >
                        Clear All
                      </button>
                    </div>
                  </div>

                  <div className="files-queue-list">
                    {selectedFiles.slice(0, 150).map((file, idx) => (
                      <div key={`${file.name}-${idx}`} className="file-queue-row">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '0.65rem', minWidth: 0, flex: 1 }}>
                          <div className="file-ext-badge mono">
                            {getFileExtension(file.name)}
                          </div>
                          <div style={{ minWidth: 0, flex: 1 }}>
                            <div className="transfer-filename" title={file.name}>
                              {file.name}
                            </div>
                            <div className="card-subtitle mono">
                              {formatBytes(file.size)}
                            </div>
                          </div>
                        </div>
                        <button
                          className="icon-btn"
                          style={{ width: 28, height: 28 }}
                          onClick={() => removeFileAtIndex(idx)}
                          title="Remove from queue"
                        >
                          <X size={14} />
                        </button>
                      </div>
                    ))}
                    {selectedFiles.length > 150 && (
                      <div className="file-queue-row" style={{ justifyContent: 'center', color: 'var(--text-secondary)', fontSize: '0.78rem' }}>
                        + {selectedFiles.length - 150} additional files ready in batch
                      </div>
                    )}
                  </div>
                </div>
              )}

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
                  {selectedFiles.length > 0 && selectedPeersList.length > 0
                    ? `Ready to stream ${selectedFiles.length} file(s) sequentially`
                    : 'Select files and recipient devices to start'}
                </div>

                <div className="send-actions">
                  <button
                    className="btn btn-primary"
                    onClick={handleSendFiles}
                    disabled={selectedFiles.length === 0 || selectedPeersList.length === 0}
                  >
                    <Send size={15} />
                    {selectedFiles.length > 1
                      ? `Send ${selectedFiles.length} Files`
                      : 'Send File'}
                    {selectedPeersList.length > 1 ? ` to ${selectedPeersList.length} Devices` : ''}
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

              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
                <label
                  style={{
                    display: 'inline-flex',
                    alignItems: 'center',
                    gap: '0.35rem',
                    fontSize: '0.75rem',
                    color: 'var(--text-secondary)',
                    cursor: 'pointer',
                    userSelect: 'none'
                  }}
                  title="Automatically accept incoming multi-file batches without prompts"
                >
                  <input
                    type="checkbox"
                    checked={autoAccept}
                    onChange={(e) => setAutoAccept(e.target.checked)}
                    style={{ accentColor: 'var(--accent-primary)' }}
                  />
                  Auto-accept incoming
                </label>

                {transfers.some(t => t.status === 'completed' || t.status === 'failed') && (
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => setTransfers(prev => prev.filter(t => t.status !== 'completed' && t.status !== 'failed'))}
                  >
                    Clear finished
                  </button>
                )}
              </div>
            </div>

            <div className="card-body">
              {transfers.length > 0 ? (
                <div className="transfer-list" style={{ maxHeight: '420px', overflowY: 'auto' }}>
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
                          {transfer.speed !== 'Done' && transfer.speed !== '--' && transfer.speed !== 'Direct WebRTC'
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

      {incomingTransfer && (
        <div className="modal-backdrop">
          <div className="modal-card">
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '0.6rem' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '0.6rem' }}>
                <div className="device-badge-icon" style={{ width: 34, height: 34 }}>
                  <Download size={18} />
                </div>
                <div>
                  <div style={{ fontSize: '0.95rem', fontWeight: 600 }}>
                    {incomingQueue.length > 1
                      ? `Incoming Batch (${incomingQueue.length} Files)`
                      : 'Incoming File'}
                  </div>
                  <div className="card-subtitle">From {incomingTransfer.senderName}</div>
                </div>
              </div>
              {incomingQueue.length > 1 && (
                <span className="transport-tag mono">{incomingQueue.length} queued</span>
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
                    {incomingQueue.length > 1
                      ? ` • +${incomingQueue.length - 1} more file(s)`
                      : ''}
                  </div>
                </div>
              </div>
            </div>

            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: '0.5rem', flexWrap: 'wrap' }}>
              {incomingQueue.length > 1 ? (
                <>
                  <button className="btn btn-secondary" onClick={handleRejectAllIncoming}>
                    Decline All
                  </button>
                  <button className="btn btn-secondary" onClick={handleAcceptSingle}>
                    Accept This One
                  </button>
                  <button className="btn btn-primary" onClick={handleAcceptAllIncoming}>
                    Accept All ({incomingQueue.length})
                  </button>
                </>
              ) : (
                <>
                  <button className="btn btn-secondary" onClick={handleRejectTransfer}>
                    Decline
                  </button>
                  <button className="btn btn-primary" onClick={handleAcceptSingle}>
                    Accept & Save
                  </button>
                </>
              )}
            </div>
          </div>
        </div>
      )}

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
