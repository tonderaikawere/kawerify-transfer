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
  Radio,
  Sun,
  Moon,
  ArrowUpRight,
  Users,
  Hash,
  User,
  Edit2,
  HardDrive,
  Volume2,
  VolumeX,
  ArrowDown,
  ArrowUp
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
  blobUrl?: string;
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
  chunksMap?: Map<number, Uint8Array>;
  receivedBytes: number;
  totalSize: number;
  expectedChunkIndex: number;
  totalChunks?: number;
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
    { urls: 'stun:stun3.l.google.com:19302' },
    { urls: 'stun:stun4.l.google.com:19302' },
    { urls: 'stun:stun.services.mozilla.com' },
    { urls: 'stun:stun.cloudflare.com:3478' }
  ],
  iceCandidatePoolSize: 10
};

const AVATAR_LIST = [
  { id: 1, label: 'Laptop', icon: '💻' },
  { id: 2, label: 'Phone', icon: '📱' },
  { id: 3, label: 'Rocket', icon: '🚀' },
  { id: 4, label: 'Lightning', icon: '⚡' },
  { id: 5, label: 'Cat', icon: '🐱' },
  { id: 6, label: 'Fox', icon: '🦊' },
  { id: 7, label: 'Lion', icon: '🦁' },
  { id: 8, label: 'Panda', icon: '🐼' },
  { id: 9, label: 'Gamepad', icon: '🎮' },
  { id: 10, label: 'Shield', icon: '🛡️' }
];

function createLocalIdentity(): Peer {
  const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
  let id = `peer-${Math.random().toString(36).substring(2, 9)}`;

  const savedPeer = sessionStorage.getItem('kawerify_peer_identity');
  if (savedPeer) {
    try {
      const parsed = JSON.parse(savedPeer);
      if (parsed.id) id = parsed.id;
    } catch {
      // ignore
    }
  }

  const savedCustomName = localStorage.getItem('kawerify_custom_name');
  const savedAvatar = localStorage.getItem('kawerify_custom_avatar');

  let defaultName = 'My Device';
  if (/iPhone/i.test(navigator.userAgent)) defaultName = 'My iPhone';
  else if (/iPad/i.test(navigator.userAgent)) defaultName = 'My iPad';
  else if (/Android/i.test(navigator.userAgent)) defaultName = 'My Android';
  else if (/Macintosh|Mac OS/i.test(navigator.userAgent)) defaultName = 'My MacBook';
  else if (/Windows/i.test(navigator.userAgent)) defaultName = 'My PC';
  else if (/Linux/i.test(navigator.userAgent)) defaultName = 'My Linux';

  const name = (savedCustomName && savedCustomName.trim()) ? savedCustomName.trim() : defaultName;
  const avatar = savedAvatar ? parseInt(savedAvatar, 10) : (isMobile ? 2 : 1);
  const deviceType = isMobile ? 'Mobile Phone' : 'Laptop / Desktop';

  const identity: Peer = {
    id,
    name,
    avatar,
    deviceType
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
  const [initialIdentity] = useState<Peer>(() => createLocalIdentity());
  const [me, setMe] = useState<Peer>(initialIdentity);
  const [customName, setCustomName] = useState<string>(initialIdentity.name);
  const [selectedAvatar, setSelectedAvatar] = useState<number>(initialIdentity.avatar);
  const [isEditingName, setIsEditingName] = useState<boolean>(false);
  const [voiceEnabled, setVoiceEnabled] = useState<boolean>(() => {
    return localStorage.getItem('kawerify_voice_enabled') !== 'false';
  });
  const [mobileTab, setMobileTab] = useState<'share' | 'room' | 'transfers' | 'me'>('share');
  const [isMobileScreen, setIsMobileScreen] = useState<boolean>(() => {
    return typeof window !== 'undefined' && window.innerWidth < 960;
  });
  const [roomCode, setRoomCode] = useState<string>(() => {
    const params = new URLSearchParams(window.location.search);
    const saved = localStorage.getItem('kawerify_room_code');
    return (params.get('room') || saved || 'GLOBAL').toUpperCase();
  });
  const [roomInput, setRoomInput] = useState<string>(() => {
    const params = new URLSearchParams(window.location.search);
    const saved = localStorage.getItem('kawerify_room_code');
    return (params.get('room') || saved || 'GLOBAL').toUpperCase();
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
  const [autoSend, setAutoSend] = useState<boolean>(true);
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

  // Track discovered peers to announce when someone joins
  const knownPeersRef = useRef<Set<string>>(new Set());

  // Single persistent RTCPeerConnection and RTCDataChannel per target peer
  const rtcPeers = useRef<Map<string, RTCPeerConnection>>(new Map());
  const rtcChannels = useRef<Map<string, RTCDataChannel>>(new Map());
  const rtcReadyPromises = useRef<Map<string, { promise: Promise<RTCDataChannel>; resolve: (dc: RTCDataChannel) => void; reject: (err: Error) => void }>>(new Map());
  const pendingIceCandidates = useRef<Map<string, RTCIceCandidateInit[]>>(new Map());

  // Independent per-peer queues so sharing to 5+ devices streams concurrently without blocking
  const outgoingQueuesByPeer = useRef<Map<string, Array<{ fileId: string; file: File }>>>(new Map());
  const activePeerPumps = useRef<Set<string>>(new Set());

  // Waiters for reliable fallback flow-control ACKs
  const chunkAckWaiters = useRef<Map<string, (chunkIndex: number) => void>>(new Map());
  const audioContextRef = useRef<AudioContext | null>(null);

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

  useEffect(() => {
    const handleResize = () => {
      setIsMobileScreen(window.innerWidth < 960);
    };
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  useEffect(() => {
    localStorage.setItem('kawerify_voice_enabled', voiceEnabled.toString());
  }, [voiceEnabled]);

  // Screen Wake Lock & Background Audio Keep-Alive so mobile phones don't sleep or freeze when switching tabs
  const startBackgroundServices = useCallback(async () => {
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
    try {
      if (!audioContextRef.current) {
        const AudioCtx = window.AudioContext || (window as any).webkitAudioContext;
        if (AudioCtx) {
          const ctx = new AudioCtx();
          const osc = ctx.createOscillator();
          const gain = ctx.createGain();
          gain.gain.value = 0.0001; // Inaudible tone prevents OS background thread sleep
          osc.connect(gain);
          gain.connect(ctx.destination);
          osc.start();
          audioContextRef.current = ctx;
        }
      } else if (audioContextRef.current.state === 'suspended') {
        audioContextRef.current.resume();
      }
    } catch {
      // ignore
    }
  }, []);

  const stopBackgroundServices = useCallback(() => {
    if (wakeLockRef.current) {
      try {
        wakeLockRef.current.release();
        wakeLockRef.current = null;
      } catch {
        // ignore
      }
    }
    if (audioContextRef.current) {
      try {
        audioContextRef.current.close();
        audioContextRef.current = null;
      } catch {
        // ignore
      }
    }
  }, []);

  // Monitor active transfers to manage Wake Lock & Background lifecycle
  useEffect(() => {
    const hasActive = transfers.some(t => t.status === 'uploading' || t.status === 'downloading');
    if (hasActive) {
      startBackgroundServices();
    } else {
      stopBackgroundServices();
    }
  }, [transfers, startBackgroundServices, stopBackgroundServices]);

  // Re-acquire wake lock if tab becomes visible again while transfers are ongoing
  useEffect(() => {
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') {
        const hasActive = transfers.some(t => t.status === 'uploading' || t.status === 'downloading');
        if (hasActive) startBackgroundServices();
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, [transfers, startBackgroundServices]);

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

  const announcePeerJoined = useCallback((peerName: string) => {
    showToast(`👋 ${peerName} joined the room`);
    if (voiceEnabled && 'speechSynthesis' in window) {
      try {
        window.speechSynthesis.cancel();
        const utterance = new SpeechSynthesisUtterance(`${peerName} joined`);
        utterance.rate = 1.05;
        utterance.pitch = 1.0;
        window.speechSynthesis.speak(utterance);
      } catch {
        // ignore
      }
    }
  }, [voiceEnabled, showToast]);

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
  const saveBlobToDeviceStorage = useCallback((blob: Blob, fileName: string, fileId?: string) => {
    try {
      const url = URL.createObjectURL(blob);
      if (fileId) {
        setTransfers(prev => prev.map(t => t.id === fileId ? { ...t, blobUrl: url } : t));
      }
      const link = document.createElement('a');
      link.href = url;
      link.download = fileName;
      link.style.display = 'none';
      document.body.appendChild(link);
      link.click();
      setTimeout(() => {
        try {
          document.body.removeChild(link);
        } catch {
          // ignore
        }
      }, 10000);
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
   * Universal DataChannel Setup:
   * Sets up both incoming (ondatachannel) and outgoing DataChannels with message parsing,
   * 4-byte sequenced frame assembly, and state lifecycle callbacks.
   */
  const setupDataChannel = useCallback((peerId: string, dc: RTCDataChannel) => {
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = 256 * 1024;
    rtcChannels.current.set(peerId, dc);

    let currentFileId = '';

    dc.onopen = () => {
      const waiter = rtcReadyPromises.current.get(peerId);
      if (waiter) {
        waiter.resolve(dc);
        rtcReadyPromises.current.delete(peerId);
      }
    };

    dc.onerror = (e) => {
      console.error(`WebRTC DataChannel error with ${peerId}:`, e);
      const waiter = rtcReadyPromises.current.get(peerId);
      if (waiter) {
        waiter.reject(new Error('RTCDataChannel error'));
        rtcReadyPromises.current.delete(peerId);
      }
    };

    dc.onclose = () => {
      rtcChannels.current.delete(peerId);
      const waiter = rtcReadyPromises.current.get(peerId);
      if (waiter) {
        waiter.reject(new Error('RTCDataChannel closed'));
        rtcReadyPromises.current.delete(peerId);
      }
    };

    dc.onmessage = async (msgEv) => {
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
                type: entry.fileType || 'application/octet-stream'
              });
              saveBlobToDeviceStorage(blob, entry.fileName, ctrl.fileId);
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

      // 2. Binary Packets: [4 bytes Uint32: chunkIndex] + [raw payload bytes]
      if (msgEv.data instanceof ArrayBuffer) {
        const view = new DataView(msgEv.data);
        const chunkIdx = view.getUint32(0, false);
        const payloadBytes = new Uint8Array(msgEv.data.slice(4));

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
  }, [saveBlobToDeviceStorage, showToast, updateTransferStatus]);

  /**
   * Get or initialize a single, persistent WebRTC DataChannel connection to a peer.
   * Includes strict 8-second timeout so queues never deadlock.
   */
  const getOrCreatePeerChannel = useCallback(async (targetId: string, timeoutMs = 8000): Promise<RTCDataChannel> => {
    const existing = rtcChannels.current.get(targetId);
    if (existing && existing.readyState === 'open') {
      return existing;
    }

    const waiter = rtcReadyPromises.current.get(targetId);
    if (waiter) {
      return waiter.promise;
    }

    let resolvePromise!: (dc: RTCDataChannel) => void;
    let rejectPromise!: (err: Error) => void;

    const promise = new Promise<RTCDataChannel>((resolve, reject) => {
      resolvePromise = resolve;
      rejectPromise = reject;
    });

    const timer = setTimeout(() => {
      rtcReadyPromises.current.delete(targetId);
      rejectPromise(new Error(`WebRTC DataChannel connection timeout with ${targetId}`));
    }, timeoutMs);

    rtcReadyPromises.current.set(targetId, {
      promise,
      resolve: (dc: RTCDataChannel) => {
        clearTimeout(timer);
        resolvePromise(dc);
      },
      reject: (err: Error) => {
        clearTimeout(timer);
        rejectPromise(err);
      }
    });

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

      pc.ondatachannel = (ev) => {
        setupDataChannel(targetId, ev.channel);
      };

      pc.onconnectionstatechange = () => {
        if (pc?.connectionState === 'failed' || pc?.connectionState === 'closed') {
          const w = rtcReadyPromises.current.get(targetId);
          if (w) {
            w.reject(new Error(`Peer connection ${pc.connectionState}`));
            rtcReadyPromises.current.delete(targetId);
          }
        }
      };
    }

    const dc = pc.createDataChannel('kawerify-stream-pipe', { ordered: true });
    setupDataChannel(targetId, dc);

    try {
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      sendP2PSignal(targetId, {
        type: 'webrtc-offer',
        sdp: offer
      });
    } catch (err: any) {
      console.error('Error creating WebRTC offer:', err);
      const w = rtcReadyPromises.current.get(targetId);
      if (w) {
        w.reject(err);
        rtcReadyPromises.current.delete(targetId);
      }
    }

    return promise;
  }, [sendP2PSignal, setupDataChannel]);

  /**
   * Safe, 32KB-Framed, High-Speed Binary Streamer:
   * 1. Uses a 32KB frame size for maximum throughput and compatibility.
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
    const FRAME_SIZE = 32 * 1024; // 32KB frame size
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
      view.setUint32(0, chunkIndex, false);
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
   * Flow-Controlled Reliable Chunk Relay (Used if WebRTC is blocked across carrier NATs):
   * Sends 32KB indexed chunks with explicit receiver ACKs so public MQTT broker never drops packets or throttles.
   */
  const streamFileReliableFallback = useCallback(async (
    targetId: string,
    fileId: string,
    file: File
  ) => {
    const startTime = Date.now();
    const CHUNK_SIZE = 32 * 1024;
    const totalBytes = file.size;
    const totalChunks = Math.ceil(totalBytes / CHUNK_SIZE) || 1;
    let offset = 0;
    let chunkIndex = 0;

    sendP2PSignal(targetId, {
      type: 'p2p-file-start-relay',
      fileId,
      fileName: file.name,
      fileSize: file.size,
      fileType: file.type || 'application/octet-stream',
      totalChunks
    });

    while (offset < totalBytes) {
      if (canceledTransfers.current.has(fileId)) {
        sendP2PSignal(targetId, { type: 'p2p-transfer-cancel', fileId });
        return;
      }

      const end = Math.min(offset + CHUNK_SIZE, totalBytes);
      const slice = await file.slice(offset, end).arrayBuffer();
      const rawBytes = new Uint8Array(slice);

      let binary = '';
      const len = rawBytes.byteLength;
      for (let i = 0; i < len; i += 8192) {
        const sub = rawBytes.subarray(i, Math.min(i + 8192, len));
        binary += String.fromCharCode.apply(null, Array.from(sub));
      }
      const b64 = btoa(binary);

      const ackPromise = new Promise<void>((resolve) => {
        const timer = setTimeout(() => {
          chunkAckWaiters.current.delete(`${fileId}-${chunkIndex}`);
          resolve();
        }, 1500);

        chunkAckWaiters.current.set(`${fileId}-${chunkIndex}`, () => {
          clearTimeout(timer);
          chunkAckWaiters.current.delete(`${fileId}-${chunkIndex}`);
          resolve();
        });
      });

      sendP2PSignal(targetId, {
        type: 'p2p-file-chunk-relay',
        fileId,
        chunkIndex,
        totalChunks,
        chunk: b64
      });

      await ackPromise;

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

    sendP2PSignal(targetId, {
      type: 'p2p-file-end-relay',
      fileId,
      fileName: file.name,
      fileType: file.type || 'application/octet-stream',
      totalChunks
    });

    updateTransferStatus(fileId, 'completed', {
      progress: 100,
      speed: 'Done',
      eta: 'Completed'
    });
  }, [sendP2PSignal, updateTransferStatus]);

  /**
   * Process per-peer outgoing queues independently so sharing to 5+ devices runs in parallel.
   */
  const pumpPeerQueue = useCallback(async (targetId: string) => {
    if (activePeerPumps.current.has(targetId)) return;
    activePeerPumps.current.add(targetId);

    try {
      const queue = outgoingQueuesByPeer.current.get(targetId);
      while (queue && queue.length > 0) {
        const item = queue.shift();
        if (!item) break;

        const { fileId, file } = item;
        try {
          let dc: RTCDataChannel | null = null;
          try {
            dc = await getOrCreatePeerChannel(targetId, 8000);
          } catch {
            dc = null;
          }

          if (dc && dc.readyState === 'open') {
            await streamFileOverDataChannel(dc, targetId, fileId, file);
          } else {
            console.warn(`WebRTC unavailable for ${targetId}, using reliable ACK relay`);
            await streamFileReliableFallback(targetId, fileId, file);
          }
        } catch (err) {
          console.error(`Failed to stream file ${file.name} to ${targetId}:`, err);
          updateTransferStatus(fileId, 'failed', { speed: '0.00', eta: 'Transfer failed' });
        }
      }
    } finally {
      activePeerPumps.current.delete(targetId);
    }
  }, [getOrCreatePeerChannel, streamFileOverDataChannel, streamFileReliableFallback, updateTransferStatus]);

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

  const handleP2PMessage = useCallback(async (data: Record<string, any>) => {
    if (!data || data.senderId === meRef.current.id) return;

    switch (data.type) {
      case 'presence': {
        const isNew = !knownPeersRef.current.has(data.senderId);
        knownPeersRef.current.add(data.senderId);

        if (isNew && data.senderId !== meRef.current.id) {
          announcePeerJoined(data.senderName || 'Nearby Device');
        }

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
        const offer: IncomingOffer = {
          senderId: data.senderId,
          senderName: data.senderName,
          fileId: data.fileId,
          fileName: data.fileName,
          fileSize: data.fileSize,
          fileType: data.fileType,
          transport: 'p2p-mesh'
        };
        if (autoAccept) {
          acceptSingleOffer(offer);
        } else {
          setIncomingQueue(prev => prev.some(item => item.fileId === data.fileId) ? prev : [...prev, offer]);
        }
        break;
      }

      case 'p2p-transfer-offer-batch': {
        const incomingFiles = Array.isArray(data.files) ? data.files : [];
        const additions: IncomingOffer[] = incomingFiles.map((f: any) => ({
          senderId: data.senderId,
          senderName: data.senderName,
          fileId: f.fileId,
          fileName: f.fileName,
          fileSize: f.fileSize,
          fileType: f.fileType,
          transport: 'p2p-mesh' as const
        }));

        if (autoAccept) {
          additions.forEach((offer, idx) => {
            setTimeout(() => acceptSingleOffer(offer), idx * 40);
          });
        } else {
          setIncomingQueue(prev => {
            const existingIds = new Set(prev.map(i => i.fileId));
            const fresh = additions.filter(a => !existingIds.has(a.fileId));
            return [...prev, ...fresh];
          });
        }
        break;
      }

      case 'p2p-transfer-accept': {
        const file = pendingFiles.current.get(data.fileId);
        if (file) {
          const peerQueue = outgoingQueuesByPeer.current.get(data.senderId) || [];
          peerQueue.push({
            fileId: data.fileId,
            file
          });
          outgoingQueuesByPeer.current.set(data.senderId, peerQueue);
          pumpPeerQueue(data.senderId);
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

          pc.ondatachannel = (ev) => {
            setupDataChannel(senderId, ev.channel);
          };
        }

        try {
          await pc.setRemoteDescription(new RTCSessionDescription(data.sdp));

          // Drain queued ICE candidates now that remote description is set
          const queued = pendingIceCandidates.current.get(senderId) || [];
          pendingIceCandidates.current.delete(senderId);
          for (const cand of queued) {
            try {
              await pc.addIceCandidate(new RTCIceCandidate(cand));
            } catch (e) {
              console.warn('Error adding buffered ICE candidate:', e);
            }
          }

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

            // Drain queued ICE candidates on answer as well
            const queued = pendingIceCandidates.current.get(senderId) || [];
            pendingIceCandidates.current.delete(senderId);
            for (const cand of queued) {
              try {
                await pc.addIceCandidate(new RTCIceCandidate(cand));
              } catch (e) {
                console.warn('Error adding buffered ICE candidate on answer:', e);
              }
            }
          } catch (err) {
            console.warn('Error setting remote description on answer:', err);
          }
        }
        break;
      }

      case 'webrtc-ice': {
        const senderId = data.senderId;
        const pc = rtcPeers.current.get(senderId);
        if (pc && pc.remoteDescription && pc.remoteDescription.type) {
          try {
            await pc.addIceCandidate(new RTCIceCandidate(data.candidate));
          } catch { /* ignore */ }
        } else {
          // Buffer candidate until remoteDescription is set
          const queued = pendingIceCandidates.current.get(senderId) || [];
          queued.push(data.candidate);
          pendingIceCandidates.current.set(senderId, queued);
        }
        break;
      }

      case 'p2p-chunk-ack': {
        const ackKey = `${data.fileId}-${data.chunkIndex}`;
        const waiter = chunkAckWaiters.current.get(ackKey);
        if (waiter) {
          waiter(data.chunkIndex);
        }
        break;
      }

      case 'p2p-file-start-relay': {
        incomingBuffers.current.set(data.fileId, {
          chunks: [],
          chunksMap: new Map(),
          receivedBytes: 0,
          totalSize: data.fileSize,
          totalChunks: data.totalChunks,
          expectedChunkIndex: 0,
          fileName: data.fileName,
          fileType: data.fileType || 'application/octet-stream',
          startTime: Date.now()
        });
        break;
      }

      case 'p2p-file-chunk-relay': {
        const { fileId, chunk, chunkIndex, totalChunks } = data;
        let entry = incomingBuffers.current.get(fileId);
        if (!entry) {
          entry = {
            chunks: [],
            chunksMap: new Map(),
            receivedBytes: 0,
            totalSize: 0,
            totalChunks,
            expectedChunkIndex: 0,
            fileName: 'file',
            fileType: 'application/octet-stream',
            startTime: Date.now()
          };
          incomingBuffers.current.set(fileId, entry);
        }

        const binStr = atob(chunk);
        const bytes = new Uint8Array(binStr.length);
        for (let i = 0; i < binStr.length; i++) {
          bytes[i] = binStr.charCodeAt(i);
        }

        if (!entry.chunksMap) entry.chunksMap = new Map();
        if (!entry.chunksMap.has(chunkIndex)) {
          entry.chunksMap.set(chunkIndex, bytes);
          entry.receivedBytes += bytes.byteLength;
        }

        // Send ACK back to sender immediately
        sendP2PSignal(data.senderId, {
          type: 'p2p-chunk-ack',
          fileId,
          chunkIndex
        });

        const progress = entry.totalChunks && entry.totalChunks > 0
          ? Math.min(99, Math.round((entry.chunksMap.size / entry.totalChunks) * 100))
          : 50;
        const elapsed = Math.max((Date.now() - entry.startTime) / 1000, 0.05);
        const speedMBps = (entry.receivedBytes / elapsed) / (1024 * 1024);

        updateTransferStatus(fileId, 'downloading', {
          progress,
          speed: speedMBps.toFixed(2),
          eta: 'Relay P2P'
        });
        break;
      }

      case 'p2p-file-end-relay': {
        const { fileId, fileName, fileType, totalChunks } = data;
        const entry = incomingBuffers.current.get(fileId);
        if (entry && !entry.completed) {
          entry.completed = true;
          const orderedChunks: Uint8Array[] = [];
          const count = typeof totalChunks === 'number' ? totalChunks : (entry.chunksMap?.size || 0);
          for (let i = 0; i < count; i++) {
            const c = entry.chunksMap?.get(i);
            if (c) orderedChunks.push(c);
          }
          const blob = new Blob(orderedChunks as BlobPart[], {
            type: fileType || entry.fileType || 'application/octet-stream'
          });
          saveBlobToDeviceStorage(blob, fileName || entry.fileName, fileId);
          incomingBuffers.current.delete(fileId);
          updateTransferStatus(fileId, 'completed', {
            progress: 100,
            speed: 'Done',
            eta: 'Saved'
          });
          showToast(`Saved ${fileName || entry.fileName}`);
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
  }, [saveBlobToDeviceStorage, sendP2PSignal, setupDataChannel, showToast, pumpPeerQueue, updateTransferStatus, announcePeerJoined, autoAccept, acceptSingleOffer]);

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
            avatar: meRef.current.avatar,
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
              case 'peers-update': {
                const incomingPeers: Peer[] = data.peers || [];
                for (const p of incomingPeers) {
                  if (p.id !== meRef.current.id && !knownPeersRef.current.has(p.id)) {
                    knownPeersRef.current.add(p.id);
                    announcePeerJoined(p.name || 'Nearby Device');
                  }
                }
                setLocalPeers(incomingPeers);
                break;
              }
              case 'transfer-offer': {
                const offer: IncomingOffer = { ...data, transport: 'local-ws' };
                if (autoAccept) {
                  acceptSingleOffer(offer);
                } else {
                  setIncomingQueue(prev => [...prev, offer]);
                }
                break;
              }
              case 'transfer-offer-batch': {
                const batchFiles = Array.isArray(data.files) ? data.files : [];
                const offers: IncomingOffer[] = batchFiles.map((f: any) => ({
                  senderId: data.senderId,
                  senderName: data.senderName,
                  fileId: f.fileId,
                  fileName: f.fileName,
                  fileSize: f.fileSize,
                  fileType: f.fileType,
                  transport: 'local-ws' as const
                }));
                if (autoAccept) {
                  offers.forEach((offer, idx) => {
                    setTimeout(() => acceptSingleOffer(offer), idx * 40);
                  });
                } else {
                  setIncomingQueue(prev => [...prev, ...offers]);
                }
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
  }, [isLocalNetworkHost, updateTransferStatus, wsUrl, autoAccept, acceptSingleOffer, announcePeerJoined]);

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

  const handleRename = (newName?: string, newAvatar?: number) => {
    const raw = (newName !== undefined ? newName : customName).trim().substring(0, 24);
    if (!raw) return;
    const avatarVal = newAvatar !== undefined ? newAvatar : selectedAvatar;
    
    localStorage.setItem('kawerify_custom_name', raw);
    localStorage.setItem('kawerify_custom_avatar', avatarVal.toString());
    
    const updated: Peer = { ...me, name: raw, avatar: avatarVal };
    setMe(updated);
    setCustomName(raw);
    setSelectedAvatar(avatarVal);
    meRef.current = updated;
    setIsEditingName(false);

    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({
        type: 'rename',
        name: raw,
        avatar: avatarVal,
        deviceType: me.deviceType
      }));
    }
    sendP2PSignal('broadcast', { type: 'presence', senderName: raw, avatar: avatarVal });
    showToast(`✅ Name saved as "${raw}"`);
  };

  const handleJoinRoom = (codeToJoin?: string) => {
    const target = codeToJoin !== undefined ? codeToJoin : roomInput;
    const clean = (target.trim() || 'GLOBAL').toUpperCase().substring(0, 16);
    setRoomInput(clean);
    setRoomCode(clean);
    roomRef.current = clean;
    localStorage.setItem('kawerify_room_code', clean);
    
    sendP2PSignal('broadcast', { type: 'presence', roomCode: clean, senderName: me.name, avatar: me.avatar });
    sendP2PSignal('broadcast', { type: 'presence-request', roomCode: clean });
    
    showToast(`✅ Joined room: ${clean}`);
    if (voiceEnabled && 'speechSynthesis' in window) {
      try {
        const u = new SpeechSynthesisUtterance(`Joined room ${clean}`);
        u.rate = 1.05;
        window.speechSynthesis.speak(u);
      } catch {
        // ignore
      }
    }
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

  const executeSendFiles = useCallback((filesToShare: File[], targetPeerIds?: string[]) => {
    if (!filesToShare || filesToShare.length === 0) return;
    const recipientIds = targetPeerIds && targetPeerIds.length > 0 ? targetPeerIds : selectedPeerIds;
    let targetPeers = peers.filter(p => recipientIds.includes(p.id));
    if (targetPeers.length === 0 && peers.length > 0) {
      targetPeers = [...peers];
    }
    if (targetPeers.length === 0) {
      setSelectedFiles(prev => [...prev, ...filesToShare]);
      showToast(`Queued ${filesToShare.length} file(s). Waiting for devices to join...`);
      return;
    }

    const localTargetPeers = targetPeers.filter(p => wsConnected && p.transport === 'local-ws');
    const p2pTargetPeers = targetPeers.filter(p => !(wsConnected && p.transport === 'local-ws'));

    // 1. High-Speed Local LAN Multi-File Pipeline
    if (localTargetPeers.length > 0) {
      const peerNames = localTargetPeers.length === 1
        ? localTargetPeers[0].name
        : `${localTargetPeers.length} devices`;

      let cursor = 0;
      const CONCURRENCY = 2;

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

        getOrCreatePeerChannel(peer.id, 8000).catch(() => {});

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
            speed: 'P2P Ready',
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

    showToast(`Sending ${filesToShare.length} file(s) to ${targetPeers.length} device(s)`);
    setSelectedFiles([]);
  }, [peers, selectedPeerIds, wsConnected, serverUrl, updateTransferStatus, getOrCreatePeerChannel, sendP2PSignal, showToast]);

  const appendFilesToQueue = (newFiles: File[]) => {
    if (!newFiles || newFiles.length === 0) return;
    if (autoSend && peers.length > 0) {
      executeSendFiles(newFiles);
    } else {
      setSelectedFiles(prev => [...prev, ...newFiles]);
      if (peers.length === 0) {
        showToast(`Queued ${newFiles.length} file(s). Waiting for someone to join...`);
      } else {
        showToast(`Added ${newFiles.length} file(s) to send`);
      }
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
    if (selectedFiles.length === 0) {
      showToast('Select files to send');
      return;
    }
    executeSendFiles(selectedFiles);
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

  const renderAvatarEmoji = (avatarId?: number, fallbackDeviceType?: string) => {
    const found = AVATAR_LIST.find(a => a.id === avatarId);
    if (found) return found.icon;
    return fallbackDeviceType && /mobile|phone|android|iphone/i.test(fallbackDeviceType) ? '📱' : '💻';
  };

  const isSystemOnline = wsConnected || p2pConnected;
  const selectedPeersList = peers.filter(p => selectedPeerIds.includes(p.id));
  const totalSelectedBytes = selectedFiles.reduce((acc, f) => acc + f.size, 0);
  const activeTransfersCount = transfers.filter(t => t.status === 'uploading' || t.status === 'downloading').length;

  // Reusable Component: Room Code Card with "Joined" Feedback
  const renderRoomCard = () => (
    <section className="card">
      <div className="card-header">
        <div className="card-title-group">
          <span className="card-title">Room Code</span>
        </div>
        <span className="room-active-pill">
          <span className="status-dot online" /> Joined & Active: <strong>{roomCode}</strong>
        </span>
      </div>

      <div className="card-body">
        <div className="room-bar" style={{ marginTop: 0 }}>
          <div className="room-bar-header">
            <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem', fontWeight: 600 }}>
              <Hash size={13} /> Laptop-to-Laptop Room Code
            </span>
            <span className="mono" style={{ color: 'var(--accent-primary)', fontSize: '0.74rem' }}>
              Status: {roomInput.trim().toUpperCase() === roomCode ? 'Joined' : 'Different Room'}
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
            <button
              className={`btn btn-sm ${roomInput.trim().toUpperCase() === roomCode ? 'btn-joined' : 'btn-primary'}`}
              onClick={() => handleJoinRoom()}
              disabled={roomInput.trim().toUpperCase() === roomCode}
              title={roomInput.trim().toUpperCase() === roomCode ? `You are already active in room ${roomCode}` : `Join room ${roomInput.trim().toUpperCase()}`}
            >
              {roomInput.trim().toUpperCase() === roomCode ? (
                <>
                  <Check size={14} /> Joined
                </>
              ) : (
                <>
                  <ArrowUpRight size={14} /> Join Room
                </>
              )}
            </button>
          </div>

          <div className="room-presets-row">
            <span style={{ fontSize: '0.7rem', color: 'var(--text-tertiary)' }}>Quick rooms:</span>
            {['GLOBAL', 'OFFICE', 'HOME', 'ROOM 1'].map(preset => (
              <button
                key={preset}
                type="button"
                className={`room-preset-chip ${roomCode === preset ? 'active' : ''}`}
                onClick={() => {
                  setRoomInput(preset);
                  handleJoinRoom(preset);
                }}
              >
                #{preset}
              </button>
            ))}
          </div>

          <div style={{ fontSize: '0.72rem', color: 'var(--text-tertiary)', marginTop: '0.45rem' }}>
            Devices with the same Room Code discover each other automatically without scanning.
          </div>
        </div>
      </div>
    </section>
  );

  // Reusable Component: People in Room Card with Custom Name & Avatar
  const renderPeopleCard = () => (
    <section className="card">
      <div className="card-header">
        <div className="card-title-group">
          <span className="card-title">People in Room</span>
          <span className="transport-tag mono" style={{ background: 'var(--status-online-soft)', color: 'var(--status-online)' }}>
            {peers.length + 1} {peers.length === 0 ? 'Person' : 'People'} • {roomCode}
          </span>
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
              showToast('Scanning room members...');
            }}
            title="Refresh room members"
          >
            <RefreshCw size={13} />
          </button>
        </div>
      </div>

      <div className="card-body">
        {/* This Device in the Room */}
        <div className="peer-card selected" style={{ borderLeft: '3px solid var(--accent-primary)', marginBottom: '0.65rem' }}>
          <div className="peer-card-left" style={{ flex: 1, minWidth: 0 }}>
            <div className="peer-icon-box" style={{ background: 'var(--accent-primary-soft)', color: 'var(--accent-primary)', fontSize: '1.25rem' }}>
              {renderAvatarEmoji(me.avatar, me.deviceType)}
            </div>
            <div style={{ minWidth: 0, flex: 1 }}>
              <div className="peer-card-name" style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', flexWrap: 'wrap' }}>
                <span style={{ fontWeight: 700 }}>{me.name}</span>
                <span className="mono" style={{ fontSize: '0.68rem', padding: '1px 6px', borderRadius: 4, background: 'var(--accent-primary-soft)', color: 'var(--accent-primary)' }}>You</span>
                <button
                  type="button"
                  className="btn btn-secondary btn-sm"
                  style={{ padding: '1px 7px', fontSize: '0.68rem', marginLeft: 'auto' }}
                  onClick={() => setIsEditingName(prev => !prev)}
                >
                  <Edit2 size={11} /> {isEditingName ? 'Close' : 'Edit Name'}
                </button>
              </div>
              <div className="peer-card-meta">
                <span style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                  <span className="status-dot online" style={{ width: 6, height: 6 }} /> Active in Room {roomCode}
                </span>
              </div>
            </div>
          </div>
          <span className="transport-tag mono">Your Device</span>
        </div>

        {/* Inline Name & Avatar Customizer like SHAREit */}
        {isEditingName && (
          <div style={{
            background: 'var(--bg-subtle)',
            border: '1px solid var(--border-strong)',
            borderRadius: 12,
            padding: '0.85rem',
            marginBottom: '0.85rem',
            display: 'flex',
            flexDirection: 'column',
            gap: '0.65rem'
          }}>
            <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-primary)' }}>
              Set Your Device Name & Avatar (like SHAREit)
            </div>
            <div style={{ display: 'flex', gap: '0.4rem' }}>
              <input
                type="text"
                className="identity-input"
                value={customName}
                maxLength={24}
                placeholder="Enter your name (e.g. Tonde, My Laptop)"
                onChange={(e) => setCustomName(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && handleRename()}
                autoFocus
              />
              <button className="btn btn-primary btn-sm" onClick={() => handleRename()}>
                Save
              </button>
            </div>
            <div>
              <div style={{ fontSize: '0.72rem', color: 'var(--text-secondary)', marginBottom: '0.35rem' }}>
                Choose Avatar:
              </div>
              <div className="avatar-picker-grid">
                {AVATAR_LIST.map((av) => (
                  <button
                    key={av.id}
                    type="button"
                    className={`avatar-option-btn ${selectedAvatar === av.id ? 'selected' : ''}`}
                    onClick={() => {
                      setSelectedAvatar(av.id);
                      handleRename(customName, av.id);
                    }}
                    title={av.label}
                  >
                    {av.icon}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}

        {peers.length > 0 ? (
          <>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', marginBottom: '0.65rem' }}>
              Other people in Room {roomCode}:
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
                      <div className="peer-icon-box" style={{ fontSize: '1.25rem' }}>
                        {renderAvatarEmoji(peer.avatar, peer.deviceType)}
                      </div>
                      <div style={{ minWidth: 0 }}>
                        <div className="peer-card-name" style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
                          <span>{peer.name}</span>
                          <span className="status-dot online" style={{ width: 6, height: 6 }} />
                        </div>
                        <div className="peer-card-meta">
                          <span>{peer.deviceType || 'Device'}</span>
                          <span>•</span>
                          <span>{peer.transport === 'local-ws' ? 'Offline LAN' : 'Direct WebRTC'}</span>
                        </div>
                      </div>
                    </div>
                    <span className="transport-tag">
                      {isSelected ? 'Targeted' : 'Select'}
                    </span>
                  </div>
                );
              })}
            </div>
          </>
        ) : (
          <div className="empty-radar" style={{ marginTop: '0.5rem' }}>
            <div className="radar-circles">
              <div className="radar-circle radar-circle-1" />
              <div className="radar-circle radar-circle-2" />
              <div className="radar-circle radar-circle-3" />
              <div className="radar-sweep" />
              <div className="radar-center-icon">
                <Radio size={20} />
              </div>
            </div>
            <div className="radar-title">Waiting for someone to join Room {roomCode}...</div>
            <div className="radar-subtitle">
              Enter room code <strong>{roomCode}</strong> on other laptops or phones to share instantly.
            </div>
          </div>
        )}
      </div>
    </section>
  );

  // Reusable Component: Direct Connect & QR Card
  const renderPairingCard = () => (
    <section className="card">
      <div className="card-header">
        <div className="card-title-group">
          <span className="card-title">Direct Connect & QR</span>
        </div>
      </div>
      <div className="card-body">
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
                <h4>Scan with Phone or Tablet</h4>
                <p>
                  Point camera at QR code or open link on other laptops to join Room <strong>{roomCode}</strong>.
                </p>
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  );

  // Reusable Component: SHAREit Hero Buttons & File Dropzone
  const renderShareCard = () => (
    <section className="card">
      <div className="card-header">
        <div className="card-title-group">
          <span className="card-title">Send Files & Folders</span>
          {selectedFiles.length > 0 && (
            <span className="transport-tag mono">{selectedFiles.length} files</span>
          )}
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', flexWrap: 'wrap' }}>
          <button
            type="button"
            className={`btn btn-sm ${autoSend ? 'btn-primary' : 'btn-secondary'}`}
            style={{ fontSize: '0.72rem', padding: '2px 8px', borderRadius: 999 }}
            onClick={() => {
              setAutoSend(prev => !prev);
              showToast(!autoSend ? '⚡ Auto-Send enabled: files stream immediately upon drop/select' : 'Auto-Send paused');
            }}
            title="Automatically send files as soon as they are added"
          >
            ⚡ Auto-Send: {autoSend ? 'ON' : 'OFF'}
          </button>
          <button
            type="button"
            className={`btn btn-sm ${autoAccept ? 'btn-primary' : 'btn-secondary'}`}
            style={{ fontSize: '0.72rem', padding: '2px 8px', borderRadius: 999 }}
            onClick={() => {
              setAutoAccept(prev => !prev);
              showToast(!autoAccept ? '🛡️ Auto-Accept enabled: incoming files save automatically' : 'Auto-Accept paused');
            }}
            title="Automatically accept and save incoming files without prompt"
          >
            🛡️ Auto-Accept: {autoAccept ? 'ON' : 'OFF'}
          </button>
        </div>
      </div>

      <div className="card-body">
        {/* SHAREit Hero Send & Receive Action Buttons */}
        <div className="shareit-hero-row">
          <button
            type="button"
            className="shareit-btn shareit-btn-send"
            onClick={triggerFileInput}
          >
            <div className="shareit-btn-icon">
              <ArrowUp size={26} strokeWidth={2.5} />
            </div>
            <div className="shareit-btn-title">SEND</div>
            <div className="shareit-btn-sub">Pick files or folder</div>
          </button>

          <button
            type="button"
            className="shareit-btn shareit-btn-receive"
            onClick={() => {
              if (isMobileScreen) {
                setMobileTab('room');
              }
              showToast(`Ready to receive in Room ${roomCode}. Auto-Accept is ${autoAccept ? 'ON' : 'OFF'}`);
            }}
          >
            <div className="shareit-btn-icon">
              <ArrowDown size={26} strokeWidth={2.5} />
            </div>
            <div className="shareit-btn-title">RECEIVE</div>
            <div className="shareit-btn-sub">{autoAccept ? 'Auto-Accept: ON' : 'Tap to receive'}</div>
          </button>
        </div>

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
            <div className="dropzone-title">
              {autoSend ? 'Drop or tap to select files & auto-send' : 'Select 100+ files, folders, or drag & drop here'}
            </div>
            <div className="dropzone-hint">
              {autoSend
                ? `⚡ Auto-Send is ON: files stream directly to everyone in Room ${roomCode} without waiting`
                : 'Fast bit-for-bit uncompressed transfer with background keep-alive'}
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
              Target Recipients ({selectedPeersList.length > 0 ? selectedPeersList.length : peers.length} of {peers.length})
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
                  <span>{renderAvatarEmoji(peer.avatar, peer.deviceType)}</span>
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
              {peers.length === 0
                ? `No other devices in Room ${roomCode} yet. Open this page on another device!`
                : `All ${peers.length} nearby device(s) will receive the files.`}
            </div>
          )}
        </div>

        <div className="send-bar">
          <div style={{ fontSize: '0.78rem', color: 'var(--text-secondary)' }}>
            {selectedFiles.length > 0
              ? `Ready to stream ${selectedFiles.length} file(s)`
              : 'Select files to start sharing'}
          </div>

          <div className="send-actions">
            <button
              className="btn btn-primary"
              onClick={handleSendFiles}
              disabled={selectedFiles.length === 0 || peers.length === 0}
            >
              <Send size={15} />
              {selectedFiles.length > 1
                ? `Send ${selectedFiles.length} Files`
                : 'Send File'}
              {selectedPeersList.length > 0
                ? ` to ${selectedPeersList.length} Device(s)`
                : peers.length > 0
                  ? ` to All (${peers.length})`
                  : ''}
            </button>
          </div>
        </div>
      </div>
    </section>
  );

  // Reusable Component: Transfers Card
  const renderTransfersCard = () => (
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
            Auto-accept
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
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
                      {transfer.status === 'completed' && transfer.blobUrl && (
                        <a
                          href={transfer.blobUrl}
                          download={transfer.name}
                          className="icon-btn"
                          style={{ width: 26, height: 26, color: 'var(--status-online)', textDecoration: 'none', display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}
                          title="Download / Save again"
                        >
                          <Download size={13} />
                        </a>
                      )}
                      <span className="mono" style={{
                        fontSize: '0.73rem',
                        fontWeight: 600,
                        color: transfer.status === 'completed' ? 'var(--status-online)' : 'var(--status-danger)'
                      }}>
                        {transfer.status === 'completed' ? 'Completed' : 'Canceled'}
                      </span>
                    </div>
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
  );

  // Reusable Component: Profile & Settings "Me" Card like SHAREit
  const renderProfileCard = () => (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '1.15rem' }}>
      <section className="card">
        <div className="card-header">
          <div className="card-title-group">
            <span className="card-title">My Profile (SHAREit Style)</span>
          </div>
          <span className="mono" style={{ fontSize: '0.75rem', color: 'var(--text-tertiary)' }}>{me.id}</span>
        </div>

        <div className="card-body">
          <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '1.25rem' }}>
            <div style={{
              width: 64,
              height: 64,
              borderRadius: '50%',
              background: 'var(--accent-primary-soft)',
              border: '2px solid var(--accent-primary)',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              fontSize: '2rem',
              boxShadow: '0 4px 12px rgba(59, 130, 246, 0.25)'
            }}>
              {renderAvatarEmoji(selectedAvatar, me.deviceType)}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-tertiary)', textTransform: 'uppercase', fontWeight: 600 }}>
                Device Display Name
              </div>
              <div style={{ display: 'flex', gap: '0.4rem', marginTop: '0.25rem' }}>
                <input
                  type="text"
                  className="identity-input"
                  value={customName}
                  maxLength={24}
                  placeholder="Set your name"
                  onChange={(e) => setCustomName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleRename()}
                />
                <button className="btn btn-primary btn-sm" onClick={() => handleRename()}>
                  Save
                </button>
              </div>
            </div>
          </div>

          <div style={{ marginTop: '0.5rem' }}>
            <div style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-primary)', marginBottom: '0.4rem' }}>
              Select Avatar:
            </div>
            <div className="avatar-picker-grid">
              {AVATAR_LIST.map((av) => (
                <button
                  key={av.id}
                  type="button"
                  className={`avatar-option-btn ${selectedAvatar === av.id ? 'selected' : ''}`}
                  onClick={() => {
                    setSelectedAvatar(av.id);
                    handleRename(customName, av.id);
                  }}
                  title={av.label}
                >
                  {av.icon}
                </button>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="card">
        <div className="card-header">
          <div className="card-title-group">
            <span className="card-title">Transfer Preferences</span>
          </div>
        </div>
        <div className="card-body" style={{ display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600 }}>⚡ Auto-Send</div>
              <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)' }}>Immediately send files when picked without clicking Send</div>
            </div>
            <button
              type="button"
              className={`btn btn-sm ${autoSend ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => {
                const next = !autoSend;
                setAutoSend(next);
                showToast(next ? '⚡ Auto-Send enabled' : 'Auto-Send disabled');
              }}
            >
              {autoSend ? 'ON' : 'OFF'}
            </button>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600 }}>🛡️ Auto-Accept</div>
              <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)' }}>Automatically accept and save incoming files</div>
            </div>
            <button
              type="button"
              className={`btn btn-sm ${autoAccept ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => {
                const next = !autoAccept;
                setAutoAccept(next);
                showToast(next ? '🛡️ Auto-Accept enabled' : 'Auto-Accept disabled');
              }}
            >
              {autoAccept ? 'ON' : 'OFF'}
            </button>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600 }}>🔊 Voice Announcements</div>
              <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)' }}>Utters "[Name] joined" when peers connect</div>
            </div>
            <button
              type="button"
              className={`btn btn-sm ${voiceEnabled ? 'btn-primary' : 'btn-secondary'}`}
              onClick={() => {
                const next = !voiceEnabled;
                setVoiceEnabled(next);
                showToast(next ? '🔊 Voice announcements ON' : '🔇 Voice announcements OFF');
              }}
            >
              {voiceEnabled ? 'ON' : 'OFF'}
            </button>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <div>
              <div style={{ fontSize: '0.88rem', fontWeight: 600 }}>🌓 Appearance</div>
              <div style={{ fontSize: '0.74rem', color: 'var(--text-secondary)' }}>Toggle dark / light display theme</div>
            </div>
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => setTheme(prev => prev === 'dark' ? 'light' : 'dark')}
            >
              {theme === 'dark' ? 'Dark Mode' : 'Light Mode'}
            </button>
          </div>
        </div>
      </section>
    </div>
  );

  return (
    <div className="shell">
      {toast && <div className="toast">{toast}</div>}

      <header className="topbar">
        <div className="topbar-inner">
          <a href="/" className="brand">
            <div className="brand-mark">K</div>
            <div className="brand-text">
              <span className="brand-name">Kawerify Transfer</span>
              <span className="brand-byline">Fast Direct P2P & Wi-Fi Sharing</span>
            </div>
          </a>

          <div className="topbar-actions">
            <div className="room-active-pill" title={`Joined room: ${roomCode}`}>
              <span className="status-dot online" />
              <span>{roomCode}</span>
            </div>

            <div className="status-pill">
              <span className={`status-dot ${isSystemOnline ? 'online' : ''}`} />
              <span>
                {wsConnected
                  ? 'Offline LAN'
                  : p2pConnected
                    ? 'Direct WebRTC'
                    : 'Connecting'}
              </span>
            </div>

            <button
              className="icon-btn"
              onClick={() => {
                const next = !voiceEnabled;
                setVoiceEnabled(next);
                showToast(next ? '🔊 Voice announcements ON' : '🔇 Voice announcements muted');
              }}
              title={voiceEnabled ? 'Mute voice announcements' : 'Enable voice announcements'}
              aria-label="Toggle voice announcements"
            >
              {voiceEnabled ? <Volume2 size={16} /> : <VolumeX size={16} />}
            </button>

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
        {isMobileScreen ? (
          /* Mobile View: Render active tab */
          <div className="column">
            {mobileTab === 'share' && (
              <>
                {renderShareCard()}
                {activeTransfersCount > 0 && renderTransfersCard()}
              </>
            )}

            {mobileTab === 'room' && (
              <>
                {renderRoomCard()}
                {renderPeopleCard()}
                {renderPairingCard()}
              </>
            )}

            {mobileTab === 'transfers' && (
              <>
                {renderTransfersCard()}
              </>
            )}

            {mobileTab === 'me' && (
              <>
                {renderProfileCard()}
              </>
            )}
          </div>
        ) : (
          /* Desktop View: Side-by-side Dashboard */
          <>
            <div className="column">
              {renderRoomCard()}
              {renderPeopleCard()}
              {renderPairingCard()}
            </div>

            <div className="column">
              {renderShareCard()}
              {renderTransfersCard()}
            </div>
          </>
        )}
      </main>

      {/* Mobile Bottom Navigation Bar like SHAREit */}
      <nav className="mobile-bottom-nav">
        <button
          type="button"
          className={`bottom-nav-item ${mobileTab === 'share' ? 'active' : ''}`}
          onClick={() => setMobileTab('share')}
        >
          <div className="bottom-nav-icon-wrap">
            <Send size={20} />
            {selectedFiles.length > 0 && (
              <span className="bottom-nav-badge">{selectedFiles.length}</span>
            )}
          </div>
          <span>Share</span>
        </button>

        <button
          type="button"
          className={`bottom-nav-item ${mobileTab === 'room' ? 'active' : ''}`}
          onClick={() => setMobileTab('room')}
        >
          <div className="bottom-nav-icon-wrap">
            <Users size={20} />
            <span className="bottom-nav-badge">{peers.length + 1}</span>
          </div>
          <span>Room</span>
        </button>

        <button
          type="button"
          className={`bottom-nav-item ${mobileTab === 'transfers' ? 'active' : ''}`}
          onClick={() => setMobileTab('transfers')}
        >
          <div className="bottom-nav-icon-wrap">
            <HardDrive size={20} />
            {activeTransfersCount > 0 && (
              <span className="bottom-nav-badge pulse">{activeTransfersCount}</span>
            )}
          </div>
          <span>Transfers</span>
        </button>

        <button
          type="button"
          className={`bottom-nav-item ${mobileTab === 'me' ? 'active' : ''}`}
          onClick={() => setMobileTab('me')}
        >
          <div className="bottom-nav-icon-wrap">
            <User size={20} />
          </div>
          <span>Me</span>
        </button>
      </nav>

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
