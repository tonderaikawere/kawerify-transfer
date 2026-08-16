import React, { useState, useEffect, useRef } from 'react';
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
  Users
} from 'lucide-react';
import QRCode from 'qrcode';

interface Peer {
  id: string;
  name: string;
  avatar: number;
}

interface Transfer {
  id: string;
  name: string;
  size: number;
  progress: number;
  speed: string; // in MB/s
  status: 'uploading' | 'waiting' | 'downloading' | 'completed' | 'failed';
  role: 'sender' | 'receiver';
  peerName: string;
  eta: string;
}

export default function App() {
  const [me, setMe] = useState<Peer | null>(null);
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [peers, setPeers] = useState<Peer[]>([]);
  const [selectedPeerId, setSelectedPeerId] = useState<string>('');
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [transfers, setTransfers] = useState<Transfer[]>([]);
  const [ips, setIps] = useState<string[]>([]);
  const [port, setPort] = useState<number>(5000);
  const [toast, setToast] = useState<string>('');
  const [qrCodeUrl, setQrCodeUrl] = useState<string>('');
  const [incomingTransfer, setIncomingTransfer] = useState<{
    senderId: string;
    senderName: string;
    fileId: string;
    fileName: string;
    fileSize: number;
    fileType: string;
  } | null>(null);
  const [isDragActive, setIsDragActive] = useState<boolean>(false);
  const [customName, setCustomName] = useState<string>('');

  const ws = useRef<WebSocket | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const activeXhrs = useRef<Map<string, XMLHttpRequest>>(new Map());

  // Determine server APIs URL
  const serverUrl = window.location.port === '5173' 
    ? 'http://localhost:5000' 
    : `${window.location.protocol}//${window.location.hostname}:${window.location.port || '5000'}`;

  const wsUrl = window.location.port === '5173'
    ? 'ws://localhost:5000'
    : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.hostname}:${window.location.port || '5000'}`;

  // Fetch server IP information
  useEffect(() => {
    fetch(`${serverUrl}/api/ips`)
      .then(res => res.json())
      .then(data => {
        setIps(data.ips || []);
        setPort(data.port || 5000);
        
        // Generate QR code for first IP
        if (data.ips && data.ips.length > 0) {
          const firstIp = data.ips[0];
          const connectUrl = `http://${firstIp}:${data.port}`;
          QRCode.toDataURL(connectUrl)
            .then(url => setQrCodeUrl(url))
            .catch(err => console.error('QR code generation failed:', err));
        }
      })
      .catch(err => {
        console.error('Failed to fetch server IPs:', err);
        // Fallback for standalone frontend dev
        setIps(['127.0.0.1']);
      });
  }, [serverUrl]);

  // Connect WebSocket
  useEffect(() => {
    connectWS();
    return () => {
      if (ws.current) ws.current.close();
    };
  }, []);

  const connectWS = () => {
    const socket = new WebSocket(wsUrl);
    ws.current = socket;

    socket.onopen = () => {
      console.log('Connected to WebSocket server');
      setWsConnected(true);
    };

    socket.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        switch (data.type) {
          case 'welcome':
            setMe(data.peer);
            setCustomName(data.peer.name);
            break;

          case 'peers-update':
            // Update the lists of peers, filtering out ourself
            if (me) {
              setPeers(data.peers.filter((p: Peer) => p.id !== me.id));
            } else {
              setPeers(data.peers);
            }
            break;

          case 'transfer-offer':
            // Prompt receiver about incoming transfer
            setIncomingTransfer(data);
            break;

          case 'transfer-accepted':
            // Triggered when receiver clicks "Accept". Sender now knows file is downloaded.
            updateTransferStatus(data.fileId, 'completed');
            showToast('File transfer completed!');
            break;

          case 'transfer-canceled':
            // Sender or receiver cancelled
            updateTransferStatus(data.fileId, 'failed');
            showToast('Transfer canceled by remote peer.');
            break;

          default:
            break;
        }
      } catch (err) {
        console.error('Error parsing WS message:', err);
      }
    };

    socket.onclose = () => {
      console.log('WebSocket disconnected. Reconnecting...');
      setWsConnected(false);
      setTimeout(() => connectWS(), 3000);
    };

    socket.onerror = (err) => {
      console.error('WebSocket error:', err);
      setWsConnected(false);
    };
  };

  // Sync me once peers update
  useEffect(() => {
    if (me && peers.length > 0) {
      setPeers(prev => prev.filter(p => p.id !== me.id));
    }
  }, [me]);

  // Helper to show copy success toast
  const showToast = (message: string) => {
    setToast(message);
    setTimeout(() => setToast(''), 3000);
  };

  const handleCopyLink = (ip: string) => {
    const link = `http://${ip}:${port}`;
    navigator.clipboard.writeText(link);
    showToast(`Link copied: ${link}`);
  };

  const handleRename = () => {
    if (ws.current && ws.current.readyState === WebSocket.OPEN && customName.trim().length > 0) {
      ws.current.send(JSON.stringify({
        type: 'rename',
        name: customName
      }));
      if (me) setMe({ ...me, name: customName });
      showToast('Device name updated!');
    }
  };

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setIsDragActive(true);
    } else if (e.type === "dragleave") {
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

  const triggerFileInput = () => {
    fileInputRef.current?.click();
  };

  const clearSelectedFile = () => {
    setSelectedFile(null);
  };

  const updateTransferStatus = (
    id: string, 
    status: Transfer['status'], 
    updates: Partial<Transfer> = {}
  ) => {
    setTransfers(prev => prev.map(t => t.id === id ? { ...t, status, ...updates } : t));
  };

  const handleSendFile = () => {
    if (!selectedFile || !selectedPeerId) {
      showToast('Please select a file and a destination device');
      return;
    }

    const file = selectedFile;
    const targetPeer = peers.find(p => p.id === selectedPeerId);
    if (!targetPeer) return;

    // Create a client-side transfer tracking item
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
      eta: '--:--'
    };

    setTransfers(prev => [newTransfer, ...prev]);
    setSelectedFile(null); // Clear search field

    // Start HTTP Streaming upload using native XMLHttpRequest
    const xhr = new XMLHttpRequest();
    activeXhrs.current.set(fileId, xhr);

    const formData = new FormData();
    formData.append('file', file);

    let startTime = Date.now();


    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        const currentTime = Date.now();
        const duration = (currentTime - startTime) / 1000; // in seconds
        const loaded = event.loaded;
        const progress = Math.round((loaded / event.total) * 100);

        let speedText = '0.00';
        let etaText = '--:--';

        if (duration > 0) {
          // calculate instantaneous speed
          const speedBps = loaded / duration;
          const speedMBps = speedBps / (1024 * 1024);
          speedText = speedMBps.toFixed(2);

          const remainingBytes = event.total - loaded;
          const remainingSeconds = speedBps > 0 ? remainingBytes / speedBps : 0;
          
          if (remainingSeconds < 60) {
            etaText = `${Math.ceil(remainingSeconds)}s remaining`;
          } else {
            const mins = Math.floor(remainingSeconds / 60);
            const secs = Math.ceil(remainingSeconds % 60);
            etaText = `${mins}m ${secs}s remaining`;
          }
        }

        updateTransferStatus(fileId, 'uploading', {
          progress,
          speed: speedText,
          eta: etaText
        });
      }
    };

    xhr.onload = () => {
      if (xhr.status === 200) {
        const response = JSON.parse(xhr.responseText);
        
        // Update transfer to waiting for receiver to download
        updateTransferStatus(fileId, 'waiting', {
          progress: 100,
          speed: 'Done',
          eta: 'Waiting for recipient'
        });

        // Send websocket offer containing server file link to receiver
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
  };

  const handleAcceptTransfer = () => {
    if (!incomingTransfer) return;

    const transfer = incomingTransfer;
    setIncomingTransfer(null);

    // Create a local download tracking item
    const newTransfer: Transfer = {
      id: transfer.fileId,
      name: transfer.fileName,
      size: transfer.fileSize,
      progress: 50, // Indeterminate progress simulation during native browser download
      speed: '--',
      status: 'downloading',
      role: 'receiver',
      peerName: transfer.senderName,
      eta: 'Downloading'
    };

    setTransfers(prev => [newTransfer, ...prev]);

    // Send accept notification on WS
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({
        type: 'transfer-accept',
        senderId: transfer.senderId,
        fileId: transfer.fileId
      }));
    }

    // Trigger browser file download stream
    const link = document.createElement('a');
    link.href = `${serverUrl}/api/download/${transfer.fileId}`;
    link.setAttribute('download', transfer.fileName);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    // After triggering download, mark download complete locally
    setTimeout(() => {
      updateTransferStatus(transfer.fileId, 'completed', {
        progress: 100,
        speed: 'Success',
        eta: 'Completed'
      });
    }, 3000);
  };

  const handleRejectTransfer = () => {
    if (!incomingTransfer) return;

    // Send cancel notification
    if (ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({
        type: 'transfer-cancel',
        targetId: incomingTransfer.senderId,
        fileId: incomingTransfer.fileId
      }));
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

    updateTransferStatus(transfer.id, 'failed', { speed: '0.00', eta: 'Cancelled' });

    // Find the peer we are talking to
    const targetPeer = peers.find(p => p.name === transfer.peerName);
    if (targetPeer && ws.current && ws.current.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify({
        type: 'transfer-cancel',
        targetId: targetPeer.id,
        fileId: transfer.id
      }));
    }
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
    // Return avatars based on seeds
    const avatars = ['🦁', '🦊', '🐻', '🐼', '🐨', '🐯', '🐰', '🦅'];
    return avatars[(num - 1) % avatars.length] || '🐱';
  };

  return (
    <div className="app-container">
      {/* Toast Notification */}
      {toast && <div className="toast-msg">{toast}</div>}

      {/* Header */}
      <header className="app-header">
        <h1 className="brand-title">
          ⚡ Kawerify <span>Transfer</span>
        </h1>
        <p className="brand-subtitle">
          Superfast, offline local-network file transfers by <a href="https://kawerifytech.com" target="_blank" rel="noopener noreferrer">Kawerify Tech</a>
        </p>
      </header>

      {/* Disconnection Warning Banner */}
      {!wsConnected && (
        <div style={{
          backgroundColor: 'rgba(239, 68, 68, 0.15)',
          border: '1px solid var(--accent-red)',
          color: 'var(--accent-red)',
          padding: '0.75rem 1rem',
          borderRadius: 'var(--border-radius-sm)',
          fontSize: '0.85rem',
          fontWeight: 500,
          textAlign: 'center',
          marginBottom: '1.5rem',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: '0.75rem'
        }}>
          <span className="no-peers-pulse" style={{ backgroundColor: 'var(--accent-red)', margin: 0, animationDuration: '1s' }}></span>
          <span><strong>Local Server Disconnected.</strong> Please run <code>npm start</code> on your laptop to start offline transfers.</span>
        </div>
      )}

      {/* Grid Dashboard */}
      <div className="app-grid">
        {/* Left Column: Device Info & Peer Discovery */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          {/* Device Profile Card */}
          <div className="panel-card">
            <h2 className="panel-title">
              <User size={20} /> My Profile
            </h2>
            {me && (
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
                      title="Click to change name"
                    />
                    <div className="device-label">Local Host Peer</div>
                  </div>
                </div>
                <button className="btn btn-secondary" style={{ padding: '0.4rem' }} onClick={handleRename} title="Save profile name">
                  <Check size={16} />
                </button>
              </div>
            )}
            
            <div className="ip-panel">
              <div style={{ fontSize: '0.85rem', color: 'var(--text-secondary)', fontWeight: 600 }}>
                Server Local URLs:
              </div>
              <div className="ip-list">
                {ips.length > 0 ? (
                  ips.map(ip => (
                    <div key={ip} className="ip-item">
                      <span>IP Link:</span>
                      <span className="ip-value" onClick={() => handleCopyLink(ip)}>
                        http://{ip}:{port} <Copy size={12} style={{ marginLeft: '4px', verticalAlign: 'middle' }} />
                      </span>
                    </div>
                  ))
                ) : (
                  <div className="ip-item" style={{ color: 'var(--text-muted)' }}>
                    Scanning interfaces...
                  </div>
                )}
              </div>
            </div>

            {qrCodeUrl && (
              <div className="qr-section">
                <div className="qr-canvas-wrapper">
                  <img src={qrCodeUrl} alt="Share QR Code" style={{ width: '130px', height: '130px', display: 'block' }} />
                </div>
                <p className="qr-caption">Scan with another device on the same network to transfer files instantly</p>
              </div>
            )}
          </div>

          {/* Discovery Card */}
          <div className="panel-card">
            <h2 className="panel-title">
              <Users size={20} /> Devices Nearby
            </h2>
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
                        <div className="peer-status">Ready to receive</div>
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
                  <p style={{ fontSize: '0.75rem', marginTop: '0.25rem' }}>Open this page on another laptop/phone on this network</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Right Column: File Sender & Active Transfers */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
          {/* File Transfer Send Card */}
          <div className="panel-card">
            <h2 className="panel-title">
              <Share2 size={20} /> Send Files
            </h2>
            
            {/* Drag & Drop Zone */}
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
                <div className="drag-drop-title">Drag & Drop files here</div>
                <div className="drag-drop-desc">or click to browse local storage</div>
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
                  <option key={p.id} value={p.id}>{p.name}</option>
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

          {/* Transfers History Card */}
          <div className="panel-card" style={{ flexGrow: 1 }}>
            <h2 className="panel-title">
              <RefreshCw size={20} /> Active Transfers
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
                        <span className={`transfer-badge ${transfer.role}`}>
                          {transfer.role}
                        </span>
                        <span className="file-name" style={{ maxWidth: '180px' }} title={transfer.name}>
                          {transfer.name}
                        </span>
                      </div>
                      
                      {/* Control buttons */}
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
                      <span>Size: {formatBytes(transfer.size)}</span>
                      <div className="transfer-stats-right">
                        <span>Speed: {transfer.speed} MB/s</span>
                        <span style={{ color: 'var(--text-muted)' }}>|</span>
                        <span>{transfer.eta}</span>
                      </div>
                    </div>
                  </div>
                ))
              ) : (
                <div style={{ padding: '2rem 1rem', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                  No transfers yet. Drop a file and choose a device to start.
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Incoming File Prompt Dialog */}
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
                Accept & Download
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

