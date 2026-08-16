# ⚡ Kawerify Transfer

Kawerify Transfer is a premium, high-performance, offline file-sharing platform designed to transfer files between laptops, phones, and other devices at maximum network speed. Fully optimized for local area networks, it works completely offline without requiring any internet connection.

Developed by **Kawerify Tech** (visit us at [kawerifytech.com](https://kawerifytech.com)).

---

## 🚀 Key Features

* **Zero Internet Required**: Works fully offline on local Wi-Fi networks or mobile hotspots.
* **Maximum Transfer Speeds**: Transfers are limited only by your local Wi-Fi router's maximum bandwidth.
* **Universal Device Compatibility**: Transfer files from laptop-to-phone, phone-to-laptop, or laptop-to-laptop.
* **Auto IP & QR Code Discovery**: Generates connection URLs and a QR code instantly for quick mobile pairing.
* **Zero Memory Streaming**: Uses chunked Node.js HTTP streams to transfer files of any size (even 10GB+) without exhausting server memory.
* **Real-time Peer Management**: Connects devices instantly via WebSocket rooms with dynamic names and avatars.
* **Premium User Experience**: Visual dashboard with fluid animations, drag-and-drop support, dark theme, and progress bars with speed (MB/s) and ETA calculations.

---

## 🛠️ Architecture & Tech Stack

Kawerify Transfer utilizes a robust and modern stack optimized for speed and compatibility:

1. **Backend**:
   - **Node.js & Express**: High-speed local web server.
   - **WebSockets (`ws`)**: Low-latency duplex communication channel for active peer signaling and events.
   - **Multer / Streaming Storage**: Streams file uploads directly to the disk rather than buffering them in RAM.
   
2. **Frontend**:
   - **React (Vite + TypeScript)**: Fast rendering, interactive single-page application.
   - **Vanilla CSS**: Premium dark-mode layout with custom glassmorphism and animated components.
   - **Lucide Icons & QRCode.js**: Interface iconography and client-side pairing code generation.

3. **Multi-Peer Discovery**:
   - The sender starts a local Node.js server.
   - The server lists all active IPv4 network interface addresses.
   - Receivers connect to the sender's local IP address via their web browsers. Once connected, they are enrolled in the local WebSocket signaling lobby and can exchange files instantly.

---

## 📦 Getting Started

### Prerequisites
Make sure you have [Node.js](https://nodejs.org) (v16+) installed.

### Setup and Start Server
1. Clone the repository to your local machine.
2. In the root directory, install server dependencies:
   ```bash
   npm install
   ```
3. Run the server:
   ```bash
   npm start
   ```
4. The terminal will print the active local URLs, for example:
   ```
   🚀 KAWERIFY TRANSFER SERVER IS RUNNING OFFLINE
   📡 Port: 5000
   📌 Connect to this server from any device on Wi-Fi:
      👉 http://192.168.1.5:5000
   ```

### Connecting Other Devices
1. Ensure all laptops, phones, or tablets are connected to the **same Wi-Fi network** or the host laptop's Wi-Fi hotspot.
2. Scan the QR code shown on the host device's dashboard, or type the printed URL (e.g., `http://192.168.1.5:5000`) into the browser on any device.
3. Once connected, the devices will appear on the dashboard under **Devices Nearby**.
4. Select the target device, drag-and-drop your files, and click **Share Now**.

---

## 📜 Licenses

Kawerify Transfer is a free, open-source tool for everyone to use. To guarantee flexibility and maximum freedom, this repository is multi-licensed under **8 open source licenses**. The full text of each license can be found in the [`licenses/`](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses) folder:

1. **[MIT License](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-MIT)**: Permits modification, distribution, private use, and commercial use.
2. **[Apache License 2.0](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-APACHE)**: Provides copyright and patent protections.
3. **[GNU GPL v3](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-GPL)**: Strong copyleft license requiring source code disclosure of modifications.
4. **[GNU AGPL v3](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-AGPL)**: Extends GPL copyleft to network services and server contexts.
5. **[BSD 3-Clause](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-BSD3)**: Simple permissive license with restrictions on advertising names.
6. **[Mozilla Public License 2.0](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-MPL)**: Weak copyleft license permitting combinations with proprietary code.
7. **[The Unlicense](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-UNLICENSE)**: Dedicated entirely to the public domain with no restrictions.
8. **[Creative Commons Zero v1.0 Universal (CC0)](file:///c:/Users/Tonde/Downloads/kawerify-transfer/licenses/LICENSE-CC0)**: Public domain dedication with global waiver.

Feel free to choose the license that best fits your packaging, distribution, or commercial integration requirements.

---

*Kawerify Transfer is engineered with pride by the team at **Kawerify Tech**.*  
*Website: [kawerifytech.com](https://kawerifytech.com)*
