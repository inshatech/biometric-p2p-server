'use strict';

require('dotenv').config();

const express    = require('express');
const hyperswarm = require('hyperswarm');
const crypto     = require('crypto');
const mongoose   = require('mongoose');
const { v4: uuidv4 } = require('uuid');

// ─── GLOBAL CRASH PROTECTION ─────────────────────────────────────────────────
process.on('uncaughtException',  err    => console.error('🔥 [CRITICAL] Uncaught Exception:', err.stack || err));
process.on('unhandledRejection', reason => console.error('🔥 [CRITICAL] Unhandled Rejection:', reason));

const app = express();
app.use(express.json({ limit: '50mb' }));

const PORT          = process.env.PORT      || 8000;
const TOPIC_KEY     = process.env.P2P_TOPIC || 'your-secure-p2p-key-2026';
const MONGO_URI     = process.env.MONGO_URI || 'mongodb+srv://Pass:cxLzQyhiSYQlXimQ@cluster0.v2uaqru.mongodb.net/attendance';
const MSG_DELIMITER = '\n';

// ─── DATABASE ─────────────────────────────────────────────────────────────────
mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 30_000 })
    .then(() => console.log('💾 MongoDB Connected'))
    .catch(err => console.error('❌ DB Error:', err));

mongoose.connection.on('disconnected', () => console.warn('⚠️  MongoDB disconnected — reconnecting…'));
mongoose.connection.on('reconnected',  () => console.log('✅ MongoDB reconnected.'));

const bridgeSchema = new mongoose.Schema({
    bridgeId: { type: String, unique: true, required: true },
    name:     String,
    status:   { type: String, default: 'offline' },
    lastSeen: Date,
});

const deviceSchema = new mongoose.Schema({
    bridgeId: { type: String, required: true },
    name:     String,
    ip:       { type: String, required: true },
    port:     { type: Number, default: 4370 },
    deviceId: { type: String, unique: true },
});

const Bridge     = mongoose.model('Bridge',       bridgeSchema);
const Device     = mongoose.model('Device',       deviceSchema);
const User       = mongoose.model('machine_user', new mongoose.Schema({ bridgeId: String, deviceId: String, uid: Number, userId: String, name: String }, { strict: false }));
const Attendance = mongoose.model('attendance_log', new mongoose.Schema({ bridgeId: String, deviceId: String, userId: String, timestamp: Date }, { strict: false }));

// ─── P2P STATE ────────────────────────────────────────────────────────────────
/**
 * bridgeMap       : bridgeId → { socket, queue, deviceStatuses: Map<deviceId, 'online'|'offline'> }
 * pendingRequests : reqId    → { resolve, reject, buffer }
 * sseClients      : Array<{ bridgeId, deviceId|null, res }>
 */
const bridgeMap       = new Map();
const pendingRequests = new Map();
let   sseClients      = [];

// ─── HELPERS ──────────────────────────────────────────────────────────────────
function socketSend(socket, payload) {
    if (socket?.writable) {
        try {
            socket.write(JSON.stringify(payload) + MSG_DELIMITER);
        } catch (err) {
            console.error('❌ socketSend error:', err.message);
        }
    }
}

function pushSSE(payload) {
    sseClients = sseClients.filter(c => {
        const match =
            c.bridgeId === payload.bridgeId &&
            (!c.deviceId || c.deviceId === payload.deviceId);
        if (!match) return true;
        try {
            c.res.write(`data: ${JSON.stringify(payload)}\n\n`);
            return true;
        } catch {
            return false; // Remove dead client
        }
    });
}

// ─── P2P SWARM ────────────────────────────────────────────────────────────────
const swarm = new hyperswarm();
const topic = crypto.createHash('sha256').update(TOPIC_KEY).digest();

swarm.join(topic, { server: true, client: true });

swarm.on('connection', socket => {
    let currentBridgeId = null;
    let frameBuffer     = '';
    let authorized      = false;

    socket.setKeepAlive(true, 5_000);

    // Kill unidentified connections after 60 s
    // Cleared BEFORE the async DB check so a slow DB can never race it.
    const authTimeout = setTimeout(() => {
        if (!authorized) {
            console.warn('⚠️  Closing unidentified/idle P2P connection.');
            socket.destroy();
        }
    }, 60_000);

    // ── Framed message parsing ────────────────────────────────────────────────
    socket.on('data', data => {
        frameBuffer += data.toString();
        const frames = frameBuffer.split(MSG_DELIMITER);
        frameBuffer  = frames.pop();

        for (const frame of frames) {
            if (!frame.trim()) continue;
            handleFrame(frame).catch(err =>
                console.error('⚠️  Frame handler error:', err.message)
            );
        }
    });

    async function handleFrame(frame) {
        let msg;
        try { msg = JSON.parse(frame); }
        catch { return console.error('⚠️  Malformed frame, ignoring.'); }

        // ── IDENTIFY ──────────────────────────────────────────────────────────
        if (msg.type === 'IDENTIFY') {
            clearTimeout(authTimeout); // Clear BEFORE any async work

            let bridgeRecord;
            try { bridgeRecord = await Bridge.findOne({ bridgeId: msg.bridgeId }); }
            catch (dbErr) {
                console.error('❌ DB lookup failed during IDENTIFY:', dbErr.message);
                return socket.destroy();
            }

            if (!bridgeRecord) {
                console.error(`🛑 Unregistered bridge rejected: ${msg.bridgeId}`);
                return socket.destroy();
            }

            authorized      = true;
            currentBridgeId = msg.bridgeId;
            console.log(`📡 Bridge authorized: ${currentBridgeId}`);

            bridgeMap.set(currentBridgeId, {
                socket,
                queue:          Promise.resolve(),
                deviceStatuses: new Map(),
            });

            await Bridge.findOneAndUpdate(
                { bridgeId: currentBridgeId },
                { status: 'online', lastSeen: new Date() }
            ).catch(() => {});
            return;
        }

        // Ignore everything until authorized
        if (!currentBridgeId || msg.type === 'KEEPALIVE') return;

        // ── REALTIME_PUNCH ────────────────────────────────────────────────────
        if (msg.type === 'REALTIME_PUNCH') {
            pushSSE({ ...msg.log, bridgeId: currentBridgeId, deviceId: msg.deviceId });
            return;
        }

        // ── DEVICE_STATUS ─────────────────────────────────────────────────────
        if (msg.type === 'DEVICE_STATUS') {
            const bridge = bridgeMap.get(currentBridgeId);
            if (bridge) bridge.deviceStatuses.set(msg.deviceId, msg.status);

            pushSSE({
                type:     'DEVICE_STATUS',
                bridgeId: currentBridgeId,
                deviceId: msg.deviceId,
                status:   msg.status,
                error:    msg.error || null,
            });
            console.log(`📟 [${currentBridgeId}] ${msg.deviceId} → ${msg.status}${msg.error ? ' (' + msg.error + ')' : ''}`);
            return;
        }

        // ── BRIDGE_READY ──────────────────────────────────────────────────────
        if (msg.type === 'BRIDGE_READY') {
            const bridge = bridgeMap.get(currentBridgeId);
            if (bridge && Array.isArray(msg.devices)) {
                for (const d of msg.devices) {
                    bridge.deviceStatuses.set(d.deviceId, d.online ? 'online' : 'offline');
                }
            }
            return;
        }

        // ── CONFIG_ACK ────────────────────────────────────────────────────────
        if (msg.type === 'CONFIG_ACK') return; // No action needed

        // ── EXEC_RESULT / DATA_CHUNK / ERROR ──────────────────────────────────
        if (msg.reqId && pendingRequests.has(msg.reqId)) {
            const state = pendingRequests.get(msg.reqId);

            if (msg.type === 'DATA_CHUNK') {
                state.buffer.push(...msg.data);
                if (msg.isLast) { state.resolve(state.buffer); pendingRequests.delete(msg.reqId); }

            } else if (msg.type === 'EXEC_RESULT') {
                const resolved = msg.result?.data !== undefined ? msg.result.data : msg.result;
                state.resolve(resolved);
                pendingRequests.delete(msg.reqId);

            } else if (msg.type === 'ERROR') {
                state.reject(new Error(msg.message));
                pendingRequests.delete(msg.reqId);
            }
        }
    }

    socket.on('error', err => {
        if (['ECONNRESET', 'EPIPE'].includes(err.code)) return;
        console.error(`🌐 Socket Error [${currentBridgeId || 'unknown'}]:`, err.message);
    });

    socket.on('close', async () => {
        clearTimeout(authTimeout);
        if (!currentBridgeId) return;
        console.log(`❌ Bridge offline: ${currentBridgeId}`);
        bridgeMap.delete(currentBridgeId);
        await Bridge.findOneAndUpdate({ bridgeId: currentBridgeId }, { status: 'offline' }).catch(() => {});
    });
});

// ─── TUNNELING ENGINE ─────────────────────────────────────────────────────────
function tunnel(bridgeId, deviceId, method, params = []) {
    return new Promise((resolve, reject) => {
        const bridge = bridgeMap.get(bridgeId);
        if (!bridge?.socket?.writable) {
            return reject(new Error(`Bridge '${bridgeId}' is offline`));
        }

        const reqId   = uuidv4();
        const timeout = setTimeout(() => {
            pendingRequests.delete(reqId);
            reject(new Error(`Timeout: ${bridgeId}/${deviceId}/${method}`));
        }, 120_000);

        pendingRequests.set(reqId, {
            resolve: data => { clearTimeout(timeout); resolve(data); },
            reject:  err  => { clearTimeout(timeout); reject(err instanceof Error ? err : new Error(err)); },
            buffer:  [],
        });

        socketSend(bridge.socket, { action: 'EXECUTE', deviceId, method, params, reqId });
    });
}

/**
 * Serialise commands per bridge with a 150 ms gap between calls.
 * Prevents "Device busy" errors on the ZK hardware.
 */
function queueTunnel(bridgeId, deviceId, method, params = []) {
    const bridge = bridgeMap.get(bridgeId);
    if (!bridge) return Promise.reject(new Error(`Bridge '${bridgeId}' not connected`));

    const next = bridge.queue.then(async () => {
        const result = await tunnel(bridgeId, deviceId, method, params);
        await new Promise(r => setTimeout(r, 150));
        return result;
    });

    bridge.queue = next.catch(() => {}); // Never poison the queue chain
    return next;
}

// ─── MIDDLEWARE ───────────────────────────────────────────────────────────────
function requireBody(...keys) {
    return (req, res, next) => {
        for (const k of keys) {
            if (req.body[k] === undefined || req.body[k] === null || req.body[k] === '') {
                return res.status(400).json({ error: `Missing required body field: ${k}` });
            }
        }
        next();
    };
}

// ─── BRIDGE ADMIN ─────────────────────────────────────────────────────────────
app.get('/api/admin/bridges', async (req, res) => {
    try {
        const bridges = await Bridge.find().lean();
        const enriched = bridges.map(b => {
            const live = bridgeMap.get(b.bridgeId);
            return {
                ...b,
                status:  live ? 'online' : 'offline',
                devices: live ? Object.fromEntries(live.deviceStatuses) : {},
            };
        });
        res.json({ status: 'success', data: enriched });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/bridges', async (req, res) => {
    try {
        const bridge = await Bridge.create({
            bridgeId: `br-${uuidv4().split('-')[0]}`,
            name:     req.body.name || 'New Office Branch',
        });
        res.status(201).json({ status: 'success', data: bridge });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/admin/bridges/:bridgeId', async (req, res) => {
    try {
        await Bridge.deleteOne({ bridgeId: req.params.bridgeId });
        await Device.deleteMany({ bridgeId: req.params.bridgeId });
        res.json({ status: 'success', message: 'Bridge and its devices removed' });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── DEVICE ADMIN ─────────────────────────────────────────────────────────────
app.get('/api/admin/:bridgeId/devices', async (req, res) => {
    try {
        const devices = await Device.find({ bridgeId: req.params.bridgeId }).lean();
        const bridge  = bridgeMap.get(req.params.bridgeId);
        const enriched = devices.map(d => ({
            ...d,
            online: bridge?.deviceStatuses.get(d.deviceId) === 'online',
        }));
        res.json({ status: 'success', data: enriched });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/admin/devices', requireBody('bridgeId', 'ip'), async (req, res) => {
    try {
        const { bridgeId, name, ip, port } = req.body;
        const device = await Device.create({
            bridgeId, name, ip,
            port:     port || 4370,
            deviceId: `dev-${uuidv4().split('-')[0]}`,
        });

        const bridge = bridgeMap.get(bridgeId);
        if (bridge?.socket?.writable) {
            socketSend(bridge.socket, {
                action: 'CONNECT_DEVICE',
                device: { ip: device.ip, port: device.port, deviceId: device.deviceId },
            });
        }

        res.status(201).json({ status: 'success', data: device });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/admin/devices/:deviceId', async (req, res) => {
    try {
        const device = await Device.findOneAndDelete({ deviceId: req.params.deviceId });
        if (!device) return res.status(404).json({ error: 'Device not found' });

        const bridge = bridgeMap.get(device.bridgeId);
        if (bridge?.socket?.writable) {
            socketSend(bridge.socket, { action: 'DISCONNECT_DEVICE', deviceId: device.deviceId });
        }

        res.json({ status: 'success' });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── STATUS ENDPOINTS ─────────────────────────────────────────────────────────
app.get('/api/:bridgeId/status', (req, res) => {
    const bridge  = bridgeMap.get(req.params.bridgeId);
    const devices = bridge ? Object.fromEntries(bridge.deviceStatuses) : {};
    res.json({ status: 'success', data: { bridgeId: req.params.bridgeId, online: !!bridge, devices } });
});

app.get('/api/:bridgeId/:deviceId/status', (req, res) => {
    const { bridgeId, deviceId } = req.params;
    const bridge = bridgeMap.get(bridgeId);
    const online = bridge?.deviceStatuses.get(deviceId) === 'online';
    res.json({ status: 'success', data: { bridgeId, deviceId, online } });
});

// ─── ATTENDANCE ───────────────────────────────────────────────────────────────
app.get('/api/:bridgeId/:deviceId/attendance', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const logs = await queueTunnel(bridgeId, deviceId, 'getAttendances');
        let filtered = Array.isArray(logs) ? logs : [];

        const { startDate, endDate, userId } = req.query;

        if (userId) {
            filtered = filtered.filter(log => String(log.user_id) === String(userId));
        }

        if (startDate) {
            const endVal     = endDate || startDate;
            const startLimit = new Date(`${startDate}T00:00:00+05:30`).getTime();
            const endLimit   = new Date(`${endVal}T23:59:59.999+05:30`).getTime();
            filtered = filtered.filter(log => {
                const t = new Date(log.timestamp).getTime();
                return t >= startLimit && t <= endLimit;
            });
        }

        res.json({ status: 'success', count: filtered.length, data: filtered });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.get('/api/:bridgeId/:deviceId/attendance/size', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const size = await queueTunnel(bridgeId, deviceId, 'getAttendanceSize');
        res.json({ status: 'success', total_records: size });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.delete('/api/:bridgeId/:deviceId/attendance/clear', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        await queueTunnel(bridgeId, deviceId, 'clearAttendanceLog');
        res.json({ status: 'success', message: 'Attendance logs wiped' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

// ─── USER MANAGEMENT ──────────────────────────────────────────────────────────
app.get('/api/:bridgeId/:deviceId/users', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const users = await queueTunnel(bridgeId, deviceId, 'getUsers');
        res.json({ status: 'success', data: users });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.post('/api/:bridgeId/:deviceId/users', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    const { uid, userid, name, password, role, cardno } = req.body;
    if (!uid) return res.status(400).json({ error: 'uid is required' });
    try {
        await queueTunnel(bridgeId, deviceId, 'setUser', uid, userid, name, password, role, cardno);
        res.status(201).json({ status: 'success', message: 'User created/updated' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.delete('/api/:bridgeId/:deviceId/users/:uid', async (req, res) => {
    const { bridgeId, deviceId, uid } = req.params;
    try {
        await queueTunnel(bridgeId, deviceId, 'deleteUser', uid);
        res.json({ status: 'success', message: `User ${uid} deleted` });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

// ─── DEVICE INFO & SYSTEM ─────────────────────────────────────────────────────
app.get('/api/:bridgeId/:deviceId/device/info', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const [info, name, version, os, platform, mac, vendor, productTime] = await Promise.all([
            queueTunnel(bridgeId, deviceId, 'getInfo'),
            queueTunnel(bridgeId, deviceId, 'getDeviceName'),
            queueTunnel(bridgeId, deviceId, 'getDeviceVersion'),
            queueTunnel(bridgeId, deviceId, 'getOS'),
            queueTunnel(bridgeId, deviceId, 'getPlatform'),
            queueTunnel(bridgeId, deviceId, 'getMacAddress'),
            queueTunnel(bridgeId, deviceId, 'getVendor'),
            queueTunnel(bridgeId, deviceId, 'getProductTime'),
        ]);
        res.json({ status: 'success', data: { name, version, os, platform, mac, vendor, productTime, stats: info } });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.get('/api/:bridgeId/:deviceId/device/settings', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const [pin, faceOn, ssr] = await Promise.all([
            queueTunnel(bridgeId, deviceId, 'getPIN'),
            queueTunnel(bridgeId, deviceId, 'getFaceOn'),
            queueTunnel(bridgeId, deviceId, 'getSSR'),
        ]);
        res.json({ status: 'success', data: { pin, faceOn, ssr } });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.get('/api/:bridgeId/:deviceId/device/time', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const time = await queueTunnel(bridgeId, deviceId, 'getTime');
        res.json({ status: 'success', data: { device_time: time } });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.put('/api/:bridgeId/:deviceId/device/time', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    if (!req.body.time) return res.status(400).json({ error: 'time is required' });
    try {
        await queueTunnel(bridgeId, deviceId, 'setTime', new Date(req.body.time));
        res.json({ status: 'success', message: 'Time updated' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

// ─── DIAGNOSTICS & MAINTENANCE ────────────────────────────────────────────────
app.post('/api/:bridgeId/:deviceId/device/voice-test', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        await queueTunnel(bridgeId, deviceId, 'voiceTest');
        res.json({ status: 'success', message: 'Voice test triggered' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.delete('/api/:bridgeId/:deviceId/device/factory-reset', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        await queueTunnel(bridgeId, deviceId, 'clearData');
        res.json({ status: 'success', message: 'All data (users + logs) cleared' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.post('/api/:bridgeId/:deviceId/connect', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        const result = await queueTunnel(bridgeId, deviceId, 'connect');
        res.json({ status: 'success', data: result });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

app.post('/api/:bridgeId/:deviceId/disconnect', async (req, res) => {
    const { bridgeId, deviceId } = req.params;
    try {
        await queueTunnel(bridgeId, deviceId, 'disconnect');
        res.json({ status: 'success', message: 'Disconnected' });
    } catch (err) { res.status(500).json({ error: err.toString() }); }
});

// ─── REAL-TIME SSE ────────────────────────────────────────────────────────────
function sseHandler(bridgeId, deviceId) {
    return (req, res) => {
        res.writeHead(200, {
            'Content-Type':      'text/event-stream',
            'Cache-Control':     'no-cache',
            'Connection':        'keep-alive',
            'X-Accel-Buffering': 'no',
        });
        res.flushHeaders?.();

        const client = { bridgeId, deviceId: deviceId || req.params.deviceId || null, res };
        sseClients.push(client);

        const heartbeat = setInterval(() => res.write(': heartbeat\n\n'), 20_000);
        req.on('close', () => {
            clearInterval(heartbeat);
            sseClients = sseClients.filter(c => c !== client);
        });
    };
}

// Device-scoped SSE — NOTE: must be registered before the bridge-scoped route
// or Express will match ":deviceId" = "attendance" on the bridge route.
app.get('/api/:bridgeId/:deviceId/attendance/realtime', (req, res) => {
    sseHandler(req.params.bridgeId, req.params.deviceId)(req, res);
});

// Bridge-scoped SSE (all devices)
app.get('/api/:bridgeId/attendance/realtime', (req, res) => {
    sseHandler(req.params.bridgeId, null)(req, res);
});

// ─── 404 HANDLER ──────────────────────────────────────────────────────────────
app.use((req, res) => res.status(404).json({ error: `Route not found: ${req.method} ${req.path}` }));

// ─── ERROR HANDLER ────────────────────────────────────────────────────────────
// eslint-disable-next-line no-unused-vars
app.use((err, req, res, next) => {
    console.error('🔥 Express Error:', err.stack || err);
    res.status(500).json({ error: 'Internal server error' });
});

// ─── GRACEFUL SHUTDOWN ────────────────────────────────────────────────────────
async function shutdown() {
    console.log('\n🛑 Shutting down server…');
    await swarm.destroy().catch(() => {});
    await mongoose.disconnect().catch(() => {});
    console.log('👋 Server stopped.');
    process.exit(0);
}

process.on('SIGINT',  shutdown);
process.on('SIGTERM', shutdown);

// ─── START ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => console.log(`🚀 Gateway active on port ${PORT}`));