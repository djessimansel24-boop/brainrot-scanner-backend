const express = require('express');
const cors = require('cors');
const app = express();

app.use(cors());
app.use(express.json());

// ========================================
// 🔔 BRAINROT NOTIFIER BACKEND v1.0
// Reçoit les alertes des scanners
// Les sert aux clients (app/web)
// ========================================

const API_KEY = process.env.API_KEY;
let pendingAlerts = [];
const MAX_ALERTS = 100;

// ========================================
// AUTH & LOG
// ========================================
function auth(req, res, next) {
    const key = req.headers['x-api-key'];
    if (!key || key !== API_KEY) {
        return res.status(401).json({ success: false, error: 'Unauthorized' });
    }
    next();
}

function log(emoji, message) {
    const time = new Date().toISOString().split('T')[1].split('.')[0];
    console.log(`[${time}] ${emoji} ${message}`);
}

// ========================================
// POST /alert — Reçoit les brainrots des scanners
// ========================================
app.post('/alert', auth, (req, res) => {
    const { brainrots, brainrotName, value, serverId, placeId, players, priority, botId } = req.body;

    // Format v9: liste complète de brainrots
    if (brainrots && Array.isArray(brainrots) && brainrots.length > 0) {
        let counter = 0;
        for (const b of brainrots) {
            pendingAlerts.push({
                id: `${Date.now()}_${counter}_${Math.random().toString(36).substr(2, 9)}`,
                brainrotName: b.brainrotName,
                value: b.value,
                serverId, placeId,
                timestamp: Date.now() + counter,
                players: players || 0,
                priority: b.priority || priority || 3,
                botId: botId || 'unknown'
            });
            counter++;
            log('🚨', `${b.brainrotName} (${b.value}) [${counter}/${brainrots.length}] from ${botId}`);
        }

        if (pendingAlerts.length > MAX_ALERTS) pendingAlerts = pendingAlerts.slice(-MAX_ALERTS);
        res.json({ success: true, count: brainrots.length });
        return;
    }

    // Format simple: 1 seul brainrot
    if (brainrotName) {
        pendingAlerts.push({
            id: `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
            brainrotName, value, serverId, placeId,
            timestamp: Date.now(),
            players: players || 0,
            priority: priority || 3,
            botId: botId || 'unknown'
        });

        if (pendingAlerts.length > MAX_ALERTS) pendingAlerts = pendingAlerts.slice(-MAX_ALERTS);
        log('🚨', `${brainrotName} (${value}) from ${botId}`);
        res.json({ success: true });
        return;
    }

    res.status(400).json({ success: false, error: 'No brainrot data' });
});

// ========================================
// GET /alerts — Les clients récupèrent les alertes
// ========================================
app.get('/alerts', (req, res) => {
    const since = parseInt(req.query.since) || 0;
    res.json({
        success: true,
        serverTime: Date.now(),
        alerts: pendingAlerts.filter(a => a.timestamp > since)
    });
});

// ========================================
// GET /health & /
// ========================================
app.get('/health', (req, res) => {
    res.json({
        success: true,
        version: '1.0',
        pendingAlerts: pendingAlerts.length
    });
});

app.get('/', (req, res) => {
    res.json({
        service: 'Brainrot Notifier v1.0',
        routes: {
            'POST /alert': 'Receive brainrot alerts from scanners',
            'GET /alerts?since=timestamp': 'Get alerts since timestamp',
            'GET /health': 'Health check'
        }
    });
});

// ========================================
// START
// ========================================
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`🔔 Brainrot Notifier v1.0 | Port ${PORT} | Auth: ${API_KEY ? 'ON' : 'OFF'}`);
});
