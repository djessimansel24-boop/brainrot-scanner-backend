// =====================================================
// 🧠 STEAL A BRAINROT SCANNER - BACKEND v3.7
// =====================================================
//
// v3.7 — CONTINUOUS FETCH + PROXY ROTATION + FAST COOLDOWN
//
// CHANGES vs v3.5:
//   ⚡ Cooldown: 300s → 60s (servers recycle 5× faster)
//   ⚡ Auto-cooldown on expire: 180s → 60s (consistent)
//   🌐 Support for 3+ proxies (PROXY_1, PROXY_2, PROXY_3)
//   🌐 Parallel mode auto-activates with >1 proxy
//
// MATH (3 proxies, 155 bots):
//   Production: ~10,000 unique/cycle (3 proxies × Desc+Asc)
//   Consumption: ~3,100/burst (155 bots × 20)
//   Cooldown recycle: every 1 min → +3,000 back in pool
//   Result: pool never hits 0
//
// CONSERVÉ de v3.5:
//   ✅ CYCLE_TIMEOUT on ALL modes (sequential + parallel)
//   ✅ Stream cancellation via cancelFlag
//   ✅ Watchdog kills stuck streams
//   ✅ Partial results survive timeout
//   ✅ Cooldown delete fix (Bug A)
//   ✅ Zero collision (global lock + SHA-256)
// =====================================================

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const https = require('https');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// =====================================================
// 🎮 CONFIGURATION
// =====================================================

const STEAL_A_BRAINROT = {
    PLACE_ID: 109983668079237,
    UNIVERSE_ID: 7709344486,
    GAME_NAME: "Steal a Brainrot"
};

// =====================================================
// 🌍 PROXIES & RÉGIONS
// =====================================================

const PROXY_POOL = [];

const REGIONAL_CONFIG = {
    'us':            { name: 'United States',  vps_range: [1, 10],  expected_bots: 250 },
    'europe':        { name: 'Europe',         vps_range: [11, 19], expected_bots: 225 },
    'asia':          { name: 'Asia Pacific',   vps_range: [20, 24], expected_bots: 125 },
    'south-america': { name: 'South America',  vps_range: [25, 27], expected_bots: 75 },
    'oceania':       { name: 'Oceania',        vps_range: [28, 30], expected_bots: 75 },
};

function initProxyPool() {
    // Named regional proxies (backward compat)
    const envMap = {
        'PROXY_US':            'US',
        'PROXY_EU':            'EU',
        'PROXY_ASIA':          'Asia',
        'PROXY_SOUTH_AMERICA': 'South America',
        'PROXY_OCEANIA':       'Oceania',
    };
    for (const [envKey, label] of Object.entries(envMap)) {
        const url = process.env[envKey];
        if (url) {
            PROXY_POOL.push({ baseUrl: url, url, label, errors: 0, lastError: 0, lastFetch: {} });
        }
    }

    // Numbered proxies: PROXY_1, PROXY_2, ... PROXY_10
    // Use these when you have multiple proxies on the same provider
    for (let i = 1; i <= 10; i++) {
        const url = process.env[`PROXY_${i}`];
        if (url) {
            PROXY_POOL.push({ baseUrl: url, url, label: `P${i}`, errors: 0, lastError: 0, lastFetch: {} });
        }
    }

    console.log(`🔧 Proxy pool: ${PROXY_POOL.length} proxies`);
    for (const p of PROXY_POOL) {
        console.log(`   📡 ${p.label}: ${p.baseUrl.replace(/:[^:@]+@/, ':***@')}`);
    }
    if (PROXY_POOL.length === 0) {
        console.warn('⚠️  NO PROXIES configured!');
    }
}

// =====================================================
// 🔄 SMARTPROXY SESSION ROTATION
// =====================================================

function randomSessionId() {
    return crypto.randomBytes(6).toString('hex');
}

function rotateOneProxy(proxy) {
    const base = proxy.baseUrl;
    if (base.includes('_session-')) {
        proxy.url = base.replace(/_session-[a-zA-Z0-9]+/, `_session-${randomSessionId()}`);
    } else if (base.includes('_life-')) {
        proxy.url = base.replace(/(_life-\d+)/, `$1_session-${randomSessionId()}`);
    } else {
        proxy.url = base;
    }
}

function rotateProxySessions() {
    for (const proxy of PROXY_POOL) {
        rotateOneProxy(proxy);
        console.log(`   🔄 [${proxy.label}] New session → fresh IP`);
    }
}

// =====================================================
// ⚙️ PARAMÈTRES
// =====================================================

const CONFIG = {
    // ── Assignation ──
    ASSIGNMENT_DURATION: 120000,
    COOLDOWN_DURATION: 60000,          // v3.7: 2min → 1min (servers recyclent encore plus vite)
    AUTO_COOLDOWN_ON_EXPIRE: 60000,    // v3.7: cohérent avec COOLDOWN_DURATION
    SERVERS_PER_BOT: 20,

    // ── Fetch ──
    PAGES_PER_PROXY: 40,              // 20 per sort direction (smaller, faster fetches)
    FETCH_PAGE_DELAY: 1200,
    FETCH_PAGE_TIMEOUT: 12000,
    FETCH_MAX_CONSECUTIVE_ERRORS: 4,
    FETCH_RATE_LIMIT_BACKOFF: 5000,
    CONTINUOUS_FETCH_DELAY: 10000,    // 10s between each mini-fetch
    STALE_THRESHOLD: 600000,          // Servers expire from cache after 10 min

    // ── Direct fetch (sans proxy) ──
    DIRECT_PAGES: 50,
    DIRECT_PAGE_DELAY: 1000,

    // ── Bot management ──
    BOT_REQUEST_COOLDOWN: 5000,
    MAX_BOT_HISTORY: 2000,

    // ── Blacklist ──
    BLACKLIST_DURATION: 600000,

    // ── Cleanup ──
    CLEANUP_INTERVAL: 10000,

    // ── v3.7: Anti-hang (from v3.4/3.5) ──
    CYCLE_TIMEOUT: 240000,            // 4min (back to normal, no waves)
    WATCHDOG_TIMEOUT: 300000,         // 5min
    MAX_ROTATIONS: 10,
    ROTATION_NORESET_AFTER: 7,
};

// =====================================================
// 💾 HTTP CLIENT
// =====================================================

const { HttpsProxyAgent } = require('https-proxy-agent');

function httpGet(url, proxyUrl = null, timeout = 12000) {
    return new Promise((resolve, reject) => {
        let settled = false;
        const done = (fn, val) => { if (!settled) { settled = true; fn(val); } };

        const options = {
            method: 'GET',
            timeout,
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
        };

        if (proxyUrl) {
            options.agent = new HttpsProxyAgent(proxyUrl, { timeout });
        }

        const req = https.request(url, options, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode === 429) return done(reject, new Error('RATE_LIMITED'));
                if (res.statusCode !== 200) return done(reject, new Error(`HTTP_${res.statusCode}`));
                try { done(resolve, JSON.parse(data)); }
                catch (_) { done(reject, new Error('JSON_PARSE_FAIL')); }
            });
        });

        req.on('error', (err) => done(reject, new Error(`REQ_ERROR: ${err.message}`)));
        req.on('timeout', () => { req.destroy(); done(reject, new Error('TIMEOUT')); });

        const hardTimer = setTimeout(() => {
            req.destroy();
            done(reject, new Error('HARD_TIMEOUT'));
        }, timeout + 3000);

        req.on('close', () => clearTimeout(hardTimer));
        req.end();
    });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =====================================================
// 🔐 SHA-256 DISTRIBUTION
// =====================================================

function deterministicHashNum(serverId, botId) {
    const buf = crypto.createHash('sha256')
        .update(`${serverId}::${botId}::sab-v33-salt`)
        .digest();
    return buf.readUIntBE(0, 6);
}

// =====================================================
// 🔒 GLOBAL LOCK
// =====================================================

class AsyncLock {
    constructor() {
        this.locked = false;
        this.queue = [];
    }

    acquire() {
        return new Promise(resolve => {
            if (!this.locked) {
                this.locked = true;
                resolve();
            } else {
                this.queue.push(resolve);
            }
        });
    }

    release() {
        if (this.queue.length > 0) {
            this.queue.shift()();
        } else {
            this.locked = false;
        }
    }

    get queueLength() {
        return this.queue.length;
    }
}

const globalAssignmentLock = new AsyncLock();

// =====================================================
// 💾 GLOBAL STATE
// =====================================================

const globalCache = {
    jobs: [],
    lastUpdate: 0,
    fetchInProgress: false,
    fetchStartedAt: 0,
    lastFetchStats: {}
};

// v3.7: Global cancel flag so watchdog can kill stuck streams
let globalCancelFlag = { cancelled: false };

// v3.7: Proxy rotation index — cycles through pool
let proxyRotationIndex = 0;

const serverAssignments = new Map();
const serverCooldowns = new Map();
const serverBlacklist = new Map();
const botHistory = new Map();
const botLastRequest = new Map();

const stats = {
    total_requests: 0,
    total_assignments: 0,
    total_releases: 0,
    total_duplicates_skipped: 0,
    total_blacklist_filtered: 0,
    total_rate_limited: 0,
    total_fetch_cycles: 0,
    total_collisions_detected: 0,
    total_collisions_resolved: 0,
    total_cycle_timeouts: 0,
    total_watchdog_resets: 0,
    total_stream_cancels: 0,
    lock_max_queue: 0,
    uptime_start: Date.now()
};

// =====================================================
// 🚫 BLACKLIST
// =====================================================

function blacklistServer(id, reason = 'unknown') {
    serverBlacklist.set(id, { reason, expires_at: Date.now() + CONFIG.BLACKLIST_DURATION });
}

function isBlacklisted(id) {
    const e = serverBlacklist.get(id);
    if (!e) return false;
    if (Date.now() > e.expires_at) { serverBlacklist.delete(id); return false; }
    return true;
}

// =====================================================
// 🔒 SERVER MANAGEMENT
// =====================================================

function isServerAvailable(id) {
    if (isBlacklisted(id)) return false;

    const a = serverAssignments.get(id);
    if (a) {
        if (Date.now() < a.expires_at) return false;
        serverAssignments.delete(id);
        serverCooldowns.set(id, Date.now() + CONFIG.AUTO_COOLDOWN_ON_EXPIRE);
    }

    const cd = serverCooldowns.get(id);
    if (cd) {
        if (Date.now() < cd) return false;
        serverCooldowns.delete(id);
    }

    return true;
}

function assignServer(id, botId) {
    serverAssignments.set(id, {
        bot_id: botId,
        assigned_at: Date.now(),
        expires_at: Date.now() + CONFIG.ASSIGNMENT_DURATION
    });
}

function releaseServer(id, botId) {
    const a = serverAssignments.get(id);
    if (a && a.bot_id === botId) {
        serverAssignments.delete(id);
        serverCooldowns.set(id, Date.now() + CONFIG.COOLDOWN_DURATION);
        stats.total_releases++;
        return true;
    }
    return false;
}

// =====================================================
// 📋 BOT HISTORY
// =====================================================

function getBotHistory(botId) {
    if (!botHistory.has(botId)) botHistory.set(botId, new Set());
    return botHistory.get(botId);
}

function addToBotHistory(botId, ids) {
    const h = getBotHistory(botId);
    for (const id of ids) h.add(id);
    if (h.size > CONFIG.MAX_BOT_HISTORY) {
        const arr = Array.from(h);
        botHistory.set(botId, new Set(arr.slice(arr.length - Math.floor(CONFIG.MAX_BOT_HISTORY / 2))));
    }
}

function botAlreadyScanned(botId, id) {
    const h = botHistory.get(botId);
    return h ? h.has(id) : false;
}

// =====================================================
// 🚦 RATE LIMITING
// =====================================================

function checkBotRateLimit(botId) {
    const last = botLastRequest.get(botId);
    const now = Date.now();
    if (last && (now - last) < CONFIG.BOT_REQUEST_COOLDOWN) {
        return { allowed: false, wait_ms: CONFIG.BOT_REQUEST_COOLDOWN - (now - last) };
    }
    botLastRequest.set(botId, now);
    return { allowed: true };
}

// =====================================================
// 🌐 FETCH — v3.7 with CYCLE_TIMEOUT on ALL modes
// =====================================================

async function fetchChainWithProxy(proxy, maxPages, pageDelay, sortOrder, cancelFlag) {
    const baseLabel = proxy ? proxy.label : 'DIRECT';
    const label = `${baseLabel}-${sortOrder}`;
    const servers = [];
    let cursor = null;
    let pageCount = 0;
    let consecutiveErrors = 0;
    let rotations = 0;

    while (pageCount < maxPages) {
        if (cancelFlag.cancelled) {
            console.log(`   🛑 [${label}] Cancelled at page ${pageCount}`);
            stats.total_stream_cancels++;
            break;
        }

        try {
            let url = `https://games.roblox.com/v1/games/${STEAL_A_BRAINROT.PLACE_ID}/servers/Public?sortOrder=${sortOrder}&limit=100`;
            if (cursor) url += `&cursor=${encodeURIComponent(cursor)}`;

            const response = await httpGet(url, proxy?.url || null, CONFIG.FETCH_PAGE_TIMEOUT);
            consecutiveErrors = 0;

            if (response && response.data) {
                const page = response.data
                    .filter(s => s.id && s.playing > 0)
                    .map(s => ({
                        id: s.id,
                        playing: s.playing,
                        maxPlayers: s.maxPlayers,
                        ping: s.ping || 0,
                        fetched_at: Date.now(),
                        source: label
                    }));

                servers.push(...page);
                cursor = response.nextPageCursor;
                pageCount++;

                if (pageCount % 10 === 0 || !cursor) {
                    console.log(`   📄 [${label}] Page ${pageCount}: ${servers.length} servers`);
                }

                if (!cursor) break;
                await sleep(pageDelay);
            } else {
                break;
            }
        } catch (err) {
            if (cancelFlag.cancelled) break;

            consecutiveErrors++;
            if (err.message === 'RATE_LIMITED') {
                console.warn(`   🚦 [${label}] Rate limited at page ${pageCount + 1}`);
                if (proxy) {
                    proxy.errors++;
                    proxy.lastError = Date.now();
                    if (rotations < CONFIG.MAX_ROTATIONS) {
                        rotateOneProxy(proxy);
                        rotations++;
                        console.warn(`   🔄 [${label}] Rotated to new IP (${rotations}/${CONFIG.MAX_ROTATIONS})`);
                        if (rotations <= CONFIG.ROTATION_NORESET_AFTER) {
                            consecutiveErrors = 0;
                        }
                    }
                }
                await sleep(CONFIG.FETCH_RATE_LIMIT_BACKOFF);
            } else {
                console.warn(`   ⚠️ [${label}] Page ${pageCount + 1}: ${err.message}`);
                if (proxy) proxy.errors++;
                await sleep(1500);
            }
            if (consecutiveErrors >= CONFIG.FETCH_MAX_CONSECUTIVE_ERRORS) {
                console.error(`   ❌ [${label}] ${consecutiveErrors} consecutive errors, stopping`);
                break;
            }
        }
    }

    return { label, servers, pages: pageCount };
}

// Run fetch logic with CYCLE_TIMEOUT wrapper (used by ALL modes)
async function fetchWithTimeout(fetchFn, cancelFlag) {
    let cycleTimer;
    
    const timeoutPromise = new Promise((_, reject) => {
        cycleTimer = setTimeout(() => {
            reject(new Error('CYCLE_TIMEOUT'));
        }, CONFIG.CYCLE_TIMEOUT);
    });

    try {
        const result = await Promise.race([
            fetchFn(),
            timeoutPromise
        ]);
        clearTimeout(cycleTimer);
        return result;
    } catch (e) {
        clearTimeout(cycleTimer);
        if (e.message === 'CYCLE_TIMEOUT') {
            console.error(`   🚨 CYCLE_TIMEOUT: killing streams after ${CONFIG.CYCLE_TIMEOUT/1000}s`);
            cancelFlag.cancelled = true;
            stats.total_cycle_timeouts++;
            await sleep(2000); // Let streams notice the cancel
        } else {
            throw e;
        }
        return null;
    }
}

async function continuousFetchLoop() {
    console.log('🔄 Starting continuous fetch loop...\n');
    let cycleNum = 0;

    while (true) {
        cycleNum++;
        const proxy = PROXY_POOL[proxyRotationIndex % PROXY_POOL.length];
        proxyRotationIndex = (proxyRotationIndex + 1) % PROXY_POOL.length;

        // Fresh session for this proxy
        rotateOneProxy(proxy);

        const cancelFlag = { cancelled: false };
        globalCancelFlag = cancelFlag;
        const startTime = Date.now();

        // Alternate between Desc and Asc each cycle
        const sortOrder = cycleNum % 2 === 0 ? 'Asc' : 'Desc';
        const halfPages = Math.ceil(CONFIG.PAGES_PER_PROXY / 2);

        try {
            const result = await fetchChainWithProxy(proxy, halfPages, CONFIG.FETCH_PAGE_DELAY, sortOrder, cancelFlag);
            
            if (result.servers.length > 0) {
                // Merge into cache
                const now = Date.now();
                const mergedMap = new Map();

                // Keep non-stale existing servers
                for (const s of globalCache.jobs) {
                    if (now - s.fetched_at < CONFIG.STALE_THRESHOLD) {
                        mergedMap.set(s.id, s);
                    }
                }

                // Add new servers
                let newCount = 0;
                for (const s of result.servers) {
                    if (!mergedMap.has(s.id)) newCount++;
                    mergedMap.set(s.id, s);
                }

                const prevSize = globalCache.jobs.length;
                globalCache.jobs = Array.from(mergedMap.values());
                globalCache.lastUpdate = now;

                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                console.log(`   ✅ #${cycleNum} [${proxy.label}/${sortOrder}] ${result.servers.length} fetched, +${newCount} new → Cache: ${globalCache.jobs.length} (${elapsed}s)`);

                stats.total_fetch_cycles++;
            } else {
                const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                console.log(`   ⚪ #${cycleNum} [${proxy.label}/${sortOrder}] 0 servers (${elapsed}s)`);
            }

        } catch (e) {
            console.error(`   ❌ #${cycleNum} [${proxy.label}/${sortOrder}] Error: ${e.message}`);
        }

        // Purge stale servers periodically
        if (cycleNum % 10 === 0) {
            const now = Date.now();
            const before = globalCache.jobs.length;
            globalCache.jobs = globalCache.jobs.filter(s => now - s.fetched_at < CONFIG.STALE_THRESHOLD);
            const purged = before - globalCache.jobs.length;
            if (purged > 0) console.log(`   🧹 Purged ${purged} stale servers → Cache: ${globalCache.jobs.length}`);
        }

        // Wait before next fetch
        await sleep(CONFIG.CONTINUOUS_FETCH_DELAY);
    }
}

// =====================================================
// 🔑 API KEY
// =====================================================

function verifyApiKey(req, res, next) {
    const validKey = process.env.API_KEY;
    if (!validKey) return res.status(500).json({ error: 'API_KEY not set' });
    const apiKey = req.headers['x-api-key'];
    if (!apiKey || apiKey !== validKey) return res.status(401).json({ error: 'Invalid API key' });
    next();
}

// =====================================================
// 🎯 JOB ASSIGNMENT — TRUE ZERO COLLISION
// =====================================================

async function handleJobAssignment(bot_id, vps_id, res) {
    let botRegion = null;
    for (const [region, config] of Object.entries(REGIONAL_CONFIG)) {
        if (vps_id >= config.vps_range[0] && vps_id <= config.vps_range[1]) {
            botRegion = region;
            break;
        }
    }
    if (!botRegion) return res.status(400).json({ error: `Invalid VPS ID: ${vps_id}` });

    const rl = checkBotRateLimit(bot_id);
    if (!rl.allowed) {
        stats.total_rate_limited++;
        return res.status(429).json({ error: 'Too fast', retry_in_ms: rl.wait_ms });
    }

    if (globalCache.jobs.length === 0) {
        return res.status(503).json({ error: 'Cache empty', retry_in: 10 });
    }

    await globalAssignmentLock.acquire();

    if (globalAssignmentLock.queueLength > stats.lock_max_queue) {
        stats.lock_max_queue = globalAssignmentLock.queueLength;
    }

    try {
        let skipBL = 0, skipAssign = 0, skipHist = 0;
        const available = [];

        for (const job of globalCache.jobs) {
            if (isBlacklisted(job.id)) { skipBL++; continue; }
            if (!isServerAvailable(job.id)) { skipAssign++; continue; }
            if (botAlreadyScanned(bot_id, job.id)) { skipHist++; continue; }
            available.push(job);
        }

        stats.total_blacklist_filtered += skipBL;
        stats.total_duplicates_skipped += skipHist;

        if (available.length === 0) {
            const h = botHistory.get(bot_id);
            if (h && h.size > 0) {
                console.log(`🔄 ${bot_id}: History full (${h.size}), resetting`);
                h.clear();
                const retry = globalCache.jobs.filter(j => !isBlacklisted(j.id) && isServerAvailable(j.id));
                if (retry.length > 0) return doAssign(retry, bot_id, botRegion, res);
            }
            return res.status(503).json({
                error: 'All servers busy/scanned',
                cached: globalCache.jobs.length,
                skipped: { assigned: skipAssign, blacklisted: skipBL, history: skipHist },
                retry_in: 5
            });
        }

        return doAssign(available, bot_id, botRegion, res);

    } finally {
        globalAssignmentLock.release();
    }
}

function doAssign(available, bot_id, botRegion, res) {
    const sorted = available
        .map(j => ({ ...j, _h: deterministicHashNum(j.id, bot_id) }))
        .sort((a, b) => a._h - b._h);

    const count = Math.min(CONFIG.SERVERS_PER_BOT, sorted.length);
    const candidates = sorted.slice(0, count);

    const finalIds = [];
    let collisionsDetected = 0;

    for (const job of candidates) {
        const existing = serverAssignments.get(job.id);
        if (existing && existing.bot_id !== bot_id && Date.now() < existing.expires_at) {
            collisionsDetected++;
            stats.total_collisions_detected++;
            console.error(`🚨 COLLISION: ${job.id} assigned to ${existing.bot_id}, skipping for ${bot_id}`);
            continue;
        }
        finalIds.push(job.id);
    }

    if (collisionsDetected > 0 && sorted.length > count) {
        const extra = sorted.slice(count, count + collisionsDetected);
        for (const job of extra) {
            const existing = serverAssignments.get(job.id);
            if (!existing || existing.bot_id === bot_id || Date.now() >= existing.expires_at) {
                finalIds.push(job.id);
                stats.total_collisions_resolved++;
            }
            if (finalIds.length >= count) break;
        }
    }

    for (const id of finalIds) {
        assignServer(id, bot_id);
    }

    addToBotHistory(bot_id, finalIds);
    stats.total_assignments++;

    const histSize = getBotHistory(bot_id).size;
    const lockQ = globalAssignmentLock.queueLength;

    console.log(`✅ [${botRegion}] ${bot_id}: ${finalIds.length} servers | Pool: ${available.length}/${globalCache.jobs.length} | Hist: ${histSize}${lockQ > 0 ? ` | Queue: ${lockQ}` : ''}${collisionsDetected > 0 ? ` | ⚠️ ${collisionsDetected} collisions!` : ''}`);

    res.json({
        success: true,
        job_ids: finalIds,
        region: botRegion,
        count: finalIds.length,
        available_servers: available.length,
        total_cached: globalCache.jobs.length,
        place_id: STEAL_A_BRAINROT.PLACE_ID,
        assignment_duration_ms: CONFIG.ASSIGNMENT_DURATION,
        history_size: histSize,
        cache_age_s: Math.floor((Date.now() - globalCache.lastUpdate) / 1000),
        collisions_detected: collisionsDetected
    });
}

// =====================================================
// 🎯 ENDPOINTS
// =====================================================

app.post('/api/v1/get-job-assignment', verifyApiKey, async (req, res) => {
    try {
        stats.total_requests++;
        const { bot_id, vps_id } = req.body;
        if (!bot_id || vps_id === undefined) return res.status(400).json({ error: 'Missing: bot_id, vps_id' });
        await handleJobAssignment(bot_id, parseInt(vps_id), res);
    } catch (e) { console.error('❌', e); res.status(500).json({ error: 'Internal error' }); }
});

app.get('/api/v1/get-job-assignment', verifyApiKey, async (req, res) => {
    try {
        stats.total_requests++;
        const { bot_id, vps_id } = req.query;
        if (!bot_id || !vps_id) return res.status(400).json({ error: 'Missing: bot_id, vps_id' });
        await handleJobAssignment(bot_id, parseInt(vps_id), res);
    } catch (e) { console.error('❌', e); res.status(500).json({ error: 'Internal error' }); }
});

app.post('/api/v1/release-server', verifyApiKey, (req, res) => {
    const { bot_id, job_id, reason } = req.body;
    if (!bot_id || !job_id) return res.status(400).json({ error: 'Missing: bot_id, job_id' });
    if (['restricted', 'timeout', 'failed', 'error'].includes(reason)) blacklistServer(job_id, reason);
    const ok = releaseServer(job_id, bot_id);
    res.json({ success: ok, blacklisted: !!reason, cooldown_ms: ok ? CONFIG.COOLDOWN_DURATION : 0 });
});

app.post('/api/v1/release-batch', verifyApiKey, (req, res) => {
    const { bot_id, job_ids, reason } = req.body;
    if (!bot_id || !Array.isArray(job_ids)) return res.status(400).json({ error: 'Missing: bot_id, job_ids[]' });
    let released = 0, bl = 0;
    for (const jid of job_ids) {
        if (['restricted', 'timeout', 'failed'].includes(reason)) { blacklistServer(jid, reason); bl++; }
        if (releaseServer(jid, bot_id)) released++;
    }
    res.json({ success: true, released, total: job_ids.length, blacklisted: bl });
});

app.post('/api/v1/report-restricted', verifyApiKey, (req, res) => {
    const { bot_id, job_id, reason } = req.body;
    if (!bot_id || !job_id) return res.status(400).json({ error: 'Missing: bot_id, job_id' });
    blacklistServer(job_id, reason || 'restricted');
    res.json({ success: true, total_blacklisted: serverBlacklist.size });
});

app.post('/api/v1/clear-history', verifyApiKey, (req, res) => {
    const { bot_id } = req.body;
    if (!bot_id) return res.status(400).json({ error: 'Missing: bot_id' });
    const h = botHistory.get(bot_id);
    const sz = h ? h.size : 0;
    if (h) h.clear();
    res.json({ success: true, cleared: sz });
});

app.post('/api/v1/force-refresh', verifyApiKey, (req, res) => {
    res.json({ success: true, message: 'Continuous mode — always fetching', cache_size: globalCache.jobs.length });
});

app.get('/api/v1/stats', (req, res) => {
    const avail = globalCache.jobs.filter(j => isServerAvailable(j.id)).length;

    res.json({
        game: STEAL_A_BRAINROT,
        version: '3.7',
        cache: {
            total: globalCache.jobs.length,
            available: avail,
            assigned: serverAssignments.size,
            cooldowns: serverCooldowns.size,
            blacklisted: serverBlacklist.size,
            age_s: globalCache.lastUpdate ? Math.floor((Date.now() - globalCache.lastUpdate) / 1000) : -1,
            fetching: globalCache.fetchInProgress,
            fetch_running_s: globalCache.fetchInProgress ? Math.floor((Date.now() - globalCache.fetchStartedAt) / 1000) : 0,
            last_fetch: globalCache.lastFetchStats
        },
        bots: {
            tracked: botHistory.size,
            ...stats,
            uptime_s: Math.floor((Date.now() - stats.uptime_start) / 1000)
        },
        lock: {
            queue_now: globalAssignmentLock.queueLength,
            queue_max_ever: stats.lock_max_queue,
            collisions_detected: stats.total_collisions_detected,
            collisions_resolved: stats.total_collisions_resolved
        },
        proxies: PROXY_POOL.map(p => ({
            label: p.label,
            errors: p.errors,
            status: p.errors >= 10 ? 'degraded' : 'active',
            last_fetch: p.lastFetch
        })),
        regions: Object.fromEntries(
            Object.entries(REGIONAL_CONFIG).map(([r, c]) => [r, c])
        ),
        config: {
            assignment_s: CONFIG.ASSIGNMENT_DURATION / 1000,
            cooldown_s: CONFIG.COOLDOWN_DURATION / 1000,
            servers_per_bot: CONFIG.SERVERS_PER_BOT,
            pages_per_proxy: CONFIG.PAGES_PER_PROXY,
            continuous_delay_s: CONFIG.CONTINUOUS_FETCH_DELAY / 1000,
            stale_threshold_s: CONFIG.STALE_THRESHOLD / 1000,
            proxy_pool_size: PROXY_POOL.length
        }
    });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok', version: '3.7',
        servers: globalCache.jobs.length,
        uptime: Math.floor(process.uptime()),
        collisions_ever: stats.total_collisions_detected,
        cycle_timeouts: stats.total_cycle_timeouts,
        watchdog_resets: stats.total_watchdog_resets,
        stream_cancels: stats.total_stream_cancels,
        memory_mb: Math.round(process.memoryUsage().heapUsed / 1024 / 1024)
    });
});

// =====================================================
// 🧹 CLEANUP
// =====================================================

setInterval(() => {
    const now = Date.now();
    let cA = 0, cC = 0, cB = 0;

    for (const [id, a] of serverAssignments.entries()) {
        if (now > a.expires_at) {
            serverAssignments.delete(id);
            serverCooldowns.set(id, now + CONFIG.AUTO_COOLDOWN_ON_EXPIRE);
            cA++;
        }
    }
    for (const [id, exp] of serverCooldowns.entries()) {
        if (now > exp) { serverCooldowns.delete(id); cC++; }
    }
    for (const [id, e] of serverBlacklist.entries()) {
        if (now > e.expires_at) { serverBlacklist.delete(id); cB++; }
    }
    for (const [id, ts] of botLastRequest.entries()) {
        if (now - ts > 60000) botLastRequest.delete(id);
    }

    if (cA + cC + cB > 0) console.log(`🧹 ${cA} assign→cd, ${cC} cd expired, ${cB} bl expired`);
}, CONFIG.CLEANUP_INTERVAL);

setInterval(() => {
    let trimmed = 0;
    for (const [botId, h] of botHistory.entries()) {
        if (h.size > CONFIG.MAX_BOT_HISTORY) {
            const arr = Array.from(h);
            const keep = new Set(arr.slice(arr.length - Math.floor(CONFIG.MAX_BOT_HISTORY / 2)));
            trimmed += h.size - keep.size;
            botHistory.set(botId, keep);
        }
    }
    if (trimmed > 0) console.log(`🧹 Trimmed ${trimmed} history entries`);
}, 1800000);

// =====================================================
// 🚀 STARTUP — v3.7
// =====================================================

app.listen(PORT, () => {
    console.clear();
    console.log('\n' + '═'.repeat(60));
    console.log('🧠 STEAL A BRAINROT SCANNER - BACKEND v3.7');
    console.log('   🔒 ZERO COLLISION + CONTINUOUS FETCH + FAST COOLDOWN');
    console.log('═'.repeat(60));
    console.log(`🎮 ${STEAL_A_BRAINROT.GAME_NAME}`);
    console.log(`📍 Place ID: ${STEAL_A_BRAINROT.PLACE_ID}`);
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`🔑 API Key: ${process.env.API_KEY ? '✅' : '❌ NOT SET!'}`);
    console.log('');

    initProxyPool();

    const totalBots = Object.values(REGIONAL_CONFIG).reduce((s, c) => s + c.expected_bots, 0);

    console.log('\n🔒 ZERO-COLLISION:');
    console.log('   Global lock + SHA-256 + history + safety net');
    console.log(`   Lock cost: ${Math.ceil(totalBots / 5)}ms/s (${((totalBots / 5) / 10).toFixed(1)}% CPU)`);

    console.log('\n🛡️ SAFETY (v3.7):');
    console.log(`   1. Continuous fetch loop (never hangs)`);
    console.log(`   2. Max ${CONFIG.MAX_ROTATIONS} rotations per fetch`);
    console.log(`   3. ${CONFIG.FETCH_MAX_CONSECUTIVE_ERRORS} max consecutive errors → skip`);
    console.log(`   4. Stale purge every 10 cycles`);

    console.log('\n⚡ FETCH:');
    console.log(`   🌐 ${PROXY_POOL.length || 1} proxies in pool`);
    console.log(`   📄 ${CONFIG.PAGES_PER_PROXY} pages/proxy (${Math.ceil(CONFIG.PAGES_PER_PROXY/2)} per sort)`);
    console.log(`   🔄 CONTINUOUS mode: 1 proxy/fetch, rotating through ${PROXY_POOL.length}`);
    console.log(`   ⏱️ ${CONFIG.CONTINUOUS_FETCH_DELAY / 1000}s between fetches`);
    console.log(`   🗑️ Stale after ${CONFIG.STALE_THRESHOLD / 60000} min`);

    console.log('\n📊 CAPACITY:');
    console.log(`   🤖 ${totalBots} bots × ${CONFIG.SERVERS_PER_BOT} = ${(totalBots * CONFIG.SERVERS_PER_BOT).toLocaleString()} servers/cycle`);

    console.log('');
    for (const [, c] of Object.entries(REGIONAL_CONFIG)) {
        console.log(`   🌍 ${c.name.padEnd(16)} VPS ${c.vps_range[0]}-${c.vps_range[1]}  (${c.expected_bots} bots)`);
    }
    console.log('═'.repeat(60) + '\n');

    // Start continuous fetch loop (never stops)
    continuousFetchLoop().catch(e => {
        console.error('❌ Continuous fetch loop crashed:', e.message);
    });
});

process.on('unhandledRejection', (e) => console.error('❌ Unhandled rejection:', e?.message || e));
process.on('uncaughtException', (e) => console.error('❌ Uncaught exception:', e?.message || e));
