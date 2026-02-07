// =====================================================
// 🧠 STEAL A BRAINROT SCANNER - BACKEND v3.3
// =====================================================
//
// v3.3 — TRUE ZERO COLLISION (prouvé mathématiquement)
//
// CORRECTIONS vs v3.2:
//   🔴 BUG CORRIGÉ: Lock par région → LOCK GLOBAL
//      Les locks par région permettaient 2 bots de régions
//      différentes d'assigner les mêmes serveurs simultanément.
//      Le lock global rend l'opération filter→sort→assign
//      ATOMIQUE pour tous les bots, peu importe la région.
//
//   🔴 AJOUT: Double-vérification post-assignation
//      Après assignation, vérifie qu'aucun serveur n'est 
//      assigné à 2 bots. Si détecté → retire + re-tire.
//
//   🔴 AJOUT: Compteur de vraies collisions dans /stats
//
// PREUVE MATHÉMATIQUE ZÉRO COLLISION:
//   1. Node.js est single-threaded
//   2. Le globalLock sérialise TOUTES les assignations
//   3. Entre acquire() et release(), un seul bot exécute:
//      a) Lire les serveurs disponibles
//      b) Filtrer (available + non-blacklisté + non-historique)
//      c) Trier avec SHA-256(serverId, botId)
//      d) Prendre les 20 premiers
//      e) Marquer comme assignés dans serverAssignments
//   4. Le bot suivant voit les serveurs du step (e) comme
//      NON-disponibles au step (a)
//   5. Donc impossible que 2 bots reçoivent le même serveur
//
//   Performance du lock: l'opération prend <1ms.
//   750 bots × 1 req/5s = 150 req/s × 1ms = 150ms/s = 15% CPU.
//   Aucun problème de contention.
//
// CONSERVÉ de v3.2:
//   ✅ 5 proxies fetch en PARALLÈLE
//   ✅ SHA-256 distribution unique par bot
//   ✅ Per-bot history (no re-scans)
//   ✅ Blacklist avec expiration
//   ✅ Rate limiting par bot
// =====================================================

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const http = require('http');
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
    console.log(`🔧 Proxy pool: ${PROXY_POOL.length} proxies`);
    if (PROXY_POOL.length === 0) {
        console.warn('⚠️  NO PROXIES configured!');
    }
}

// =====================================================
// 🔄 SMARTPROXY SESSION ROTATION
// =====================================================
// Smartproxy format:
//   http://user_area-US_life-15_session-XXXXX:pass@proxy.smartproxy.net:3120
//
// Le session ID détermine quelle IP résidentielle tu reçois.
// Même session = même IP pendant 15 min.
// Nouvelle session = nouvelle IP = pas de rate limit Roblox.
//
// On génère un nouveau session ID À CHAQUE CYCLE DE FETCH
// pour toujours avoir une IP fraîche.
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
    ASSIGNMENT_DURATION: 120000,     // 2 min
    COOLDOWN_DURATION: 300000,       // 5 min après release
    AUTO_COOLDOWN_ON_EXPIRE: 180000, // 3 min si bot ne release pas
    SERVERS_PER_BOT: 20,

    // ── Fetch parallèle ──
    CACHE_REFRESH_INTERVAL: 180000,  // 3 min (80 pages + rotations takes ~2min)
    PAGES_PER_PROXY: 80,             // 80 pages × 100 = 8,000/proxy
    FETCH_PAGE_DELAY: 1500,           // 1.5s entre pages (Roblox rate limit strict)
    FETCH_PAGE_TIMEOUT: 12000,
    FETCH_MAX_CONSECUTIVE_ERRORS: 4,
    FETCH_RATE_LIMIT_BACKOFF: 5000,

    // ── Direct fetch (backup sans proxy) ──
    DIRECT_PAGES: 50,
    DIRECT_PAGE_DELAY: 1000,

    // ── Bot management ──
    BOT_REQUEST_COOLDOWN: 5000,
    MAX_BOT_HISTORY: 2000,

    // ── Blacklist ──
    BLACKLIST_DURATION: 600000,

    // ── Cleanup ──
    CLEANUP_INTERVAL: 10000,
};

// =====================================================
// 💾 HTTP CLIENT — https-proxy-agent pour HTTPS via proxy
// =====================================================
// npm install https-proxy-agent
// Gère automatiquement: CONNECT tunnel, auth, TLS
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

        // Proxy support via https-proxy-agent
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

        // HARD TIMEOUT — kills hanging proxy connections that ignore socket timeout
        const hardTimer = setTimeout(() => {
            req.destroy();
            done(reject, new Error('HARD_TIMEOUT'));
        }, timeout + 3000); // 3s grace period after soft timeout

        // Clear hard timer when request completes normally
        req.on('close', () => clearTimeout(hardTimer));

        req.end();
    });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// =====================================================
// 🔐 SHA-256 DISTRIBUTION
// =====================================================
//
// Pour chaque paire (serverId, botId), SHA-256 produit
// un nombre sur 48 bits (281 trillion de valeurs possibles).
//
// Probabilité de collision de tri entre 2 bots pour
// 15,000 serveurs: ~15000² / 2^49 ≈ 0.0000004
// = virtuellement impossible.
//
// Chaque bot obtient un ordre de serveurs UNIQUE.
// =====================================================

function deterministicHashNum(serverId, botId) {
    const buf = crypto.createHash('sha256')
        .update(`${serverId}::${botId}::sab-v33-salt`)
        .digest();
    return buf.readUIntBE(0, 6);
}

// =====================================================
// 🔒 GLOBAL LOCK — LA CLÉ DU ZÉRO COLLISION
// =====================================================
//
// UN SEUL lock pour TOUTES les régions.
// Pourquoi? Parce que serverAssignments est GLOBAL.
//
// Si 2 bots de régions différentes tournent en parallèle,
// ils peuvent voir les mêmes serveurs comme "available"
// et les assigner tous les deux → COLLISION.
//
// Avec un lock global, c'est IMPOSSIBLE:
// Bot A entre dans le lock → filtre → assigne → sort du lock
// Bot B entre dans le lock → voit les assignations de A → filtre les exclut
//
// Coût: <1ms par requête. Même avec 150 req/s = 150ms/s de lock.
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
    lastFetchStats: {}
};

const serverAssignments = new Map();  // serverId → { bot_id, expires_at }
const serverCooldowns = new Map();    // serverId → expires_at (timestamp)
const serverBlacklist = new Map();    // serverId → { reason, expires_at }
const botHistory = new Map();         // bot_id → Set<serverId>
const botLastRequest = new Map();     // bot_id → timestamp

const stats = {
    total_requests: 0,
    total_assignments: 0,
    total_releases: 0,
    total_duplicates_skipped: 0,
    total_blacklist_filtered: 0,
    total_rate_limited: 0,
    total_fetch_cycles: 0,
    // v3.3: collision tracking
    total_collisions_detected: 0,     // Should ALWAYS be 0
    total_collisions_resolved: 0,
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
        // Expired → cleanup + auto-cooldown
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
// 🌐 PARALLEL FETCH
// =====================================================

async function fetchChainWithProxy(proxy, maxPages, pageDelay, sortOrder = 'Desc') {
    const baseLabel = proxy ? proxy.label : 'DIRECT';
    const label = `${baseLabel}-${sortOrder}`;
    const servers = [];
    let cursor = null;
    let pageCount = 0;
    let consecutiveErrors = 0;
    let rotations = 0;
    const MAX_ROTATIONS = 15;

    while (pageCount < maxPages) {
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
            consecutiveErrors++;
            if (err.message === 'RATE_LIMITED') {
                console.warn(`   🚦 [${label}] Rate limited at page ${pageCount + 1}`);
                if (proxy) {
                    proxy.errors++;
                    proxy.lastError = Date.now();
                    // Rotate session → get fresh IP (max 5 rotations)
                    if (rotations < MAX_ROTATIONS) {
                        rotateOneProxy(proxy);
                        rotations++;
                        console.warn(`   🔄 [${label}] Rotated to new IP (${rotations}/${MAX_ROTATIONS})`);
                        consecutiveErrors = 0; // New IP = reset error count
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

async function fetchAllServersParallel() {
    if (globalCache.fetchInProgress) {
        console.log('⏭️ Fetch already running');
        return;
    }

    globalCache.fetchInProgress = true;
    const startTime = Date.now();

    console.log('\n' + '═'.repeat(60));
    console.log('🌐 PARALLEL FETCH — All proxies simultaneously');
    console.log('═'.repeat(60));

    // Rotate proxy sessions → fresh IP for each cycle
    rotateProxySessions();

    try {
        const promises = [];
        const halfPages = Math.ceil(CONFIG.PAGES_PER_PROXY / 2);

        for (const proxy of PROXY_POOL) {
            // Desc = serveurs les plus remplis d'abord
            console.log(`   🚀 ${proxy.label} ↓Desc (${halfPages} pages)`);
            promises.push(fetchChainWithProxy(proxy, halfPages, CONFIG.FETCH_PAGE_DELAY, 'Desc'));
            
            // Asc = serveurs les moins remplis d'abord (différente partie de la liste)
            // Utilise un clone du proxy pour éviter les conflits de session
            const proxyClone = { ...proxy, url: proxy.baseUrl };
            rotateOneProxy(proxyClone);
            console.log(`   🚀 ${proxy.label} ↑Asc  (${halfPages} pages)`);
            promises.push(fetchChainWithProxy(proxyClone, halfPages, CONFIG.FETCH_PAGE_DELAY, 'Asc'));
        }

        if (PROXY_POOL.length === 0) {
            console.log(`   🚀 DIRECT ↓Desc (${CONFIG.DIRECT_PAGES} pages)`);
            promises.push(fetchChainWithProxy(null, CONFIG.DIRECT_PAGES, CONFIG.DIRECT_PAGE_DELAY, 'Desc'));
            console.log(`   🚀 DIRECT ↑Asc  (${CONFIG.DIRECT_PAGES} pages)`);
            promises.push(fetchChainWithProxy(null, CONFIG.DIRECT_PAGES, CONFIG.DIRECT_PAGE_DELAY, 'Asc'));
        }

        console.log(`   ⏳ ${promises.length} streams running...\n`);

        const results = await Promise.allSettled(promises);

        const allServers = [];
        const perStream = {};
        let totalPages = 0;

        for (const r of results) {
            if (r.status === 'fulfilled') {
                const { label, servers, pages } = r.value;
                allServers.push(...servers);
                totalPages += pages;
                perStream[label] = { servers: servers.length, pages };
                console.log(`   ✅ [${label}] ${servers.length} servers (${pages} pages)`);
            } else {
                console.error(`   ❌ Stream failed: ${r.reason?.message}`);
            }
        }

        // Dedup
        const uniqueMap = new Map();
        for (const s of allServers) {
            const existing = uniqueMap.get(s.id);
            if (!existing || s.fetched_at > existing.fetched_at) {
                uniqueMap.set(s.id, s);
            }
        }
        const unique = Array.from(uniqueMap.values());
        const dupes = allServers.length - unique.length;
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

        if (unique.length > 0) {
            const prev = globalCache.jobs.length;
            globalCache.jobs = unique;
            globalCache.lastUpdate = Date.now();
            globalCache.lastFetchStats = {
                total: unique.length, raw: allServers.length, dupes,
                pages: totalPages, streams: perStream, duration_s: parseFloat(elapsed)
            };

            console.log('\n' + '═'.repeat(60));
            console.log(`✅ ${allServers.length} raw → ${unique.length} unique (${dupes} dupes) in ${elapsed}s`);

            const bySource = {};
            for (const s of unique) bySource[s.source] = (bySource[s.source] || 0) + 1;
            console.log('   🏆 Unique per proxy:');
            for (const [src, cnt] of Object.entries(bySource).sort((a, b) => b[1] - a[1])) {
                console.log(`      ${src.padEnd(16)} → ${cnt} (${((cnt / unique.length) * 100).toFixed(1)}%)`);
            }
            console.log(`   📈 Delta: ${unique.length > prev ? '+' : ''}${unique.length - prev}`);
            console.log('═'.repeat(60) + '\n');
        } else {
            console.warn(`⚠️ 0 servers, keeping cache (${globalCache.jobs.length})`);
        }

        stats.total_fetch_cycles++;
        for (const p of PROXY_POOL) {
            if (perStream[p.label]) p.lastFetch = perStream[p.label];
        }

    } catch (e) {
        console.error(`❌ Fatal: ${e.message}`);
    }

    globalCache.fetchInProgress = false;
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
//
// FLOW:
//   1. Detect region (from VPS ID)
//   2. Rate limit check (OUTSIDE lock — fast reject)
//   3. Cache check (OUTSIDE lock — fast reject)
//   4. ACQUIRE GLOBAL LOCK ←── everything below is ATOMIC
//   5.   Filter: available + not blacklisted + not in history
//   6.   Sort with SHA-256(serverId, botId) → unique order
//   7.   Take top 20
//   8.   VERIFY no collision (safety net)
//   9.   Mark as assigned in serverAssignments
//   10.  Add to bot history
//   11. RELEASE GLOBAL LOCK
//   12. Respond
//
// Between steps 4 and 11, NO OTHER BOT can execute this code.
// Therefore, step 5 always sees the LATEST state of serverAssignments,
// including all assignments from all previous bots.
// =====================================================

async function handleJobAssignment(bot_id, vps_id, res) {
    // ── 1. Detect region (no lock needed) ──
    let botRegion = null;
    for (const [region, config] of Object.entries(REGIONAL_CONFIG)) {
        if (vps_id >= config.vps_range[0] && vps_id <= config.vps_range[1]) {
            botRegion = region;
            break;
        }
    }
    if (!botRegion) return res.status(400).json({ error: `Invalid VPS ID: ${vps_id}` });

    // ── 2. Rate limit (no lock needed) ──
    const rl = checkBotRateLimit(bot_id);
    if (!rl.allowed) {
        stats.total_rate_limited++;
        return res.status(429).json({ error: 'Too fast', retry_in_ms: rl.wait_ms });
    }

    // ── 3. Cache empty check (no lock needed) ──
    if (globalCache.jobs.length === 0) {
        return res.status(503).json({ error: 'Cache empty', retry_in: 10 });
    }

    // ── 4. ACQUIRE GLOBAL LOCK ──
    // From here, ONLY this bot is executing assignment logic.
    await globalAssignmentLock.acquire();

    // Track max queue depth for monitoring
    if (globalAssignmentLock.queueLength > stats.lock_max_queue) {
        stats.lock_max_queue = globalAssignmentLock.queueLength;
    }

    try {
        // ── 5. Filter available servers ──
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

        // ── Handle empty pool ──
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
        // ── 11. ALWAYS release lock ──
        globalAssignmentLock.release();
    }
}

function doAssign(available, bot_id, botRegion, res) {
    // ── 6. SHA-256 sort → unique order for this bot ──
    const sorted = available
        .map(j => ({ ...j, _h: deterministicHashNum(j.id, bot_id) }))
        .sort((a, b) => a._h - b._h);

    const count = Math.min(CONFIG.SERVERS_PER_BOT, sorted.length);
    const candidates = sorted.slice(0, count);

    // ── 8. COLLISION SAFETY NET ──
    // This should NEVER trigger if the lock works correctly.
    // But defense-in-depth is good practice.
    const finalIds = [];
    let collisionsDetected = 0;

    for (const job of candidates) {
        const existing = serverAssignments.get(job.id);
        if (existing && existing.bot_id !== bot_id && Date.now() < existing.expires_at) {
            // THIS SHOULD NEVER HAPPEN with the global lock.
            // If it does, log it loudly and skip this server.
            collisionsDetected++;
            stats.total_collisions_detected++;
            console.error(`🚨🚨🚨 COLLISION DETECTED: ${job.id} already assigned to ${existing.bot_id}, skipping for ${bot_id}`);
            continue;
        }
        finalIds.push(job.id);
    }

    // If we lost some servers to collisions, try to replace them
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

    // ── 9. Mark as assigned ──
    for (const id of finalIds) {
        assignServer(id, bot_id);
    }

    // ── 10. Add to history ──
    addToBotHistory(bot_id, finalIds);

    stats.total_assignments++;

    const histSize = getBotHistory(bot_id).size;
    const lockQ = globalAssignmentLock.queueLength;

    console.log(`✅ [${botRegion}] ${bot_id}: ${finalIds.length} servers | Pool: ${available.length}/${globalCache.jobs.length} | Hist: ${histSize}${lockQ > 0 ? ` | Queue: ${lockQ}` : ''}${collisionsDetected > 0 ? ` | ⚠️ ${collisionsDetected} collisions!` : ''}`);

    // ── 12. Respond ──
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
        // v3.3: collision info (should always be 0)
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
    if (globalCache.fetchInProgress) return res.json({ success: false, message: 'Already running' });
    res.json({ success: true, message: 'Started' });
    fetchAllServersParallel();
});

app.get('/api/v1/stats', (req, res) => {
    const avail = globalCache.jobs.filter(j => isServerAvailable(j.id)).length;

    res.json({
        game: STEAL_A_BRAINROT,
        version: '3.3',
        cache: {
            total: globalCache.jobs.length,
            available: avail,
            assigned: serverAssignments.size,
            cooldowns: serverCooldowns.size,
            blacklisted: serverBlacklist.size,
            age_s: globalCache.lastUpdate ? Math.floor((Date.now() - globalCache.lastUpdate) / 1000) : -1,
            fetching: globalCache.fetchInProgress,
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
            // If collisions_detected > 0, something is VERY wrong
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
            refresh_s: CONFIG.CACHE_REFRESH_INTERVAL / 1000
        }
    });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok', version: '3.3',
        servers: globalCache.jobs.length,
        uptime: Math.floor(process.uptime()),
        collisions_ever: stats.total_collisions_detected,
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
// 🚀 STARTUP
// =====================================================

app.listen(PORT, async () => {
    console.clear();
    console.log('\n' + '═'.repeat(60));
    console.log('🧠 STEAL A BRAINROT SCANNER - BACKEND v3.3');
    console.log('   🔒 TRUE ZERO COLLISION (global lock)');
    console.log('═'.repeat(60));
    console.log(`🎮 ${STEAL_A_BRAINROT.GAME_NAME}`);
    console.log(`📍 Place ID: ${STEAL_A_BRAINROT.PLACE_ID}`);
    console.log(`🚀 http://localhost:${PORT}`);
    console.log(`🔑 API Key: ${process.env.API_KEY ? '✅' : '❌ NOT SET!'}`);
    console.log('');

    initProxyPool();

    const totalBots = Object.values(REGIONAL_CONFIG).reduce((s, c) => s + c.expected_bots, 0);

    console.log('\n🔒 ZERO-COLLISION PROOF:');
    console.log('   1. Node.js = single-threaded');
    console.log('   2. Global lock = only 1 bot assigns at a time');
    console.log('   3. Filter sees ALL previous assignments');
    console.log('   4. SHA-256 = unique sort order per bot');
    console.log('   5. History = never re-scan same server');
    console.log('   6. Safety net = collision detection + auto-resolve');
    console.log(`   7. Lock cost: <1ms × ${totalBots} bots / 5s = ${Math.ceil(totalBots / 5)}ms/s (${((totalBots / 5) / 10).toFixed(1)}% CPU)`);

    console.log('\n⚡ FETCH:');
    console.log(`   🌐 ${PROXY_POOL.length || 1} proxies × 2 sort orders (Desc+Asc)`);
    console.log(`   📄 ${CONFIG.PAGES_PER_PROXY} pages/proxy (${Math.ceil(CONFIG.PAGES_PER_PROXY/2)} per sort)`);
    console.log(`   ⏱️  ~${Math.ceil(CONFIG.PAGES_PER_PROXY * (CONFIG.FETCH_PAGE_DELAY + 400) / 1000)}s per cycle`);
    console.log(`   🔄 Every ${CONFIG.CACHE_REFRESH_INTERVAL / 1000}s`);

    console.log('\n📊 CAPACITY:');
    console.log(`   🤖 ${totalBots} bots × ${CONFIG.SERVERS_PER_BOT} = ${(totalBots * CONFIG.SERVERS_PER_BOT).toLocaleString()} servers/cycle`);

    console.log('');
    for (const [, c] of Object.entries(REGIONAL_CONFIG)) {
        console.log(`   🌍 ${c.name.padEnd(16)} VPS ${c.vps_range[0]}-${c.vps_range[1]}  (${c.expected_bots} bots)`);
    }
    console.log('═'.repeat(60) + '\n');

    console.log('🔄 Initial fetch...\n');
    await fetchAllServersParallel();
    setInterval(fetchAllServersParallel, CONFIG.CACHE_REFRESH_INTERVAL);
    console.log('✅ Backend v3.3 ready!\n');
});

process.on('unhandledRejection', (e) => console.error('❌ Unhandled:', e));
process.on('uncaughtException', (e) => console.error('❌ Uncaught:', e));
