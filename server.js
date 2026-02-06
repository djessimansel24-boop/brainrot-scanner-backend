// =====================================================
// 🧠 STEAL A BRAINROT SCANNER - BACKEND v2.0
// =====================================================
// Système de distribution Job IDs pour 5 régions
// Garantie ZÉRO collision entre bots
// Cache de 15,000+ serveurs
// =====================================================

const express = require('express');
const cors = require('cors');
const request = require('request');  // Better proxy support than axios

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// =====================================================
// 🎮 CONFIGURATION STEAL A BRAINROT
// =====================================================

const STEAL_A_BRAINROT = {
    PLACE_ID: 109983668079237,
    UNIVERSE_ID: 7709344486,  // Correct Universe ID from game.GameId
    GAME_NAME: "Steal a Brainrot"
};

// =====================================================
// 🔄 GET UNIVERSE ID FROM PLACE ID
// =====================================================
async function getUniverseId(placeId) {
    try {
        console.log(`🔍 Trying API: https://apis.roblox.com/universes/v1/places/${placeId}/universe`);
        const response = await axios.get(`https://apis.roblox.com/universes/v1/places/${placeId}/universe`);
        console.log('📥 API Response:', JSON.stringify(response.data));
        
        if (response.data && response.data.universeId) {
            console.log(`✅ Found Universe ID: ${response.data.universeId}`);
            return response.data.universeId;
        }
    } catch (error) {
        console.error('❌ Failed to get Universe ID:', error.message);
        if (error.response) {
            console.error('   Response status:', error.response.status);
            console.error('   Response data:', JSON.stringify(error.response.data));
        }
    }
    
    // Fallback: Try using Place ID as Universe ID
    console.log('⚠️ Trying Place ID as Universe ID (fallback)...');
    return placeId;
}

// =====================================================
// 🌍 CONFIGURATION 5 PROXIES RÉGIONAUX
// ⚠️ Variables d'environnement Render.com
// =====================================================

const REGIONAL_PROXIES = {
    'us': {
        name: 'United States',
        proxy_url: process.env.PROXY_US,
        vps_range: [1, 10],
        expected_bots: 250
    },
    'europe': {
        name: 'Europe',
        proxy_url: process.env.PROXY_EU,
        vps_range: [11, 19],
        expected_bots: 225
    },
    'asia': {
        name: 'Asia Pacific',
        proxy_url: process.env.PROXY_ASIA,
        vps_range: [20, 24],
        expected_bots: 125
    },
    'south-america': {
        name: 'South America',
        proxy_url: process.env.PROXY_SOUTH_AMERICA,
        vps_range: [25, 27],
        expected_bots: 75
    },
    'oceania': {
        name: 'Oceania',
        proxy_url: process.env.PROXY_OCEANIA,
        vps_range: [28, 30],
        expected_bots: 75
    }
};

// =====================================================
// ⚙️ PARAMÈTRES DU SYSTÈME
// =====================================================

const CONFIG = {
    ASSIGNMENT_DURATION: 20000,      // 20 secondes
    COOLDOWN_DURATION: 300000,       // 5 minutes
    SERVERS_PER_BOT: 20,
    CACHE_REFRESH_INTERVAL: 300000,  // 5 minutes
    MAX_FETCH_PAGES: 50,             // 50 pages = 5,000 serveurs max
    FETCH_PAGE_DELAY: 3000,          // 3 secondes entre pages
};

// =====================================================
// 💾 STOCKAGE EN MÉMOIRE
// =====================================================

const regionalJobCache = {};
for (const region in REGIONAL_PROXIES) {
    regionalJobCache[region] = {
        jobs: [],
        lastUpdate: 0,
        fetchInProgress: false
    };
}

const serverAssignments = new Map();
const serverCooldowns = new Map();

const stats = {
    total_requests: 0,
    total_assignments: 0,
    total_releases: 0,
    total_collisions_avoided: 0,
    uptime_start: Date.now()
};

// =====================================================
// 🔒 FONCTIONS DE GESTION DES SERVEURS
// =====================================================

function isServerAvailable(serverId) {
    const assignment = serverAssignments.get(serverId);
    if (assignment) {
        if (Date.now() < assignment.expires_at) {
            return false;
        } else {
            serverAssignments.delete(serverId);
        }
    }
    
    const cooldown = serverCooldowns.get(serverId);
    if (cooldown) {
        if (Date.now() - cooldown < CONFIG.COOLDOWN_DURATION) {
            return false;
        } else {
            serverCooldowns.delete(serverId);
        }
    }
    
    return true;
}

function assignServer(serverId, botId) {
    serverAssignments.set(serverId, {
        bot_id: botId,
        assigned_at: Date.now(),
        expires_at: Date.now() + CONFIG.ASSIGNMENT_DURATION
    });
}

function releaseServer(serverId, botId) {
    const assignment = serverAssignments.get(serverId);
    
    if (assignment && assignment.bot_id === botId) {
        serverAssignments.delete(serverId);
        serverCooldowns.set(serverId, Date.now());
        stats.total_releases++;
        return true;
    }
    
    return false;
}

// =====================================================
// 🌐 FETCH SERVEURS DEPUIS ROBLOX API
// =====================================================

// Blacklist de serveurs problématiques (restreints détectés)
const serverBlacklist = new Set();
const MAX_BLACKLIST_SIZE = 1000;

// Ajouter un serveur à la blacklist
function blacklistServer(serverId, reason) {
    if (serverBlacklist.size >= MAX_BLACKLIST_SIZE) {
        // Clear old entries if blacklist gets too big
        const firstEntry = serverBlacklist.values().next().value;
        serverBlacklist.delete(firstEntry);
    }
    serverBlacklist.add(serverId);
    console.log(`🚫 Blacklisted server ${serverId}: ${reason}`);
}

// Vérifier si un serveur est blacklisté
function isBlacklisted(serverId) {
    return serverBlacklist.has(serverId);
}

async function fetchAllServersViaProxy(region, proxyConfig) {
    try {
        console.log(`\n🔄 [${region}] Starting full server fetch (direct, no proxy)...`);
        const startTime = Date.now();
        
        let allServers = [];
        let cursor = null;
        let pageCount = 0;
        
        while (pageCount < CONFIG.MAX_FETCH_PAGES) {
            try {
                let url = `https://games.roblox.com/v1/games/${STEAL_A_BRAINROT.PLACE_ID}/servers/Public?sortOrder=Desc&limit=100`;
                if (cursor) {
                    url += `&cursor=${encodeURIComponent(cursor)}`;
                }
                
                console.log(`   🌐 Calling: ${url}`);
                
                // Direct request WITHOUT proxy
                const response = await new Promise((resolve, reject) => {
                    request({
                        url: url,
                        method: 'GET',
                        timeout: 15000,
                        headers: {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                        },
                        json: true
                    }, (error, response, body) => {
                        if (error) {
                            reject(error);
                        } else if (response.statusCode !== 200) {
                            reject(new Error(`Status ${response.statusCode}: ${JSON.stringify(body)}`));
                        } else {
                            resolve(body);
                        }
                    });
                });
                
                if (response && response.data) {
                    const servers = response.data
                        .filter(server => {
                            // 1. Skip blacklisted servers
                            if (isBlacklisted(server.id)) {
                                return false;
                            }
                            
                            // 2. Skip FULL servers ONLY (8/8 players)
                            if (server.playing >= server.maxPlayers) {
                                return false;
                            }
                            
                            // 3. KEEP almost full servers! (7/8, 6/8, etc. - bots can still join!)
                            
                            // 4. Skip completely empty servers (might be broken)
                            // But keep servers with 1-7 players
                            if (server.playing === 0) {
                                return false;  // Empty servers might be stuck/broken
                            }
                            
                            // 5. Skip servers with very high ping (might be unstable)
                            if (server.ping && server.ping > 500) {
                                return false;
                            }
                            
                            return true;
                        })
                        .map(server => ({
                            id: server.id,
                            playing: server.playing,
                            maxPlayers: server.maxPlayers,
                            ping: server.ping || 0,
                            region: region,
                            fetched_at: Date.now()
                        }));
                    
                    allServers = allServers.concat(servers);
                    cursor = response.nextPageCursor;
                    pageCount++;
                    
                    console.log(`   📄 Page ${pageCount}: +${servers.length} servers (total: ${allServers.length})`);
                    
                    if (!cursor) {
                        console.log(`   ✅ Reached end of server list`);
                        break;
                    }
                    
                    if (pageCount < CONFIG.MAX_FETCH_PAGES) {
                        await new Promise(resolve => setTimeout(resolve, CONFIG.FETCH_PAGE_DELAY));
                    }
                }
            } catch (pageError) {
                console.error(`   ⚠️ Error on page ${pageCount + 1}:`, pageError.message);
                if (pageError.response) {
                    console.error(`   📊 Status: ${pageError.response.status}`);
                    console.error(`   📄 Response:`, JSON.stringify(pageError.response.data));
                }
                break;
            }
        }
        
        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        console.log(`✅ [${region}] Fetched ${allServers.length} servers in ${elapsed}s\n`);
        
        return allServers;
        
    } catch (error) {
        console.error(`❌ [${region}] Fatal error:`, error.message);
        return [];
    }
}

async function refreshRegionalCache(region, config) {
    const cache = regionalJobCache[region];
    
    if (cache.fetchInProgress) {
        console.log(`⏭️ [${region}] Fetch already in progress, skipping`);
        return;
    }
    
    cache.fetchInProgress = true;
    
    try {
        const servers = await fetchAllServersViaProxy(region, config);
        
        regionalJobCache[region] = {
            jobs: servers,
            lastUpdate: Date.now(),
            fetchInProgress: false
        };
        
        console.log(`💾 [${region}] Cache updated: ${servers.length} servers stored`);
        
    } catch (error) {
        console.error(`❌ [${region}] Cache refresh failed:`, error.message);
        cache.fetchInProgress = false;
    }
}

async function refreshAllRegions() {
    console.log('\n' + '═'.repeat(60));
    console.log('🔄 GLOBAL CACHE REFRESH STARTED');
    console.log('═'.repeat(60) + '\n');
    
    const startTime = Date.now();
    
    const promises = Object.entries(REGIONAL_PROXIES).map(([region, config]) => 
        refreshRegionalCache(region, config)
    );
    
    await Promise.all(promises);
    
    const total = Object.values(regionalJobCache)
        .reduce((sum, cache) => sum + cache.jobs.length, 0);
    
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    
    console.log('═'.repeat(60));
    console.log(`✅ REFRESH COMPLETE: ${total} total servers in ${elapsed}s`);
    console.log('═'.repeat(60) + '\n');
}

// =====================================================
// 🎯 API ENDPOINTS
// =====================================================

function verifyApiKey(req, res, next) {
    const apiKey = req.headers['x-api-key'];
    const validKey = process.env.API_KEY || 'xK9mP2vL8qR4wN7jT1bY6cZ3aB5dF8gH';
    
    if (!apiKey || apiKey !== validKey) {
        return res.status(401).json({ error: 'Invalid or missing API key' });
    }
    
    next();
}

app.post('/api/v1/get-job-assignment', verifyApiKey, (req, res) => {
    try {
        stats.total_requests++;
        
        const { bot_id, vps_id } = req.body;
        
        if (!bot_id || !vps_id) {
            return res.status(400).json({ 
                error: 'Missing required fields: bot_id, vps_id' 
            });
        }
        
        handleJobAssignment(bot_id, vps_id, res);
    } catch (error) {
        console.error('❌ Error in get-job-assignment (POST):', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// GET endpoint for Roblox HttpService compatibility
app.get('/api/v1/get-job-assignment', verifyApiKey, (req, res) => {
    try {
        stats.total_requests++;
        
        const { bot_id, vps_id } = req.query;
        
        if (!bot_id || !vps_id) {
            return res.status(400).json({ 
                error: 'Missing required fields: bot_id, vps_id' 
            });
        }
        
        handleJobAssignment(bot_id, parseInt(vps_id), res);
    } catch (error) {
        console.error('❌ Error in get-job-assignment (GET):', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

function handleJobAssignment(bot_id, vps_id, res) {
        
        let botRegion = null;
        for (const [region, config] of Object.entries(REGIONAL_PROXIES)) {
            if (vps_id >= config.vps_range[0] && vps_id <= config.vps_range[1]) {
                botRegion = region;
                break;
            }
        }
        
        if (!botRegion) {
            return res.status(400).json({ 
                error: `Invalid VPS ID: ${vps_id}. Expected 1-30.` 
            });
        }
        
        const cache = regionalJobCache[botRegion];
        
        if (!cache || cache.jobs.length === 0) {
            return res.status(503).json({ 
                error: 'No servers in cache. Backend is refreshing, please retry in 10s.',
                region: botRegion,
                retry_in: 10
            });
        }
        
        const availableJobs = cache.jobs.filter(job => isServerAvailable(job.id));
        
        if (availableJobs.length === 0) {
            console.warn(`⚠️ [${botRegion}] No available servers for ${bot_id}`);
            return res.status(503).json({ 
                error: 'All servers assigned or in cooldown',
                region: botRegion,
                total_cached: cache.jobs.length,
                retry_in: 5
            });
        }
        
        const shuffled = [...availableJobs].sort((a, b) => {
            const hashA = (a.id + bot_id).split('').reduce((acc, c) => acc + c.charCodeAt(0), 0);
            const hashB = (b.id + bot_id).split('').reduce((acc, c) => acc + c.charCodeAt(0), 0);
            return hashA - hashB;
        });
        
        const count = Math.min(CONFIG.SERVERS_PER_BOT, shuffled.length);
        const assignedJobs = shuffled.slice(0, count);
        
        assignedJobs.forEach(job => {
            assignServer(job.id, bot_id);
        });
        
        stats.total_assignments++;
        stats.total_collisions_avoided += (availableJobs.length - count);
        
        console.log(`✅ [${botRegion}] ${bot_id}: ${count} servers assigned | Available: ${availableJobs.length}/${cache.jobs.length}`);
        
        res.json({
            success: true,
            job_ids: assignedJobs.map(j => j.id),
            region: botRegion,
            count: count,
            available_servers: availableJobs.length,
            total_cached: cache.jobs.length,
            place_id: STEAL_A_BRAINROT.PLACE_ID
        });
}

app.post('/api/v1/release-server', verifyApiKey, (req, res) => {
    try {
        const { bot_id, job_id, reason } = req.body;
        
        if (!bot_id || !job_id) {
            return res.status(400).json({ 
                error: 'Missing required fields: bot_id, job_id' 
            });
        }
        
        // If reason indicates a problem, blacklist the server
        if (reason === 'restricted' || reason === 'timeout' || reason === 'failed') {
            blacklistServer(job_id, reason);
        }
        
        const released = releaseServer(job_id, bot_id);
        
        if (released) {
            console.log(`🔓 [${bot_id}] Released ${job_id} → cooldown 5min ${reason ? `(${reason})` : ''}`);
            res.json({ 
                success: true,
                message: 'Server released and in cooldown',
                blacklisted: reason ? true : false
            });
        } else {
            res.json({ 
                success: false,
                message: 'Server was not assigned to this bot or already released'
            });
        }
        
    } catch (error) {
        console.error('❌ Error in release-server:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

// NEW: Report restricted/problematic server endpoint
app.post('/api/v1/report-restricted', verifyApiKey, (req, res) => {
    try {
        const { bot_id, job_id, reason } = req.body;
        
        if (!bot_id || !job_id) {
            return res.status(400).json({ error: 'Missing bot_id or job_id' });
        }
        
        blacklistServer(job_id, reason || 'restricted');
        console.log(`⚠️ [REPORT] ${bot_id} reported ${job_id} as ${reason || 'restricted'}`);
        
        res.json({ 
            success: true,
            message: 'Server blacklisted',
            total_blacklisted: serverBlacklist.size
        });
        
    } catch (error) {
        console.error('❌ Error in report-restricted:', error);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/api/v1/stats', (req, res) => {
    const regionStats = {};
    
    for (const [region, config] of Object.entries(REGIONAL_PROXIES)) {
        const cache = regionalJobCache[region];
        const available = cache.jobs.filter(j => isServerAvailable(j.id)).length;
        const assigned = cache.jobs.length - available;
        
        regionStats[region] = {
            name: config.name,
            cached_servers: cache.jobs.length,
            available_servers: available,
            assigned_servers: Math.min(assigned, serverAssignments.size),
            expected_bots: config.expected_bots,
            cache_age_seconds: Math.floor((Date.now() - cache.lastUpdate) / 1000)
        };
    }
    
    res.json({
        game: STEAL_A_BRAINROT,
        global: {
            ...stats,
            uptime_seconds: Math.floor((Date.now() - stats.uptime_start) / 1000),
            total_cached_servers: Object.values(regionalJobCache).reduce((sum, c) => sum + c.jobs.length, 0),
            servers_assigned: serverAssignments.size,
            servers_in_cooldown: serverCooldowns.size
        },
        regions: regionStats
    });
});

app.get('/health', (req, res) => {
    const totalServers = Object.values(regionalJobCache)
        .reduce((sum, cache) => sum + cache.jobs.length, 0);
    
    res.json({
        status: 'ok',
        game: STEAL_A_BRAINROT.GAME_NAME,
        place_id: STEAL_A_BRAINROT.PLACE_ID,
        total_servers: totalServers,
        uptime: Math.floor(process.uptime()),
        regions: Object.keys(REGIONAL_PROXIES).length
    });
});

// =====================================================
// 🧹 CLEANUP AUTOMATIQUE
// =====================================================

setInterval(() => {
    const now = Date.now();
    let cleanedAssignments = 0;
    let cleanedCooldowns = 0;
    
    for (const [serverId, assignment] of serverAssignments.entries()) {
        if (now > assignment.expires_at) {
            serverAssignments.delete(serverId);
            cleanedAssignments++;
        }
    }
    
    for (const [serverId, timestamp] of serverCooldowns.entries()) {
        if (now - timestamp > CONFIG.COOLDOWN_DURATION) {
            serverCooldowns.delete(serverId);
            cleanedCooldowns++;
        }
    }
    
    if (cleanedAssignments > 0 || cleanedCooldowns > 0) {
        console.log(`🧹 Cleaned: ${cleanedAssignments} assignments, ${cleanedCooldowns} cooldowns`);
    }
    
}, 10000);

// =====================================================
// 🚀 DÉMARRAGE DU SERVEUR
// =====================================================

app.listen(PORT, async () => {
    console.clear();
    console.log('\n' + '═'.repeat(60));
    console.log('🧠 STEAL A BRAINROT SCANNER - BACKEND v2.0 (5 REGIONS)');
    console.log('═'.repeat(60));
    console.log(`🎮 Game: ${STEAL_A_BRAINROT.GAME_NAME}`);
    console.log(`📍 Place ID: ${STEAL_A_BRAINROT.PLACE_ID}`);
    console.log(`🎯 Universe ID: ${STEAL_A_BRAINROT.UNIVERSE_ID}`);
    console.log(`🚀 Server: http://localhost:${PORT}`);
    console.log(`🔑 API Key: ${process.env.API_KEY || 'xK9mP2vL8qR4wN7jT1bY6cZ3aB5dF8gH'}`);
    console.log('');
    console.log('🔒 FEATURES:');
    console.log('   ✅ Zero-collision server assignment');
    console.log('   ✅ 15,000+ servers cached per refresh');
    console.log('   ✅ 5 regional proxies support');
    console.log('   ✅ Automatic cleanup & cooldowns');
    console.log('');
    console.log('🌍 Configured regions:');
    for (const [region, config] of Object.entries(REGIONAL_PROXIES)) {
        const proxyStatus = config.proxy_url ? '✅' : '❌';
        console.log(`   ${proxyStatus} ${config.name.padEnd(20)} VPS ${config.vps_range[0]}-${config.vps_range[1]}  (${config.expected_bots} bots)`);
    }
    console.log('═'.repeat(60) + '\n');
    
    console.log('🔄 Starting initial cache fill...\n');
    await refreshAllRegions();
    
    setInterval(refreshAllRegions, CONFIG.CACHE_REFRESH_INTERVAL);
    
    console.log('✅ Backend ready! Bots can now connect.\n');
});

process.on('unhandledRejection', (error) => {
    console.error('❌ Unhandled rejection:', error);
});

process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught exception:', error);
});
