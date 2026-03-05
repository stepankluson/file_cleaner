require('dotenv').config();
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const os = require('os');
const cron = require('node-cron');
const crypto = require('crypto');
const http = require('http'); // Nové pro svázání expressu se sockety
const { Server } = require('socket.io'); // Socket.io server
const trash = require('trash'); // Smazání do koše
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);


const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = 3000;

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// --- LOGOVACÍ HELPER (zápis do terminálu i odeslání do UI) ---
let totalOdmasteno = 0;

function addLog(message, type = 'info') {
    const timestamp = new Date().toLocaleTimeString();
    const formatted = `[${timestamp}] [${type.toUpperCase()}] ${message}`;

    // Klasický console log
    console.log(formatted);

    // Odeslání všem připojeným klientům (Real-time UI okno)
    io.emit('log', { timestamp, type, message });
}

// Při připojení nového klienta
io.on('connection', (socket) => {
    console.log('Klient připojen k Socket.io');
    socket.emit('log', { timestamp: new Date().toLocaleTimeString(), type: 'info', message: 'Připojeno k WebSocket serveru...' });
});

const extensionsMap = {
    'Obrazky': ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.bmp', '.ico'],
    'Videa': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.wmv', '.flv'],
    'Hudba': ['.mp3', '.wav', '.flac', '.ogg', '.m4a', '.aac'],
    'Dokumenty': ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt', '.csv', '.md', '.rtf'],
    'Programovani': ['.html', '.css', '.js', '.ts', '.json', '.py', '.cpp', '.php', '.sql', '.xml', '.java'],
    'Archivy': ['.zip', '.rar', '.7z', '.tar', '.gz'],
    'Aplikace': ['.exe', '.msi', '.dmg', '.pkg', '.deb', '.apk'],
    'Design_3D': ['.psd', '.ai', '.blend', '.fbx', '.obj']
};

function getCategory(ext) {
    for (const [category, exts] of Object.entries(extensionsMap)) {
        if (exts.includes(ext.toLowerCase())) {
            return category;
        }
    }
    return 'Ostatni';
}

// -- AUTOPILOT STATE --
let activeCronJob = null;
let autopilotSettings = {
    active: false,
    schedule: null, // Bude uchovávat formát pro node-cron
    targetPath: null
};

// -- HLAVNÍ FUNKCE ÚKLIDU --
// Extraktovaná, aby se dala volat manuálně i automatem
function performCleanup(targetPath) {
    if (!fs.existsSync(targetPath)) {
        throw new Error(`Složka ${targetPath} neexistuje na disku.`);
    }

    const uklizenoPath = path.join(targetPath, '_UKLIZENO_');
    if (!fs.existsSync(uklizenoPath)) {
        fs.mkdirSync(uklizenoPath, { recursive: true });
    }

    let items = [];
    try {
        items = fs.readdirSync(targetPath);
    } catch (err) {
        if (err && /EPERM|EACCES|EBUSY/.test(err.code || err.message)) {
            console.log(`Přeskočeno kvůli oprávnění: ${targetPath}`);
            return 0; // Nelze číst, vracíme 0 
        }
        throw err;
    }

    let movedCount = 0;

    items.forEach(item => {
        const oldPath = path.join(targetPath, item);

        // Ignorujeme složku _UKLIZENO_ a skryté soubory
        if (item === '_UKLIZENO_' || item.startsWith('.')) return;

        let stat;
        try {
            stat = fs.statSync(oldPath);
        } catch (err) {
            if (err && /EPERM|EACCES|EBUSY/.test(err.code || err.message)) {
                console.log(`Přeskočeno kvůli oprávnění: ${oldPath}`);
            }
            return;
        }

        if (!stat.isFile()) return;

        const ext = path.extname(item);
        let category = getCategory(ext);

        // Inteligentní klasifikátor (AI logika)
        const lowerItem = item.toLowerCase();
        if (lowerItem.includes('screen') || lowerItem.includes('snímek') || lowerItem.includes('shot')) {
            category = path.join('Obrazky', 'Screenshoty');
        } else {
            const match = item.match(/\b(202[0-9])\b/);
            if (match) {
                category = path.join(category, match[1]);
            }
        }

        const categoryFolder = path.join(uklizenoPath, category);
        if (!fs.existsSync(categoryFolder)) {
            fs.mkdirSync(categoryFolder, { recursive: true });
        }

        const newPath = path.join(categoryFolder, item);
        try {
            fs.renameSync(oldPath, newPath);
            movedCount++;
        } catch (err) {
            console.error(`Nelze přesunout soubor ${item}:`, err);
        }
    });

    return movedCount;
}

// 1. Manuální úklid na požádání
app.post('/api/clean', (req, res) => {
    try {
        // Zkusíme vzít cestu z frontendu, jinak defaultně 'Downloads'
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        console.log('Pokus o úklid ve složce:', targetPath);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje nebo je špatně zadaná cesta' });
        }

        const movedCount = performCleanup(targetPath);
        res.json({ success: true, movedCount });

    } catch (error) {
        console.error('Kritická chyba při manuálním úklidu:', error);
        res.status(500).json({ error: error.message });
    }
});

// 2. Autopilot Start
app.post('/api/cron/start', (req, res) => {
    try {
        const { path: rawPath, schedule } = req.body;

        if (!rawPath || !schedule) {
            return res.status(400).json({ error: 'Chybí konfigurace (cesta nebo cron zápis)' });
        }

        const targetPath = path.resolve(path.normalize(rawPath));
        console.log('Pokus o zapnutí Autopilota pre složku:', targetPath);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje nebo je špatně zadaná cesta' });
        }

        // Zastavíme předchozí úlohu, pokud nějaká běžela
        if (activeCronJob) {
            activeCronJob.stop();
        }

        // Vytvoření cron jobu
        activeCronJob = cron.schedule(schedule, () => {
            console.log(`\n[CRON] ${new Date().toLocaleString()} - Autopilot iniciován...`);
            try {
                const moved = performCleanup(targetPath);
                console.log(`[CRON] Úklid složky [${targetPath}] hotov. Přesunuto ${moved} souborů.`);
            } catch (err) {
                console.error(`[CRON] Autopilot narazil na chybu:`, err.message);
            }
        });

        autopilotSettings = {
            active: true,
            schedule,
            targetPath
        };

        console.log(`\n[INFO] Autopilot ZAPNUT: ${schedule} -> ${targetPath}`);
        res.json({ success: true, message: 'Autopilot aktivován', autopilot: autopilotSettings });

    } catch (error) {
        console.error('Chyba při startu cronu:', error);
        res.status(500).json({ error: error.message });
    }
});

// 3. Autopilot Stop
app.post('/api/cron/stop', (req, res) => {
    if (activeCronJob) {
        activeCronJob.stop();
        activeCronJob = null;
    }

    autopilotSettings.active = false;
    console.log('\n[INFO] Autopilot VYPNUT.');
    res.json({ success: true, message: 'Autopilot vypnut', autopilot: autopilotSettings });
});

// 4. Dotaz na status frontendu (pro načtení defaultní cesty i stavu cronu)
app.get('/api/status', (req, res) => {
    const defaultPath = path.join(os.homedir(), 'Downloads');
    res.json({ defaultPath, autopilot: autopilotSettings });
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// -- MAZÁNÍ PRÁZDNÝCH SLOŽEK --

async function deleteEmptyFoldersRecursive(dir) {
    let deletedCount = 0;
    let items;
    try {
        items = fs.readdirSync(dir);
    } catch (err) {
        if (err && /EPERM|EACCES|EBUSY/.test(err.code || err.message)) {
            console.log(`Přeskočena složka kvůli oprávnění: ${dir}`);
        }
        return deletedCount;
    }

    for (const item of items) {
        const fullPath = path.join(dir, item);

        // KRITICKÝ BLACKLIST
        if (item === 'node_modules' ||
            item === '.git' ||
            item === 'Windows' ||
            item === 'AppData' ||
            item === '_UKLIZENO_' ||
            item.startsWith('_ARCHIV_') ||
            item.startsWith('.')) {
            continue;
        }

        let stat;
        try {
            stat = fs.statSync(fullPath);
        } catch (err) {
            continue;
        }

        if (stat.isDirectory()) {
            // Nejdříve se zanoříme dolů (post-order traversal)
            deletedCount += await deleteEmptyFoldersRecursive(fullPath);

            // Poté zkontrolujeme, zda se složka nevyprázdnila
            try {
                const remaining = fs.readdirSync(fullPath);
                if (remaining.length === 0) {
                    await trash(fullPath);
                    deletedCount++;
                }
            } catch (err) {
                // Ignorujeme, pravděpodobně oprávnění nebo smazáno jinak
            }
        }
    }

    return deletedCount;
}

app.post('/api/delete-empty-folders', async (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        console.log('Pokus o MAZÁNÍ PRÁZDNÝCH SLOŽEK ve složce:', targetPath);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje nebo je špatně zadaná cesta' });
        }

        const deletedCount = await deleteEmptyFoldersRecursive(targetPath);
        res.json({ success: true, deletedCount });

    } catch (error) {
        console.error('Kritická chyba při mazání prázdných podsložek:', error);
        res.status(500).json({ error: error.message });
    }
});

// --- NOVÉ FUNKCE PRO ARCHIVACI A ANALÝZU ---

// Pomocná fce na formátování bytů
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Rekurzivní hloubkové prohledávání
function getSafeFilesRecursive(dir, fileList = []) {
    let items;
    try {
        items = fs.readdirSync(dir);
    } catch (err) {
        if (err && /EPERM|EACCES|EBUSY/.test(err.code || err.message)) {
            addLog(`Přeskočena složka kvůli oprávnění: ${dir}`, 'warning');
        }
        return fileList;
    }

    items.forEach(item => {
        const fullPath = path.join(dir, item);
        if (item === 'node_modules' ||
            item === '.git' ||
            item === 'Windows' ||
            item === 'AppData' ||
            item === '_UKLIZENO_' ||
            item === '_ARCHIV_' ||
            item.startsWith('.')) {
            return;
        }

        let stat;
        try { stat = fs.statSync(fullPath); } catch (err) { return; }

        if (stat.isDirectory()) {
            getSafeFilesRecursive(fullPath, fileList);
        } else if (stat.isFile()) {
            fileList.push(fullPath);
        }
    });
    return fileList;
}

// 5. Archivace starých souborů
app.post('/api/archive', (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const days = parseInt(req.body.days, 10) || 180;
        const targetPath = path.resolve(path.normalize(rawPath));

        if (days <= 0) {
            return res.status(400).json({ error: 'Počet dní musí být kladné číslo.' });
        }

        addLog(`Pokus o ARCHIVACI (${days} dní): ${targetPath}`);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje' });
        }

        let movedCount = 0;
        const now = new Date();
        const maxAgeMs = days * 24 * 60 * 60 * 1000;

        const allFiles = getSafeFilesRecursive(targetPath);
        const folderName = `_ARCHIV_${days}_DNI`;
        const archivePath = path.join(targetPath, folderName);

        allFiles.forEach(fullPath => {
            let stat;
            try { stat = fs.statSync(fullPath); } catch (err) { return; }

            const mTime = new Date(stat.mtime);
            if (now - mTime > maxAgeMs) {
                if (!fs.existsSync(archivePath)) {
                    fs.mkdirSync(archivePath, { recursive: true });
                }

                let fileName = path.basename(fullPath);
                let newPath = path.join(archivePath, fileName);

                if (fs.existsSync(newPath) && newPath !== fullPath) {
                    const ext = path.extname(fileName);
                    const nameWithoutExt = path.basename(fileName, ext);
                    fileName = `${nameWithoutExt}_${Date.now()}${ext}`;
                    newPath = path.join(archivePath, fileName);
                }

                try {
                    fs.renameSync(fullPath, newPath);
                    addLog(`Archivován: ${fileName}`, 'success');
                    movedCount++;
                } catch (err) { }
            }
        });

        res.json({ success: true, movedCount });
        addLog(`Archivace hotova. Přesunuto ${movedCount} souborů.`);

    } catch (error) {
        addLog(`Chyba archivace: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/analyze-storage', (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        addLog(`Spouštím ANALÝZU: ${targetPath}`);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje' });
        }

        const allFiles = getSafeFilesRecursive(targetPath);
        const filesData = [];
        const categoryStats = {
            'Obrazky': 0, 'Videa': 0, 'Hudba': 0,
            'Dokumenty': 0, 'Programovani': 0, 'Archivy': 0,
            'Aplikace': 0, 'Ostatni': 0
        };

        allFiles.forEach(fullPath => {
            let stat;
            try { stat = fs.statSync(fullPath); } catch (err) { return; }

            const ext = path.extname(fullPath).toLowerCase();
            const cat = getCategory(ext);

            if (categoryStats.hasOwnProperty(cat)) {
                categoryStats[cat]++;
            } else {
                categoryStats['Ostatni']++;
            }

            const fileName = path.basename(fullPath);
            const lowerFileName = fileName.toLowerCase();
            const isInstaller = (ext === '.exe' || ext === '.msi' || ext === '.dmg') &&
                (lowerFileName.includes('setup') || lowerFileName.includes('install'));

            filesData.push({
                name: fileName,
                path: fullPath,
                sizeRaw: stat.size,
                sizeFormatted: formatBytes(stat.size),
                isInstaller: isInstaller
            });
        });

        filesData.sort((a, b) => b.sizeRaw - a.sizeRaw);
        const topFiles = filesData.slice(0, 10);

        res.json({ success: true, topFiles, categoryStats });
        addLog('Analýza složky plně dokončena.', 'success');

    } catch (error) {
        addLog(`Kritická chyba analýzy: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

// MD5 Duplicates Finder
function generateChecksum(filePath) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('md5');
        const stream = fs.createReadStream(filePath);
        stream.on('error', err => reject(err));
        stream.on('data', chunk => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
    });
}

// Endpoint pro nalezení a odeslání duplikátů
app.post('/api/duplicates', async (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        addLog(`Startuji MD5 Hash sken pro duplikáty: ${targetPath}`);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka neexistuje.' });
        }

        const allFiles = getSafeFilesRecursive(targetPath);
        const hashmap = new Map();
        const duplicates = [];

        for (const filePath of allFiles) {
            try {
                // Přeskočit 0 Btt
                const st = fs.statSync(filePath);
                if (st.size === 0) continue;

                const sum = await generateChecksum(filePath);

                if (hashmap.has(sum)) {
                    hashmap.get(sum).push({ path: filePath, size: st.size });
                } else {
                    hashmap.set(sum, [{ path: filePath, size: st.size }]);
                }
            } catch (e) { /* ignore read errors */ }
        }

        hashmap.forEach((fileList, hashValue) => {
            if (fileList.length > 1) {
                duplicates.push({
                    hash: hashValue,
                    sizeFormatted: formatBytes(fileList[0].size),
                    files: fileList.map(f => f.path)
                });
            }
        });

        addLog(`Nalezeno ${duplicates.length} skupin duplicitních souborů.`, 'info');
        res.json({ success: true, duplicates });
    } catch (error) {
        addLog(`Chyba MD5 skenu: ${error.message}`, 'error');
        res.status(500).json({ error: error.message });
    }
});

// Endpoint pro smazání vybraných listů souborů
app.post('/api/delete-files', async (req, res) => {
    try {
        const filesToDelete = req.body.files;
        if (!Array.isArray(filesToDelete)) return res.status(400).json({ error: 'Očekáváno pole cest' });

        let deleted = 0;
        for (const fp of filesToDelete) {
            try {
                await trash(fp);
                addLog(`Přesunut duplikát do Koše: ${path.basename(fp)}`, 'success');
                deleted++;
            } catch (e) {
                addLog(`Nelze přesunout duplikát do Koše: ${fp}`, 'error');
            }
        }

        res.json({ success: true, deletedCount: deleted });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// --- CACHE STRIKER ---
const cachePaths = [
    os.tmpdir(),
    path.join(os.homedir(), 'AppData', 'Local', 'Temp'),
    path.join(os.homedir(), 'AppData', 'Local', 'Google', 'Chrome', 'User Data', 'Default', 'Cache'),
    path.join(os.homedir(), 'AppData', 'Local', 'Microsoft', 'Edge', 'User Data', 'Default', 'Cache')
];

app.get('/api/scan-cache', async (req, res) => {
    try {
        let totalSize = 0;
        let fileCount = 0;

        for (const cacheDir of cachePaths) {
            if (fs.existsSync(cacheDir)) {
                const files = getSafeFilesRecursive(cacheDir);
                for (const fp of files) {
                    try {
                        const st = fs.statSync(fp);
                        totalSize += st.size;
                        fileCount++;
                    } catch (e) { }
                }
            }
        }
        res.json({ success: true, totalSize, totalSizeFormatted: formatBytes(totalSize), fileCount });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/api/clean-cache', async (req, res) => {
    try {
        let deletedCount = 0;
        let freedSpace = 0;

        addLog(`[CACHE STRIKER] Startuji hloubkové čištění mezipamětí...`);

        for (const cacheDir of cachePaths) {
            if (fs.existsSync(cacheDir)) {
                const files = getSafeFilesRecursive(cacheDir);
                for (const fp of files) {
                    try {
                        const st = fs.statSync(fp);
                        await trash(fp);
                        freedSpace += st.size;
                        deletedCount++;
                    } catch (e) { }
                }
            }
        }

        addLog(`[CACHE STRIKER] Hloubkové čištění dokončeno. Uvolněno: ${formatBytes(freedSpace)} (${deletedCount} souborů).`, 'success');
        res.json({ success: true, deletedCount, freedSpaceFormatted: formatBytes(freedSpace) });
    } catch (err) {
        addLog(`Chyba při hloubkovém čištění: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

// --- VIRUSTOTAL & MRTVI ZASTUPCI ---
function generateSHA256Checksum(filePath) {
    return new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const stream = fs.createReadStream(filePath);
        stream.on('error', err => reject(err));
        stream.on('data', chunk => hash.update(chunk));
        stream.on('end', () => resolve(hash.digest('hex')));
    });
}

app.post('/api/virus-check', async (req, res) => {
    try {
        const filePath = req.body.path;
        if (!filePath || !fs.existsSync(filePath)) {
            return res.status(400).json({ error: 'Soubor neexistuje nebo nebyla zadaná cesta.' });
        }

        addLog(`Počítám SHA-256 hash pro: ${path.basename(filePath)}`);
        const fileHash = await generateSHA256Checksum(filePath);

        // Zde vložte svůj VirusTotal API klíč
        const vtApiKey = process.env.VT_API_KEY;

        addLog(`Dotazuji se VirusTotal API pro hash: ${fileHash.substring(0, 8)}...`, 'info');

        if (vtApiKey === 'PLACEHOLDER_KEY') {
            // Nemáme reálný klíč, použijeme simulaci
            const isClean = Math.random() > 0.1;
            const maliciousCount = isClean ? 0 : Math.floor(Math.random() * 5) + 1;
            const totalScanners = 75;

            setTimeout(() => {
                addLog(`[SIMULACE] VirusTotal scan dokončen. Detekce: ${maliciousCount}/${totalScanners}`, maliciousCount > 0 ? 'warning' : 'success');
                res.json({
                    success: true,
                    score: `${maliciousCount}/${totalScanners}`
                });
            }, 1000);
            return;
        }

        // Reálný dotaz na API (pokud je zadán klíč)
        const response = await fetch(`https://www.virustotal.com/api/v3/files/${fileHash}`, {
            method: 'GET',
            headers: {
                'x-apikey': vtApiKey
            }
        });

        if (response.status === 404) {
            addLog(`Soubor není ve VirusTotal databázi.`, 'info');
            return res.json({ success: true, score: 'Neznámý' });
        }

        if (!response.ok) {
            throw new Error(`API chyba: ${response.statusText}`);
        }

        const data = await response.json();
        const stats = data.data.attributes.last_analysis_stats;
        const malicious = stats.malicious;
        const total = malicious + stats.suspicious + stats.undetected + stats.harmless + stats.timeout;

        addLog(`VirusTotal scan dokončen. Detekce: ${malicious}/${total}`, malicious > 0 ? 'warning' : 'success');

        res.json({
            success: true,
            score: `${malicious}/${total}`
        });

    } catch (err) {
        addLog(`Chyba při kontrolu virů: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

// Endpoint pro detekci mrtvých zástupců
app.post('/api/dead-links', async (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Desktop');
        const targetPath = path.resolve(path.normalize(rawPath));

        addLog(`Startuji detekci mrtvých zástupců (LNK): ${targetPath}`);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka neexistuje.' });
        }

        const allFiles = getSafeFilesRecursive(targetPath);
        const lnkFiles = allFiles.filter(f => f.toLowerCase().endsWith('.lnk'));
        const deadLinks = [];

        for (const lnk of lnkFiles) {
            try {
                // Použití PowerShellu pro získání cílové cesty zástupce
                const command = `powershell.exe -NoProfile -Command "(New-Object -COM WScript.Shell).CreateShortcut('${lnk.replace(/'/g, "''")}').TargetPath"`;
                const { stdout } = await execPromise(command);
                const targetPoint = stdout.trim();

                if (targetPoint && !fs.existsSync(targetPoint)) {
                    deadLinks.push(lnk);
                }
            } catch (err) {
                // Přeskočit pokud nelze zpracovat
            }
        }

        addLog(`Nalezeno ${deadLinks.length} nefunkčních zástupců.`, 'info');
        res.json({ success: true, deadLinks });
    } catch (err) {
        addLog(`Chyba skenu mrtvých zástupců: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

// --- SMART SECURITY SCAN ---
const RISKY_EXTENSIONS = ['.exe', '.dll', '.bat', '.js', '.vbs', '.msi', '.ps1'];
const MAX_SIZE_BYTES = 50 * 1024 * 1024; // 50 MB
const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
const VT_API_KEY = process.env.VT_API_KEY;

app.post('/api/smart-scan', async (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka neexistuje.' });
        }

        addLog(`[SMART SCAN] Spouštím inteligentní bezpečnostní sken: ${targetPath}`);

        const allFiles = getSafeFilesRecursive(targetPath);
        const now = Date.now();
        const downloadsPath = path.join(os.homedir(), 'Downloads').toLowerCase();

        const riskyFiles = [];

        for (const fp of allFiles) {
            const ext = path.extname(fp).toLowerCase();
            if (!RISKY_EXTENSIONS.includes(ext)) continue;

            let st;
            try { st = fs.statSync(fp); } catch (e) { continue; }

            if (st.size > MAX_SIZE_BYTES) continue; // Přeskočit soubory > 50 MB

            const isFromDownloads = fp.toLowerCase().startsWith(downloadsPath);
            const isRecent = (now - new Date(st.birthtime).getTime()) < SEVEN_DAYS_MS;
            const priority = isFromDownloads && isRecent ? 0 : (isFromDownloads ? 1 : 2);

            riskyFiles.push({
                path: fp,
                name: path.basename(fp),
                ext,
                size: st.size,
                sizeFormatted: formatBytes(st.size),
                isRecent,
                isFromDownloads,
                priority,
                birthtime: st.birthtime
            });
        }

        // Seřadit: nejprve priorita (0 = stažené+nové), pak velikost vzestupně (malé exe jsou rizikovější)
        riskyFiles.sort((a, b) => a.priority - b.priority || a.size - b.size);

        addLog(`[SMART SCAN] Nalezeno ${riskyFiles.length} potenciálně rizikových souborů.`, 'info');
        res.json({ success: true, riskyFiles });

    } catch (err) {
        addLog(`[SMART SCAN] Chyba: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

// Hromadná dávková kontrola VirusTotal (s ohledem na rate limit)
app.post('/api/bulk-virus-check', async (req, res) => {
    try {
        const filePaths = req.body.files;
        if (!Array.isArray(filePaths) || filePaths.length === 0) {
            return res.status(400).json({ error: 'Očekáváno pole cest.' });
        }

        addLog(`[BULK VT] Spouštím hromadnou kontrolu ${filePaths.length} souborů...`);
        const results = [];

        for (const fp of filePaths) {
            try {
                if (!fs.existsSync(fp)) {
                    results.push({ path: fp, score: 'Soubor nenalezen', error: true });
                    continue;
                }

                const fileHash = await generateSHA256Checksum(fp);
                addLog(`[BULK VT] Kontroluji: ${path.basename(fp)} (${fileHash.substring(0, 8)}...)`, 'info');

                const response = await fetch(`https://www.virustotal.com/api/v3/files/${fileHash}`, {
                    method: 'GET',
                    headers: { 'x-apikey': VT_API_KEY }
                });

                if (response.status === 404) {
                    const st = fs.statSync(fp);
                    const risk = st.size < 1024 * 1024 ? 'VYSOKÉ (malý .exe - neznámý!)' : 'Nízké (velký soubor)';
                    results.push({ path: fp, score: 'Neznámý', unknown: true, risk });
                    addLog(`[BULK VT] ${path.basename(fp)}: Neznámý soubor (riziko: ${risk})`, 'warning');
                } else if (response.ok) {
                    const data = await response.json();
                    const stats = data.data.attributes.last_analysis_stats;
                    const malicious = stats.malicious;
                    const total = malicious + stats.suspicious + stats.undetected + stats.harmless + stats.timeout;
                    const score = `${malicious}/${total}`;
                    results.push({ path: fp, score, malicious });
                    addLog(`[BULK VT] ${path.basename(fp)}: ${score} detekcí`, malicious > 0 ? 'warning' : 'success');
                } else {
                    results.push({ path: fp, score: `API chyba: ${response.status}`, error: true });
                }

                // Rate limit VirusTotal free tier: ~4 req/min → čekáme 15s mezi dotazy
                await new Promise(resolve => setTimeout(resolve, 15000));

            } catch (e) {
                results.push({ path: fp, score: `Chyba: ${e.message}`, error: true });
            }
        }

        addLog(`[BULK VT] Hromadná kontrola dokončena. Zkontrolováno ${results.length} souborů.`, 'success');
        res.json({ success: true, results });

    } catch (err) {
        addLog(`[BULK VT] Kritická chyba: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

server.listen(PORT, () => {
    console.log(`[OK] Web+Socket Server běží na portu ${PORT}`);
});
