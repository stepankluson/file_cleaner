const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const os = require('os');
const cron = require('node-cron');
const crypto = require('crypto');
const http = require('http'); // Nové pro svázání expressu se sockety
const { Server } = require('socket.io'); // Socket.io server

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
        const category = getCategory(ext);

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

function deleteEmptyFoldersRecursive(dir) {
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

    items.forEach(item => {
        const fullPath = path.join(dir, item);

        // KRITICKÝ BLACKLIST
        if (item === 'node_modules' ||
            item === '.git' ||
            item === 'Windows' ||
            item === 'AppData' ||
            item === '_UKLIZENO_' ||
            item.startsWith('_ARCHIV_') ||
            item.startsWith('.')) {
            return;
        }

        let stat;
        try {
            stat = fs.statSync(fullPath);
        } catch (err) {
            return;
        }

        if (stat.isDirectory()) {
            // Nejdříve se zanoříme dolů (post-order traversal)
            deletedCount += deleteEmptyFoldersRecursive(fullPath);

            // Poté zkontrolujeme, zda se složka nevyprázdnila
            try {
                const remaining = fs.readdirSync(fullPath);
                if (remaining.length === 0) {
                    fs.rmdirSync(fullPath);
                    deletedCount++;
                }
            } catch (err) {
                // Ignorujeme, pravděpodobně oprávnění nebo smazáno jinak
            }
        }
    });

    return deletedCount;
}

app.post('/api/delete-empty-folders', (req, res) => {
    try {
        const rawPath = req.body.path || path.join(os.homedir(), 'Downloads');
        const targetPath = path.resolve(path.normalize(rawPath));

        console.log('Pokus o MAZÁNÍ PRÁZDNÝCH SLOŽEK ve složce:', targetPath);

        if (!fs.existsSync(targetPath)) {
            return res.status(400).json({ error: 'Složka na disku neexistuje nebo je špatně zadaná cesta' });
        }

        const deletedCount = deleteEmptyFoldersRecursive(targetPath);
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

            filesData.push({
                name: path.basename(fullPath),
                path: fullPath,
                sizeRaw: stat.size,
                sizeFormatted: formatBytes(stat.size)
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
app.post('/api/delete-files', (req, res) => {
    try {
        const filesToDelete = req.body.files;
        if (!Array.isArray(filesToDelete)) return res.status(400).json({ error: 'Očekáváno pole cest' });

        let deleted = 0;
        filesToDelete.forEach(fp => {
            try {
                fs.unlinkSync(fp);
                addLog(`Smazán duplikát: ${path.basename(fp)}`, 'success');
                deleted++;
            } catch (e) {
                addLog(`Nelze smazat duplikát ${fp}`, 'error');
            }
        });

        res.json({ success: true, deletedCount: deleted });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

server.listen(PORT, () => {
    console.log(`[OK] Web+Socket Server běží na portu ${PORT}`);
});
