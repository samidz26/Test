const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const { WebcastPushConnection } = require('tiktok-live-connector');

const PORT = 3000, ROUND_SECONDS = 15, POINTS = [5, 3, 1];
// [السؤال, الوحدة, الجواب] — أسئلة تجريبية
const QUESTIONS = [
  ['كم يبلغ عدد سكان العالم تقريباً؟', 'بالمليار', 8.1],
  ['كم يبلغ ارتفاع برج إيفل؟', 'بالمتر', 330],
  ['كم عدد عظام جسم الإنسان البالغ؟', 'عظمة', 206],
  ['كم عدد الدول الأعضاء في الأمم المتحدة؟', 'دولة', 193],
  ['كم يبلغ ارتفاع قمة إيفرست؟', 'بالمتر', 8849],
  ['كم عدد أسنان الإنسان البالغ؟', 'سن', 32],
  ['كم تبلغ سرعة الضوء تقريباً؟', 'ألف كم/ثانية', 299.8],
  ['كم عدد ألوان قوس قزح؟', 'لون', 7],
  ['في أي سنة هبط الإنسان على القمر؟', 'سنة ميلادية', 1969],
  ['كم عدد كواكب المجموعة الشمسية؟', 'كوكب', 8],
  ['عند أي درجة يغلي الماء بالفهرنهايت؟', 'درجة فهرنهايت', 212],
  ['كم عدد حروف اللغة العربية؟', 'حرف', 28],
  ['كم تبلغ مساحة الجزائر؟', 'مليون كم²', 2.38],
  ['في أي سنة تأسست شركة جوجل؟', 'سنة ميلادية', 1998],
  ['كم تبلغ حرارة سطح الشمس تقريباً؟', 'درجة مئوية', 5500],
];

const AV_DIR = path.join(__dirname, 'avatars');
fs.mkdirSync(AV_DIR, { recursive: true });

const server = http.createServer((req, res) => {
  const url = decodeURIComponent(req.url.split('?')[0]);
  if (url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  }
  if (url.startsWith('/avatars/')) {
    const f = path.join(AV_DIR, path.basename(url));
    if (fs.existsSync(f)) {
      res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'max-age=86400' });
      return fs.createReadStream(f).pipe(res);
    }
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server });

let phase = 'connect', idx = -1, endsAt = 0, timer = null, result = null, conn = null, demoTimer = null;
let answers = new Map();
const players = new Map();

const broadcast = (m) => { const s = JSON.stringify(m); wss.clients.forEach(c => c.readyState === 1 && c.send(s)); };
const av = (p) => p.local ? `/avatars/${p.id}.img` : p.remote;

function card(id) {
  const a = answers.get(id), p = players.get(id);
  return { id, name: p.name, avatar: av(p), value: a.value,
    diff: phase === 'revealed' ? Math.abs(a.value - QUESTIONS[idx][2]) : undefined };
}
function ranking() {
  return [...players.values()]
    .sort((a, b) => b.points - a.points || b.wins[0] - a.wins[0] || b.wins[1] - a.wins[1])
    .map(p => ({ id: p.id, name: p.name, avatar: av(p), points: p.points, wins: p.wins }));
}
function snap() {
  return { type: 'state', phase, idx, total: QUESTIONS.length,
    q: idx >= 0 ? { text: QUESTIONS[idx][0], unit: QUESTIONS[idx][1] } : null,
    remaining: phase === 'open' ? Math.max(0, endsAt - Date.now()) : 0,
    seconds: ROUND_SECONDS, players: players.size,
    answers: [...answers.keys()].map(card), result,
    ranking: phase === 'final' ? ranking() : null };
}

// أرقام فقط: يدعم الأرقام العربية والفواصل كفواصل آلاف
function parseNum(t) {
  const s = String(t).trim()
    .replace(/[٠-٩]/g, d => d.charCodeAt(0) - 1632)
    .replace(/[۰-۹]/g, d => d.charCodeAt(0) - 1776)
    .replace(/٫/g, '.').replace(/[,٬،]/g, '');
  return /^-?\d+(\.\d+)?$/.test(s) ? parseFloat(s) : null;
}

async function cacheAvatar(p) {
  if (!p.remote || p.local) return;
  try {
    const r = await fetch(p.remote);
    if (!r.ok) return;
    fs.writeFileSync(path.join(AV_DIR, p.id + '.img'), Buffer.from(await r.arrayBuffer()));
    p.local = true;
    broadcast({ type: 'avatar', id: p.id, url: av(p) });
  } catch {}
}

function onComment(d) {
  if (phase !== 'open' || Date.now() > endsAt) return;
  const v = parseNum(d.comment || '');
  if (v === null) return;
  const id = String(d.userId || d.uniqueId).replace(/[^\w-]/g, '_');
  if (answers.has(id)) return;
  let p = players.get(id);
  if (!p) {
    p = { id, name: d.nickname || d.uniqueId, remote: d.profilePictureUrl || '', local: false, points: 0, wins: [0, 0, 0] };
    players.set(id, p);
  }
  answers.set(id, { value: v, t: process.hrtime.bigint() });
  broadcast({ type: 'answer', card: card(id), players: players.size });
  cacheAvatar(p);
}

function startRound() {
  idx++; answers = new Map(); result = null; phase = 'open';
  endsAt = Date.now() + ROUND_SECONDS * 1000;
  clearTimeout(timer); timer = setTimeout(reveal, ROUND_SECONDS * 1000);
  broadcast(snap());
}

function reveal() {
  if (phase !== 'open') return;
  const correct = QUESTIONS[idx][2];
  const rows = [...answers.entries()]
    .map(([id, a]) => ({ id, t: a.t, diff: Math.abs(a.value - correct) }))
    .sort((a, b) => a.diff - b.diff || (a.t < b.t ? -1 : a.t > b.t ? 1 : 0));
  phase = 'revealed';
  rows.slice(0, 3).forEach((r, i) => { const p = players.get(r.id); p.points += POINTS[i]; p.wins[i]++; });
  result = { correct, winners: rows.slice(0, 3).map((r, i) => ({ ...card(r.id), points: POINTS[i] })) };
  broadcast(snap());
}

function startDemo() {
  clearInterval(demoTimer);
  demoTimer = setInterval(() => {
    if (phase !== 'open') return;
    for (let i = 0; i < 3; i++) {
      const n = Math.floor(Math.random() * 40), c = QUESTIONS[idx][2];
      onComment({ userId: 'demo' + n, nickname: 'لاعب ' + (n + 1),
        comment: String(Math.round(c * (0.3 + Math.random() * 1.4) * 100) / 100) });
    }
  }, 500);
}

function connectTikTok(ws, username) {
  const user = String(username || '').replace('@', '').trim();
  if (!user) return ws.send(JSON.stringify({ type: 'conn', ok: false, msg: 'أدخل اسم المستخدم' }));
  conn = new WebcastPushConnection(user, {
    processInitialData: false, enableWebsocketUpgrade: true, requestPollingIntervalMs: 1000,
  });
  conn.on('chat', onComment);
  conn.on('disconnected', () => broadcast({ type: 'conn', ok: false, msg: 'انقطع الاتصال بالبث' }));
  conn.connect().then(() => {
    phase = 'idle'; broadcast(snap());
  }).catch(e => ws.send(JSON.stringify({ type: 'conn', ok: false, msg: 'فشل الاتصال: ' + (e.message || e) })));
}

wss.on('connection', (ws) => {
  ws.send(JSON.stringify(snap()));
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.type === 'connect' && phase === 'connect') connectTikTok(ws, m.username);
    else if (m.type === 'demo' && phase === 'connect') { phase = 'idle'; startDemo(); broadcast(snap()); }
    else if (m.type === 'next' && (phase === 'idle' || phase === 'revealed')) {
      if (idx >= QUESTIONS.length - 1) { phase = 'final'; broadcast(snap()); } else startRound();
    } else if (m.type === 'newgame' && phase === 'final') {
      players.clear(); answers = new Map(); idx = -1; result = null; phase = 'idle'; broadcast(snap());
    }
  });
});

server.listen(PORT, () => console.log(`افتح http://localhost:${PORT}`));
