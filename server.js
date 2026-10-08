// Общий сервер игры «Арктическая база». Запуск: npm install && npm start
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 32;
const GAME_VER = '2026.10.08'; // должна совпадать с GAME_VER в игре
const clients = new Map();
let hostId = null, counter = 0;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Arctic server OK. players: ' + [...clients.values()].filter(c => c.joined).length + '\n');
});
const wss = new WebSocketServer({ server, maxPayload: 4 * 1024 * 1024 });

const send = (c, o) => { if (c && c.ws.readyState === 1) { try { c.ws.send(typeof o === 'string' ? o : JSON.stringify(o)); } catch (e) {} } };
const joinedList = () => [...clients.entries()].filter(([, c]) => c.joined);
const broadcast = (o, exceptId) => { const s = JSON.stringify(o); for (const [id, c] of joinedList()) if (id !== exceptId) send(c, s); };
const count = () => joinedList().length;

function electHost() {
  if (hostId && clients.has(hostId) && clients.get(hostId).joined) return;
  const first = joinedList().sort((a, b) => a[1].t - b[1].t)[0];
  hostId = first ? first[0] : null;
  if (hostId) send(clients.get(hostId), { t: 'role', id: hostId, host: true, n: count() });
}

wss.on('connection', (ws) => {
  const id = (++counter).toString(36) + Math.random().toString(36).slice(2, 5);
  const c = { ws, joined: false, t: Date.now(), win: Date.now(), cnt: 0, alive: true };
  clients.set(id, c);
  ws.on('pong', () => { c.alive = true; });
  const kill = setTimeout(() => { if (!c.joined) ws.close(); }, 10000);

  ws.on('message', (data) => {
    let m; try { m = JSON.parse(data); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const now = Date.now();
    if (now - c.win > 1000) { c.win = now; c.cnt = 0; }
    if (++c.cnt > 600) return;
    if (!c.joined) {
      if (m.t !== 'join') return;
      if (m.ver !== GAME_VER) { send(c, { t: 'err', m: 'Версия игры не совпадает с сервером. Обновите файл игры.' }); ws.close(); return; }
      if (count() >= MAX_PLAYERS) { send(c, { t: 'err', m: 'Сервер заполнен' }); ws.close(); return; }
      c.joined = true; c.t = now; clearTimeout(kill);
      if (!hostId) hostId = id;
      send(c, { t: 'role', id, host: hostId === id, n: count() });
      broadcast({ t: 'cnt', n: count() });
      return;
    }
    if (m.t === 'hb') { send(c, { t: 'hr', ts: m.ts }); return; }
    m.from = id;
    const to = m.to; delete m.to;
    if (to === 'host') { if (hostId && hostId !== id) send(clients.get(hostId), m); }
    else if (to) { const o = clients.get(to); if (o && o.joined) send(o, m); }
    else broadcast(m, id);
  });

  ws.on('close', () => {
    clearTimeout(kill);
    const was = c.joined;
    clients.delete(id);
    if (!was) return;
    broadcast({ t: 'left', id });
    if (hostId === id) { hostId = null; electHost(); }
    broadcast({ t: 'cnt', n: count() });
  });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const [id, c] of clients) {
    if (!c.alive) { try { c.ws.terminate(); } catch (e) {} continue; }
    c.alive = false; try { c.ws.ping(); } catch (e) {}
  }
}, 30000);

server.listen(PORT, () => console.log('Сервер запущен на порту ' + PORT));
