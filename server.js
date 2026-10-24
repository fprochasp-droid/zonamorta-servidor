// Servidor de partidas do Zona Morta 3D.
// Repassa posições, tiros e eventos entre os jogadores de cada sala (máximo de 20 por sala).
// Roda em qualquer hospedagem Node.js com WebSocket (ex.: Render). Porta: variável PORT.

const http = require('http');
const { WebSocketServer } = require('ws');

const MAX_POR_SALA = 20;
const SALAS = ['sala-1', 'sala-2', 'sala-3'];
const ORIGENS = (process.env.ORIGENS || '').split(',').map(s => s.trim()).filter(Boolean); // ex.: https://zonamorta.netlify.app

const rooms = new Map(SALAS.map(id => [id, { id, clients: new Map(), host: null, started: false, alive: new Set(), ending: false }]));
let nextId = 1;

function list() {
  return [...rooms.values()].map(r => ({ id: r.id, n: r.clients.size, started: r.started }));
}
function bc(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const [id, c] of room.clients) if (id !== except && c.ws.readyState === 1) c.ws.send(s);
}
function checkEnd(room) {
  if (!room.started || room.ending) return;
  if (room.alive.size <= 1 || room.clients.size === 0) {
    room.ending = true;
    setTimeout(() => {
      room.started = false; room.ending = false; room.alive = new Set();
      bc(room, { t: 'reset' });
    }, 8000);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/salas') {
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify(list()));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  res.end('Servidor do Zona Morta funcionando');
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

wss.on('connection', (ws, req) => {
  if (ORIGENS.length && !ORIGENS.includes(req.headers.origin)) { ws.close(1008, 'origem'); return; }
  let me = null, room = null;
  let budget = 40, last = Date.now(); // limite simples: ~40 mensagens/s por jogador
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.send(JSON.stringify({ t: 'rooms', rooms: list() }));

  function leave() {
    if (!room || !me) return;
    const r = room;
    r.clients.delete(me.id); r.alive.delete(me.id);
    if (r.host === me.id) r.host = r.clients.size ? r.clients.keys().next().value : null;
    bc(r, { t: 'leave', id: me.id, host: r.host });
    if (!r.clients.size) { r.started = false; r.ending = false; r.alive = new Set(); }
    else checkEnd(r);
    room = null; me = null;
  }

  ws.on('message', raw => {
    const now = Date.now();
    budget = Math.min(40, budget + (now - last) * 0.04); last = now;
    if (budget < 1) return;
    budget -= 1;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'list') { ws.send(JSON.stringify({ t: 'rooms', rooms: list() })); return; }
    if (m.t === 'join') {
      if (room) return;
      const r = rooms.get(m.room);
      if (!r) return;
      if (r.clients.size >= MAX_POR_SALA) { ws.send('{"t":"full"}'); return; }
      if (r.started) { ws.send('{"t":"busy"}'); return; }
      me = { id: nextId++, name: String(m.name || 'Sobrevivente').slice(0, 16), skin: (m.skin | 0) % 4, ws };
      room = r; r.clients.set(me.id, me);
      if (!r.host) r.host = me.id;
      ws.send(JSON.stringify({ t: 'welcome', id: me.id, host: r.host, now: Date.now(), room: r.id,
        players: [...r.clients.values()].map(p => ({ id: p.id, name: p.name, skin: p.skin })) }));
      bc(r, { t: 'join', id: me.id, name: me.name, skin: me.skin }, me.id);
      return;
    }
    if (!room || !me) return;
    if (m.t === 'leave') { leave(); ws.send(JSON.stringify({ t: 'rooms', rooms: list() })); return; }
    if (m.t === 'start') {
      if (me.id !== room.host || room.started) return;
      room.started = true; room.ending = false;
      room.alive = new Set(room.clients.keys());
      const seed = (Math.random() * 1e9) | 0, startAt = Date.now() + 4000;
      bc(room, { t: 'go', seed, startAt, now: Date.now(), players: [...room.clients.keys()] });
      return;
    }
    if (m.t === 'die') { room.alive.delete(me.id); checkEnd(room); }
    m.from = me.id;
    if (m.t === 'hit') {
      const c = room.clients.get(m.to);
      if (c && c.ws.readyState === 1) c.ws.send(JSON.stringify(m));
      return;
    }
    if (['s', 'f', 'die', 'pick', 'drop', 'chest'].includes(m.t)) bc(room, m, me.id);
  });
  ws.on('close', leave);
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 30000);

server.listen(process.env.PORT || 8080, () => console.log('Zona Morta servidor na porta', process.env.PORT || 8080));
