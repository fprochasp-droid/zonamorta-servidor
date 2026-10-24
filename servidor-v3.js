// Servidor de partidas do Zona Morta 3D (versão 3: grupos, chat, voz, bots).
// Salas de até 20 jogadores, grupos (squad) de até 4, chat de sala e de grupo,
// voz só entre membros do mesmo grupo e presença para a lista de amigos.
// Roda em qualquer hospedagem Node.js com WebSocket (ex.: Render). Porta: variável PORT.

const http = require('http');
const { WebSocketServer } = require('ws');

const MAX_POR_SALA = 20;
const MAX_GRUPO = 4;
const SALAS = ['sala-1', 'sala-2', 'sala-3'];
const COMPLETAR_ATE = Math.min(20, Math.max(0, parseInt(process.env.BOTS_ATE || '20', 10) || 20)); // total de sobreviventes com bots
const SKINS_BOT = ['mateiro', 'urbano', 'cacador', 'noturno', 'selva', 'deserto', 'motoca', 'socorrista', 'artico', 'cangaceiro', 'sombra', 'piloto', 'forcas'];
const NA = ['Lobo', 'Sniper', 'Fantasma', 'Corvo', 'Tigrão', 'Pantera', 'Mito', 'Brabo', 'Tropa', 'Caçador', 'Raio', 'Falcão', 'Bala', 'Ninja', 'Zé', 'Tonhão', 'Bigode', 'Jaguar', 'Trovão', 'Sombra', 'Vulto', 'Cobra', 'Gavião', 'Neblina', 'Furacão', 'Pólvora', 'Capitão', 'Marreta', 'Faísca', 'Coruja'];
const NB = ['BR', '_SP', 'Zika', '007', 'Pro', 'Real', 'XD', 'Doido', 'TV', 'Mata', 'Rei', 'God', 'Sz', 'Jr', 'Kid'];
const pk = a => a[(Math.random() * a.length) | 0];
function botName(used) {
  for (let k = 0; k < 30; k++) {
    let n = pk(NA); if (Math.random() < .55) n += (Math.random() < .5 ? '_' : '') + pk(NB); if (Math.random() < .55) n += ((Math.random() * 99) | 0) + 1;
    if (!used.has(n.toLowerCase())) { used.add(n.toLowerCase()); return n; }
  }
  return 'Sobrevivente' + ((Math.random() * 999) | 0);
}
const ORIGENS = (process.env.ORIGENS || '').split(',').map(s => s.trim().replace(/\/$/, '')).filter(Boolean);

const rooms = new Map(SALAS.map(id => [id, { id, clients: new Map(), host: null, started: false, alive: new Set(), ending: false, nextSquad: 1 }]));
const online = new Map(); // nome em minúsculas -> cliente
let nextId = 1;

const list = () => [...rooms.values()].map(r => ({ id: r.id, n: r.clients.size, started: r.started }));
const send = (ws, msg) => { if (ws.readyState === 1) ws.send(typeof msg === 'string' ? msg : JSON.stringify(msg)); };
function bc(room, msg, except) {
  const s = JSON.stringify(msg);
  for (const [id, c] of room.clients) if (id !== except) send(c.ws, s);
}
function squadMap(room) {
  const m = {};
  for (const [id, c] of room.clients) if (c.squad) m[id] = c.squad;
  return m;
}
function squadMembers(room, sid) {
  return [...room.clients.values()].filter(c => c.squad && c.squad === sid);
}
function cleanSquad(room, sid) {
  if (!sid) return;
  const mem = squadMembers(room, sid);
  if (mem.length === 1) mem[0].squad = null;
}
function pushSquads(room) { bc(room, { t: 'squads', map: squadMap(room) }); }
function teamKey(room, id) { if (typeof id === 'string') return id; const c = room.clients.get(id); return c && c.squad ? 's' + c.squad : 'p' + id; }
function checkEnd(room) {
  if (!room.started || room.ending) return;
  const ids = [...room.alive].filter(id => typeof id === 'string' || room.clients.has(id));
  const humans = ids.filter(id => typeof id === 'number').length;
  const teams = new Set(ids.map(id => teamKey(room, id)));
  if (teams.size <= 1 || humans === 0 || room.clients.size === 0) {
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
  res.end('Servidor do Zona Morta funcionando (v3)');
});

const wss = new WebSocketServer({ server, maxPayload: 16 * 1024 });

wss.on('connection', (ws, req) => {
  const origin = (req.headers.origin || '').replace(/\/$/, '');
  if (ORIGENS.length && !ORIGENS.includes(origin)) { ws.close(1008, 'origem'); return; }
  let me = null, room = null;
  let budget = 40, last = Date.now();       // mensagens de jogo: ~40/s
  let vBudget = 25, vLast = Date.now();     // pacotes de voz: ~20/s
  let lastChat = 0;
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  send(ws, { t: 'rooms', rooms: list() });

  function leave() {
    if (!room || !me) return;
    const r = room, sid = me.squad;
    r.clients.delete(me.id); r.alive.delete(me.id);
    if (online.get(me.name.toLowerCase()) === me) online.delete(me.name.toLowerCase());
    if (r.host === me.id) r.host = r.clients.size ? r.clients.keys().next().value : null;
    bc(r, { t: 'leave', id: me.id, host: r.host });
    if (!r.started) { cleanSquad(r, sid); pushSquads(r); }
    if (!r.clients.size) { r.started = false; r.ending = false; r.alive = new Set(); }
    else checkEnd(r);
    room = null; me = null;
  }

  ws.on('message', (raw, isBinary) => {
    // ---- voz: só para quem está no mesmo grupo ----
    if (isBinary) {
      if (!room || !me || !me.squad || raw.length > 4000) return;
      const now = Date.now();
      vBudget = Math.min(25, vBudget + (now - vLast) * 0.022); vLast = now;
      if (vBudget < 1) return;
      vBudget -= 1;
      const out = Buffer.allocUnsafe(raw.length + 4);
      out.writeUInt32BE(me.id, 0); raw.copy(out, 4);
      for (const c of squadMembers(room, me.squad)) if (c.id !== me.id && c.ws.readyState === 1) c.ws.send(out, { binary: true });
      return;
    }
    const now = Date.now();
    budget = Math.min(40, budget + (now - last) * 0.04); last = now;
    if (budget < 1) return;
    budget -= 1;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'list') { send(ws, { t: 'rooms', rooms: list() }); return; }
    if (m.t === 'presence') {
      const names = Array.isArray(m.names) ? m.names.slice(0, 60) : [];
      const res = names.map(n => {
        const c = online.get(String(n).toLowerCase());
        return c ? { name: c.name, room: c.room, started: rooms.get(c.room).started } : null;
      }).filter(Boolean);
      send(ws, { t: 'presence', list: res });
      return;
    }
    if (m.t === 'join') {
      if (room) return;
      const r = rooms.get(m.room);
      if (!r) return;
      if (r.clients.size >= MAX_POR_SALA) { send(ws, '{"t":"full"}'); return; }
      if (r.started) { send(ws, '{"t":"busy"}'); return; }
      const skin = /^[a-z]{3,12}$/.test(String(m.skin || '')) ? m.skin : 'mateiro';
      me = { id: nextId++, name: String(m.name || 'Sobrevivente').slice(0, 16), skin, lvl: Math.max(1, Math.min(99, m.lvl | 0)), ws, squad: null, room: r.id };
      room = r; r.clients.set(me.id, me);
      online.set(me.name.toLowerCase(), me);
      if (!r.host) r.host = me.id;
      send(ws, { t: 'welcome', id: me.id, host: r.host, now: Date.now(), room: r.id, squads: squadMap(r),
        players: [...r.clients.values()].map(p => ({ id: p.id, name: p.name, skin: p.skin, lvl: p.lvl })) });
      bc(r, { t: 'join', id: me.id, name: me.name, skin: me.skin, lvl: me.lvl }, me.id);
      return;
    }
    if (!room || !me) return;

    if (m.t === 'leave') { leave(); send(ws, { t: 'rooms', rooms: list() }); return; }

    if (m.t === 'chat') {
      if (now - lastChat < 700) return;
      lastChat = now;
      const txt = String(m.txt || '').replace(/\s+/g, ' ').trim().slice(0, 140);
      if (!txt) return;
      const out = { t: 'chat', from: me.id, name: me.name, ch: m.ch === 'grupo' ? 'grupo' : 'sala', txt };
      if (out.ch === 'grupo') {
        if (!me.squad) return;
        for (const c of squadMembers(room, me.squad)) if (c.id !== me.id) send(c.ws, out);
      } else bc(room, out, me.id);
      return;
    }

    // ---- grupos (squad) ----
    if (m.t === 'sq_invite') {
      if (room.started) return;
      const target = room.clients.get(m.to);
      if (!target || target === me) return;
      if (me.squad && squadMembers(room, me.squad).length >= MAX_GRUPO) { send(ws, { t: 'sq_full' }); return; }
      if (target.squad && target.squad === me.squad) return;
      send(target.ws, { t: 'sq_invite', from: me.id, name: me.name });
      return;
    }
    if (m.t === 'sq_accept') {
      if (room.started) return;
      const inviter = room.clients.get(m.from);
      if (!inviter || inviter === me) return;
      if (!inviter.squad) inviter.squad = room.nextSquad++;
      if (squadMembers(room, inviter.squad).length >= MAX_GRUPO) { send(ws, { t: 'sq_full' }); return; }
      const old = me.squad;
      me.squad = inviter.squad;
      cleanSquad(room, old);
      pushSquads(room);
      return;
    }
    if (m.t === 'sq_leave') {
      if (room.started || !me.squad) return;
      const old = me.squad; me.squad = null; cleanSquad(room, old); pushSquads(room);
      return;
    }

    if (m.t === 'start') {
      if (me.id !== room.host || room.started) return;
      room.started = true; room.ending = false;
      room.alive = new Set(room.clients.keys());
      const bots = [];
      if (m.bots !== false) {
        const used = new Set([...room.clients.values()].map(c => c.name.toLowerCase()));
        const n = Math.max(0, COMPLETAR_ATE - room.clients.size);
        for (let i = 0; i < n; i++) { const id = 100001 + i; bots.push({ id, name: botName(used), skin: pk(SKINS_BOT) }); room.alive.add('b' + id); }
      }
      const seed = (Math.random() * 1e9) | 0, startAt = Date.now() + 4000;
      bc(room, { t: 'go', seed, startAt, now: Date.now(), players: [...room.clients.keys()], squads: squadMap(room), bots });
      return;
    }
    if (m.t === 'die') { room.alive.delete(me.id); checkEnd(room); }
    // bots: só o anfitrião manda o estado e a morte deles
    if (m.t === 'bs' || m.t === 'bdie') {
      if (me.id !== room.host || !room.started) return;
      if (m.t === 'bdie') { room.alive.delete('b' + (m.id | 0)); checkEnd(room); }
      m.from = me.id; bc(room, m, me.id); return;
    }
    m.from = me.id;
    if (m.t === 'hit') {
      if (m.bot !== undefined) { const h = room.clients.get(room.host); if (h && h !== me) send(h.ws, m); return; } // tiro em bot vai para o anfitrião
      const c = room.clients.get(m.to);
      if (!c) return;
      if (!m.bn && me.squad && c.squad === me.squad) return; // sem fogo amigo (exceto tiro de bot)
      if (m.bn && me.id !== room.host) return;
      send(c.ws, m);
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

server.listen(process.env.PORT || 8080, () => console.log('Zona Morta servidor v2 na porta', process.env.PORT || 8080));
