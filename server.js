'use strict';

// Сервер многопользовательской песочницы в стиле GMod.
// Запуск на Render: npm install && npm start (порт берётся из process.env.PORT).
// Клиент подключается по WebSocket: wss://<хост>/  (путь /ws тоже принимается).
//
// Протокол — JSON. Клиент → сервер:
//   hello, player, prop_spawn, prop, prop_remove, prop_hit, event, chat, ping
// Сервер → клиент:
//   welcome, player, snapshot, prop_spawn, prop_remove, prop_hit, event,
//   chat, pong, peer_join, peer_leave, error

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT) || 10000;
const SNAPSHOT_HZ = 20;
const MAX_PLAYERS_PER_ROOM = 16;
const MAX_MESSAGE_BYTES = 64 * 1024;
const MAX_NAME_LEN = 32;
const MAX_ROOM_LEN = 32;
const MAX_CHAT_LEN = 256;
const MAX_EVENT_BYTES = 16 * 1024;
const MAX_MSGS_PER_SEC = 150;
const STARTED = Date.now();

// roomId -> { players: Map<id, client>, props: Map<id, prop> }
const rooms = new Map();
let idCounter = 0;

function newId() {
  return Date.now().toString(36) + '-' + (++idCounter).toString(36) +
    '-' + Math.random().toString(36).slice(2, 8);
}

function str(value, max) {
  return typeof value === 'string' ? value.slice(0, max) : '';
}

function num(value, fallback) {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function bool(value, fallback = false) {
  return typeof value === 'boolean' ? value : fallback;
}

function vec3(value) {
  if (!Array.isArray(value) || value.length !== 3) return undefined;
  if (!value.every((n) => typeof n === 'number' && Number.isFinite(n))) return undefined;
  return value.map((n) => Math.round(n * 1000) / 1000);
}

function vec4(value) {
  if (!Array.isArray(value) || value.length !== 4) return undefined;
  if (!value.every((n) => typeof n === 'number' && Number.isFinite(n))) return undefined;
  return value.map((n) => Math.round(n * 10000) / 10000);
}

function cleanRoomId(value) {
  return str(value, MAX_ROOM_LEN).replace(/[^a-zA-Z0-9_\-]/g, '') || 'main';
}

function cleanName(value, id) {
  return str(value, MAX_NAME_LEN).trim() || ('Player_' + id.slice(0, 4));
}

// Поля состояния игрока, которые сервер пропускает от клиента.
const PLAYER_VECS = ['p', 'v'];
const PLAYER_QUATS = ['q'];
const PLAYER_NUMS = ['yaw', 'pitch', 'anim', 'view', 'weapon', 'tool', 'health'];
const PLAYER_BOOLS = ['crouch', 'slide', 'noclip', 'dead'];

function pickPlayerState(raw) {
  const out = {};
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return out;
  for (const key of PLAYER_VECS) {
    const v = vec3(raw[key]);
    if (v) out[key] = v;
  }
  for (const key of PLAYER_QUATS) {
    const v = vec4(raw[key]);
    if (v) out[key] = v;
  }
  for (const key of PLAYER_NUMS) {
    if (typeof raw[key] === 'number' && Number.isFinite(raw[key])) out[key] = num(raw[key], 0);
  }
  for (const key of PLAYER_BOOLS) {
    out[key] = bool(raw[key], false);
  }
  if (typeof raw.held === 'string') out.held = str(raw.held, 64);
  return out;
}

function getRoom(id) {
  let room = rooms.get(id);
  if (!room) {
    room = { players: new Map(), props: new Map() };
    rooms.set(id, room);
  }
  return room;
}

function send(ws, object) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try {
    ws.send(JSON.stringify(object));
  } catch (_) {
    // Клиент отключился между проверкой и отправкой — это нормально.
  }
}

function sendError(ws, code, message) {
  send(ws, { t: 'error', code: code, message: message });
}

function broadcast(room, object, exceptId = null) {
  const payload = JSON.stringify(object);
  for (const [id, player] of room.players) {
    if (id === exceptId || !player.ws || player.ws.readyState !== WebSocket.OPEN) continue;
    try {
      player.ws.send(payload);
    } catch (_) {
      // Игнорируем: следующий snapshot всё равно синхронизирует состояние.
    }
  }
}

function roomPlayers(room) {
  const list = [];
  for (const p of room.players.values()) {
    list.push({ id: p.id, name: p.name, s: p.state });
  }
  return list;
}

function roomProps(room) {
  const list = [];
  for (const p of room.props.values()) list.push(p);
  return list;
}

// --- HTTP: health-check для Render и краткая справка ---

const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  let path = '/';
  try {
    path = new URL(req.url, 'http://localhost').pathname;
  } catch (_) {
    // Некорректный URL — отдаём справку.
  }

  if (path === '/health' || path === '/healthz') {
    let players = 0;
    for (const room of rooms.values()) players += room.players.size;
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify({
      ok: true,
      uptime: Math.floor((Date.now() - STARTED) / 1000),
      rooms: rooms.size,
      players: players,
    }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('GMod sandbox multiplayer server is running.\nWebSocket: wss://<host>/\n');
});

// --- WebSocket ---

const wss = new WebSocketServer({
  server: server,
  maxPayload: MAX_MESSAGE_BYTES,
  perMessageDeflate: false,
});

wss.on('connection', (ws) => {
  const client = {
    id: newId(),
    ws: ws,
    roomId: null,
    name: '',
    state: {},
    msgs: 0,
    windowStart: Date.now(),
  };

  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    if (raw.length > MAX_MESSAGE_BYTES) {
      sendError(ws, 'too_large', 'Message is too large');
      return;
    }

    const now = Date.now();
    if (now - client.windowStart >= 1000) {
      client.windowStart = now;
      client.msgs = 0;
    }
    if (++client.msgs > MAX_MSGS_PER_SEC) return;

    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (_) {
      sendError(ws, 'bad_json', 'Invalid JSON');
      return;
    }
    if (!msg || typeof msg !== 'object' || typeof msg.t !== 'string') return;

    const room = client.roomId ? rooms.get(client.roomId) : null;

    switch (msg.t) {
      case 'hello': {
        if (client.roomId) return;
        const roomId = cleanRoomId(msg.room);
        const target = getRoom(roomId);
        if (target.players.size >= MAX_PLAYERS_PER_ROOM) {
          sendError(ws, 'room_full', 'Room is full');
          ws.close(1008, 'room_full');
          return;
        }
        client.roomId = roomId;
        client.name = cleanName(msg.name, client.id);
        client.state = pickPlayerState(msg.s);
        target.players.set(client.id, client);

        send(ws, {
          t: 'welcome',
          id: client.id,
          room: roomId,
          serverTime: Date.now(),
          tickRate: SNAPSHOT_HZ,
          players: roomPlayers(target),
          props: roomProps(target),
        });
        broadcast(target, {
          t: 'peer_join',
          id: client.id,
          name: client.name,
          s: client.state,
        }, client.id);
        break;
      }

      case 'player': {
        if (!room) return;
        client.state = pickPlayerState(msg.s);
        // Мгновенная передача движения; полные snapshot'ы всё равно
        // отправляются периодически для надёжной синхронизации.
        broadcast(room, { t: 'player', id: client.id, s: client.state }, client.id);
        break;
      }

      case 'prop_spawn': {
        if (!room) return;
        const id = str(msg.id, 64);
        if (!id || room.props.has(id)) return;
        const prop = {
          id: id,
          owner: client.id,
          model: str(msg.model, 64),
          dir: str(msg.dir, 128),
          scale: num(msg.scale, 1),
          box: vec3(msg.box) || [0.5, 0.5, 0.5],
          color: str(msg.color, 16),
          material: str(msg.material, 16),
          base_mass: num(msg.base_mass, 5),
          mass: num(msg.mass, 5),
          p: vec3(msg.p) || [0, 0, 0],
          q: vec4(msg.q) || [0, 0, 0, 1],
          v: vec3(msg.v) || [0, 0, 0],
          av: vec3(msg.av) || [0, 0, 0],
          freeze: bool(msg.freeze, false),
          lock_rot: bool(msg.lock_rot, false),
          cat: bool(msg.cat, false),
        };
        room.props.set(id, prop);
        broadcast(room, { t: 'prop_spawn', prop: prop }, client.id);
        break;
      }

      case 'prop': {
        if (!room) return;
        const prop = room.props.get(str(msg.id, 64));
        // Физику пропа обновляет только его создатель: так нельзя «отобрать»
        // чужой предмет простой подделкой пакета.
        if (!prop || prop.owner !== client.id) return;
        const p = vec3(msg.p);
        const q = vec4(msg.q);
        const v = vec3(msg.v);
        const av = vec3(msg.av);
        if (p) prop.p = p;
        if (q) prop.q = q;
        if (v) prop.v = v;
        if (av) prop.av = av;
        if (typeof msg.freeze === 'boolean') prop.freeze = msg.freeze;
        broadcast(room, {
          t: 'prop',
          id: prop.id,
          p: prop.p,
          q: prop.q,
          v: prop.v,
          av: prop.av,
          freeze: prop.freeze,
        }, client.id);
        break;
      }

      case 'prop_remove': {
        if (!room) return;
        const id = str(msg.id, 64);
        const prop = room.props.get(id);
        if (!prop || prop.owner !== client.id) return;
        room.props.delete(id);
        broadcast(room, { t: 'prop_remove', id: id }, client.id);
        break;
      }

      case 'prop_hit': {
        if (!room) return;
        if (JSON.stringify(msg.data || {}).length > MAX_EVENT_BYTES) return;
        broadcast(room, {
          t: 'prop_hit',
          id: str(msg.id, 64),
          damage: num(msg.damage, 0),
          point: vec3(msg.point) || [0, 0, 0],
          normal: vec3(msg.normal) || [0, 1, 0],
          by: client.id,
          name: str(msg.name, 64),
          data: msg.data || {},
        });
        break;
      }

      case 'event': {
        if (!room) return;
        const name = str(msg.name, 64);
        if (!name) return;
        if (JSON.stringify(msg.data || {}).length > MAX_EVENT_BYTES) return;
        broadcast(room, {
          t: 'event',
          from: client.id,
          name: name,
          data: msg.data || {},
        }, client.id);
        break;
      }

      case 'chat': {
        if (!room) return;
        const text = str(msg.text, MAX_CHAT_LEN).trim();
        if (!text) return;
        broadcast(room, {
          t: 'chat',
          from: client.id,
          name: client.name,
          text: text,
          time: Date.now(),
        });
        break;
      }

      case 'ping': {
        send(ws, {
          t: 'pong',
          id: msg.id,
          time: msg.time,
          serverTime: Date.now(),
        });
        break;
      }

      default:
        break;
    }
  });

  ws.on('close', () => {
    if (!client.roomId) return;
    const room = rooms.get(client.roomId);
    if (!room) return;
    room.players.delete(client.id);
    // Propы игрока остаются в комнате, пока в ней есть хотя бы один игрок.
    broadcast(room, { t: 'peer_leave', id: client.id });
    if (room.players.size === 0) rooms.delete(client.roomId);
  });

  ws.on('error', () => {
    // Событие 'close' всё равно придёт и уберёт клиента из комнаты.
  });
});

// Периодические полные snapshot'ы: восстанавливают состояние, если клиент
// пропустил мгновенное сообщение. В payload только простые данные (без ws).
const tick = setInterval(() => {
  const payloads = new Map();
  for (const [roomId, room] of rooms) {
    if (room.players.size === 0) {
      rooms.delete(roomId);
      continue;
    }
    const players = {};
    for (const [id, p] of room.players) {
      players[id] = { id: id, name: p.name, s: p.state };
    }
    const props = {};
    for (const [id, p] of room.props) props[id] = p;
    payloads.set(roomId, JSON.stringify({
      t: 'snapshot',
      room: roomId,
      time: Date.now(),
      players: players,
      props: props,
    }));
  }
  for (const [roomId, payload] of payloads) {
    const room = rooms.get(roomId);
    if (!room) continue;
    for (const player of room.players.values()) {
      if (player.ws.readyState !== WebSocket.OPEN) continue;
      try {
        player.ws.send(payload);
      } catch (_) {
        // Игнорируем.
      }
    }
  }
}, Math.floor(1000 / SNAPSHOT_HZ));

// Render ждёт, что порт открыт сразу после запуска.
server.listen(PORT, '0.0.0.0', () => {
  console.log('Multiplayer server listening on 0.0.0.0:' + PORT);
});

function shutdown() {
  clearInterval(tick);
  for (const room of rooms.values()) {
    for (const player of room.players.values()) {
      try {
        player.ws.close(1001, 'server shutdown');
      } catch (_) {
        // Игнорируем.
      }
    }
  }
  try {
    wss.close();
  } catch (_) {
    // Игнорируем.
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3000).unref();
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
