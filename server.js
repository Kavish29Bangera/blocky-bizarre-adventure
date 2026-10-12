/* =============================================================================
 *  BLOCKY BIZARRE ADVENTURE — co-op relay server
 * =============================================================================
 *  Run:   node server.js            (optionally: PORT=9000 node server.js)
 *  Then:  open http://localhost:8080 in your browser and press "JOIN CO-OP".
 *         Friends on the same network open http://<your-LAN-IP>:8080
 *
 *  This file has NO dependencies — it speaks the WebSocket protocol by hand
 *  using only Node's built-in http + crypto modules, so there is no
 *  `npm install` step.
 *
 *  Design: the server is a "dumb relay".
 *    - It gives every player an id and tells everyone who the HOST is
 *      (the longest-connected player).
 *    - The host's browser simulates the enemies and broadcasts snapshots.
 *    - Every other message is stamped with the sender's id and forwarded
 *      either to one player (if the message has a `to` field) or to everyone
 *      else.
 *    - When the host leaves, the next player is promoted to host.
 * ========================================================================== */
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

const PORT = Number(process.env.PORT) || 8080;
const MAX_PLAYERS = 8;
const MAX_MESSAGE_BYTES = 64 * 1024;       // drop clients that send huge frames
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'; // fixed by RFC 6455

/** id -> { id, name, socket, buffer, fragments } */
const clients = new Map();
let nextId = 1;
let hostId = null;

// -----------------------------------------------------------------------------
// HTTP: serve game.html so friends can simply browse to this machine.
// -----------------------------------------------------------------------------
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/game.html') {
    fs.readFile(path.join(__dirname, 'game.html'), (err, data) => {
      if (err) { res.writeHead(500); res.end('game.html not found next to server.js'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      res.end(data);
    });
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// -----------------------------------------------------------------------------
// WebSocket handshake (HTTP "Upgrade" request)
// -----------------------------------------------------------------------------
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || clients.size >= MAX_PLAYERS) { socket.destroy(); return; }

  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );
  socket.setNoDelay(true);

  const client = { id: nextId++, name: null, socket, buffer: Buffer.alloc(0), fragments: [] };
  clients.set(client.id, client);

  socket.on('data', chunk => {
    client.buffer = Buffer.concat([client.buffer, chunk]);
    try { readFrames(client); } catch (e) { drop(client); }
  });
  socket.on('close', () => drop(client));
  socket.on('error', () => drop(client));
});

// -----------------------------------------------------------------------------
// WebSocket framing
// -----------------------------------------------------------------------------
/** Pull as many complete frames as possible out of client.buffer. */
function readFrames(client) {
  for (;;) {
    const buf = client.buffer;
    if (buf.length < 2) return;
    const fin = (buf[0] & 0x80) !== 0;
    const opcode = buf[0] & 0x0f;
    const masked = (buf[1] & 0x80) !== 0;
    let len = buf[1] & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (buf.length < 4) return;
      len = buf.readUInt16BE(2); offset = 4;
    } else if (len === 127) {
      if (buf.length < 10) return;
      if (buf.readUInt32BE(2) !== 0) throw new Error('frame too large');
      len = buf.readUInt32BE(6); offset = 10;
    }
    if (len > MAX_MESSAGE_BYTES) throw new Error('frame too large');
    if (!masked) throw new Error('client frames must be masked');
    if (buf.length < offset + 4 + len) return;           // wait for more data

    const mask = buf.subarray(offset, offset + 4);
    const payload = Buffer.from(buf.subarray(offset + 4, offset + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    client.buffer = buf.subarray(offset + 4 + len);

    if (opcode === 0x8) { drop(client); return; }                       // close
    if (opcode === 0x9) { client.socket.write(frame(payload, 0xA)); continue; } // ping -> pong
    if (opcode === 0xA) continue;                                        // pong
    if (opcode === 0x1 || opcode === 0x0) {                              // text / continuation
      client.fragments.push(payload);
      if (fin) {
        const text = Buffer.concat(client.fragments).toString('utf8');
        client.fragments = [];
        onMessage(client, text);
      }
    }
  }
}

/** Build one unmasked server->client frame. */
function frame(payload, opcode = 0x1) {
  const len = payload.length;
  let head;
  if (len < 126) {
    head = Buffer.from([0x80 | opcode, len]);
  } else if (len < 65536) {
    head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127;
    head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6);
  }
  return Buffer.concat([head, payload]);
}

function send(client, obj) {
  if (client.socket.destroyed) return;
  client.socket.write(frame(Buffer.from(JSON.stringify(obj), 'utf8')));
}
function broadcast(obj, exceptId) {
  const data = frame(Buffer.from(JSON.stringify(obj), 'utf8'));
  for (const c of clients.values()) {
    if (c.id !== exceptId && c.name !== null && !c.socket.destroyed) c.socket.write(data);
  }
}

// -----------------------------------------------------------------------------
// Game-level message handling
// -----------------------------------------------------------------------------
function onMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (e) { return; }
  if (!msg || typeof msg !== 'object' || typeof msg.t !== 'string') return;

  // First message must be "hello": registers the player.
  if (client.name === null) {
    if (msg.t !== 'hello') return;
    client.name = String(msg.name || 'JOJO').slice(0, 14);
    if (hostId === null) hostId = client.id;
    send(client, {
      t: 'welcome', id: client.id, host: hostId,
      players: [...clients.values()].filter(c => c.name !== null && c.id !== client.id)
        .map(c => ({ id: c.id, name: c.name })),
    });
    broadcast({ t: 'join', id: client.id, name: client.name }, client.id);
    console.log(`+ ${client.name} joined (id ${client.id}) — ${countPlayers()} online`);
    return;
  }

  // Only the host may publish enemy snapshots / enemy attacks.
  if ((msg.t === 'e' || msg.t === 'ea' || msg.t === 'ep') && client.id !== hostId) return;

  msg.from = client.id;                    // never trust a client-supplied "from"
  if (msg.to !== undefined) {
    const target = clients.get(Number(msg.to));
    if (target && target.name !== null) send(target, msg);
  } else {
    broadcast(msg, client.id);
  }
}

function countPlayers() {
  let n = 0;
  for (const c of clients.values()) if (c.name !== null) n++;
  return n;
}

function drop(client) {
  if (!clients.has(client.id)) return;
  clients.delete(client.id);
  client.socket.destroy();
  if (client.name === null) return;        // never finished saying hello
  broadcast({ t: 'leave', id: client.id });
  console.log(`- ${client.name} left — ${countPlayers()} online`);
  if (client.id === hostId) {              // promote the next player to host
    const next = [...clients.values()].find(c => c.name !== null);
    hostId = next ? next.id : null;
    if (next) { broadcast({ t: 'host', id: hostId }); console.log(`  ${next.name} is the new host`); }
  }
}

// -----------------------------------------------------------------------------
server.listen(PORT, () => {
  console.log('==============================================');
  console.log('  BLOCKY BIZARRE ADVENTURE — co-op server');
  console.log('==============================================');
  console.log(`  You:      http://localhost:${PORT}`);
  for (const list of Object.values(os.networkInterfaces())) {
    for (const ni of list || []) {
      if (ni.family === 'IPv4' && !ni.internal) console.log(`  Friends:  http://${ni.address}:${PORT}`);
    }
  }
  console.log('  (Ctrl+C to stop)');
});
