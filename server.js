// 潜水艦人狼 サーバー(追加パッケージ不要 / Node.js 18以上)
// 使い方: node server.js  →  ブラウザで http://localhost:3000 を開く
'use strict';
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT) || 3000;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || ''; // 例: https://example.com(空なら制限なし)
const MAX_ROOM_SIZE = 30;   // 1ルームの最大接続数(ゲームの上限20人に余裕を足した値)
const MAX_ROOMS = 300;      // 同時に存在できるルーム数
const GRACE_MS = 60000;     // 切断後に席を保持する時間(この間に再接続すれば復帰できる)
const LOG_SIZE = 400;       // 再接続時に再送するイベントの保持数
const MAX_MSG = 8192;       // 1メッセージの最大サイズ(文字数)
const MAX_PRES = 4096;      // 1人ぶんの状態(位置など)の最大サイズ
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const NAME_RE = /^[a-z0-9][a-z0-9_-]{2,31}$/;
const TOPIC_RE = /^[a-z0-9_-]{1,32}$/;
const BAD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

const rooms = new Map();   // ルーム名 -> { name, members: Map(id -> member), seq, log }
const sockets = new Set();
let counter = 0;

/* ---------- HTTP(ゲーム画面の配信) ---------- */
const indexPath = path.join(__dirname, 'index.html');
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    fs.readFile(indexPath, (err, data) => {
      if (err) { res.writeHead(500, { 'Content-Type': 'text/plain' }); res.end('index.html not found'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' });
      res.end(data);
    });
  } else if (url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok');
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('not found');
  }
});

/* ---------- WebSocket(自前実装) ---------- */
server.on('upgrade', (req, sock) => {
  const url = req.url.split('?')[0];
  const key = req.headers['sec-websocket-key'];
  const origin = req.headers.origin;
  if (url !== '/ws' || !key || String(req.headers.upgrade).toLowerCase() !== 'websocket' ||
      (ALLOWED_ORIGIN && origin && origin !== ALLOWED_ORIGIN)) {
    sock.destroy();
    return;
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const s = { sock, buf: Buffer.alloc(0), member: null, last: Date.now(), tokens: 120, tk: Date.now() };
  sockets.add(s);
  sock.on('data', chunk => onData(s, chunk));
  sock.on('error', () => {});
  sock.on('close', () => {
    sockets.delete(s);
    const m = s.member;
    s.member = null;
    if (m && m.sock === s) { m.sock = null; m.timer = setTimeout(() => drop(m), GRACE_MS); }
  });
});

function frame(op, payload) {
  const n = payload.length;
  let head;
  if (n < 126) head = Buffer.from([0x80 | op, n]);
  else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([head, payload]);
}

function sendSock(s, str) {
  const k = s.sock;
  if (!k.writable) return;
  if (k.writableLength > 2000000) { k.destroy(); return; } // 受信が極端に遅い接続は切る
  k.write(frame(1, Buffer.from(str)));
}
const sendObj = (s, o) => sendSock(s, JSON.stringify(o));
const sendTo = (m, str) => { if (m.sock) sendSock(m.sock, str); };

function onData(s, chunk) {
  s.buf = Buffer.concat([s.buf, chunk]);
  if (s.buf.length > 200000) { s.sock.destroy(); return; }
  for (;;) {
    const b = s.buf;
    if (b.length < 2) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2) !== 0) { s.sock.destroy(); return; } len = b.readUInt32BE(6); off = 10; }
    if (!masked || len > 65536) { s.sock.destroy(); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4);
    const pl = Buffer.from(b.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) pl[i] ^= mask[i & 3];
    s.buf = b.subarray(off + 4 + len);
    s.last = Date.now();
    if (op === 8) { try { s.sock.write(frame(8, pl.subarray(0, 2))); } catch (e) {} s.sock.end(); return; }
    if (op === 9) { s.sock.write(frame(10, pl)); continue; }
    if (op === 10) continue;
    if (op === 1 && fin) {
      try { handle(s, pl.toString('utf8')); } catch (e) { console.error('handler error:', e); }
      continue;
    }
    s.sock.destroy(); // 断片化フレームやバイナリは使わない
    return;
  }
}

/* ---------- ルームの処理 ---------- */
const snapshot = r => [...r.members.values()].map(m => [m.id, m.pres]);
const broadcast = (r, str, except) => { for (const x of r.members.values()) if (x !== except) sendTo(x, str); };

function handle(s, text) {
  if (text.length > MAX_MSG) return;
  let m;
  try { m = JSON.parse(text); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  const now = Date.now();
  s.tokens = Math.min(120, s.tokens + (now - s.tk) * 0.06); // 毎秒60通まで(瞬間的には120通)
  s.tk = now;
  if (s.tokens < 1) return;
  s.tokens -= 1;
  const mem = s.member;
  if (!mem) {
    if (m.t === 'join') join(s, m.room);
    else if (m.t === 'resume') resume(s, m);
    return;
  }
  const r = mem.room;
  if (m.t === 'pres') {
    const p = m.patch;
    if (!p || typeof p !== 'object' || Array.isArray(p)) return;
    const n = Object.assign({}, mem.pres), patch = {};
    for (const k of Object.keys(p)) {
      if (k.length > 16 || BAD_KEYS.has(k)) continue;
      patch[k] = p[k];
      if (p[k] === null) delete n[k]; else n[k] = p[k];
    }
    if (JSON.stringify(n).length > MAX_PRES) return;
    mem.pres = n;
    broadcast(r, JSON.stringify({ t: 'pres', peer: mem.id, patch }), mem);
  } else if (m.t === 'emit') {
    if (typeof m.topic !== 'string' || !TOPIC_RE.test(m.topic)) return;
    const sq = r.seq + 1;
    const str = JSON.stringify({ t: 'msg', topic: m.topic, peer: mem.id, data: m.data === undefined ? null : m.data, s: sq });
    if (str.length > MAX_MSG) return;
    r.seq = sq;
    r.log.push({ s: sq, str });
    if (r.log.length > LOG_SIZE) r.log.shift();
    broadcast(r, str, null); // 送った本人にも届ける
  } else if (m.t === 'leave') {
    s.member = null;
    mem.sock = null;
    drop(mem);
    s.sock.end();
  }
}

function join(s, name) {
  if (typeof name !== 'string' || !NAME_RE.test(name)) return sendObj(s, { t: 'err', code: 'bad_room' });
  let r = rooms.get(name);
  if (!r) {
    if (rooms.size >= MAX_ROOMS) return sendObj(s, { t: 'err', code: 'busy' });
    r = { name, members: new Map(), seq: 0, log: [] };
    rooms.set(name, r);
  }
  if (r.members.size >= MAX_ROOM_SIZE) return sendObj(s, { t: 'err', code: 'full' });
  const mem = {
    id: 'p' + (++counter).toString(36) + crypto.randomBytes(2).toString('hex'),
    token: crypto.randomBytes(12).toString('hex'),
    pres: {}, room: r, sock: s, timer: null
  };
  r.members.set(mem.id, mem);
  s.member = mem;
  sendObj(s, { t: 'joined', you: mem.id, token: mem.token, peers: snapshot(r) });
  broadcast(r, JSON.stringify({ t: 'join', peer: mem.id }), mem);
}

function resume(s, m) {
  const r = typeof m.room === 'string' ? rooms.get(m.room) : null;
  const mem = r && r.members.get(m.id);
  if (!mem || mem.token !== m.token) return sendObj(s, { t: 'err', code: 'no_session' });
  clearTimeout(mem.timer);
  mem.timer = null;
  const old = mem.sock;
  if (old && old !== s) { old.member = null; try { old.sock.destroy(); } catch (e) {} }
  mem.sock = s;
  s.member = mem;
  sendObj(s, { t: 'joined', you: mem.id, token: mem.token, peers: snapshot(r), resumed: true });
  const last = Number(m.last) || 0;
  for (const e of r.log) if (e.s > last) sendSock(s, e.str); // 切れていた間のイベントを再送
}

function drop(mem) {
  clearTimeout(mem.timer);
  const r = mem.room;
  if (!r.members.delete(mem.id)) return;
  mem.sock = null;
  if (r.members.size === 0) { rooms.delete(r.name); return; }
  broadcast(r, JSON.stringify({ t: 'left', peer: mem.id }), null);
}

// 生存確認(切れた接続を片づける)
setInterval(() => {
  const now = Date.now();
  for (const s of sockets) {
    if (now - s.last > 45000) { s.sock.destroy(); continue; }
    try { s.sock.write(frame(9, Buffer.alloc(0))); } catch (e) {}
    if (s.member) sendObj(s, { t: 'hb' });
  }
}, 10000);

server.listen(PORT, () => console.log('潜水艦人狼サーバー起動: http://localhost:' + PORT));
