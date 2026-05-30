const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

const games = {};

function broadcast(code, data) {
  const game = games[code];
  if (!game) return;
  const msg = JSON.stringify(data);
  game.clients.forEach(function(client) {
    if (client.readyState === 1) client.send(msg);
  });
}

function safeGame(code) {
  const g = games[code];
  if (!g) return null;
  return {
    code: g.code, dur: g.dur, status: g.status,
    startTime: g.startTime, foundLog: g.foundLog,
    players: g.players, settings: g.settings,
    centerLat: g.centerLat, centerLng: g.centerLng,
    endReason: g.endReason || null
  };
}

function checkEndGame(code) {
  const g = games[code];
  if (!g || g.status !== 'started') return;
  const hidden = Object.values(g.players).filter(p => !p.isSeeker && !p.isFound);
  if (hidden.length === 0) {
    g.status = 'ended';
    g.endReason = 'all_found';
    broadcast(code, { type: 'game_state', game: safeGame(code) });
    broadcast(code, { type: 'game_end', reason: 'all_found' });
  }
}

wss.on('connection', function(ws) {
  let playerCode = null;
  let playerKey = null;

  ws.on('message', function(raw) {
    let msg;
    try { msg = JSON.parse(raw); } catch(e) { return; }

    if (msg.type === 'create') {
      const code = msg.code;
      games[code] = {
        code, dur: msg.settings.dur, status: 'waiting',
        startTime: null, foundLog: [], players: {},
        settings: msg.settings,
        centerLat: null, centerLng: null,
        clients: new Set(), endReason: null
      };
      games[code].players[msg.playerKey] = {
        name: msg.name, isSeeker: true, isFound: false,
        lat: null, lng: null, secretCode: null, key: msg.playerKey,
        powerUsed: false, fakeMarker: null, hidden: false, hiddenUntil: null,
        penalty: 0, lastLat: null, lastLng: null, moved: false
      };
      games[code].clients.add(ws);
      playerCode = code; playerKey = msg.playerKey;
      ws.send(JSON.stringify({ type: 'created', code }));
      broadcast(code, { type: 'game_state', game: safeGame(code) });
    }

    else if (msg.type === 'join') {
      const code = msg.code;
      if (!games[code]) { ws.send(JSON.stringify({ type: 'error', msg: 'Partie introuvable !' })); return; }
      if (games[code].status === 'ended') { ws.send(JSON.stringify({ type: 'error', msg: 'Partie terminée.' })); return; }
      
      // Reconnexion : même prénom déjà dans la partie ?
      let existingKey = null;
      Object.entries(games[code].players).forEach(([k, p]) => {
        if (p.name === msg.name && !p.isSeeker) existingKey = k;
      });

      if (existingKey) {
        // Reprend l'ancienne entrée
        playerKey = existingKey;
        games[code].clients.add(ws);
        playerCode = code;
        ws.send(JSON.stringify({ type: 'rejoined', game: safeGame(code), playerKey: existingKey, secretCode: games[code].players[existingKey].secretCode }));
      } else {
        const key = msg.playerKey;
        games[code].players[key] = {
          name: msg.name, isSeeker: false, isFound: false,
          lat: null, lng: null, secretCode: msg.secretCode, key,
          powerUsed: false, fakeMarker: null, hidden: false, hiddenUntil: null,
          penalty: 0, lastLat: null, lastLng: null, moved: false
        };
        games[code].clients.add(ws);
        playerCode = code; playerKey = key;
        ws.send(JSON.stringify({ type: 'joined', game: safeGame(code) }));
      }
      broadcast(code, { type: 'game_state', game: safeGame(code) });
    }

    else if (msg.type === 'start') {
      const code = msg.code;
      if (!games[code]) return;
      games[code].status = 'started';
      games[code].startTime = Date.now();
      // Stocker le centre de jeu (position du chercheur)
      if (msg.centerLat) { games[code].centerLat = msg.centerLat; games[code].centerLng = msg.centerLng; }
      broadcast(code, { type: 'game_state', game: safeGame(code) });

      // Timer fin de partie
      const dur = games[code].settings.dur * 60 * 1000;
      setTimeout(function() {
        if (games[code] && games[code].status === 'started') {
          games[code].status = 'ended';
          games[code].endReason = 'timeout';
          broadcast(code, { type: 'game_state', game: safeGame(code) });
          broadcast(code, { type: 'game_end', reason: 'timeout' });
        }
      }, dur);
    }

    else if (msg.type === 'position') {
      const code = msg.code;
      if (!games[code] || !games[code].players[msg.playerKey]) return;
      const p = games[code].players[msg.playerKey];
      const g = games[code];

      // Détection déplacement excessif après fin cachette
      const hideTime = g.settings.hideTime * 60 * 1000;
      const elapsed = Date.now() - g.startTime;
      if (!p.isSeeker && elapsed > hideTime) {
        if (p.lastLat !== null) {
          const dist = haversine(p.lastLat, p.lastLng, msg.lat, msg.lng);
          if (dist > 50) {
            p.moved = true;
            p.penalty = Math.min((p.penalty || 0) + 1, 5);
          }
        }
        p.lastLat = msg.lat; p.lastLng = msg.lng;
      }

      p.lat = msg.lat; p.lng = msg.lng;

      // Expiration pouvoir hidden
      if (p.hiddenUntil && Date.now() > p.hiddenUntil) {
        p.hidden = false; p.hiddenUntil = null;
      }

      broadcast(code, { type: 'positions', players: games[code].players, centerLat: g.centerLat, centerLng: g.centerLng });
    }

    else if (msg.type === 'found') {
      const code = msg.code;
      if (!games[code]) return;
      let found = null, foundKey = null;
      Object.entries(games[code].players).forEach(([k, p]) => {
        if (!p.isSeeker && !p.isFound && p.secretCode === msg.secretCode) { found = p; foundKey = k; }
      });
      if (!found) { ws.send(JSON.stringify({ type: 'error', msg: 'Code invalide ou joueur déjà trouvé !' })); return; }
      games[code].players[foundKey].isFound = true;
      games[code].players[foundKey].isSeeker = true;
      games[code].foundLog.push({ name: found.name, t: Date.now() });
      broadcast(code, { type: 'game_state', game: safeGame(code) });
      broadcast(code, { type: 'found_announce', name: found.name });
      checkEndGame(code);
    }

    else if (msg.type === 'use_power') {
      const code = msg.code;
      if (!games[code] || !games[code].players[msg.playerKey]) return;
      const p = games[code].players[msg.playerKey];
      if (p.powerUsed) { ws.send(JSON.stringify({ type: 'error', msg: 'Pouvoir déjà utilisé !' })); return; }
      p.powerUsed = true;

      if (msg.power === 'hide') {
        p.hidden = true;
        p.hiddenUntil = Date.now() + 60000;
        ws.send(JSON.stringify({ type: 'power_ok', power: 'hide', until: p.hiddenUntil }));
        broadcast(code, { type: 'power_announce', name: p.name, power: 'hide' });
      }
      else if (msg.power === 'fake') {
        p.fakeMarker = { lat: msg.fakeLat, lng: msg.fakeLng };
        broadcast(code, { type: 'positions', players: games[code].players, centerLat: games[code].centerLat, centerLng: games[code].centerLng });
        ws.send(JSON.stringify({ type: 'power_ok', power: 'fake' }));
        broadcast(code, { type: 'power_announce', name: p.name, power: 'fake' });
      }
      else if (msg.power === 'sabotage') {
        // sabotage: reveal / halve / block
        broadcast(code, { type: 'sabotage', effect: msg.effect, seekerName: p.name });
        ws.send(JSON.stringify({ type: 'power_ok', power: 'sabotage', effect: msg.effect }));
      }
    }

    else if (msg.type === 'get_state') {
      const code = msg.code;
      if (!games[code]) { ws.send(JSON.stringify({ type: 'error', msg: 'Partie introuvable !' })); return; }
      games[code].clients.add(ws);
      playerCode = code;
      ws.send(JSON.stringify({ type: 'game_state', game: safeGame(code) }));
    }
  });

  ws.on('close', function() {
    if (playerCode && games[playerCode]) {
      games[playerCode].clients.delete(ws);
    }
  });
});

function haversine(lat1, lng1, lat2, lng2) {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLng = (lng2 - lng1) * Math.PI / 180;
  const a = Math.sin(dLat/2)*Math.sin(dLat/2) +
    Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*
    Math.sin(dLng/2)*Math.sin(dLng/2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

setInterval(function() {
  const now = Date.now();
  Object.keys(games).forEach(function(code) {
    const g = games[code];
    if (g.startTime && now - g.startTime > 6 * 3600 * 1000) delete games[code];
  });
}, 3600 * 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, function() { console.log('Serveur lancé port ' + PORT); });
