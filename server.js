const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static('./'));

const sessions = new Map();

app.post('/api/sessao/criar', (req, res) => {
  const sessionId = crypto.randomBytes(4).toString('hex').toUpperCase();
  sessions.set(sessionId, { tecnico: null, cliente: null, criada: new Date().toISOString(), aceita: false, info: {} });
  res.json({ sessionId, link: `${req.protocol}://${req.get('host')}/cliente.html?s=${sessionId}`, expires: '30min' });
});

app.get('/api/sessao/:id', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ erro: 'Sessão não encontrada' });
  res.json({ sessionId: req.params.id, tecnicoConectado: !!s.tecnico, clienteConectado: !!s.cliente, aceita: s.aceita, criada: s.criada, dispositivo: s.info });
});

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const sessionId = url.searchParams.get('s');
  const papel = url.searchParams.get('papel');

  if (!sessionId || !papel) { ws.close(1008, 'Parâmetros inválidos'); return; }
  if (!sessions.has(sessionId)) { ws.close(1008, 'Sessão não encontrada'); return; }

  const sessao = sessions.get(sessionId);
  sessao[papel] = ws;

  const outro = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;
  if (outro && outro.readyState === WebSocket.OPEN) {
    outro.send(JSON.stringify({ tipo: 'par_conectado', papel }));
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const destino = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;

    switch (msg.tipo) {
      case 'info_dispositivo':
        sessao.info = msg.dados;
        break;

      case 'consentimento':
        sessao.aceita = msg.aceito;
        if (sessao.tecnico && sessao.tecnico.readyState === WebSocket.OPEN) {
          sessao.tecnico.send(JSON.stringify({ tipo: 'consentimento', aceito: msg.aceito, dispositivo: sessao.info }));
        }
        return;

      case 'frame_tela':
        if (sessao.tecnico && sessao.tecnico.readyState === WebSocket.OPEN) {
          sessao.tecnico.send(JSON.stringify(msg));
        }
        return;
    }

    if (destino && destino.readyState === WebSocket.OPEN) {
      destino.send(JSON.stringify(msg));
    }
  });

  ws.on('close', () => {
    sessao[papel] = null;
    const outro = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;
    if (outro && outro.readyState === WebSocket.OPEN) {
      outro.send(JSON.stringify({ tipo: 'par_desconectado', papel }));
    }
    if (!sessao.tecnico && !sessao.cliente) {
      setTimeout(() => { if (!sessao.tecnico && !sessao.cliente) sessions.delete(sessionId); }, 60000);
    }
  });

  ws.on('error', (err) => console.error(`[ERRO WS] sessão ${sessionId}:`, err.message));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor rodando em http://localhost:${PORT}`));
