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
app.use(express.static('../painel'));

// Sessões ativas: { sessionId: { tecnico: ws, cliente: ws, info: {} } }
const sessions = new Map();

// ─── REST API ─────────────────────────────────────────────────────────────────

// Gerar nova sessão
app.post('/api/sessao/criar', (req, res) => {
  const sessionId = crypto.randomBytes(4).toString('hex').toUpperCase();
  sessions.set(sessionId, {
    tecnico: null,
    cliente: null,
    criada: new Date().toISOString(),
    aceita: false,
    info: {}
  });
  console.log(`[SESSÃO] Criada: ${sessionId}`);
  res.json({
    sessionId,
    link: `${req.protocol}://${req.get('host')}/cliente.html?s=${sessionId}`,
    expires: '30min'
  });
});

// Status da sessão
app.get('/api/sessao/:id', (req, res) => {
  const s = sessions.get(req.params.id);
  if (!s) return res.status(404).json({ erro: 'Sessão não encontrada' });
  res.json({
    sessionId: req.params.id,
    tecnicoConectado: !!s.tecnico,
    clienteConectado: !!s.cliente,
    aceita: s.aceita,
    criada: s.criada,
    dispositivo: s.info
  });
});

// Listar sessões ativas
app.get('/api/sessoes', (req, res) => {
  const lista = [];
  sessions.forEach((s, id) => {
    lista.push({
      sessionId: id,
      tecnico: !!s.tecnico,
      cliente: !!s.cliente,
      aceita: s.aceita,
      criada: s.criada
    });
  });
  res.json(lista);
});

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const sessionId = url.searchParams.get('s');
  const papel = url.searchParams.get('papel'); // 'tecnico' ou 'cliente'

  if (!sessionId || !papel) {
    ws.close(1008, 'Parâmetros inválidos');
    return;
  }

  if (!sessions.has(sessionId)) {
    ws.close(1008, 'Sessão não encontrada');
    return;
  }

  const sessao = sessions.get(sessionId);
  sessao[papel] = ws;

  console.log(`[WS] ${papel.toUpperCase()} conectou na sessão ${sessionId}`);

  // Notifica o outro lado que houve conexão
  const outro = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;
  if (outro && outro.readyState === WebSocket.OPEN) {
    outro.send(JSON.stringify({ tipo: 'par_conectado', papel }));
  }

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const destino = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;

    // Processar mensagens especiais no servidor
    switch (msg.tipo) {
      case 'info_dispositivo':
        sessao.info = msg.dados;
        console.log(`[INFO] Dispositivo ${sessionId}:`, msg.dados);
        break;

      case 'consentimento':
        sessao.aceita = msg.aceito;
        console.log(`[CONSENT] Sessão ${sessionId}: ${msg.aceito ? 'ACEITA' : 'RECUSADA'}`);
        // Notifica técnico
        if (sessao.tecnico && sessao.tecnico.readyState === WebSocket.OPEN) {
          sessao.tecnico.send(JSON.stringify({
            tipo: 'consentimento',
            aceito: msg.aceito,
            dispositivo: sessao.info
          }));
        }
        return;

      case 'frame_tela':
        // Frame de vídeo — repassa direto para o técnico sem log (alto volume)
        if (destino && destino.readyState === WebSocket.OPEN) {
          destino.send(raw);
        }
        return;
    }

    // Repassar mensagem para o outro lado
    if (destino && destino.readyState === WebSocket.OPEN) {
      destino.send(JSON.stringify(msg));
    }
  });

  ws.on('close', () => {
    console.log(`[WS] ${papel.toUpperCase()} desconectou da sessão ${sessionId}`);
    sessao[papel] = null;

    const outro = papel === 'tecnico' ? sessao.cliente : sessao.tecnico;
    if (outro && outro.readyState === WebSocket.OPEN) {
      outro.send(JSON.stringify({ tipo: 'par_desconectado', papel }));
    }

    // Limpar sessão se ambos desconectaram
    if (!sessao.tecnico && !sessao.cliente) {
      setTimeout(() => {
        if (!sessao.tecnico && !sessao.cliente) {
          sessions.delete(sessionId);
          console.log(`[SESSÃO] Removida: ${sessionId}`);
        }
      }, 60000); // aguarda 60s antes de remover
    }
  });

  ws.on('error', (err) => {
    console.error(`[ERRO WS] sessão ${sessionId}:`, err.message);
  });
});

// ─── START ────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n✅ Servidor rodando em http://localhost:${PORT}`);
  console.log(`🔌 WebSocket em ws://localhost:${PORT}`);
  console.log(`📱 Painel do técnico: http://localhost:${PORT}/index.html\n`);
});
