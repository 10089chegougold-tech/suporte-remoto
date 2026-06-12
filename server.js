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
app.use(express.static(__dirname));

// Clientes conectados aguardando atendimento
const clientes = new Map();

// Técnicos conectados
const tecnicos = new Map();

// Sessões ativas (técnico atendendo cliente)
const sessoes = new Map();

function broadcastTecnicos(msg) {
  const data = JSON.stringify(msg);
  tecnicos.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) ws.send(data);
  });
}

function listaClientes() {
  const lista = [];
  clientes.forEach((c, id) => {
    lista.push({ clienteId: id, info: c.info, sessaoId: c.sessaoId });
  });
  return lista;
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const papel = url.searchParams.get('papel');
  const id = crypto.randomBytes(4).toString('hex').toUpperCase();

  if (papel === 'tecnico') {
    tecnicos.set(id, ws);
    console.log(`[TÉCNICO] Conectou: ${id}`);

    ws.send(JSON.stringify({ tipo: 'lista_clientes', clientes: listaClientes() }));

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      switch (msg.tipo) {
        case 'conectar_cliente': {
          const cliente = clientes.get(msg.clienteId);
          if (!cliente || !cliente.ws || cliente.ws.readyState !== WebSocket.OPEN) {
            ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente não disponível' }));
            return;
          }
          const sessaoId = crypto.randomBytes(4).toString('hex').toUpperCase();
          sessoes.set(sessaoId, { tecnicoId: id, clienteId: msg.clienteId });
          cliente.sessaoId = sessaoId;

          cliente.ws.send(JSON.stringify({ tipo: 'tecnico_conectou', sessaoId }));

          ws.send(JSON.stringify({
            tipo: 'sessao_iniciada',
            sessaoId,
            clienteId: msg.clienteId,
            info: cliente.info
          }));
          console.log(`[SESSÃO] Iniciada: ${sessaoId}`);
          break;
        }

        default: {
          // Repassa comando para o cliente — busca sessão pelo tecnicoId
          let sessaoEncontrada = null;
          sessoes.forEach((s, sid) => {
            if (s.tecnicoId === id) sessaoEncontrada = s;
          });
          if (!sessaoEncontrada) return;
          const cliente = clientes.get(sessaoEncontrada.clienteId);
          if (cliente?.ws?.readyState === WebSocket.OPEN) {
            cliente.ws.send(JSON.stringify(msg));
          }
          break;
        }
      }
    });

    ws.on('close', () => {
      tecnicos.delete(id);
      console.log(`[TÉCNICO] Desconectou: ${id}`);
    });

  } else if (papel === 'cliente') {
    clientes.set(id, { ws, info: {}, sessaoId: null });
    console.log(`[CLIENTE] Conectou: ${id}`);

    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      switch (msg.tipo) {
        case 'info_dispositivo': {
          const c = clientes.get(id);
          if (c) {
            c.info = msg.dados;
            broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
          }
          break;
        }
        case 'frame_tela': {
          const c = clientes.get(id);
          if (!c?.sessaoId) return;
          const sessao = sessoes.get(c.sessaoId);
          if (!sessao) return;
          const tecnicoWs = tecnicos.get(sessao.tecnicoId);
          if (tecnicoWs?.readyState === WebSocket.OPEN) {
            tecnicoWs.send(raw);
          }
          break;
        }
        default: {
          const c = clientes.get(id);
          if (!c?.sessaoId) return;
          const sessao = sessoes.get(c.sessaoId);
          if (!sessao) return;
          const tecnicoWs = tecnicos.get(sessao.tecnicoId);
          if (tecnicoWs?.readyState === WebSocket.OPEN) {
            tecnicoWs.send(JSON.stringify(msg));
          }
          break;
        }
      }
    });

    ws.on('close', () => {
      const c = clientes.get(id);
      if (c?.sessaoId) {
        const sessao = sessoes.get(c.sessaoId);
        if (sessao) {
          const tecnicoWs = tecnicos.get(sessao.tecnicoId);
          if (tecnicoWs?.readyState === WebSocket.OPEN) {
            tecnicoWs.send(JSON.stringify({ tipo: 'cliente_desconectou', clienteId: id, sessaoId: c.sessaoId }));
          }
          sessoes.delete(c.sessaoId);
        }
      }
      clientes.delete(id);
      broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
      console.log(`[CLIENTE] Desconectou: ${id}`);
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n✅ Servidor rodando em http://localhost:${PORT}`);
});
