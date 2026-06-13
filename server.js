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

const clientes = new Map();
const tecnicos = new Map();

function broadcastTecnicos(msg) {
  const data = JSON.stringify(msg);
  tecnicos.forEach((t) => {
    if (t.ws.readyState === WebSocket.OPEN) t.ws.send(data);
  });
}

function listaClientes() {
  const lista = [];
  clientes.forEach((c, token) => {
    lista.push({ deviceToken: token, info: c.info, emAtendimento: !!c.tecnicoId });
  });
  return lista;
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const papel = url.searchParams.get('papel');
  const deviceToken = url.searchParams.get('token');

  if (papel === 'tecnico') {
    const tecnicoId = crypto.randomBytes(4).toString('hex').toUpperCase();
    tecnicos.set(tecnicoId, { ws, deviceToken: null });
    console.log(`[TÉCNICO] Conectou: ${tecnicoId}`);
    ws.send(JSON.stringify({ tipo: 'lista_clientes', clientes: listaClientes() }));

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      if (msg.tipo === 'conectar_cliente') {
        const cliente = clientes.get(msg.deviceToken);
        if (!cliente || cliente.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente não disponível' }));
          return;
        }
        cliente.tecnicoId = tecnicoId;
        tecnicos.get(tecnicoId).deviceToken = msg.deviceToken;
        cliente.ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
        ws.send(JSON.stringify({ tipo: 'sessao_iniciada', deviceToken: msg.deviceToken, info: cliente.info }));
        console.log(`[SESSÃO] Técnico ${tecnicoId} → Cliente ${msg.deviceToken}`);
        broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        return;
      }

      // Repassa pro cliente
      const t = tecnicos.get(tecnicoId);
      if (!t?.deviceToken) return;
      const cliente = clientes.get(t.deviceToken);
      if (cliente?.ws.readyState === WebSocket.OPEN) {
        cliente.ws.send(JSON.stringify(msg));
      }
    });

    ws.on('close', () => {
      const t = tecnicos.get(tecnicoId);
      if (t?.deviceToken) {
        const cliente = clientes.get(t.deviceToken);
        if (cliente) {
          cliente.tecnicoId = null;
          if (cliente.ws.readyState === WebSocket.OPEN) {
            cliente.ws.send(JSON.stringify({ tipo: 'tecnico_desconectou' }));
          }
        }
      }
      tecnicos.delete(tecnicoId);
      console.log(`[TÉCNICO] Desconectou: ${tecnicoId}`);
    });

  } else if (papel === 'cliente' && deviceToken) {
    const existing = clientes.get(deviceToken);
    if (existing) {
      existing.ws = ws;
      console.log(`[CLIENTE] Reconectou: ${deviceToken}`);
      if (existing.tecnicoId) {
        const t = tecnicos.get(existing.tecnicoId);
        if (t?.ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
        } else {
          existing.tecnicoId = null;
        }
      }
    } else {
      clientes.set(deviceToken, { ws, info: {}, tecnicoId: null });
      console.log(`[CLIENTE] Novo: ${deviceToken}`);
    }

    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });

    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }
      const cliente = clientes.get(deviceToken);

      if (msg.tipo === 'info_dispositivo') {
        if (cliente) {
          cliente.info = msg.dados;
          broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        }
        return;
      }

      // Repassa pro técnico
      if (!cliente?.tecnicoId) return;
      const t = tecnicos.get(cliente.tecnicoId);
      if (t?.ws.readyState === WebSocket.OPEN) {
        t.ws.send(JSON.stringify(msg));
      }
    });

    ws.on('close', () => {
      console.log(`[CLIENTE] Desconectou: ${deviceToken}`);
      const cliente = clientes.get(deviceToken);
      if (cliente?.tecnicoId) {
        const t = tecnicos.get(cliente.tecnicoId);
        if (t?.ws.readyState === WebSocket.OPEN) {
          t.ws.send(JSON.stringify({ tipo: 'cliente_desconectou' }));
        }
      }
      clientes.delete(deviceToken); // Remove da lista ao desconectar
      broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
    });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor rodando na porta ${PORT}`));
