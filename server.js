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

// Cada cliente tem um token único — nunca sobrescreve outro
const clientes = new Map();  // token -> { ws, info, tecnicoId }
const tecnicos = new Map();  // tecnicoId -> { ws, deviceToken }

function listaClientes() {
  const lista = [];
  clientes.forEach((c, token) => {
    // Só lista clientes com WebSocket ativo
    if (c.ws.readyState === WebSocket.OPEN) {
      lista.push({ deviceToken: token, info: c.info, emAtendimento: !!c.tecnicoId });
    }
  });
  return lista;
}

function broadcastTecnicos(msg) {
  const data = JSON.stringify(msg);
  tecnicos.forEach((t) => {
    if (t.ws.readyState === WebSocket.OPEN) t.ws.send(data);
  });
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const papel = url.searchParams.get('papel');
  const deviceToken = url.searchParams.get('token');

  // ── TÉCNICO ───────────────────────────────────────────────────────────
  if (papel === 'tecnico') {
    const tecnicoId = crypto.randomBytes(4).toString('hex').toUpperCase();
    tecnicos.set(tecnicoId, { ws, deviceToken: null });
    ws.send(JSON.stringify({ tipo: 'lista_clientes', clientes: listaClientes() }));
    console.log(`[+] Técnico ${tecnicoId}`);

    ws.on('message', (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const t = tecnicos.get(tecnicoId);

      if (msg.tipo === 'conectar_cliente') {
        const cliente = clientes.get(msg.deviceToken);
        if (!cliente || cliente.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente offline' }));
          return;
        }
        // Libera cliente anterior MAS não desconecta — técnico pode voltar a ele
        if (t.deviceToken && t.deviceToken !== msg.deviceToken) {
          const anterior = clientes.get(t.deviceToken);
          if (anterior) anterior.tecnicoId = null;
        }
        cliente.tecnicoId = tecnicoId;
        t.deviceToken = msg.deviceToken;
        cliente.ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
        ws.send(JSON.stringify({ tipo: 'sessao_iniciada', deviceToken: msg.deviceToken, info: cliente.info }));
        broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        return;
      }

      if (msg.tipo === 'voltar_cliente') {
        // Técnico voltou para um cliente que já atendeu antes
        const cliente = clientes.get(msg.deviceToken);
        if (!cliente || cliente.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente offline' }));
          return;
        }
        if (t.deviceToken && t.deviceToken !== msg.deviceToken) {
          const anterior = clientes.get(t.deviceToken);
          if (anterior) anterior.tecnicoId = null;
        }
        cliente.tecnicoId = tecnicoId;
        t.deviceToken = msg.deviceToken;
        cliente.ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
        ws.send(JSON.stringify({ tipo: 'sessao_iniciada', deviceToken: msg.deviceToken, info: cliente.info }));
        broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        return;
      }

      // Repassa pro cliente
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
      console.log(`[-] Técnico ${tecnicoId}`);
    });

  // ── CLIENTE ───────────────────────────────────────────────────────────
  } else if (papel === 'cliente' && deviceToken) {
    // Se já existe esse token com conexão ativa, fecha a antiga
    const existing = clientes.get(deviceToken);
    if (existing && existing.ws.readyState === WebSocket.OPEN) {
      existing.ws.close(1000, 'Reconexão');
    }

    // Registra novo WebSocket para esse token
    clientes.set(deviceToken, { ws, info: existing?.info || {}, tecnicoId: existing?.tecnicoId || null });
    console.log(`[+] Cliente ${deviceToken}`);

    // Se tinha técnico, reconecta
    const cliente = clientes.get(deviceToken);
    if (cliente.tecnicoId) {
      const t = tecnicos.get(cliente.tecnicoId);
      if (t?.ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
      } else {
        cliente.tecnicoId = null;
      }
    }

    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });

    ws.on('message', (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const cliente = clientes.get(deviceToken);

      if (msg.tipo === 'info_dispositivo') {
        if (cliente) {
          cliente.info = msg.dados;
          broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        }
        return;
      }

      if (!cliente?.tecnicoId) return;
      const t = tecnicos.get(cliente.tecnicoId);
      if (t?.ws.readyState === WebSocket.OPEN) {
        t.ws.send(JSON.stringify(msg));
      }
    });

    ws.on('close', () => {
      console.log(`[-] Cliente ${deviceToken}`);
      const cliente = clientes.get(deviceToken);
      if (cliente?.tecnicoId) {
        const t = tecnicos.get(cliente.tecnicoId);
        if (t?.ws.readyState === WebSocket.OPEN) {
          t.ws.send(JSON.stringify({ tipo: 'cliente_desconectou' }));
        }
      }
      // Remove da lista imediatamente
      clientes.delete(deviceToken);
      broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
    });
  }
});

// Limpa clientes com WebSocket morto a cada 30s
setInterval(() => {
  let removidos = 0;
  clientes.forEach((c, token) => {
    if (c.ws.readyState !== WebSocket.OPEN) {
      clientes.delete(token);
      removidos++;
    }
  });
  if (removidos > 0) {
    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
  }
}, 30000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor rodando na porta ${PORT}`));
