const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

const DB_FILE = path.join('/tmp', 'clientes_db.json');

// Carrega clientes salvos do disco
function carregarClientes() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const dados = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
      dados.forEach(({ token, info }) => {
        // Volta como offline — ws null até reconectar
        clientes.set(token, { ws: null, info, tecnicoId: null, ultimaVez: Date.now() });
      });
      console.log(`[DB] ${dados.length} clientes carregados`);
    }
  } catch (e) {
    console.log('[DB] Erro ao carregar:', e.message);
  }
}

// Salva clientes no disco
function salvarClientes() {
  try {
    const dados = [];
    clientes.forEach((c, token) => {
      dados.push({ token, info: c.info });
    });
    fs.writeFileSync(DB_FILE, JSON.stringify(dados), 'utf8');
  } catch (e) {
    console.log('[DB] Erro ao salvar:', e.message);
  }
}

// Clientes persistem mesmo offline
const clientes = new Map(); // token -> { ws, info, tecnicoId, ultimaVez }
const tecnicos = new Map(); // tecnicoId -> { ws, deviceToken }

// Carrega ao iniciar
carregarClientes();

function listaClientes() {
  const lista = [];
  clientes.forEach((c, token) => {
    lista.push({
      deviceToken: token,
      info: c.info,
      online: c.ws && c.ws.readyState === WebSocket.OPEN,
      emAtendimento: !!c.tecnicoId
    });
  });
  return lista;
}

function broadcastTecnicos(msg) {
  const data = JSON.stringify(msg);
  tecnicos.forEach((t) => {
    if (t.ws.readyState === WebSocket.OPEN) t.ws.send(data);
  });
}

function repassarParaCliente(tecnicoId, msg) {
  const t = tecnicos.get(tecnicoId);
  if (!t?.deviceToken) return;
  const c = clientes.get(t.deviceToken);
  if (c?.ws?.readyState === WebSocket.OPEN) {
    c.ws.send(JSON.stringify(msg));
  }
}

function repassarParaTecnico(deviceToken, msg) {
  const c = clientes.get(deviceToken);
  if (!c?.tecnicoId) return;
  const t = tecnicos.get(c.tecnicoId);
  if (t?.ws?.readyState === WebSocket.OPEN) {
    t.ws.send(JSON.stringify(msg));
  }
}

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const papel = url.searchParams.get('papel');
  const deviceToken = url.searchParams.get('token');

  // ── TÉCNICO ───────────────────────────────────────────────────────────
  if (papel === 'tecnico') {
    const tecnicoId = crypto.randomBytes(4).toString('hex').toUpperCase();
    tecnicos.set(tecnicoId, { ws, deviceToken: null });
    console.log(`[TÉCNICO +] ${tecnicoId}`);

    // Envia lista completa incluindo offline
    ws.send(JSON.stringify({ tipo: 'lista_clientes', clientes: listaClientes() }));

    ws.on('message', (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const t = tecnicos.get(tecnicoId);

      // Entrar em sessão com cliente
      if (msg.tipo === 'conectar_cliente' || msg.tipo === 'voltar_cliente') {
        const c = clientes.get(msg.deviceToken);
        if (!c) {
          ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente não encontrado' }));
          return;
        }
        if (!c.ws || c.ws.readyState !== WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'erro', msg: 'Cliente offline' }));
          return;
        }
        // Libera cliente anterior
        if (t.deviceToken && t.deviceToken !== msg.deviceToken) {
          const anterior = clientes.get(t.deviceToken);
          if (anterior) {
            anterior.tecnicoId = null;
            if (anterior.ws?.readyState === WebSocket.OPEN) {
              anterior.ws.send(JSON.stringify({ tipo: 'tecnico_desconectou' }));
            }
          }
        }
        c.tecnicoId = tecnicoId;
        t.deviceToken = msg.deviceToken;
        c.ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
        ws.send(JSON.stringify({ tipo: 'sessao_iniciada', deviceToken: msg.deviceToken, info: c.info }));
        console.log(`[SESSÃO] Técnico ${tecnicoId} → ${msg.deviceToken}`);
        broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        return;
      }

      // Sair da sessão atual (voltar para lista)
      if (msg.tipo === 'sair_sessao') {
        if (t.deviceToken) {
          const c = clientes.get(t.deviceToken);
          if (c) {
            c.tecnicoId = null;
            if (c.ws?.readyState === WebSocket.OPEN) {
              c.ws.send(JSON.stringify({ tipo: 'tecnico_desconectou' }));
            }
          }
          t.deviceToken = null;
        }
        broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        return;
      }

      // Repassa qualquer outro comando pro cliente atual
      repassarParaCliente(tecnicoId, msg);
    });

    ws.on('close', () => {
      const t = tecnicos.get(tecnicoId);
      if (t?.deviceToken) {
        const c = clientes.get(t.deviceToken);
        if (c) {
          c.tecnicoId = null;
          if (c.ws?.readyState === WebSocket.OPEN) {
            c.ws.send(JSON.stringify({ tipo: 'tecnico_desconectou' }));
          }
        }
      }
      tecnicos.delete(tecnicoId);
      console.log(`[TÉCNICO -] ${tecnicoId}`);
    });

  // ── CLIENTE ───────────────────────────────────────────────────────────
  } else if (papel === 'cliente' && deviceToken) {
    const existing = clientes.get(deviceToken);

    if (existing) {
      // Reconexão — atualiza só o WebSocket, preserva info e tecnicoId
      if (existing.ws?.readyState === WebSocket.OPEN) {
        existing.ws.close(1000, 'Reconexão');
      }
      existing.ws = ws;
      existing.ultimaVez = Date.now();
      console.log(`[CLIENTE ~] Reconectou: ${deviceToken}`);

      // Se tinha técnico ativo, reconecta a sessão
      if (existing.tecnicoId) {
        const t = tecnicos.get(existing.tecnicoId);
        if (t?.ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ tipo: 'tecnico_conectou' }));
          t.ws.send(JSON.stringify({ tipo: 'sessao_iniciada', deviceToken, info: existing.info }));
        } else {
          existing.tecnicoId = null;
        }
      }
    } else {
      // Novo cliente
      clientes.set(deviceToken, { ws, info: {}, tecnicoId: null, ultimaVez: Date.now() });
      console.log(`[CLIENTE +] Novo: ${deviceToken}`);
      salvarClientes();
    }

    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });

    ws.on('message', (raw) => {
      let msg; try { msg = JSON.parse(raw); } catch { return; }
      const c = clientes.get(deviceToken);

      if (msg.tipo === 'info_dispositivo') {
        if (c) {
          c.info = msg.dados;
          salvarClientes(); // Persiste info atualizada
          broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
        }
        return;
      }

      repassarParaTecnico(deviceToken, msg);
    });

    ws.on('close', () => {
      console.log(`[CLIENTE ~] Desconectou: ${deviceToken}`);
      const c = clientes.get(deviceToken);
      if (c) {
        c.ultimaVez = Date.now();
        // NÃO remove da lista — cliente reconecta em 1.5s
        // Notifica técnico que cliente ficou offline
        if (c.tecnicoId) {
          const t = tecnicos.get(c.tecnicoId);
          if (t?.ws.readyState === WebSocket.OPEN) {
            t.ws.send(JSON.stringify({ tipo: 'cliente_offline' }));
          }
        }
      }
      broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
    });
  }
});

// Remove clientes que ficaram offline por mais de 1 hora
setInterval(() => {
  const limite = Date.now() - 60 * 60 * 1000; // 1 hora
  let removidos = 0;
  clientes.forEach((c, token) => {
    const offline = !c.ws || c.ws.readyState !== WebSocket.OPEN;
    if (offline && c.ultimaVez < limite) {
      clientes.delete(token);
      removidos++;
    }
  });
  if (removidos > 0) {
    console.log(`[LIMPEZA] ${removidos} clientes removidos`);
    broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
  }
}, 5 * 60 * 1000); // checa a cada 5 minutos

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`✅ Servidor rodando na porta ${PORT}`));
