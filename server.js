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
const sessoes = new Map();

// Guarda última sessão por deviceId para reconexão
const sessoesPorDispositivo = new Map();

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

          // Salva sessão por modelo do dispositivo para reconexão
          const modelo = cliente.info?.modelo || '';
          if (modelo) sessoesPorDispositivo.set(modelo, { sessaoId, tecnicoId: id });

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
          let sessaoEncontrada = null;
          sessoes.forEach((s) => {
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
            console.log(`[INFO] Cliente ${id}: ${JSON.stringify(msg.dados)}`);

            // Tenta reconectar à sessão anterior pelo modelo do dispositivo
            const modelo = msg.dados?.modelo || '';
            const sessaoAnterior = sessoesPorDispositivo.get(modelo);
            if (sessaoAnterior) {
              const sessao = sessoes.get(sessaoAnterior.sessaoId);
              const tecnicoWs = tecnicos.get(sessaoAnterior.tecnicoId);
              if (sessao && tecnicoWs?.readyState === WebSocket.OPEN) {
                // Atualiza clienteId na sessão
                sessao.clienteId = id;
                c.sessaoId = sessaoAnterior.sessaoId;
                console.log(`[RECONEXÃO] Cliente ${id} reconectado à sessão ${sessaoAnterior.sessaoId}`);
                // Avisa técnico que cliente reconectou
                tecnicoWs.send(JSON.stringify({
                  tipo: 'sessao_iniciada',
                  sessaoId: sessaoAnterior.sessaoId,
                  clienteId: id,
                  info: msg.dados
                }));
                // Avisa cliente para iniciar stream novamente
                ws.send(JSON.stringify({ tipo: 'tecnico_conectou', sessaoId: sessaoAnterior.sessaoId }));
              } else {
                sessoesPorDispositivo.delete(modelo);
              }
            }

            broadcastTecnicos({ tipo: 'lista_clientes', clientes: listaClientes() });
          }
          break;
        }

        case 'frame_tela': {
          const c = clientes.get(id);
          if (!c?.sessaoId) {
            console.log(`[FRAME] Cliente ${id} sem sessão — descartando frame`);
            return;
          }
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
          // Não deleta sessão imediatamente — aguarda reconexão
          setTimeout(() => {
            const clienteAtual = clientes.get(sessao.clienteId);
            if (!clienteAtual) {
              sessoes.delete(c.sessaoId);
              console.log(`[SESSÃO] Removida: ${c.sessaoId}`);
            }
          }, 10000); // aguarda 10s para reconexão
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
