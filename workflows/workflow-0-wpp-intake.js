
import { workflow, node, trigger, switchCase, newCredential, expr, placeholder } from '@n8n/workflow-sdk';

// Workflow 0: WPP Intake - Google Maps
// ID: D9WYlDvt9QDuP50k
// Folder: Prospecção (7sA9HT5TB1YHClCw)
//
// Conversational WhatsApp questionnaire:
//  Q1 - Segmento (ex: restaurante)
//  Q2 - Tipo localização (cidade / estado / pais)
//  Q3 - Localização (ex: São Paulo)
//  Q4 - Tem site? (sim / nao)
//  Q5 - Avaliação mínima (1-5 ou "qualquer")
// → Triggers Workflow A with normalised parameters

const WORKFLOW_A_ID = '5L3SyzDkZqf1N6vW';

const wppTrigger = trigger({
  type: 'n8n-nodes-base.whatsAppTrigger',
  version: 1,
  config: {
    name: 'Mensagem WhatsApp recebida',
    parameters: {
      updates: ['messages'],
    },
    credentials: newCredential('whatsAppTriggerApi'),
  },
});

const extrairDados = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Extrair telefone e mensagem',
    parameters: {
      assignments: {
        assignments: [
          { id: '1', name: 'telefone', value: expr('{{ $json.from }}'), type: 'string' },
          { id: '2', name: 'mensagem', value: expr('{{ $json.text.body }}'), type: 'string' },
        ],
      },
    },
  },
});

const gerenciarEstado = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Gerenciar estado da conversa',
    parameters: {
      mode: 'runOnceForEachItem',
      jsCode: `
const staticData = $getWorkflowStaticData("global");
const conversas = staticData.conversas || {};
const telefone = $json.telefone;
const mensagem = $json.mensagem;
if (!conversas[telefone]) {
  conversas[telefone] = { etapa: "0", segmento: "", tipo_localizacao: "", localizacao: "", tem_site: "", avaliacao_minima: "" };
}
const conversa = conversas[telefone];
const etapaAnterior = conversa.etapa;
if (etapaAnterior === "1") conversa.segmento = mensagem;
else if (etapaAnterior === "2") conversa.tipo_localizacao = mensagem;
else if (etapaAnterior === "3") conversa.localizacao = mensagem;
else if (etapaAnterior === "4") conversa.tem_site = mensagem;
else if (etapaAnterior === "5") conversa.avaliacao_minima = mensagem;
const mapa = { "0": "1", "1": "2", "2": "3", "3": "4", "4": "5", "5": "completo" };
conversa.etapa = mapa[etapaAnterior] || "completo";
staticData.conversas = conversas;
return { json: { ...conversa, telefone, mensagem, etapa_anterior: etapaAnterior } };
`,
    },
  },
});

const roteador = switchCase({
  name: 'Roteador de etapas',
  rules: [
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '0' }], outputKey: 'Iniciar' },
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '1' }], outputKey: 'Filtro Tipo' },
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '2' }], outputKey: 'Localizacao' },
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '3' }], outputKey: 'Site' },
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '4' }], outputKey: 'Avaliacao' },
    { conditions: [{ left: expr('{{ $json.etapa_anterior }}'), operator: 'equals', right: '5' }], outputKey: 'Finalizar' },
  ],
});

const PHONE_ID = placeholder('ID do numero WhatsApp Business');

const perguntarSegmento = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Perguntar segmento',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $json.telefone }}'),
      messageType: 'text',
      textBody: 'Ola! Vou configurar uma extracao de leads no Google Maps.\n\n*1/5* Qual e o segmento de negocio?\n_(ex: academias, clinicas, restaurantes)_',
    },
    credentials: newCredential('whatsAppApi'),
  },
});

const perguntarTipoFiltro = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Perguntar tipo de filtro',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $json.telefone }}'),
      messageType: 'text',
      textBody: '*2/5* Deseja filtrar por *cidade*, *estado* ou *pais*?',
    },
    credentials: newCredential('whatsAppApi'),
  },
});

const perguntarLocalizacao = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Perguntar localizacao',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $json.telefone }}'),
      messageType: 'text',
      textBody: '*3/5* Qual a localizacao?\n_(ex: Sao Paulo, SP, ou Brasil)_',
    },
    credentials: newCredential('whatsAppApi'),
  },
});

const perguntarSite = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Perguntar sobre site',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $json.telefone }}'),
      messageType: 'text',
      textBody: '*4/5* Filtrar apenas empresas com site? (*sim* ou *nao*)',
    },
    credentials: newCredential('whatsAppApi'),
  },
});

const perguntarAvaliacao = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Perguntar avaliacao minima',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $json.telefone }}'),
      messageType: 'text',
      textBody: '*5/5* Qual a avaliacao minima no Google?\n_(Digite 1 a 5, ou "qualquer" para sem filtro)_',
    },
    credentials: newCredential('whatsAppApi'),
  },
});

const prepararParametros = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Preparar parametros para extracao',
    parameters: {
      assignments: {
        assignments: [
          { id: 'p_segmento', name: 'segmento', value: expr('{{ $json.segmento }}'), type: 'string' },
          { id: 'p_tipo', name: 'tipo_localizacao', value: expr('{{ $json.tipo_localizacao }}'), type: 'string' },
          { id: 'p_local', name: 'localizacao', value: expr('{{ $json.localizacao }}'), type: 'string' },
          { id: 'p_site', name: 'tem_site', value: expr('{{ $json.tem_site }}'), type: 'string' },
          { id: 'p_aval', name: 'avaliacao_minima', value: expr('{{ $json.avaliacao_minima === "qualquer" ? 0 : parseFloat($json.avaliacao_minima) || 0 }}'), type: 'number' },
        ],
      },
    },
  },
});

const dispararExtracao = node({
  type: 'n8n-nodes-base.executeWorkflow',
  version: 1.3,
  config: {
    name: 'Disparar extracao de leads',
    parameters: {
      source: 'database',
      workflowId: WORKFLOW_A_ID,
      mode: 'once',
      options: {
        waitForSubWorkflow: false,
      },
    },
  },
});

const confirmarExtracao = node({
  type: 'n8n-nodes-base.whatsApp',
  version: 1.1,
  config: {
    name: 'Confirmar extracao iniciada',
    parameters: {
      resource: 'message',
      operation: 'send',
      phoneNumberId: PHONE_ID,
      recipientPhoneNumber: expr('{{ $("Gerenciar estado da conversa").item.json.telefone }}'),
      messageType: 'text',
      textBody: expr(`{{ 'Extracao iniciada!\\n\\n' +
        '*Segmento:* ' + $("Gerenciar estado da conversa").item.json.segmento + '\\n' +
        '*Local:* ' + $("Gerenciar estado da conversa").item.json.localizacao + '\\n' +
        '*Com site:* ' + $("Gerenciar estado da conversa").item.json.tem_site + '\\n' +
        '*Avaliacao minima:* ' + $("Gerenciar estado da conversa").item.json.avaliacao_minima + '\\n\\n' +
        'Voce recebera um resumo em breve!' }}`),
    },
    credentials: newCredential('whatsAppApi'),
  },
});

export default workflow('D9WYlDvt9QDuP50k', 'WPP Intake - Google Maps')
  .add(wppTrigger)
  .to(extrairDados)
  .to(gerenciarEstado)
  .to(roteador
    .onCase(0, perguntarSegmento)
    .onCase(1, perguntarTipoFiltro)
    .onCase(2, perguntarLocalizacao)
    .onCase(3, perguntarSite)
    .onCase(4, perguntarAvaliacao)
    .onCase(5, prepararParametros.to(dispararExtracao).to(confirmarExtracao))
  );
