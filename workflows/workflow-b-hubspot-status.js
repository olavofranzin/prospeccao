
import { workflow, node, trigger, newCredential, expr, placeholder } from '@n8n/workflow-sdk';

// Workflow B: HubSpot - Atualizar status e disparar extracao
// ID: kED2AlXJjIYgvHXH
// Folder: Prospecção (7sA9HT5TB1YHClCw)
//
// Triggered when any HubSpot deal stage changes:
// 1. Extract deal_id and new stage
// 2. Map stage ID → human-readable status (configure stageMap with your pipeline IDs)
// 3. Update matching row in Google Sheets leads tab
// 4. Set default extraction parameters
// 5. Trigger Workflow A (which self-regulates: only extracts if slots < 500)
//
// Manual setup required:
//  - HubSpot Developer API credential (for webhook trigger)
//  - Update stageMap in "Mapear estagio para status" with your actual HubSpot stage IDs
//    (HubSpot > Settings > Objects > Deals > Pipeline)
//  - Fill in "Preparar parametros de re-extracao" placeholders:
//    segmento and localizacao defaults for auto re-trigger

const SPREADSHEET_ID = '1MuetJ4N7xiazkw55YOSHtq_nIaHPRKOE-g6GwfaNJKM';
const WORKFLOW_A_ID = '5L3SyzDkZqf1N6vW';

const hubspotTrigger = trigger({
  type: 'n8n-nodes-base.hubspotTrigger',
  version: 1,
  config: {
    name: 'HubSpot - Deal stage alterado',
    parameters: {
      eventsUi: {
        eventValues: [
          {
            name: 'deal.propertyChange',
            property: 'dealstage',
          },
        ],
      },
    },
    credentials: newCredential('hubspotDeveloperApi'),
  },
});

const extrairDados = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Extrair dados do deal',
    parameters: {
      assignments: {
        assignments: [
          { id: 'deal_id', name: 'deal_id', value: expr('{{ $json.objectId }}'), type: 'string' },
          { id: 'novo_estagio', name: 'novo_estagio', value: expr('{{ $json.propertyValue }}'), type: 'string' },
        ],
      },
    },
  },
});

const mapearStatus = node({
  type: 'n8n-nodes-base.code',
  version: 2,
  config: {
    name: 'Mapear estagio para status',
    parameters: {
      jsCode: `
// Configure os IDs dos estágios do seu pipeline HubSpot:
// Acesse HubSpot > Configurações > Objetos > Deals > Pipeline para ver os IDs
const stageMap = {
  'appointmentscheduled': 'prospectado',
  'qualifiedtobuy': 'em prospecção',
  'presentationscheduled': 'negociando',
  'decisionmakerboughtin': 'proposta enviada',
  'contractsent': 'contrato enviado',
  'closedwon': 'ganho',
  'closedlost': 'perdido',
};

const dealId = $input.item.json.deal_id;
const novoEstagio = $input.item.json.novo_estagio;
const statusMapeado = stageMap[novoEstagio] || novoEstagio;

return [{
  json: {
    deal_id: dealId,
    novo_estagio: novoEstagio,
    status_mapeado: statusMapeado,
  }
}];
`,
    },
  },
});

const sheetsAtualizarStatus = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.5,
  config: {
    name: 'Atualizar status do lead na planilha',
    parameters: {
      operation: 'appendOrUpdate',
      documentId: expr(`{{ "${SPREADSHEET_ID}" }}`),
      sheetName: expr('{{ "gid=0" }}'),
      columns: {
        mappingMode: 'defineBelow',
        value: {
          hubspot_deal_id: expr('{{ $json.deal_id }}'),
          status: expr('{{ $json.status_mapeado }}'),
        },
        matchingColumns: ['hubspot_deal_id'],
      },
      options: {},
    },
    credentials: newCredential('googleSheetsOAuth2Api'),
  },
});

const prepararParametros = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Preparar parametros de re-extracao',
    parameters: {
      assignments: {
        assignments: [
          { id: 'segmento', name: 'segmento', value: placeholder('Segmento padrao para re-extracao (ex: restaurante)'), type: 'string' },
          { id: 'tipo_localizacao', name: 'tipo_localizacao', value: 'cidade', type: 'string' },
          { id: 'localizacao', name: 'localizacao', value: placeholder('Cidade padrao (ex: São Paulo)'), type: 'string' },
          { id: 'tem_site', name: 'tem_site', value: 'nao', type: 'string' },
          { id: 'avaliacao_minima', name: 'avaliacao_minima', value: 0, type: 'number' },
        ],
      },
    },
  },
});

const dispararExtracao = node({
  type: 'n8n-nodes-base.executeWorkflow',
  version: 1.3,
  config: {
    name: 'Disparar nova extracao de leads',
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

export default workflow('workflow-b', 'HubSpot - Atualizar status e disparar extracao')
  .add(hubspotTrigger)
  .to(extrairDados)
  .to(mapearStatus)
  .to(sheetsAtualizarStatus)
  .to(prepararParametros)
  .to(dispararExtracao);
