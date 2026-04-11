
import { workflow, node, trigger, ifElse, newCredential, expr, placeholder, splitInBatches, nextBatch, languageModel } from '@n8n/workflow-sdk';

// Workflow A: Google Maps - Extracao e Enriquecimento
// ID: 5L3SyzDkZqf1N6vW
// Folder: Prospecção (7sA9HT5TB1YHClCw)
//
// Receives: segmento, tipo_localizacao, localizacao, tem_site, avaliacao_minima
// 1. Reads lead count from "qtd leads mes" sheet
// 2. Calculates available slots (max 500)
// 3. If slots > 0: runs Apify Google Maps scraper
// 4. Loops per lead (batch=1):
//    - Normalise fields
//    - Save to Sheets (status: novo)
//    - Enrich via Gemini agent
//    - Update Sheets (status: enriquecido)
//    - Create HubSpot deal (stage: prospectado)
//    - Update Sheets (status: prospectado, hubspot_deal_id)
//
// Manual setup required:
//  - Apify URL: https://api.apify.com/v2/acts/compass~crawler-google-places/run-sync-get-dataset-items?token=YOUR_TOKEN
//  - HTTP Header Auth credential: Authorization: Bearer YOUR_APIFY_TOKEN
//  - HubSpot stage ID for "Prospectado"
//  - Google Sheets "qtd leads mes" sheet must have column named "quantidade"
//  - Google Sheets "leads" sheet columns: place_id, nome, telefone, site, rating,
//    endereco, categoria, status, data_extracao, enriquecimento, hubspot_deal_id

const SPREADSHEET_ID = '1MuetJ4N7xiazkw55YOSHtq_nIaHPRKOE-g6GwfaNJKM';

const execTrigger = trigger({
  type: 'n8n-nodes-base.executeWorkflowTrigger',
  version: 1.1,
  config: {
    name: 'Receber parametros de extracao',
    parameters: {},
  },
});

const lerContagem = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.5,
  config: {
    name: 'Ler contagem de leads',
    parameters: {
      operation: 'read',
      documentId: expr(`{{ "${SPREADSHEET_ID}" }}`),
      sheetName: expr('{{ "gid=624786381" }}'),
      filtersUI: {},
      options: { executeOnce: true },
    },
    credentials: newCredential('googleSheetsOAuth2Api'),
  },
});

const calcularLimite = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Calcular vagas disponiveis',
    parameters: {
      assignments: {
        assignments: [
          { id: 'vagas', name: 'vagas', value: expr('{{ Math.max(0, 500 - ($json.quantidade || 0)) }}'), type: 'number' },
          { id: 'segmento', name: 'segmento', value: expr('{{ $("Receber parametros de extracao").item.json.segmento }}'), type: 'string' },
          { id: 'tipo_localizacao', name: 'tipo_localizacao', value: expr('{{ $("Receber parametros de extracao").item.json.tipo_localizacao }}'), type: 'string' },
          { id: 'localizacao', name: 'localizacao', value: expr('{{ $("Receber parametros de extracao").item.json.localizacao }}'), type: 'string' },
          { id: 'tem_site', name: 'tem_site', value: expr('{{ $("Receber parametros de extracao").item.json.tem_site }}'), type: 'string' },
          { id: 'avaliacao_minima', name: 'avaliacao_minima', value: expr('{{ $("Receber parametros de extracao").item.json.avaliacao_minima }}'), type: 'number' },
        ],
      },
    },
  },
});

const ifTemVaga = ifElse({
  name: 'Tem vaga disponivel?',
  condition: expr('{{ $json.vagas > 0 }}'),
});

const apifyRun = node({
  type: 'n8n-nodes-base.httpRequest',
  version: 4.2,
  config: {
    name: 'Executar Apify Google Maps',
    parameters: {
      method: 'POST',
      url: placeholder('URL do Apify run-sync-get-dataset-items'),
      authentication: 'genericCredentialType',
      genericAuthType: 'httpHeaderAuth',
      sendHeaders: true,
      headerParameters: {
        parameters: [
          { name: 'Content-Type', value: 'application/json' },
        ],
      },
      sendBody: true,
      specifyBody: 'json',
      jsonBody: expr('{{ JSON.stringify({ searchStrings: [$json.segmento + " " + $json.localizacao], maxCrawledPlacesPerSearch: $json.vagas, language: "pt", countryCode: "br", website: $json.tem_site === "sim" ? "allWithWebsite" : "allWithoutWebsite", minimumStarRating: $json.avaliacao_minima }) }}'),
      options: {},
    },
    credentials: newCredential('httpHeaderAuth'),
  },
});

const sibLoop = splitInBatches({
  name: 'Loop por lead',
  batchSize: 1,
});

const normalizarCampos = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Normalizar campos do lead',
    parameters: {
      assignments: {
        assignments: [
          { id: 'place_id', name: 'place_id', value: expr('{{ $json.placeId }}'), type: 'string' },
          { id: 'nome', name: 'nome', value: expr('{{ $json.title }}'), type: 'string' },
          { id: 'telefone', name: 'telefone', value: expr('{{ $json.phone }}'), type: 'string' },
          { id: 'site', name: 'site', value: expr('{{ $json.website }}'), type: 'string' },
          { id: 'rating', name: 'rating', value: expr('{{ $json.totalScore }}'), type: 'number' },
          { id: 'endereco', name: 'endereco', value: expr('{{ $json.address }}'), type: 'string' },
          { id: 'categoria', name: 'categoria', value: expr('{{ $json.categoryName }}'), type: 'string' },
          { id: 'status_novo', name: 'status', value: 'novo', type: 'string' },
          { id: 'data_extracao', name: 'data_extracao', value: expr('{{ $now }}'), type: 'string' },
        ],
      },
    },
  },
});

const sheetsUpsert = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.5,
  config: {
    name: 'Salvar lead bruto na planilha',
    parameters: {
      operation: 'appendOrUpdate',
      documentId: expr(`{{ "${SPREADSHEET_ID}" }}`),
      sheetName: expr('{{ "gid=0" }}'),
      columns: {
        mappingMode: 'autoMapInputData',
        matchingColumns: ['place_id'],
      },
      options: {},
    },
    credentials: newCredential('googleSheetsOAuth2Api'),
  },
});

const geminiModel = languageModel({
  type: 'lmChatGoogleGemini',
  config: {
    name: 'Gemini Flash',
    parameters: {
      modelName: 'models/gemini-2.5-flash',
      options: {},
    },
    credentials: newCredential('googleGeminiApi'),
  },
});

const agentEnriquecimento = node({
  type: '@n8n/n8n-nodes-langchain.agent',
  version: 3.1,
  config: {
    name: 'Agente de Enriquecimento',
    parameters: {
      promptType: 'define',
      systemMessage: 'Você é um especialista em prospecção B2B. Analise os dados do lead e forneça insights estratégicos e acionáveis para a equipe de vendas.',
      text: expr(`{{ 'Enriqueça os dados deste lead para prospecção B2B:\\n' +
        'Nome: ' + $("Normalizar campos do lead").item.json.nome + '\\n' +
        'Telefone: ' + $("Normalizar campos do lead").item.json.telefone + '\\n' +
        'Site: ' + $("Normalizar campos do lead").item.json.site + '\\n' +
        'Endereço: ' + $("Normalizar campos do lead").item.json.endereco + '\\n' +
        'Categoria: ' + $("Normalizar campos do lead").item.json.categoria + '\\n' +
        'Avaliação: ' + $("Normalizar campos do lead").item.json.rating + '\\n\\n' +
        'Forneça: 1) Resumo do negócio, 2) Potencial de prospecção (alto/médio/baixo), 3) Sugestão de abordagem inicial, 4) Possíveis necessidades e dores.' }}`),
      options: {},
    },
    subnodes: {
      model: geminiModel,
    },
  },
});

const sheetsUpdateEnriquecido = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.5,
  config: {
    name: 'Atualizar lead enriquecido na planilha',
    parameters: {
      operation: 'appendOrUpdate',
      documentId: expr(`{{ "${SPREADSHEET_ID}" }}`),
      sheetName: expr('{{ "gid=0" }}'),
      columns: {
        mappingMode: 'defineBelow',
        value: {
          place_id: expr('{{ $("Normalizar campos do lead").item.json.place_id }}'),
          enriquecimento: expr('{{ $json.output }}'),
          status: 'enriquecido',
        },
        matchingColumns: ['place_id'],
      },
      options: {},
    },
    credentials: newCredential('googleSheetsOAuth2Api'),
  },
});

const hubspotCreate = node({
  type: 'n8n-nodes-base.hubspot',
  version: 2.2,
  config: {
    name: 'Criar deal no HubSpot',
    parameters: {
      resource: 'deal',
      operation: 'create',
      authentication: 'appToken',
      stage: placeholder('ID do estagio Prospectado no HubSpot'),
      additionalFields: {
        dealName: expr('{{ $("Normalizar campos do lead").item.json.nome }}'),
        description: expr('{{ $("Agente de Enriquecimento").item.json.output }}'),
      },
    },
    credentials: newCredential('hubspotAppToken'),
  },
});

const sheetsUpdateProspectado = node({
  type: 'n8n-nodes-base.googleSheets',
  version: 4.5,
  config: {
    name: 'Atualizar status prospectado na planilha',
    parameters: {
      operation: 'appendOrUpdate',
      documentId: expr(`{{ "${SPREADSHEET_ID}" }}`),
      sheetName: expr('{{ "gid=0" }}'),
      columns: {
        mappingMode: 'defineBelow',
        value: {
          place_id: expr('{{ $("Normalizar campos do lead").item.json.place_id }}'),
          hubspot_deal_id: expr('{{ $json.id }}'),
          status: 'prospectado',
        },
        matchingColumns: ['place_id'],
      },
      options: {},
    },
    credentials: newCredential('googleSheetsOAuth2Api'),
  },
});

const finalizarExtracao = node({
  type: 'n8n-nodes-base.set',
  version: 3.4,
  config: {
    name: 'Finalizar extracao',
    parameters: {
      assignments: {
        assignments: [
          { id: 'status_final', name: 'status', value: 'concluido', type: 'string' },
          { id: 'timestamp_final', name: 'timestamp', value: expr('{{ $now }}'), type: 'string' },
        ],
      },
    },
  },
});

export default workflow('5L3SyzDkZqf1N6vW', 'Google Maps - Extracao e Enriquecimento')
  .add(execTrigger)
  .to(lerContagem)
  .to(calcularLimite)
  .to(ifTemVaga
    .onTrue(
      apifyRun.to(
        sibLoop
          .onDone(finalizarExtracao)
          .onEachBatch(
            normalizarCampos
              .to(sheetsUpsert)
              .to(agentEnriquecimento)
              .to(sheetsUpdateEnriquecido)
              .to(hubspotCreate)
              .to(sheetsUpdateProspectado)
              .to(nextBatch(sibLoop))
          )
      )
    )
  );
