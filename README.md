# Pipeline event-driven Medalhão de vendas (Prefect + MinIO + PostgreSQL)

Trabalho Final da disciplina **Orquestração de Workflows**. Pipeline de dados **event-driven**
que recebe arquivos de vendas de filiais e os processa em **arquitetura Medalhão
(Bronze → Silver → Gold)**, orquestrado por **Prefect**. Quando um arquivo chega ao MinIO, sua
chegada **dispara automaticamente** o pipeline (evento → Automation → deployment) — sem
agendamento periódico.

> Este README é a **documentação oficial e única** do projeto: todo o necessário para instalar,
> executar, testar e validar está aqui. Todos os comandos foram testados no WSL2 + Docker.

---

## 1. Problema / contexto

Uma empresa recebe arquivos de vendas de diferentes **filiais** (SP01, SP02, RJ01, MG01). O
processo manual de recebimento, conferência e importação causa **atrasos**, **duplicidade**
(mesmo arquivo/venda importados 2×), **erros de formato** e **falta de rastreabilidade**. Este
projeto automatiza tudo, do recebimento do arquivo até indicadores consolidados, com
**idempotência**, **resiliência** e **rastreabilidade** ponta a ponta.

---

## 2. Arquitetura

**Cadeia event-driven:**

```
Filial envia CSV ─▶ MinIO (bucket landing)
                      │ s3:ObjectCreated (notification/webhook)
                      ▼
                 event-bridge (FastAPI)  ── emit_event("vendas.arquivo.recebido")
                      │
                      ▼
                 Prefect Event ─▶ Automation ─▶ Deployment "ingestao-vendas"
                      │
                      ▼
                 Worker executa o flow  ──▶  Bronze ▶ Silver ▶ Gold
```

**Camadas Medalhão:**

| Camada | Onde | O que faz |
|---|---|---|
| **Bronze** | MinIO (`bronze/`) + tabela `bronze_arquivos` | Preserva o arquivo **bruto imutável** e registra o recebimento (sha256, origem, métricas). |
| **Silver** | PostgreSQL (`silver_vendas`, `silver_vendas_rejeitadas`) | Valida, limpa, tipa e **deduplica**; separa válidas de **rejeitadas** (com motivo). |
| **Gold** | PostgreSQL (`gold_*`) | Consolida **faturamento**, **quantidade de vendas** e **ticket médio** por filial, categoria e produto. |

**Idempotência em 3 camadas:** arquivo (`sha256` UNIQUE) · registro (`UNIQUE(origem,
venda_id_origem)`) · Gold (recomputação determinística a partir da Silver).
**Rastreabilidade:** `file_id` liga `bronze_arquivos` → `silver_vendas` /
`silver_vendas_rejeitadas`; cada execução publica um **artifact** de resumo.

---

## 3. Ferramentas e justificativas

| Ferramenta | Papel | Por quê |
|---|---|---|
| **Prefect 3.7.8 (OSS)** | Orquestrador | Pipeline é **event-driven**: Prefect OSS suporta **Events + Automations** self-hosted (sem Cloud), disparando o deployment por evento — não precisamos de scheduler. |
| **MinIO** | Object store (landing + bronze) | S3-compatível, com **bucket notification** nativa que gera o evento de chegada do arquivo. |
| **PostgreSQL 16** | Silver/Gold/controle | Relacional com constraints (UNIQUE/CHECK) que garantem idempotência e integridade. |
| **event-bridge (FastAPI)** | Ponte MinIO→Prefect | Prefect OSS não ingere webhooks externos; a ponte recebe a notification e **emite o evento** no Prefect. |
| **Adminer** | Inspeção do banco | Interface web leve para consultar Silver/Gold. |
| **Docker Compose** | Empacotamento | Sobe tudo com um comando; ambiente reprodutível. |

Versões fixadas (pin) em `docker-compose.yml` e `requirements.txt`.

---

## 4. Pré-requisitos

- **Docker** e **Docker Compose v2** (`docker compose version`).
- **WSL2** (ou Linux). Portas livres: **4200** (Prefect), **9000/9001** (MinIO), **8080**
  (Adminer), **8000** (event-bridge).
- (Opcional) `make` e `python3` — só para regerar amostras; **os samples já vêm versionados**.

---

## 5. Subir o ambiente

```bash
# 1) subir toda a stack (build das imagens locais na 1ª vez)
docker compose up -d

# 2) acompanhar a saúde dos serviços
docker compose ps
```

> As variáveis (credenciais de lab, buckets, work pool) têm **defaults embutidos** no
> `docker-compose.yml` — a stack sobe sem configuração. Para customizar, crie um arquivo `.env`
> na raiz do projeto (lido automaticamente pelo Compose) ou exporte as variáveis no shell.

Na **primeira subida**, os serviços de init criam **automaticamente**: os bancos `prefect` e
`vendas` + o schema Medalhão; os buckets `landing`/`bronze` + a notification; o work pool
`vendas-pool`, o deployment `ingestao-vendas` e a **Automation** de disparo.

**Saúde esperada** (`docker compose ps`): `postgres` e `prefect-server` **healthy**;
`event-bridge` **healthy**; `prefect-worker`, `minio`, `adminer` **running**; `minio-init` e
`prefect-init` **exited (0)** (são one-shot).

### Acessos

| Serviço | URL | Credenciais (lab) |
|---|---|---|
| Prefect UI | http://localhost:4200 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Adminer | http://localhost:8080 | Sistema **PostgreSQL**, Servidor `postgres`, Usuário `puc`, Senha `puc`, Base `vendas` |
| event-bridge (health) | http://localhost:8000/health | — |

---

## 6. Estrutura do repositório

```
docker-compose.yml        # toda a stack (versões pinadas)
Dockerfile                # imagem do worker (Prefect + minio + psycopg2)
requirements.txt          # deps do pipeline
Makefile                  # atalhos: up/down/reset/samples/enviar
flows/       pipeline.py  # flow processar_arquivo_vendas + deploy.py
tasks/       *.py         # tasks da Medalhão (bronze/silver/gold, storage, db, catalogo)
event-bridge/            # FastAPI que emite o evento (app.py, Dockerfile)
automations/             # criar_automation.py (Automation event->deployment)
sql/init/                # schema criado no 1º boot (00-... 01-...)
sql/gold_refresh.sql     # recompute idempotente da Gold
sql/inspecao.sql         # consultas de verificação
samples/                 # 7 CSVs canônicos versionados (fonte dos testes)
scripts/gerar_vendas.py  # gerador determinístico (§ Amostras)
scripts/enviar_arquivo.sh# envia um CSV para o landing (dispara o pipeline)
docs/pitch.md            # roteiro do pitch
```

---

## 7. Enviar arquivos de vendas

Os **7 arquivos canônicos** já estão em `samples/` (nomeados por filial):

| Arquivo | Cenário | Conteúdo |
|---|---|---|
| `VENDAS_SP01_20260723_001.csv` | válido | 20 vendas válidas |
| `VENDAS_SP02_20260723_001.csv` | válido | 20 vendas válidas |
| `VENDAS_RJ01_20260723_001.csv` | válido | 20 vendas válidas |
| `VENDAS_MG01_20260723_001.csv` | válido | 20 vendas válidas |
| `VENDAS_SP01_20260723_002.csv` | inválido | 15 válidas + 5 inválidas (1 por tipo de erro) |
| `VENDAS_SP02_20260723_002.csv` | duplicado | 18 únicas + 2 repetidas (dedup no arquivo) |
| `VENDAS_MG01_20260723_FALHA_001.csv` | falha+retry | 20 válidas; marcador `FALHA` dispara retries |

**Enviar (dispara o pipeline event-driven):**

```bash
./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_001.csv
# ou:  make enviar ARQ=samples/VENDAS_SP01_20260723_001.csv
```

### Gerar arquivos adicionais (opcional)

O gerador é **determinístico** (mesma seed + parâmetros ⇒ arquivo byte-a-byte idêntico). **Não
é pré-requisito** — os samples já estão versionados.

```bash
# regenerar os 7 canônicos (seeds fixas):
make samples
# gerar um lote maior de uma filial:
python3 scripts/gerar_vendas.py --origem RJ01 --cenario valido --linhas 100 --seq 009 --seed 7 --saida samples
```

Convenção de nome: `VENDAS_<ORIGEM>_<AAAAMMDD>[_FALHA]_<seq>.csv`. Cada execução gera **um CSV de
uma única filial**; a data do nome é usada em `data_venda`.

---

## 8. Acompanhar no Prefect

Na **UI (http://localhost:4200)**:

- **Events** → filtre por `vendas.arquivo.recebido` (evento emitido a cada arquivo).
- **Automations** → `disparar-ingestao-vendas` (histórico de disparos).
- **Runs / Deployments** → `processar_arquivo_vendas/ingestao-vendas`; abra um run para ver as
  **tasks**, **logs**, **retries** e o **Artifact** de resumo.

Por linha de comando:

```bash
docker compose exec prefect-worker prefect deployment ls
docker compose exec prefect-worker prefect flow-run ls
docker compose logs -f prefect-worker
```

---

## 9. Validar as camadas

**Consulta consolidada (Bronze-controle, idempotência, rejeitados, rastreabilidade, Gold):**

```bash
docker compose exec -T postgres psql -U puc -d vendas < sql/inspecao.sql
```

**Bronze (objeto bruto imutável no MinIO):**

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mc ls --recursive local/bronze'
```

**Consultas pontuais:**

```bash
# Silver (válidas)
docker compose exec postgres psql -U puc -d vendas -c "SELECT filial, COUNT(*) FROM silver_vendas GROUP BY filial ORDER BY filial;"
# Rejeitados com motivo
docker compose exec postgres psql -U puc -d vendas -c "SELECT motivo, COUNT(*) FROM silver_vendas_rejeitadas GROUP BY motivo ORDER BY motivo;"
# Gold
docker compose exec postgres psql -U puc -d vendas -c "SELECT * FROM gold_vendas_por_filial ORDER BY filial;"
```

---

## 10. Testes (cenários obrigatórios)

> Faça sempre a partir de um ambiente limpo: `docker compose down -v && docker compose up -d`
> (aguarde ~30–60s a inicialização). Envie os arquivos na ordem abaixo.

### Teste 1 — Arquivo válido

- **Objetivo:** provar o fluxo feliz Bronze→Silver→Gold disparado por evento.
- **Preparação:** ambiente no ar (`docker compose ps` saudável).
- **Procedimento:**
  ```bash
  ./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_001.csv
  ./scripts/enviar_arquivo.sh samples/VENDAS_SP02_20260723_001.csv
  ./scripts/enviar_arquivo.sh samples/VENDAS_RJ01_20260723_001.csv
  ./scripts/enviar_arquivo.sh samples/VENDAS_MG01_20260723_001.csv
  ```
- **Resultado esperado:** 4 flow runs **Completed**; **80 vendas** na Silver (20 por filial); 0
  rejeitadas; Gold com 4 filiais.
- **Forma de validação:**
  ```bash
  docker compose exec postgres psql -U puc -d vendas -c "SELECT COUNT(*) FROM silver_vendas;"          # 80
  docker compose exec postgres psql -U puc -d vendas -c "SELECT filial,quantidade_vendas FROM gold_vendas_por_filial ORDER BY filial;"
  ```
- **Evidência de sucesso:** UI mostra 4 runs verdes + Artifacts; `silver=80`.
- **Em caso de falha:** ver `docker compose logs prefect-worker`; conferir se o deployment/automation existem (`prefect deployment ls`).

### Teste 2 — Registros inválidos (carga parcial)

- **Objetivo:** demonstrar separação de válidos e rejeitados com motivo.
- **Preparação:** Teste 1 concluído.
- **Procedimento:** `./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_002.csv`
- **Resultado esperado:** **+15 válidas** e **+5 rejeitadas**, uma para cada motivo:
  `venda_id_origem_ausente`, `data_venda_invalida`, `quantidade_invalida`,
  `valor_unitario_invalido`, `produto_invalido_ou_incompativel`.
- **Forma de validação:**
  ```bash
  docker compose exec postgres psql -U puc -d vendas -c "SELECT motivo,COUNT(*) FROM silver_vendas_rejeitadas GROUP BY motivo ORDER BY motivo;"
  ```
- **Evidência de sucesso:** `bronze_arquivos` mostra o arquivo `20 | 15 | 5`; 5 motivos distintos.
- **Em caso de falha:** verifique a coluna `motivo`/`payload_raw` das rejeitadas.

### Teste 3 — Arquivo/venda duplicados (idempotência)

- **Objetivo:** provar idempotência de arquivo (sha256) e de registro (chave de negócio).
- **Preparação:** Testes 1–2 concluídos.
- **Procedimento:**
  ```bash
  ./scripts/enviar_arquivo.sh samples/VENDAS_SP02_20260723_002.csv      # 18 únicas + 2 repetidas (dedup no arquivo)
  ./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_001.csv      # REENVIO do baseline (mesmo sha256)
  ```
- **Resultado esperado:** o duplicado insere **18 válidas** + **2** `duplicado_no_arquivo`; o
  reenvio **não** cria novo registro em `bronze_arquivos` (status `duplicado`, short-circuit).
  As duas consultas de duplicidade retornam **0 linhas**.
- **Forma de validação:**
  ```bash
  docker compose exec postgres psql -U puc -d vendas -c "SELECT sha256,COUNT(*) FROM bronze_arquivos GROUP BY sha256 HAVING COUNT(*)>1;"                 -- 0 linhas
  docker compose exec postgres psql -U puc -d vendas -c "SELECT origem,venda_id_origem,COUNT(*) FROM silver_vendas GROUP BY 1,2 HAVING COUNT(*)>1;"      -- 0 linhas
  ```
- **Evidência de sucesso:** ambas as consultas **vazias**; `bronze_arquivos` continua com 7 arquivos.
- **Em caso de falha:** havendo duplicidade, revise as constraints (`\d silver_vendas`).

### Teste 4 — Falha com retry (resiliência)

- **Objetivo:** demonstrar retries do Prefect (recurso **de teste**, não de produção).
- **Preparação:** Testes 1–3 concluídos.
- **Procedimento:** `./scripts/enviar_arquivo.sh samples/VENDAS_MG01_20260723_FALHA_001.csv`
- **Resultado esperado:** a task `checkpoint_falha_controlada` **falha nas 2 primeiras
  tentativas e conclui na 3ª** (`run_count = 3`); o flow termina **Completed**; +20 válidas.
- **Forma de validação:** na UI, o run mostra a task com estados
  `AwaitingRetry → Retrying → Completed`. Por CLI:
  ```bash
  docker compose exec prefect-worker prefect flow-run ls   # o run do arquivo FALHA está Completed
  ```
- **Evidência de sucesso:** 3 tentativas na task; flow verde.
- **Em caso de falha:** se esgotar os retries (não deveria), o run fica `Failed` — reenvie o arquivo.

### Estado final consolidado esperado (após os 4 testes)

```bash
docker compose exec -T postgres psql -U puc -d vendas < sql/inspecao.sql
```

| Métrica | Valor esperado |
|---|---|
| `bronze_arquivos` | **7** |
| `silver_vendas` | **133** |
| `silver_vendas_rejeitadas` | **7** (5 validação + 2 duplicado_no_arquivo) |
| duplicidade sha256 / venda | **0 / 0** |
| Gold — faturamento / vendas / ticket | **290366.46 / 133 / 2183.21** |
| Gold por filial (vendas) | SP01 **35**, SP02 **38**, RJ01 **20**, MG01 **40** |

---

## 11. Encerrar, limpar e reiniciar

```bash
docker compose down        # para a stack (mantém dados nos volumes)
docker compose down -v     # para e APAGA volumes (recomeça do zero)
docker compose up -d       # sobe novamente
```

---

## 12. Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| UI do Prefect: "Can't connect to Server API at `0.0.0.0:4200`" | Cache do navegador | `Ctrl+F5`. A API usa `http://localhost:4200/api` (`PREFECT_UI_API_URL`). |
| Arquivo enviado não dispara run | Automation/notification ausente | `docker compose up prefect-init` e `docker compose up minio-init`; confira **Automations** na UI. |
| `prefect-init` falhou na 1ª subida | work pool ainda não existia | Já tratado (o init cria o pool antes). Reexecute `docker compose up prefect-init`. |
| Porta ocupada | 4200/9001/8080/8000 em uso | Libere a porta ou ajuste o mapeamento no `docker-compose.yml`. |
| Nada no `bronze/` | pipeline não rodou | Veja `docker compose logs prefect-worker` e o feed **Events**. |

---

## 13. Decisões técnicas

- **Prefect event-driven (Events + Automations, OSS self-hosted):** o pipeline reage à chegada
  do arquivo; não há scheduler. A ponte `event-bridge` emite o evento e uma **Automation**
  dispara o deployment, repassando `bucket/key/origem` por templating do evento. *(Alternativa
  de contingência disponível: `BRIDGE_TRIGGER_MODE=direct`, em que a ponte chama o deployment
  diretamente — não foi necessária.)*
- **Idempotência em 3 camadas:** `sha256` (arquivo), `UNIQUE(origem, venda_id_origem)` +
  `ON CONFLICT DO NOTHING` (registro), recomputação determinística da Gold. O registro Bronze
  usa `status`; só é considerado duplicado quando `concluido` (resiliente a falha parcial).
- **Bronze imutável no MinIO**, Silver/Gold no PostgreSQL (constraints garantem a integridade).
- **Uma filial por arquivo** (`origem` = filial), refletindo o recebimento real por filial.

---

## 14. Checklist de validação final (requisitos do enunciado)

- [ ] **Dockerizado:** sobe com `docker compose up -d` (§5).
- [ ] **Trigger/event-driven:** arquivo em `landing` gera evento → Automation → run (Testes 1–4).
- [ ] **Resiliência:** retries visíveis no cenário `FALHA` (Teste 4).
- [ ] **Idempotência:** duplicidade de arquivo e de venda = 0 (Teste 3).
- [ ] **Modularidade:** tasks independentes em `tasks/` (Bronze/Silver/Gold).
- [ ] **Persistência:** MinIO (Bronze) + PostgreSQL (Silver/Gold/controle).
- [ ] **Observabilidade:** UI do Prefect (runs, logs, retries, Events, Automations) + Artifacts + Adminer.
- [ ] **Documentação:** este README executável (§1–13).
- [ ] **Pitch em Vídeo:** link preenchido na seção abaixo.

---

## Pitch em Vídeo

O vídeo apresenta o problema, a arquitetura, a demonstração do pipeline em execução e as
principais decisões técnicas do projeto.

**Link do vídeo:** [INSERIR LINK DO PITCH AQUI]

**Duração:** [INFORMAR DURAÇÃO]

**Forma de acesso:** vídeo não listado ou compartilhado com permissão de visualização.
