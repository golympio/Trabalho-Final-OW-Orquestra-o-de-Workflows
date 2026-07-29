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
Filial envia CSV ▶ MinIO (bucket landing)
                      │ s3:ObjectCreated (notification/webhook)
                      ▼
                 event-bridge (FastAPI)  ▶ emit_event("vendas.arquivo.recebido")
                      │
                      ▼
                 Prefect Event ▶ Automation ▶ Deployment "ingestao-vendas"
                      │
                      ▼
                 Worker executa o flow  ▶  Bronze ▶ Silver ▶ Gold
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

**Saúde esperada** (`docker compose ps`): `postgres`, `prefect-server` e `event-bridge` com
**(healthy)**; `minio`, `prefect-worker` e `adminer` **Up** (running). Os containers de
inicialização **`minio-init`** e **`prefect-init`** rodam uma vez e **saem (Exited 0)** — por
isso **não aparecem** no `docker compose ps` comum; para vê-los (e confirmar que terminaram sem
erro), use:

```bash
docker compose ps -a          # mostra também os containers já encerrados (init)
```

### Acessos

| Serviço | URL | Credenciais (lab) |
|---|---|---|
| Prefect UI | http://localhost:4200 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Adminer | http://localhost:8080 | Sistema **PostgreSQL**, Servidor `postgres`, Usuário `puc`, Senha `puc`, Base `vendas` |

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

**Parâmetros:**

| Parâmetro | O que controla | Exemplo |
|---|---|---|
| `--origem` | a filial (uma por arquivo) | `SP01`, `SP02`, `RJ01`, `MG01` |
| `--cenario` | o tipo de arquivo | `valido`, `invalido`, `duplicado`, `falha` |
| `--linhas` | quantas vendas gerar | `100` |
| `--seq` | o sequencial (3 dígitos) no nome | `009` |
| `--data` | a data do lote (AAAAMMDD) | `20260723` (padrão) |
| `--seed` | a semente (reprodutibilidade: mesma seed ⇒ mesmo arquivo) | `7` |
| `--saida` | pasta onde salvar | `samples` |

Convenção de nome: `VENDAS_<ORIGEM>_<AAAAMMDD>[_FALHA]_<seq>.csv`. Cada execução gera **um CSV de
uma única filial**; a data do nome é usada em `data_venda`.

> **Importante:** gerar um arquivo **não** o processa — apenas o cria em `samples/`. Para ele
> entrar no pipeline (e aparecer no banco/Adminer), **envie-o** com
> `./scripts/enviar_arquivo.sh samples/<arquivo>.csv` (§7).

---

## 8. Acompanhar no Prefect

Na **UI do Prefect** (http://localhost:4200):

- **Event Feed** → filtre por `vendas.arquivo.recebido` (um evento emitido a cada arquivo; o
  campo **Resource** mostra o objeto de origem, ex.: `minio.object.landing/VENDAS_SP01_...csv`).
  **Expanda o evento** (seta ▾) para ver o **payload** com `bucket`, `key`, `origem` e
  `object_id` — são justamente os dados que a Automation repassa ao flow.
- **Automations** → `disparar-ingestao-vendas` (ativa): **Trigger** = evento customizado,
  **Action** = *Run deployment* `ingestao-vendas`. Clique em **Show parameters** para ver os
  parâmetros templados do evento (`{{ event.payload.bucket }}`, `key`, `origem`). A evidência de
  que ela **disparou** está em **Runs** (o run criado) e no **Event Feed**.
- **Runs** → aba **Flow runs**: cada arquivo enviado vira **um flow run** de
  `processar_arquivo_vendas`, **nomeado pelo arquivo** (ex.: `ingestao-VENDAS_SP01_20260723_001.csv`),
  com **3 Parameters** = bucket/key/origem e **6 Task runs**.
  **Abra o flow run** para ver, no mesmo lugar, as **tasks** (Bronze→Silver→Gold), os **logs**
  (que narram cada etapa: `Arquivo registrado…`, `Bronze preservado…`, `Silver: N válidas, M
  rejeitadas`, `Gold recomputada`, `Ingestão concluída`), os **retries** e o **Artifact** de
  resumo. *(A aba **Task runs** é uma lista solta das tasks de todos os runs — secundária.)*
- **Deployments** → `ingestao-vendas`: a configuração do deployment disparado pela Automation.

Por linha de comando:

```bash
docker compose exec prefect-worker prefect deployment ls
docker compose exec prefect-worker prefect flow-run ls
docker compose logs -f prefect-worker
```

---

## 9. Validar as camadas

As verificações abaixo estão por **linha de comando**, mas **todas têm equivalente na interface
web** — use o que preferir.

**Como consultar no Adminer (banco):**
1. Abra **http://localhost:8080** e faça login: Sistema **PostgreSQL**, Servidor `postgres`,
   Usuário `puc`, Senha `puc`, Base de dados `vendas`.
2. No menu à **esquerda** (canto superior, abaixo do logo/seletores): clique em
   **`selecionar <tabela>`** (ex.: `bronze_arquivos`, `silver_vendas`, `gold_vendas_por_filial`)
   para **ver os dados**; ou em **`Comando SQL`** (link no topo, ao lado de *Importar/Exportar*)
   para **rodar uma consulta** — abre uma caixa de texto; cole a consulta e clique em **Executar**.
3. **Importante — cole só o SQL:** no **Comando SQL** cole **apenas o SQL** (o texto **dentro das
   aspas** do `-c "..."` mostrado nos exemplos). **Não** cole o `docker compose exec … psql …`
   nem linhas `#` — isso é comando de **terminal**, e o Adminer só entende SQL. Ex.: em vez de
   `docker compose exec postgres psql -U puc -d vendas -c "SELECT * FROM gold_kpis_gerais;"`,
   cole apenas `SELECT * FROM gold_kpis_gerais;`.
4. **Dica:** a tabela pode ser larga — **role na horizontal** para ver todas as colunas (ex.: em
   `bronze_arquivos`, `original_name` fica mais à esquerda e `linhas_total/validas/rejeitadas`
   mais à direita).

**Como ver os buckets no MinIO (arquivos):** abra **http://localhost:9001**
(`minioadmin`/`minioadmin`) e navegue pelos buckets `landing` (arquivos recebidos) e `bronze`
(brutos preservados).

**Consulta consolidada (Bronze-controle, idempotência, rejeitados, rastreabilidade, Gold):**

```bash
docker compose exec -T postgres psql -U puc -d vendas < sql/inspecao.sql
```
> **Adminer (web):** abra **Comando SQL**, cole o conteúdo de `sql/inspecao.sql` e clique em
> **Executar**.

**Bronze (objeto bruto imutável no MinIO):**

```bash
docker compose run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null; mc ls --recursive local/bronze'
```
> **MinIO Console (web):** abra o bucket **`bronze`** e navegue por `ingest_date=…/origem=…/`. O
> **preview** de CSV fica indisponível — use **Download** para ver o conteúdo.

**Consultas pontuais:**

```bash
# Silver (válidas)
docker compose exec postgres psql -U puc -d vendas -c "SELECT filial, COUNT(*) FROM silver_vendas GROUP BY filial ORDER BY filial;"
# Rejeitados com motivo
docker compose exec postgres psql -U puc -d vendas -c "SELECT motivo, COUNT(*) FROM silver_vendas_rejeitadas GROUP BY motivo ORDER BY motivo;"
# Gold
docker compose exec postgres psql -U puc -d vendas -c "SELECT * FROM gold_vendas_por_filial ORDER BY filial;"
```
> **Adminer (web):** clique em **selecionar `silver_vendas`**, **`silver_vendas_rejeitadas`** e
> **`gold_vendas_por_filial`** para navegar os dados; ou rode as mesmas queries em **Comando SQL**.

---

## 10. Testes (cenários obrigatórios)

> Faça sempre a partir de um ambiente limpo: `docker compose down -v && docker compose up -d`
> (aguarde ~30–60s a inicialização). Envie os arquivos na ordem abaixo.
>
> As validações abaixo estão em CLI, mas cada consulta ao banco pode ser feita também no
> **Adminer** (http://localhost:8080 → **Comando SQL** ou **selecionar `<tabela>`**) e as
> checagens de bucket no **MinIO Console** (http://localhost:9001). Ver §9.

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
- **Evidência de sucesso:**
  - **Prefect UI → Runs (aba Flow runs):** 4 runs **verdes (Completed)**, um por arquivo (com
    Artifacts). *(Esta tela mostra os 4 **runs** — não a contagem de vendas.)*
  - **Contagem de 80 vendas:** é a quantidade de linhas na **Silver** (banco), não aparece na
    tela de Runs. Veja pelo `psql` acima (`SELECT COUNT(*) FROM silver_vendas` → 80) **ou** no
    **Adminer** → `selecionar silver_vendas` (lista as 80 linhas).
  - **MinIO Console** (bucket `bronze`): os 4 objetos brutos preservados.
- **Em caso de falha:** ver `docker compose logs prefect-worker` e conferir se o deployment e a
  automation existem (devem listar `ingestao-vendas` e `disparar-ingestao-vendas`):
  ```bash
  docker compose exec prefect-worker prefect deployment ls
  docker compose exec prefect-worker prefect automation ls
  ```
  Se algum faltar, recrie com `docker compose up prefect-init`.

### Teste 2 — Registros inválidos (carga parcial)

- **Objetivo:** demonstrar separação de válidos e rejeitados com motivo.
- **Preparação:** Teste 1 concluído.
- **Procedimento:** `./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_002.csv`
- **Resultado esperado:** **+15 válidas** e **+5 rejeitadas**, uma para cada motivo:
  `venda_id_origem_ausente`, `data_venda_invalida`, `quantidade_invalida`,
  `valor_unitario_invalido`, `produto_invalido_ou_incompativel`.
- **Forma de validação (no Prefect — UI e CLI):** o run do arquivo inválido **conclui
  normalmente** (6 tasks) e as **contagens da carga parcial** aparecem nos logs e no artifact.
  - **UI do Prefect → Runs:** abra o run do `_002` → os **logs** mostram
    `Silver: 15 válidas, 5 rejeitadas.` e `Ingestão concluída: {'validas': 15, 'rejeitadas': 5, 'total': 20}`;
    o **Artifact `resumo-ingestao`** também traz válidas/rejeitadas.
  - **CLI:**
    ```bash
    docker compose exec prefect-worker prefect flow-run ls           # copie o ID do run do arquivo _002
    docker compose exec prefect-worker prefect flow-run logs <ID>    # 'Silver: 15 válidas, 5 rejeitadas'
    ```
- **Forma de validação (no banco — os motivos):** o Prefect mostra **quantas** foram rejeitadas;
  o banco mostra **por quê** (o motivo e o conteúdo original de cada uma):
  ```bash
  docker compose exec postgres psql -U puc -d vendas -c "SELECT motivo,COUNT(*) FROM silver_vendas_rejeitadas GROUP BY motivo ORDER BY motivo;"
  ```
  > **Adminer (web):** `selecionar silver_vendas_rejeitadas` mostra as 5 linhas com `motivo`,
  > `detalhe` e `payload_raw` (o conteúdo original de cada linha rejeitada).

  A consulta acima retorna **5 motivos distintos** (1 linha cada) em `silver_vendas_rejeitadas`.
- **Em caso de falha:** verifique a coluna `motivo`/`payload_raw` das rejeitadas.
- **Evidência de sucesso:** o registro de controle confirma a **carga parcial** do arquivo —
  no **Adminer** (§9), **`selecionar bronze_arquivos`**: a coluna **`original_name`** identifica a
  linha de `VENDAS_SP01_20260723_002.csv`, e as colunas
  **`linhas_total / linhas_validas / linhas_rejeitadas`** mostram **20 / 15 / 5** (role na
  horizontal para vê-las). Por CLI:
  ```bash
  docker compose exec postgres psql -U puc -d vendas -c "SELECT original_name, linhas_total, linhas_validas, linhas_rejeitadas FROM bronze_arquivos WHERE original_name = 'VENDAS_SP01_20260723_002.csv';"
  ```

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
- **Forma de validação (no Prefect — UI e CLI):** o reenvio **cria um novo flow run** (o evento
  disparou de novo), mas ele **conclui com apenas 1 task** (`registrar_arquivo`) em vez das **6**
  de um run normal — o pipeline detecta o sha256 já processado e **para** (short-circuit).
  - **UI do Prefect → Runs:** abra o run do reenvio → **1 task** e o log
    *"Arquivo duplicado (idempotência de arquivo): nada a reprocessar."*
  - **CLI:**
    ```bash
    # lista os runs; o reenvio aparece Completed (copie o ID dele)
    docker compose exec prefect-worker prefect flow-run ls
    # mostra os logs do run (inclui 'Arquivo duplicado ... nada a reprocessar')
    docker compose exec prefect-worker prefect flow-run logs <ID-DO-RUN>
    ```
- **Forma de validação (no banco — complementar):** o Prefect mostra que o pipeline **não
  reprocessou**; o banco confirma que **não há linhas duplicadas** nas tabelas:
  ```bash
  # ambas devem retornar 0 linhas (nenhuma duplicidade)
  docker compose exec postgres psql -U puc -d vendas -c "SELECT sha256,COUNT(*) FROM bronze_arquivos GROUP BY sha256 HAVING COUNT(*)>1;"
  docker compose exec postgres psql -U puc -d vendas -c "SELECT origem,venda_id_origem,COUNT(*) FROM silver_vendas GROUP BY 1,2 HAVING COUNT(*)>1;"
  ```
  > **Adminer (web):** cole as duas consultas em **Comando SQL** (ambas devem voltar vazias). Em
  > `selecionar bronze_arquivos`, o reenviado aparece com `status = duplicado`.
- **Evidência de sucesso:** no Prefect, o run do reenvio tem **1 task** (vs 6) e log de duplicado;
  no banco, ambas as consultas ficam **vazias** e `bronze_arquivos` continua com 7 arquivos.
- **Em caso de falha:** havendo duplicidade, revise as constraints (`\d silver_vendas`).

### Teste 4 — Falha com retry (resiliência)

- **Objetivo:** demonstrar retries do Prefect (recurso **de teste**, não de produção).
- **Preparação:** Testes 1–3 concluídos.
- **Procedimento:** `./scripts/enviar_arquivo.sh samples/VENDAS_MG01_20260723_FALHA_001.csv`
- **Resultado esperado:** a task `checkpoint_falha_controlada` **falha nas 2 primeiras
  tentativas e conclui na 3ª** (`run_count = 3`); o flow termina **Completed**; +20 válidas.
- **Forma de validação:** na **UI do Prefect** (Runs → abra o flow run do arquivo `FALHA`), a
  task `checkpoint_falha_controlada` mostra os estados `AwaitingRetry → Retrying → Completed`.
  Por CLI:
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
| Arquivo enviado não dispara run | Automation/notification ausente | `docker compose up prefect-init` e `docker compose up minio-init`; confira **Automations** na UI do Prefect. |
| `prefect-init` falhou na 1ª subida | work pool ainda não existia | Já tratado (o init cria o pool antes). Reexecute `docker compose up prefect-init`. |
| Porta ocupada | 4200/9001/8080/8000 em uso | Libere a porta ou ajuste o mapeamento no `docker-compose.yml`. |
| Nada no `bronze/` | pipeline não rodou | Veja `docker compose logs prefect-worker` e o **Event Feed** da UI do Prefect. |
| Conferir se a ponte MinIO→Prefect está viva | — | Acesse `http://localhost:8000/health` — deve responder `{"status":"ok","mode":"automation"}`. (O MinIO chama a ponte internamente; essa URL é só diagnóstico.) |

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
- [ ] **Observabilidade:** UI do Prefect (Runs, logs, retries, Event Feed, Automations, Artifacts) + Console do MinIO + Adminer.
- [ ] **Documentação:** este README reproduz o projeto do zero, do §1 ao §13.
- [ ] **Pitch em Vídeo:** link preenchido na seção abaixo.

---

## Pitch em Vídeo

O vídeo apresenta o problema, a arquitetura, a demonstração do pipeline em execução e as
principais decisões técnicas do projeto.

**Link do vídeo:** [INSERIR LINK DO PITCH AQUI]

**Duração:** [INFORMAR DURAÇÃO]

**Forma de acesso:** vídeo não listado ou compartilhado com permissão de visualização.
