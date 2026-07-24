-- =============================================================================
-- Schema da arquitetura Medalhão no banco `vendas`.
-- Roda automaticamente na primeira subida (via /docker-entrypoint-initdb.d),
-- após 00-create-databases.sql. O \connect abaixo garante que as tabelas sejam
-- criadas no banco `vendas` (o entrypoint conecta em POSTGRES_DB = prefect).
-- =============================================================================

\connect vendas

-- -----------------------------------------------------------------------------
-- BRONZE (controle) — registro imutável de cada arquivo recebido.
-- Idempotência de ARQUIVO: sha256 UNIQUE (o mesmo conteúdo nunca é reprocessado).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_arquivos (
    file_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_name      TEXT        NOT NULL,
    origem             TEXT        NOT NULL,
    sha256             CHAR(64)    NOT NULL UNIQUE,
    size_bytes         BIGINT      NOT NULL,
    received_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    bronze_uri         TEXT,
    flow_run_id        TEXT,
    status             TEXT        NOT NULL DEFAULT 'recebido'
                       CONSTRAINT ck_bronze_status
                       CHECK (status IN ('recebido','processando','concluido','duplicado','falha')),
    linhas_total       INTEGER,
    linhas_validas     INTEGER,
    linhas_rejeitadas  INTEGER
);

-- -----------------------------------------------------------------------------
-- SILVER — vendas válidas, tipadas e deduplicadas.
-- Idempotência de REGISTRO: UNIQUE (origem, venda_id_origem).
-- Rastreabilidade: file_id -> bronze_arquivos.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_vendas (
    venda_sk         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id          UUID    NOT NULL REFERENCES bronze_arquivos(file_id),
    origem           TEXT    NOT NULL,
    filial           TEXT    NOT NULL,
    categoria        TEXT    NOT NULL,
    produto          TEXT    NOT NULL,
    venda_id_origem  TEXT    NOT NULL,
    data_venda       DATE    NOT NULL,
    quantidade       INTEGER NOT NULL CHECK (quantidade > 0),
    valor_unitario   NUMERIC(12,2) NOT NULL CHECK (valor_unitario > 0),
    valor_total      NUMERIC(14,2) NOT NULL CHECK (valor_total > 0),
    linha_origem     INTEGER NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_silver_vendas_negocio  UNIQUE (origem, venda_id_origem),
    CONSTRAINT ck_silver_valor_total     CHECK (valor_total = quantidade * valor_unitario)
);
CREATE INDEX IF NOT EXISTS ix_silver_vendas_file    ON silver_vendas(file_id);
CREATE INDEX IF NOT EXISTS ix_silver_vendas_filial  ON silver_vendas(filial);

-- -----------------------------------------------------------------------------
-- SILVER (rejeitados) — registros inválidos, com conteúdo original e motivo.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_vendas_rejeitadas (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_id       UUID    NOT NULL REFERENCES bronze_arquivos(file_id),
    linha_origem  INTEGER NOT NULL,
    payload_raw   TEXT    NOT NULL,
    motivo        TEXT    NOT NULL,
    detalhe       TEXT,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rejeitadas_file ON silver_vendas_rejeitadas(file_id);

-- -----------------------------------------------------------------------------
-- GOLD — indicadores consolidados (recomputados de forma idempotente a partir
-- da Silver; ver sql/gold_refresh.sql). Convenções:
--   faturamento       = SUM(valor_total)
--   quantidade_vendas = COUNT(*)  (nº de registros de venda)
--   ticket_medio      = faturamento / quantidade_vendas
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_kpis_gerais (
    id                 INTEGER PRIMARY KEY DEFAULT 1,
    faturamento_total  NUMERIC(16,2) NOT NULL,
    quantidade_vendas  BIGINT        NOT NULL,
    ticket_medio       NUMERIC(14,2) NOT NULL,
    atualizado_em      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT ck_kpis_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS gold_vendas_por_filial (
    filial             TEXT PRIMARY KEY,
    faturamento        NUMERIC(16,2) NOT NULL,
    quantidade_vendas  BIGINT        NOT NULL,
    ticket_medio       NUMERIC(14,2) NOT NULL,
    atualizado_em      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_vendas_por_categoria (
    categoria          TEXT PRIMARY KEY,
    faturamento        NUMERIC(16,2) NOT NULL,
    quantidade_vendas  BIGINT        NOT NULL,
    ticket_medio       NUMERIC(14,2) NOT NULL,
    atualizado_em      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold_vendas_por_produto (
    produto            TEXT PRIMARY KEY,
    categoria          TEXT          NOT NULL,
    faturamento        NUMERIC(16,2) NOT NULL,
    quantidade_vendas  BIGINT        NOT NULL,
    ticket_medio       NUMERIC(14,2) NOT NULL,
    atualizado_em      TIMESTAMPTZ   NOT NULL DEFAULT now()
);
