-- =============================================================================
-- Recomputação IDEMPOTENTE da camada Gold a partir da Silver.
-- Executar no banco `vendas` (ex.: psql -U puc -d vendas -f sql/gold_refresh.sql).
-- Estratégia: KPIs gerais por upsert de linha única (id=1); tabelas por dimensão
-- por full-refresh (TRUNCATE + INSERT). Rodar N vezes produz sempre o mesmo estado.
-- Usada pela task `atualizar_gold` do flow (HANDOFF 03).
-- =============================================================================

BEGIN;

INSERT INTO gold_kpis_gerais (id, faturamento_total, quantidade_vendas, ticket_medio, atualizado_em)
SELECT 1,
       COALESCE(SUM(valor_total), 0),
       COUNT(*),
       CASE WHEN COUNT(*) = 0 THEN 0 ELSE ROUND(SUM(valor_total) / COUNT(*), 2) END,
       now()
FROM silver_vendas
ON CONFLICT (id) DO UPDATE SET
    faturamento_total = EXCLUDED.faturamento_total,
    quantidade_vendas = EXCLUDED.quantidade_vendas,
    ticket_medio      = EXCLUDED.ticket_medio,
    atualizado_em     = EXCLUDED.atualizado_em;

TRUNCATE gold_vendas_por_filial;
INSERT INTO gold_vendas_por_filial (filial, faturamento, quantidade_vendas, ticket_medio)
SELECT filial, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total) / COUNT(*), 2)
FROM silver_vendas
GROUP BY filial;

TRUNCATE gold_vendas_por_categoria;
INSERT INTO gold_vendas_por_categoria (categoria, faturamento, quantidade_vendas, ticket_medio)
SELECT categoria, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total) / COUNT(*), 2)
FROM silver_vendas
GROUP BY categoria;

TRUNCATE gold_vendas_por_produto;
INSERT INTO gold_vendas_por_produto (produto, categoria, faturamento, quantidade_vendas, ticket_medio)
SELECT produto, categoria, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total) / COUNT(*), 2)
FROM silver_vendas
GROUP BY produto, categoria;

COMMIT;
