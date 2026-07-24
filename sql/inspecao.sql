-- =============================================================================
-- Consultas de inspeção e verificação de idempotência (banco `vendas`).
-- Uso: psql -U puc -d vendas -f sql/inspecao.sql
-- =============================================================================

\echo '== Arquivos recebidos (Bronze-controle) =='
SELECT origem, original_name, LEFT(sha256, 12) AS sha256_ini,
       status, linhas_total, linhas_validas, linhas_rejeitadas, received_at
FROM bronze_arquivos
ORDER BY received_at DESC;

\echo '== Idempotência de ARQUIVO: duplicidade de sha256 (esperado: 0 linhas) =='
SELECT sha256, COUNT(*) AS total
FROM bronze_arquivos
GROUP BY sha256
HAVING COUNT(*) > 1;

\echo '== Idempotência de REGISTRO: duplicidade de venda (esperado: 0 linhas) =='
SELECT origem, venda_id_origem, COUNT(*) AS total
FROM silver_vendas
GROUP BY origem, venda_id_origem
HAVING COUNT(*) > 1;

\echo '== Rejeitados por motivo =='
SELECT motivo, COUNT(*) AS total
FROM silver_vendas_rejeitadas
GROUP BY motivo
ORDER BY total DESC;

\echo '== Rastreabilidade: arquivo -> validas/rejeitadas =='
SELECT b.origem, b.original_name,
       COUNT(DISTINCT sv.venda_sk)              AS validas,
       COUNT(DISTINCT r.id)                     AS rejeitadas
FROM bronze_arquivos b
LEFT JOIN silver_vendas sv           ON sv.file_id = b.file_id
LEFT JOIN silver_vendas_rejeitadas r ON r.file_id = b.file_id
GROUP BY b.file_id, b.origem, b.original_name
ORDER BY b.origem;

\echo '== Gold: KPIs gerais =='
SELECT * FROM gold_kpis_gerais;

\echo '== Gold: por filial =='
SELECT * FROM gold_vendas_por_filial ORDER BY faturamento DESC;

\echo '== Gold: por categoria =='
SELECT * FROM gold_vendas_por_categoria ORDER BY faturamento DESC;

\echo '== Gold: por produto =='
SELECT * FROM gold_vendas_por_produto ORDER BY faturamento DESC;
