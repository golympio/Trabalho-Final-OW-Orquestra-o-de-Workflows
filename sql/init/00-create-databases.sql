-- =============================================================================
-- Bootstrap do PostgreSQL — cria o banco analítico `vendas`.
-- O banco do Prefect (`prefect`) é criado pela variável POSTGRES_DB do container.
-- Este script roda uma única vez, na primeira inicialização do volume de dados,
-- via /docker-entrypoint-initdb.d, conectado como POSTGRES_USER em POSTGRES_DB.
-- As tabelas Bronze-controle/Silver/Gold serão criadas no HANDOFF 02.
-- =============================================================================

CREATE DATABASE vendas;
