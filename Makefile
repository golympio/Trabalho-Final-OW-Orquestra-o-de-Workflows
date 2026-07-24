# Atalhos do projeto. Todos os comandos também estão documentados no README.md.
# `make` é opcional: os 4 samples canônicos já vêm versionados em samples/.
.PHONY: help up down reset ps logs samples enviar

PY ?= python3
GEN = $(PY) scripts/gerar_vendas.py --saida samples

help:
	@echo "Alvos: up | down | reset | ps | logs | samples | enviar ARQ=samples/<arquivo>.csv"

up:            ## sobe toda a stack
	docker compose up -d

down:          ## para a stack (mantém volumes)
	docker compose down

reset:         ## para e apaga volumes (recomeça do zero)
	docker compose down -v

ps:
	docker compose ps

logs:
	docker compose logs -f

# Regera os 7 CSVs canônicos com SEEDS FIXAS (reprodutíveis byte-a-byte).
samples:
	$(GEN) --origem SP01 --cenario valido    --seq 001 --seed 101
	$(GEN) --origem SP02 --cenario valido    --seq 001 --seed 102
	$(GEN) --origem RJ01 --cenario valido    --seq 001 --seed 103
	$(GEN) --origem MG01 --cenario valido    --seq 001 --seed 104
	$(GEN) --origem SP01 --cenario invalido  --seq 002 --seed 201
	$(GEN) --origem SP02 --cenario duplicado --seq 002 --seed 202
	$(GEN) --origem MG01 --cenario falha     --seq 001 --seed 301

# Envia um arquivo para o bucket landing (dispara o pipeline). Ex.:
#   make enviar ARQ=samples/VENDAS_SP01_20260723_001.csv
enviar:
	./scripts/enviar_arquivo.sh $(ARQ)
