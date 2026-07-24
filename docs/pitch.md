# Roteiro do Pitch em Vídeo (5–10 min)

Material de apoio para gravar a apresentação. A demonstração ao vivo vale mais que slides —
mostre o pipeline **rodando**. Antes de gravar: `docker compose down -v && docker compose up -d`
e aguarde ~30–60s.

## 0. Abertura (~30s)
- "Pipeline event-driven de vendas em arquitetura Medalhão, orquestrado por Prefect."
- Uma frase de valor: da chegada do arquivo aos indicadores, sem intervenção manual.

## 1. Problema (~1 min)
- Empresa recebe CSVs de vendas de várias filiais (SP01, SP02, RJ01, MG01).
- Processo manual → atraso, duplicidade, erro de formato, sem rastreabilidade.
- Objetivo: automatizar do recebimento aos KPIs, com idempotência e resiliência.

## 2. Arquitetura (~1,5 min)
- Diagrama (README §2): MinIO → notification → **event-bridge (emit_event)** → **Automation** →
  deployment → worker → Bronze/Silver/Gold.
- Decisão-chave: "Escolhi Prefect porque o pipeline é **event-driven** — Events + Automations no
  OSS self-hosted disparam o deployment sem scheduler."
- Medalhão: Bronze (bruto imutável) → Silver (validação/dedup) → Gold (KPIs).

## 3. Demonstração ao vivo (~4–5 min) — o núcleo
1. `docker compose ps` — stack saudável. Abrir as 3 UIs (Prefect 4200, MinIO 9001, Adminer 8080).
2. **Válido:** enviar os 4 baselines
   (`./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_001.csv` etc.).
   - Na UI: **Events** (`vendas.arquivo.recebido`) → **Automations** (disparo) → **Run** verde
     com tasks Bronze/Silver/Gold e o **Artifact** de resumo.
   - Mostrar `silver=80` e a Gold por filial.
3. **Inválido:** enviar `VENDAS_SP01_20260723_002.csv` → mostrar os **5 motivos** de rejeição e
   que as 15 válidas entraram (carga parcial).
4. **Duplicado:** enviar `VENDAS_SP02_20260723_002.csv` e **reenviar** um baseline → mostrar as
   duas consultas de duplicidade retornando **0** (idempotência de venda e de arquivo).
5. **Falha/retry:** enviar `VENDAS_MG01_20260723_FALHA_001.csv` → na UI, a task
   `checkpoint_falha_controlada` em **Retrying** 2× e depois **Completed**. Frisar: "recurso de
   teste para demonstrar resiliência".
6. Fechar com `sql/inspecao.sql`: KPIs finais (**290366.46 / 133 / 2183.21**).

## 4. Decisões técnicas (~1 min)
- Idempotência em 3 camadas (sha256 / chave de negócio / recompute da Gold).
- Rastreabilidade por `file_id` (Bronze → Silver → rejeitados) + Artifact por run.
- Reprodutibilidade: gerador determinístico e samples versionados; versões pinadas.

## 5. Encerramento (~30s)
- Requisitos do enunciado atendidos (checklist do README §14).
- Como rodar do zero: `docker compose up -d` + README.

---

### Checklist antes de publicar o vídeo
- [ ] Duração entre 5 e 10 min.
- [ ] Contém: problema, arquitetura, demonstração funcional, decisões técnicas.
- [ ] Link (não listado / com permissão) preenchido no README, seção **Pitch em Vídeo**.
- [ ] O link abre sem pedir permissão adicional.
