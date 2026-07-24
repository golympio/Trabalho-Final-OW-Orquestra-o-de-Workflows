"""Cria (idempotente) a Automation que dispara o deployment `ingestao-vendas`
quando o event-bridge emite o evento `vendas.arquivo.recebido`.

Os parâmetros do flow (bucket/key/origem) são preenchidos a partir do payload do
evento via templating Jinja (GATE §3.3.1 do PRD).
"""
import asyncio
import os

from prefect import get_client
from prefect.automations import Automation
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.events.actions import RunDeployment

EVENTO = os.getenv("EVENTO_VENDAS", "vendas.arquivo.recebido")
DEPLOYMENT_NAME = "processar_arquivo_vendas/ingestao-vendas"
AUTOMATION_NAME = "disparar-ingestao-vendas"


async def main():
    async with get_client() as client:
        dep = await client.read_deployment_by_name(DEPLOYMENT_NAME)

        # Idempotência: remove automations homônimas antes de recriar.
        for a in await client.read_automations():
            if a.name == AUTOMATION_NAME:
                await client.delete_automation(a.id)

        trigger = EventTrigger(
            expect={EVENTO},
            posture=Posture.Reactive,
            threshold=1,
            within=0,
        )
        action = RunDeployment(
            source="selected",
            deployment_id=dep.id,
            parameters={
                "bucket": "{{ event.payload.bucket }}",
                "key": "{{ event.payload.key }}",
                "origem": "{{ event.payload.origem }}",
            },
        )
        automation = Automation(name=AUTOMATION_NAME, trigger=trigger, actions=[action])
        created = await automation.acreate()
        print(f"Automation '{AUTOMATION_NAME}' criada (id={created.id}) -> {DEPLOYMENT_NAME}")


if __name__ == "__main__":
    asyncio.run(main())
