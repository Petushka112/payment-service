"""Tiny webhook receiver for local demos (``docker compose`` service ``webhook-receiver``).

* ``POST /webhook``      -> 200, stores and logs the payload
* ``POST /webhook/fail`` -> 500 always (to demonstrate retries and the DLQ)
* ``GET  /webhooks``     -> everything received so far
"""

import logging
from typing import Any

from fastapi import FastAPI, Request, Response

logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)-7s [%(name)s] %(message)s")
log = logging.getLogger("webhook-receiver")

app = FastAPI(title="Webhook receiver (demo)")
received: list[dict[str, Any]] = []


@app.post("/webhook")
async def webhook(request: Request) -> dict[str, str]:
    body = await request.json()
    received.append({"headers": dict(request.headers), "body": body})
    payment = body.get("payment", {})
    log.info("Webhook received: payment %s status=%s", payment.get("payment_id"), payment.get("status"))
    return {"status": "ok"}


@app.post("/webhook/fail")
async def webhook_fail(request: Request) -> Response:
    body = await request.json()
    log.warning(
        "Webhook received on failing endpoint: %s -> answering 500", body.get("payment", {}).get("payment_id")
    )
    return Response(status_code=500, content="simulated failure")


@app.get("/webhooks")
async def list_webhooks() -> list[dict[str, Any]]:
    return received
