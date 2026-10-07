import hashlib
import hmac
import json

import httpx
import pytest

from app.consumer.webhook import HttpWebhookSender, WebhookDeliveryError


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def test_sends_signed_json():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["headers"] = request.headers
        seen["body"] = request.content
        return httpx.Response(200)

    payload = {"event": "payment.processed", "payment": {"payment_id": "1"}}
    await HttpWebhookSender(make_client(handler), secret="s3cret").send("http://client.test/hook", payload)

    assert json.loads(seen["body"]) == payload
    assert seen["headers"]["content-type"] == "application/json"
    assert seen["headers"]["x-webhook-event"] == "payment.processed"
    expected = "sha256=" + hmac.new(b"s3cret", seen["body"], hashlib.sha256).hexdigest()
    assert seen["headers"]["x-webhook-signature"] == expected


async def test_no_signature_without_secret():
    def handler(request: httpx.Request) -> httpx.Response:
        assert "x-webhook-signature" not in request.headers
        return httpx.Response(204)

    await HttpWebhookSender(make_client(handler)).send("http://client.test/hook", {"event": "e"})


@pytest.mark.parametrize("status", [400, 404, 500, 503])
async def test_non_2xx_is_delivery_error(status):
    sender = HttpWebhookSender(make_client(lambda r: httpx.Response(status)))
    with pytest.raises(WebhookDeliveryError, match=str(status)):
        await sender.send("http://client.test/hook", {"event": "e"})


async def test_transport_error_is_delivery_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    with pytest.raises(WebhookDeliveryError, match="ConnectError"):
        await HttpWebhookSender(make_client(handler)).send("http://client.test/hook", {"event": "e"})
