# payment-service

Сервис асинхронной обработки платежей. API принимает платёж и отвечает 202, дальше платёж
уходит через RabbitMQ в consumer, который прогоняет его через (эмулированный) платёжный шлюз
и дёргает webhook клиента.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ + FastStream, Alembic, docker compose.

## Запуск

```
docker compose up --build -d
docker compose logs -f api outbox-relay consumer webhook-receiver
```

Поднимается postgres, rabbitmq, миграции (`migrate`, одноразовый контейнер), `api` на :8000
(swagger на /docs), `outbox-relay`, `consumer` и `webhook-receiver` на :9000 — простой
приёмник вебхуков, чтобы было куда слать и где посмотреть результат.

RabbitMQ management: http://localhost:15672 (guest/guest).

Все настройки через env, дефолты зашиты в compose. Список — в `.env.example`.
API-ключ по умолчанию `secret-api-key`.

Остановить: `docker compose down`, вместе с данными — `docker compose down -v`.

## API

Везде нужен заголовок `X-API-Key`, иначе 401.

### POST /api/v1/payments

Заголовок `Idempotency-Key` обязателен.

```
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{"amount": "1500.00", "currency": "RUB", "description": "Order #1001",
       "metadata": {"order_id": 1001}, "webhook_url": "http://webhook-receiver:9000/webhook"}'
```

Ответ 202:

```
{"payment_id": "6f0d4d1e-...", "status": "pending", "created_at": "2026-10-07T10:00:00.123456Z"}
```

Повтор с тем же ключом и тем же телом вернёт тот же ответ (плюс заголовок `Idempotent-Replayed: true`),
второй платёж не создастся. Тот же ключ с другим телом — 409.

### GET /api/v1/payments/{payment_id}

```
curl http://localhost:8000/api/v1/payments/<id> -H "X-API-Key: secret-api-key"
```

```
{
  "payment_id": "6f0d4d1e-...",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Order #1001",
  "metadata": {"order_id": 1001},
  "status": "succeeded",
  "idempotency_key": "order-1001",
  "webhook_url": "http://webhook-receiver:9000/webhook",
  "failure_reason": null,
  "last_error": null,
  "created_at": "...",
  "updated_at": "...",
  "processed_at": "...",
  "webhook_delivered_at": "..."
}
```

Статусы: `pending` → `succeeded` / `failed`. Для `failed` в `failure_reason` причина отказа шлюза.
`last_error` — последняя техническая ошибка consumer'а (например, не доставился webhook), чисто для диагностики.

### Webhook

Через 2–5 секунд на `webhook_url` прилетает POST:

```
{
  "event": "payment.processed",
  "payment": {
    "payment_id": "...", "status": "succeeded", "amount": "1500.00", "currency": "RUB",
    "description": "Order #1001", "metadata": {"order_id": 1001},
    "failure_reason": null, "created_at": "...", "processed_at": "..."
  }
}
```

Заголовок `X-Webhook-Event: payment.processed`. Если задан `WEBHOOK_SECRET`, добавляется
`X-Webhook-Signature: sha256=<hmac тела>`. Доставленным считается любой 2xx.

Что пришло в демо-приёмник: `curl http://localhost:9000/webhooks`.

### Посмотреть retry и DLQ

У приёмника есть ручка `/webhook/fail`, которая всегда отвечает 500:

```
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" -H "Idempotency-Key: dlq-test" -H "Content-Type: application/json" \
  -d '{"amount": "10.00", "currency": "USD", "webhook_url": "http://webhook-receiver:9000/webhook/fail"}'
```

В логах consumer'а будет три попытки (пауза 2с, потом 4с), после чего сообщение окажется
в очереди `payments.dlq`. Платёж при этом уже в финальном статусе (шлюз вызывался один раз),
`webhook_delivered_at` пустой, в `last_error` — `HTTP 500 from ...`.

## Как это устроено

Поток: `api` → таблицы `payments` + `outbox` (одна транзакция) → `outbox-relay` → RabbitMQ
exchange `payments` → очередь `payments.new` → `consumer`.

**Outbox.** API в RabbitMQ не ходит вообще. Платёж и событие пишутся в одной транзакции,
relay раз в полсекунды забирает неопубликованные строки (`FOR UPDATE SKIP LOCKED`, так что
реплик relay может быть несколько), публикует с publisher confirms и только после
подтверждения брокера проставляет `published_at`. Получается at-least-once: если relay упадёт
между publish и commit, событие уйдёт второй раз. Это нормально, consumer к дублям устойчив.

**Идемпотентность API.** Уникальный индекс по `idempotency_key` — он же защита от гонки двух
одновременных запросов: кто проиграл, ловит IntegrityError и возвращает уже созданный платёж.
Рядом хранится sha256 от канонического тела запроса, чтобы отличать повтор от попытки
переиспользовать ключ с другими данными.

**Идемпотентность consumer'а.** Обработка разбита на шаги, каждый проверяет состояние в БД:

1. если `status = pending` — зовём шлюз и переводим в `succeeded`/`failed` через
   `UPDATE ... WHERE status = 'pending'` (compare-and-set, второй раз не перезапишет);
2. если `webhook_delivered_at IS NULL` — шлём webhook и проставляем дату.

Поэтому повторная доставка сообщения (например, после упавшего вебхука) не вызовет шлюз
повторно, а просто дошлёт webhook. Транзакции короткие, на время сетевых вызовов ничего не
блокируется. Реальному шлюзу передавался бы `payment_id` как idempotency key.

**Retry.** Без `sleep` в обработчике — через очереди RabbitMQ. При ошибке consumer кладёт копию
сообщения в `payments.retry.N` (N — номер упавшей попытки, в заголовке `x-attempt`) и ack'ает
оригинал. У `payments.retry.N` стоит `x-message-ttl = RETRY_BASE_DELAY * RETRY_BACKOFF_FACTOR^(N-1)`
и `x-dead-letter-exchange = payments`, так что по истечении TTL брокер сам возвращает сообщение в
`payments.new`. Очередь на каждую попытку, а не одна с per-message TTL, потому что RabbitMQ
выкидывает протухшие сообщения только из головы очереди — с разными TTL в одной очереди
короткие ждали бы длинных. По умолчанию 3 попытки: исходная + 2 повтора через 2с и 4с.

**DLQ.** У `payments.new` задан `x-dead-letter-exchange = payments.dlx`. Когда попытки кончились
или ошибка неустранимая (платежа нет в БД), consumer делает reject без requeue, и сообщение
уезжает в `payments.dlq` с заголовками `x-death`, `x-attempt`, `x-last-error`. Дальше её
разбирать руками или отдельной тулзой, автоматически оттуда ничего не читается.

**Авторизация.** Статический ключ в `X-API-Key`, сравнение через `secrets.compare_digest`,
зависимость навешана на роутер целиком. `/health` без ключа, он для healthcheck'ов.

Топология RabbitMQ описана один раз в `app/messaging/topology.py` и объявляется при старте и
relay, и consumer'ом — declare идемпотентный. Если менять `MAX_ATTEMPTS` или задержки после того,
как очереди уже созданы, их нужно удалить (`docker compose down -v`), иначе RabbitMQ
ответит PRECONDITION_FAILED.

## Настройки

| Переменная | Дефолт | |
|---|---|---|
| `API_KEY` | `secret-api-key` | ключ для `X-API-Key` |
| `WEBHOOK_SECRET` | пусто | если задан, вебхуки подписываются |
| `DATABASE_URL` | postgres в compose | `postgresql+asyncpg://...` |
| `RABBITMQ_URL` | rabbitmq в compose | |
| `MAX_ATTEMPTS` | 3 | попыток до DLQ |
| `RETRY_BASE_DELAY` | 2 | секунд до первого повтора |
| `RETRY_BACKOFF_FACTOR` | 2 | |
| `GATEWAY_MIN_DELAY` / `GATEWAY_MAX_DELAY` | 2 / 5 | задержка эмуляции шлюза |
| `GATEWAY_SUCCESS_RATE` | 0.9 | |
| `WEBHOOK_TIMEOUT` | 5 | |
| `CONSUMER_PREFETCH` | 10 | |
| `OUTBOX_POLL_INTERVAL` | 0.5 | |

## Структура

```
app/
  main.py               FastAPI app, /health
  config.py             настройки
  db.py                 engine / session factory
  models.py             Payment, OutboxEvent
  schemas.py            схемы API
  api/                  auth (X-API-Key), эндпоинты
  services/payments.py  создание платежа + outbox, идемпотентность
  messaging/            топология RabbitMQ, RetryPolicy, контракт события
  outbox/relay.py       outbox -> RabbitMQ
  consumer/
    app.py              FastStream app, обработчик, retry/DLQ
    processor.py        шаги обработки платежа
    repository.py       запросы к БД для consumer'а
    gateway.py          эмуляция шлюза
    webhook.py          отправка вебхуков
alembic/                миграции
tools/webhook_receiver.py
tests/
```

## Тесты

```
pip install -r requirements-dev.txt
pytest
ruff check . && ruff format --check .
```

Внешние сервисы для тестов не нужны: API тестируется через ASGI-транспорт httpx с подменённым
сервисным слоем, логика consumer'а — на in-memory репозитории, отправка вебхуков — через
`httpx.MockTransport`.

Запуск без docker (нужны свои postgres и rabbitmq, см. `.env.example`):

```
alembic upgrade head
uvicorn app.main:app --reload
python -m app.outbox.relay
faststream run app.consumer.app:app
```
