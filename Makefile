.PHONY: up down logs ps test lint fmt migrate

up:            ## Build and start the whole stack
	docker compose up --build -d

down:          ## Stop the stack (keep data)
	docker compose down

clean:         ## Stop the stack and drop volumes (DB + RabbitMQ state)
	docker compose down -v

logs:          ## Follow logs of the application services
	docker compose logs -f api outbox-relay consumer webhook-receiver

ps:
	docker compose ps

test:          ## Run unit tests
	pytest -q

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

migrate:       ## Apply migrations against DATABASE_URL (local run)
	alembic upgrade head
