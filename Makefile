.PHONY: up down logs ps topics clean

UP:
	docker compose up -d
	@echo "waiting for services to report healthy..."
	@docker compose ps

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

topics:
	docker exec wagerflow-kafka kafka-topics --bootstrap-server localhost:9092 --list

clean:
	docker compose down -v
	@echo "removed all containers and volumes - full reset."