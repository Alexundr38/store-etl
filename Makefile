.PHONY: up-backend up-airflow down-backend down-airflow

up-backend:
	docker compose -f backend/docker-compose.yml --env-file .env up --build -d

up-airflow:
	docker compose -f airflow/docker-compose.yml --env-file .env up --build -d

down-backend:
	docker compose -f backend/docker-compose.yml --env-file .env down

down-airflow:
	docker compose -f airflow/docker-compose.yml --env-file .env down