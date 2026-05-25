.PHONY: up-backend up-airflow up-dwh down-backend down-backend-v down-airflow down-airflow-v down-dwh down-dwh-v

up-backend:
	docker compose -f backend/docker-compose.yml --env-file .env up --build -d

up-airflow:
	docker compose -f airflow/docker-compose.yml --env-file .env up --build -d

up-dwh:
	docker compose -f dwh/docker-compose.yml --env-file .env up --build -d

down-backend:
	docker compose -f backend/docker-compose.yml --env-file .env down

down-backend-v:
	docker compose -f backend/docker-compose.yml --env-file .env down -v

down-airflow:
	docker compose -f airflow/docker-compose.yml --env-file .env down

down-airflow-v:
	docker compose -f airflow/docker-compose.yml --env-file .env down -v

down-dwh:
	docker compose -f dwh/docker-compose.yml --env-file .env down

down-dwh-v:
	docker compose -f dwh/docker-compose.yml --env-file .env down -v