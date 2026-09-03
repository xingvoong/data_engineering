.PHONY: help up down logs ingest-batch dbt-deps dbt-run dbt-test dbt-docs dagster-dev format

help:
	@echo ""
	@echo "GitHub Pulse — Data Engineering Platform"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make up            Start all services (MinIO, ClickHouse)"
	@echo "    make down          Stop all services"
	@echo "    make logs          Stream service logs"
	@echo ""
	@echo "  Ingestion"
	@echo "    make ingest-batch  Run dlt GitHub pipeline"
	@echo ""
	@echo "  Transformations"
	@echo "    make dbt-deps      Install dbt packages"
	@echo "    make dbt-run       Run all dbt models"
	@echo "    make dbt-test      Run dbt tests"
	@echo "    make dbt-docs      Generate and serve dbt docs"
	@echo ""
	@echo "  Orchestration"
	@echo "    make dagster-dev   Launch Dagster UI (http://localhost:3000)"
	@echo ""
	@echo "  Dev"
	@echo "    make format        Format code with black + ruff"
	@echo ""

up:
	docker compose up -d
	@echo "MinIO console: http://localhost:9001  (minioadmin / minioadmin)"
	@echo "ClickHouse:    http://localhost:8123"

down:
	docker compose down

logs:
	docker compose logs -f

ingest-batch:
	cd ingestion/batch && pip install -r requirements.txt -q && python github_pipeline.py

dbt-deps:
	cd processing/dbt && dbt deps

dbt-run: dbt-deps
	cd processing/dbt && dbt run

dbt-test: dbt-deps
	cd processing/dbt && dbt test

dbt-docs: dbt-deps
	cd processing/dbt && dbt docs generate && dbt docs serve

dagster-dev:
	cd orchestration/dagster && pip install -r requirements.txt -q
	cd processing/dbt && dbt parse
	dagster dev -f orchestration/dagster/__init__.py

format:
	black ingestion/ processing/ orchestration/ --line-length 100
	ruff check ingestion/ processing/ orchestration/ --fix
