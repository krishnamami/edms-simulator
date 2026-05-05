.PHONY: install dev test smoke stress lint up down logs clean docker-build docker-run schema

install:
	pip install -r requirements.txt -r requirements-dev.txt

dev:
	uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload

test:
	pytest tests/ --ignore=tests/integration -v --tb=short

test-integration:
	pytest tests/integration -v --tb=short

smoke:
	python scripts/smoke_aggregation.py

stress:
	python scripts/stress_test.py --applications 1000 --concurrency 50

lint:
	cfn-lint infra/cloudformation/*.yaml

up:
	docker-compose up -d postgres redis

up-all:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f api

clean:
	docker-compose down -v
	rm -rf local_storage __pycache__ .pytest_cache .coverage

docker-build:
	docker build -t edms-simulator:latest .

docker-run:
	docker run --rm -p 8001:8001 --env-file .env edms-simulator:latest

schema:
	psql postgresql://edms:edms_dev@localhost:5433/edms -f infra/schema.sql
