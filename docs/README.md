# EDMS Simulator

Production-grade Enterprise Data Management Simulator for mortgage loan origination data.

- **Aurora Serverless v2 (Postgres 15)** for raw EDMS data
- **ElastiCache Redis** for hot aggregated cache
- **S3** (versioned, KMS-encrypted) for raw documents
- **SQS FIFO + Lambda** for async pipelines
- **ECS Fargate** for the API container (port 8001)
- **CloudFormation** for all infrastructure
- **GitHub Actions** for CI + AWS deploy (matches `ai_document_decision_engine` patterns)

## Quick start (local)

```bash
make install           # python deps
make up                # postgres + redis (docker-compose)
make schema            # apply infra/schema.sql to local Postgres
make dev               # uvicorn api.main:app on :8001
```

In another shell:

```bash
make smoke             # in-process aggregation smoke test
make test              # unit tests
SMOKE_TARGET=api make smoke   # against the running API
```

## Configuration

See `.env.example`. Key flags:

| Var                | Default     | Purpose |
|--------------------|-------------|---------|
| `USE_AWS_SECRETS`  | `false`     | Read DB/Redis/API creds from Secrets Manager |
| `USE_AWS_SQS`      | `false`     | Bind SQS consumers (otherwise direct API calls) |
| `USE_LOCAL_STORAGE`| `true`      | Local-FS for documents instead of S3 |
| `USE_FAKE_REDIS`   | `false`     | fakeredis (CI/unit tests) |

## Project layout

See [ARCHITECTURE.md](ARCHITECTURE.md) for the data flow and component boundaries.

## Deploy

Push to `main` triggers `.github/workflows/aws.yaml`:
1. Build + push container to ECR
2. Render `task_definition.json` with the new image SHA
3. Deploy to ECS Fargate, wait for service stability

## API

See [API.md](API.md) for routes, request/response schemas, and auth.
