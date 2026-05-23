# Deployment

## Local Development

## Local Control Plane Setup

This section is the fastest way to run the current Python control plane on your machine.

Prerequisite:
- Python 3

1. Install dependencies:

```bash
python3 -m pip install -r control_plane/requirements.txt
```

2. Run control-plane tests:

```bash
python3 -m pytest control_plane/tests
```

3. Run the API:

```bash
python3 -m uvicorn control_plane.api:app --reload
```

4. Open API docs:
- `http://127.0.0.1:8000/docs`

5. Health endpoint:
- `http://127.0.0.1:8000/health`

Notes for deployment planning:
- This setup is local-only; the **API runs on your machine** (Postgres can run in Docker—see **Local PostgreSQL Setup**).
- The API is **not wired yet** to Postgres, Kafka, Redis, or Go workers in production style.
- Those integrations will be added later as the deployment path matures.

## Local PostgreSQL Setup

KernelQ ships a **Postgres** service in `docker-compose.yml` for local development. Run these commands from the **repository root**.

**1. Start Postgres in the background**

```bash
docker compose up -d postgres
```

**2. Confirm the container is running**

```bash
docker compose ps
```

You should see `kernelq-postgres` (or the compose service name) listed as running.

**3. Open an interactive SQL shell inside the container**

```bash
docker exec -it kernelq-postgres psql -U kernelq -d kernelq
```

- `-U kernelq` is the database user (matches `POSTGRES_USER` in compose).
- `-d kernelq` is the database name (matches `POSTGRES_DB`).

**4. Apply the first migration (from your host machine, not inside psql)**

Leave psql if you are already inside it (`\q`), then run:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/001_create_jobs.sql
```

- `-i` lets Docker attach stdin so the SQL file is piped into `psql`.
- This creates the `jobs` table (and indexes) idempotently where the migration uses `IF NOT EXISTS`.

**5. Verify the table exists**

Connect again with `docker exec -it kernelq-postgres psql -U kernelq -d kernelq`, then at the `psql` prompt:

```text
\dt
```

You should see `jobs` listed among relations.

**6. Quit psql**

```text
\q
```

That returns you to your normal terminal shell.

## Running Repository Tests

Integration tests in `control_plane/tests/test_job_repository.py` talk to **real Postgres** on your machine. **Most other control-plane unit tests do not need Postgres** and can run without Docker.

From the repository root:

**1. Start Postgres**

```bash
docker compose up -d postgres
```

**2. Apply migration if needed** (safe to re-run when the SQL uses `IF NOT EXISTS`)

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/001_create_jobs.sql
```

**3. Install Python dependencies** (includes `psycopg`)

```bash
python3 -m pip install -r control_plane/requirements.txt
```

**4. Run repository tests only**

```bash
python3 -m pytest control_plane/tests/test_job_repository.py
```

If Postgres is not running, these tests will skip or fail when connecting—start the container first.

## Inspecting Scheduler Query Plans

After Postgres is up and the `jobs` migration is applied, you can inspect how Postgres plans scheduler-related queries (`EXPLAIN` / `EXPLAIN ANALYZE`). That helps verify whether **indexes are useful** (index scan vs full table scan) before the table grows. **Local-only for now**—not part of CI or cloud deploy yet.

From the repository root:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

See `docs/perf.md` (**Postgres EXPLAIN for Scheduler Queries**) for what to look for in the output.

### Docker Compose Setup

The repo includes `docker-compose.yml` with a **Postgres 16** service for local development (see **Local PostgreSQL Setup** above).

TODO later:
- Redis instance
- Message broker (Kafka)
- Control plane API container (Python FastAPI)
- Worker processes (Go)

### Running Locally

TODO: Document commands to:
- Start full stack: `docker compose up`
- Seed test data (when scripts exist)
- View logs: `docker compose logs -f`

## Cloud Deployment (AWS)

### Infrastructure Overview

- **Control Plane**: Containerized Python service
- **Worker Plane**: Containerized Go services
- **Database**: AWS RDS Postgres (multi-AZ for HA)
- **Cache/Broker**: AWS ElastiCache Redis or managed message broker
- **Load Balancer**: Application Load Balancer for API
- **Container Orchestration**: TBD (ECS Fargate vs EKS) - see ADR-0002

### Infrastructure as Code

All infrastructure defined in Terraform:
- VPC and networking
- RDS Postgres instances
- ElastiCache Redis
- ECS/EKS clusters
- Load balancers
- IAM roles and policies
- Secrets Manager

### Deployment Process

TODO: Document CI/CD pipeline:
1. Code changes trigger build
2. Docker images built and pushed to ECR
3. Terraform plan/apply for infra changes
4. Blue-green or rolling deployment for services
5. Health checks and rollback procedures

## Container Orchestration Decision

We will decide between ECS Fargate and EKS in a later ADR (ADR-0002). Factors to consider:
- Team expertise
- Cost
- Operational complexity
- Scaling requirements
- Integration with other AWS services
