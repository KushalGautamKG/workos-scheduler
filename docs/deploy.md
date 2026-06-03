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

## Running Kafka Locally

KernelQ’s `docker-compose.yml` includes **Zookeeper** and **Kafka** for local broker infrastructure. This is **setup only**—the control plane does **not** publish jobs to Kafka yet. Postgres and scheduling still work without the broker; Kafka prepares the environment for the next milestone.

From the repository root:

**1. Start Zookeeper and Kafka**

```bash
docker compose up -d zookeeper kafka
```

**2. Confirm services are running**

```bash
docker compose ps
```

You should see `kernelq-zookeeper` and `kernelq-kafka` (and optionally `kernelq-postgres` if Postgres is also up).

**Notes:**

- **Kafka** listens on **`localhost:9092`** from your laptop (for future producers/consumers).
- **Postgres** is a separate service—start it with `docker compose up -d postgres` or run `docker compose up -d` to start everything together.
- No application containers yet; only infrastructure.

See `docs/decisions/ADR-0002-kafka-choice.md` and **Kafka Event Backbone** in `docs/architecture.md`.

## Creating Kafka Topics Locally

After Zookeeper and Kafka are running, create KernelQ’s three job topics (`dispatch`, `retry`, `dlq`) with the repo script. From the **repository root**:

**1. Make the script executable (once)**

```bash
chmod +x infra/kafka/create-topics.sh
```

**2. Create topics**

```bash
./infra/kafka/create-topics.sh
```

The script uses `docker exec` against **`kernelq-kafka`**, runs `kafka-topics` inside the container, and creates:

- `kernelq.jobs.dispatch` — normal runnable work
- `kernelq.jobs.retry` — jobs that failed but can run again
- `kernelq.jobs.dlq` — dead-letter / poison messages

Each topic gets **3 partitions** and **replication factor 1** (fine for local dev). The script is **safe to rerun** (`--if-not-exists`). See **Kafka Topics** in `docs/architecture.md` for why these names exist.

### Kafka CLI Smoke Test

Once topics exist, you can prove the broker and topic wiring work **before** any Python or Go code publishes jobs. Kafka ships two small CLI tools inside the container: a **producer** (writes messages) and a **consumer** (reads messages).

**1. Produce one message**

Run this from your host. It opens an interactive producer; paste one line of JSON, then press **Enter**, then **Ctrl+D** to finish:

```bash
docker exec -i kernelq-kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.dispatch
```

Example message (one line):

```json
{"job_id":"smoke-test-1","tenant_id":"tenant-a","priority":5}
```

**What happened:** the **producer** appended your JSON string to the **`kernelq.jobs.dispatch`** topic log on the broker (`kafka:29092` is the in-Docker bootstrap address).

**2. Consume messages**

In a **new terminal**, read back from the same topic (including messages already on the log):

```bash
docker exec -it kernelq-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.dispatch \
  --from-beginning \
  --max-messages 1
```

You should see the same JSON line printed. Press **Ctrl+C** if the consumer keeps waiting after one message.

**What happened:** the **consumer** subscribed to `kernelq.jobs.dispatch`, read from the start of the log (`--from-beginning`), and printed up to one message (`--max-messages 1`).

**Why this matters:** if produce and consume both work, **local Kafka topic wiring is healthy**—broker up, topic created, messages durable on the log. The control plane and Go workers will use the same topic names later; this smoke test confirms infrastructure only, not application code.

## Manual Scheduler-to-Kafka Smoke Test

This walkthrough runs the **real** Python scheduler path: a **`queued`** row in Postgres → one **`SchedulerTickRunner`** pass → a message on **`kernelq.jobs.dispatch`**. It uses **`control_plane/scripts/run_scheduler_tick_once.py`** (not fake producers in pytest).

**What this proves:** Postgres → scheduler tick → Kafka dispatch topic.

**What this does not prove yet:** **Go worker execution**, retries, exactly-once behavior, or production-grade reliability. Workers are not wired in; you are only checking that the control plane can hand work to the broker.

From the **repository root**:

**1. Start infrastructure**

```bash
docker compose up -d postgres zookeeper kafka
```

Apply the jobs migration if you have not already (see **Local PostgreSQL Setup**):

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/001_create_jobs.sql
```

**2. Create Kafka topics**

```bash
chmod +x infra/kafka/create-topics.sh   # once
./infra/kafka/create-topics.sh
```

**3. Create a queued job (API or SQL)**

The tick script **does not create jobs**—it only claims rows already in state **`queued`**.

**API option** — start the control plane in one terminal:

```bash
python3 -m pip install -r control_plane/requirements.txt   # if needed
python3 -m uvicorn control_plane.api:app --reload
```

In **another terminal**, enqueue a job (use a fresh `job_id` if you rerun):

```bash
curl -X POST http://127.0.0.1:8000/jobs/day31-smoke/enqueue \
  -H "Content-Type: application/json" \
  -d '{"tenant_id":"tenant-a","priority":999999,"payload":{"kind":"day31-smoke"}}'
```

You should get a JSON response with `"state": "queued"`. High **priority** helps this job win the claim when other queued rows exist in a shared local database.

**SQL option** — insert a row directly with `psql` if you prefer; state must be **`queued`**.

**4. Run one scheduler tick**

```bash
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py
```

Read the printed summary. On success you should see something like:

- `selected_count: 1`
- `dispatched_count: 1`
- `published_count: 1`
- `dispatched_job_ids` listing your job id
- `errors` and `publish_errors` empty

The script claims **at most one** job per run (`max_jobs_per_tick=1`) and publishes to **`localhost:9092`**.

**5. Consume from Kafka**

Confirm the dispatch event reached the topic (read from the start of the log; you may see older messages if you ran this before):

```bash
docker exec -i kernelq-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.dispatch \
  --from-beginning \
  --max-messages 1
```

Look for JSON containing your **`job_id`**, **`tenant_id`**, **`priority`**, and **`payload`**. Press **Ctrl+C** if the consumer keeps waiting after printing messages.

**End-to-end check:** queued job in Postgres → tick marks **`dispatched`** and publishes → message visible on **`kernelq.jobs.dispatch`**. That is the full control-plane handoff for local dev; execution on workers comes later.

## Running Go Worker Tests

The **`worker/`** directory holds the Go worker plane. From the **repository root**:

```bash
cd worker
go test ./...
```

**Prerequisite:** Go **1.22+** installed (`go version`). No Docker or Kafka required for these tests.

**What this runs today:** unit tests for the worker **foundation** only (for example `Task` validation in `internal/worker/`). There is **no Kafka consumer** and **no live broker** in this test suite yet—**Kafka consumer behavior** comes in a later milestone.

See also **`worker/README.md`**.

## Running Worker Consumer Tests

Worker tests for **`ConsumerRunner`** (message parsing → handler) use the same command:

```bash
cd worker
go test ./...
```

These tests validate **dispatch JSON parsing**, **validation**, and **handler flow** with fake in-memory messages. They do **not** require Kafka or Docker yet—real broker consumption comes later.

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

## Database Test Isolation

Your local Postgres can contain many kinds of rows at the same time: **seed data**, **manual API test data**, and **benchmark data**. Because of that, integration tests should not assume the database is empty.

Use **test-specific `job_id` prefixes** (for example `test-repo-`, `test-tick-`, `test-api-`) so each test module can find only its own rows.

Tests should also **clean up their own rows before and after** running. That makes repeated runs stable and prevents accidental failures caused by unrelated local data left behind from earlier experiments.

## Inspecting Scheduler Query Plans

After Postgres is up and the `jobs` migration is applied, you can inspect how Postgres plans scheduler-related queries (`EXPLAIN` / `EXPLAIN ANALYZE`). That helps verify whether **indexes are useful** (index scan vs full table scan) before the table grows. **Local-only for now**—not part of CI or cloud deploy yet.

From the repository root:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

See `docs/perf.md` (**Postgres EXPLAIN for Scheduler Queries**) for what to look for in the output.

## Applying Scheduler Query Index Migration

Migration **`002_add_scheduler_claim_index.sql`** adds `idx_jobs_state_priority_created_at` for the scheduler claim query. From the repository root (Postgres running, migration **`001`** already applied):

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/002_add_scheduler_claim_index.sql
```

Then rerun **EXPLAIN** to compare plans before vs after:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

The script lists indexes on `jobs` and prints query plans. See `docs/perf.md` (**Scheduler Query Indexing**) for what to paste in before/after notes.

### Docker Compose Setup

The repo includes `docker-compose.yml` with **Postgres 16**, **Zookeeper**, and **Kafka** for local development (see **Local PostgreSQL Setup**, **Running Kafka Locally**, and **Creating Kafka Topics Locally** above).

TODO later:
- Redis instance
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
