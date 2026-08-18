# WagerFlow

A real-time betting, wallet, and settlement platform for an iGaming operator —
built to demonstrate production-grade event-driven architecture on Kafka,
with a downstream batch analytics layer (PySpark → dbt → Airflow) for
Gross Gaming Revenue and player lifetime value reporting.

> Status: 🚧 in progress. This README is filled in incrementally as each
> phase of the build completes — see `docs/architecture.md` for the target
> design and the reasoning behind it.

## What this demonstrates

- Event-sourced wallet ledger with exactly-once transactional writes
- Saga-based settlement (bet → game result → payout, with compensation on failure)
- CDC (Debezium) from a Postgres system-of-record into Kafka
- Schema Registry with Avro, versioned and evolution-tested
- Real-time responsible-gambling risk signals
- CQRS read model for fast player-facing balance/history queries
- Kafka Connect S3 sink → PySpark batch aggregation → dbt marts → Airflow orchestration
- Secured cluster (SASL_SSL, ACLs) — see `docs/architecture.md#security`
- Tests + CI

## Quickstart

```bash
cp .env.example .env
make up
make topics   # sanity check once services report healthy
```

Kafka UI: http://localhost:8080
Schema Registry: http://localhost:8081
Kafka Connect: http://localhost:8083
ksqlDB: http://localhost:8088
MinIO console: http://localhost:9001

## Architecture

See [`docs/architecture.md`](docs/architecture.md) for the full design,
including the trade-offs behind each major decision (why events, not shared
databases; why a saga instead of a distributed transaction; why security is
layered in rather than built in from the start).

## Repo layout

```
services/            Each Kafka-native service (bet, wallet, game engine,
                      settlement, RG monitor, CQRS projection)
schemas/              Avro schemas, versioned
infra/                Docker Compose infra config, cert generation
warehouse/            PySpark batch jobs, dbt project, Airflow DAGs
tests/                Unit + integration tests
docs/                 Architecture write-up and diagrams
```

## Build log

Each phase's status and key design decisions are tracked here as they land.

- [x] Phase 1 — Repo scaffold + core Kafka stack (Kafka, Schema Registry,
      Connect, Postgres, MinIO, ksqlDB, Kafka UI)
- [ ] Phase 2 — Domain schemas + Postgres CDC
- [ ] Phase 3 — Bet + Wallet services (exactly-once ledger)
- [ ] Phase 4 — Game Engine + Settlement saga
- [ ] Phase 5 — Responsible gambling monitoring
- [ ] Phase 6 — CQRS read model
- [ ] Phase 7 — S3 sink → PySpark nightly batch job
- [ ] Phase 8 — dbt models
- [ ] Phase 9 — Airflow orchestration
- [ ] Phase 10 — Security (SASL_SSL, ACLs)
- [ ] Phase 11 — Tests, CI, final docs
