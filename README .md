# WagerFlow

A production-grade, event-driven iGaming platform for real-time bet placement, wallet management, and settlement — built to demonstrate the full lifecycle of a modern data/streaming engineering system, from Kafka-backed transactional services through to a nightly analytics pipeline on Redshift.

> **Why iGaming?** The domain forces a system to deal with real-money correctness under concurrency (wallet balances, exactly-once settlement), regulatory-style monitoring (responsible-gambling detection), and analytics-grade reconciliation (GGR/LTV) — the same hard problems that show up in fintech, without requiring access to a bank's actual rails.

See [ARCHITECTURE.md](./ARCHITECTURE.md) for the full technical deep-dive: design decisions, trade-offs, and every bug that got fixed along the way.

---

## What it does

A player places a bet → the bet is validated against their wallet balance and self-exclusion status → the outcome is settled through a saga-based settlement flow → the wallet is updated via exactly-once event-sourced transactions → responsible-gambling rules watch every deposit and bet in real time → every event feeds a CQRS read model for instant queries → every night, a Spark job aggregates GGR (gross gaming revenue) and player LTV, lands it in Redshift, and dbt transforms it into clean marts — all orchestrated by Airflow.

## Architecture at a glance

```
Bet Command → Bet Service → Kafka (BetPlaced/BetRejected, Avro)
                                │
                    ┌───────────┴────────────┐
                    ▼                         ▼
            Game Engine + Settlement    RG Monitor (Faust)
              Saga → Wallet Service      deposit velocity,
              (exactly-once, CDC)        loss-chasing,
                                         session-length
                    │                         │
                    └───────────┬─────────────┘
                                ▼
                        CQRS Read Model
                                │
                                ▼
                  Kafka Connect S3 Sink (event archive)
                                │
                                ▼
              Nightly Airflow DAG (Spark → Redshift → dbt)
                                │
                                ▼
                  GGR Daily / Player LTV marts
```

## Tech stack

| Layer | Tools |
|---|---|
| Streaming backbone | Kafka, Debezium (CDC), Avro + Schema Registry |
| Transactional services | Python, exactly-once wallet processing, saga-based settlement |
| Stream processing | Faust (responsible-gambling monitoring) |
| Read model | CQRS projections |
| Batch analytics | PySpark, Redshift Serverless |
| Transformation | dbt |
| Orchestration | Airflow (Docker Compose, SSHOperator) |
| Infra | Docker Compose, AWS (S3, Redshift Serverless, IAM) |
| Testing / CI | pytest, GitHub Actions |

## Highlights worth reading about

- **Exactly-once wallet transactions** built on manual offset commits, not framework defaults — see the [Settlement Service auto-commit bug](./ARCHITECTURE.md#lessons-learned) that motivated it.
- **Event-sourced balances**: the wallet cache is deliberately *not* pre-seeded with a default balance — it's built exclusively from real wallet-events, avoiding a subtle double-counting bug (also in ARCHITECTURE.md).
- **A deliberate schema choice**: internal commands stay plain JSON; only cross-service contracts (BetPlaced, BetRejected, wallet-events) get Avro + Schema Registry — a real cost/benefit call, not a default.
- **Migrated from Postgres to Redshift Serverless mid-project**, chosen for near-zero idle cost, with the original Postgres tables kept intentionally as a narrative of that evolution.
- **A fully orchestrated nightly pipeline** (Spark batch → Redshift write → dbt run → dbt test) running end-to-end through Airflow, including three separate infrastructure bugs diagnosed and fixed to get there.

## Running it

```bash
docker-compose up -d          # Kafka, Debezium, Kafka Connect, Schema Registry
./create_topics.sh
python service/seed_wallet_deposits.py
python service/place_bet.py   # sends a sample batch of bet commands
```

Nightly analytics pipeline (Spark → Redshift → dbt) runs via the Airflow DAG in `airflow/dags/wagerflow_nightly_batch.py`.

## Tests & CI

```bash
pytest tests/ -v
```

11 unit tests covering wallet/settlement logic, RG monitoring, cache bootstrapping, and the GGR/LTV batch job's aggregation formulas. CI runs the same suite on every push via GitHub Actions (`.github/workflows/ci.yml`).

## Known limitations

Documented honestly in [ARCHITECTURE.md](./ARCHITECTURE.md#known-limitations) — including what's deliberately deferred (security hardening, a couple of edge-case idempotency issues) and why.

---

*Built as a portfolio capstone project, transitioning from Oracle/FLEXCUBE core-banking database administration toward modern data engineering.*
