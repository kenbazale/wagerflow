# WagerFlow — Architecture & Design Decisions

This document is the technical companion to the [README](./README.md). It covers *why* the system is built the way it is, what broke along the way, and what's deliberately left unfinished.

## Contents

- [System overview](#system-overview)
- [Design decisions worth defending](#design-decisions-worth-defending)
- [Build history by phase](#build-history-by-phase)
- [Lessons learned](#lessons-learned)
- [Known limitations](#known-limitations)
- [CI](#ci)

---

## System overview

WagerFlow simulates a real-money betting platform end to end:

1. **Bet placement** — a bet command is validated (self-exclusion check, balance check) and either accepted or rejected.
2. **Settlement** — a saga-based flow resolves match outcomes and triggers payouts.
3. **Wallet** — an event-sourced, exactly-once ledger; balances are never a hardcoded value, only the sum of real events.
4. **Responsible gambling (RG) monitoring** — a Faust stream-processing app watches deposits and bets in real time for deposit-velocity spikes, loss-chasing patterns, and abnormal session length.
5. **CQRS read model** — a denormalized projection built from the same event stream, for fast queries without touching the transactional path.
6. **Archival** — every cross-service event is sunk to S3 via Kafka Connect for durable storage and downstream batch processing.
7. **Nightly batch analytics** — a PySpark job computes GGR (gross gaming revenue) and player LTV, writing to Redshift Serverless; dbt then transforms raw landed tables into clean staging → mart models; Airflow orchestrates the whole nightly run.

## Design decisions worth defending

### JSON for internal commands, Avro for cross-service events

`bet-commands` (the topic carrying player bet submissions into the Bet Service) is plain JSON, not Avro. This was a deliberate call, not an oversight: it's an internal command, produced and consumed by a single service, never read by anything else and never exposed externally. The overhead of registering and versioning a schema for it buys nothing. By contrast, `BetPlaced`, `BetRejected`, and every `wallet-events` message *are* cross-service contracts — other services and the S3 archive depend on their shape being stable and versioned, so those go through Schema Registry with Avro.

The general principle: schema enforcement earns its cost at a service boundary, not everywhere a message crosses a topic.

### Balances are never pre-seeded

Early in Phase 11's test cleanup, a test (`test_cache_bootstrap.py`) asserted that `seed_default_caches` should populate a player's wallet balance to a hardcoded `500.0`. That test's *expectation* was itself the bug: pre-seeding a balance and then processing that player's real deposit events caused deposits to be double-counted — once as the fake default, once as the real ledger event.

The fix: `seed_default_caches` now seeds only player profile data (self-exclusion status, KYC status). Balances are built exclusively from real `wallet-events` consumed by `wallet_cache_worker`, matching the event-sourced design everywhere else in the system. The test was rewritten to assert the cache starts with *no* balances at all.

### Redshift Serverless over provisioned Redshift

The nightly batch job originally wrote to Postgres (Phase 7), then migrated to Redshift Serverless (Phase 7.5). Serverless was chosen specifically for its $0 idle-compute cost — at this project's data volume, a provisioned cluster would be needlessly expensive to keep running between nightly runs. Realistic monthly cost at this scale is well under $2.

The original Postgres analytics tables were deliberately kept rather than cleaned up, as a visible record of that migration decision for anyone reading the repo.

### Manual offset commits over framework auto-commit

Multiple services in this project explicitly disable Kafka's `enable.auto.commit` in favor of manual commits *after* the corresponding in-memory state update completes. This is not the default posture — it's a direct response to a real bug (see [Lessons learned](#lessons-learned)) where auto-commit advanced offsets before an in-memory cache was durably updated, creating a window where a crash could silently lose state.

## Build history by phase

| Phase | Scope | Status |
|---|---|---|
| 1 | Repo scaffold, core Docker Compose stack | ✅ Complete |
| 2 | Avro domain schemas, Debezium CDC | ✅ Complete |
| 3 | Bet Service + Wallet Service | ✅ Complete, verified |
| 4 | Game Engine + Settlement saga | ✅ WON/LOST verified end-to-end; VOID refund and saga-compensation-failure paths deferred by choice |
| 5 | RG Monitoring (Faust): deposit velocity, loss-chasing, session-length | ✅ All 3 rules verified individually; loss-chasing idempotency issue noted (see Known limitations) |
| 6 | CQRS read model | ✅ Complete — bet placement, deposit, RG alert, and settlement projections all confirmed |
| 7 | Kafka Connect S3 sink + PySpark nightly GGR/LTV batch job | ✅ Complete, verified against real settlement data |
| 7.5 | Redshift Serverless migration | ✅ Complete |
| 8 | dbt models (staging → marts) | ✅ Complete — 4 models, 8 tests, all passing |
| 9 | Airflow DAG orchestration | ✅ Complete — full nightly pipeline runs end-to-end |
| 10 | Security hardening (SASL_SSL, ACLs) | ⏸ Deliberately deferred |
| 11 | Tests, CI, docs | ✅ Tests and CI complete (CI blocked externally — see [CI](#ci)); this document is the final piece |

### Phase 7 — nightly batch job

Reads `BetPlaced` and settlement events from the S3 archive, joins on `bet_id`, computes daily GGR and player LTV, and writes to Postgres. Verified against real settlement data, including sanity-checking a match that showed *negative* GGR on a day with a large player win — correctly reflecting payouts exceeding stakes.

### Phase 7.5 — Redshift Serverless migration

Migrated batch output to Redshift Serverless (base capacity 4, publicly-accessible workgroup). The job now writes to both Postgres and Redshift via the community `spark-redshift` connector plus the official Redshift JDBC driver, staging through an S3 tempdir, authenticated via a dedicated IAM role. Getting this working required: trust-policy updates for both `redshift.amazonaws.com` and `redshift-serverless.amazonaws.com`; VPC/security-group configuration scoping port 5439 to a known IP; and extending the S3 IAM user's permissions to include delete operations, which the connector's `FileOutputCommitter` needs but the earlier append-only S3 sink never required.

### Phase 8 — dbt

A thin staging → pass-through marts structure on top of the Redshift tables Spark writes. Since Spark already does the aggregation, the dbt models are intentionally simple — the value here is in `not_null`/`unique` tests and lineage documentation, not further transformation.

### Phase 9 — Airflow orchestration

The nightly DAG (`run_spark_ggr_ltv_batch → dbt_run → dbt_test`) runs via `SSHOperator`, since Spark and dbt tooling live in the WSL2 host's Python venv rather than inside Airflow's containers. This is a known architectural shortcut: a production deployment would containerize the Spark/dbt execution environment rather than SSH back out to the host. Three distinct infrastructure bugs were fixed to get the DAG green end-to-end (see [Lessons learned](#lessons-learned)).

## Lessons learned

A selection of real bugs hit and fixed during the build — kept here rather than papered over, since debugging history is part of what this project demonstrates.

**Settlement Service auto-commit race.** `bet_cache_worker` had `enable.auto.commit=True`, which let Kafka advance consumer offsets *before* the in-memory `open_bets` cache was durably updated — a crash between those two points would silently lose in-flight bet state. Fixed by switching to `enable.auto.commit=False` with an explicit `consumer.commit()` after each cache update, matching the pattern already used elsewhere in `game_result_worker`.

**Spark dependency resolution.** The batch job's `spark.jars.packages` list included a stale AWS SDK v1 coordinate that doesn't exist at that groupId/version, breaking Ivy dependency resolution. `hadoop-aws:3.5.0` alone pulls in the correct v2 bundle transitively — the explicit v1 pin was unnecessary and wrong.

**S3A credentials provider misconfiguration.** The job explicitly set `SimpleAWSCredentialsProvider`, which doesn't read `~/.aws` profile files. Removing the override let the default provider chain fall through to environment variables, which is how credentials are actually supplied in this setup.

**Silent S3 sink under-flushing.** The `settlement-events` S3 sink connector had `flush.size: 50` but no `rotate.schedule.interval.ms`, so low-volume partitions never crossed the count threshold and simply never flushed — despite 49+ messages sitting in Kafka, zero files landed in S3. Adding `rotate.schedule.interval.ms: 60000` fixed it.

**Redshift overwrite-mode dependency conflict.** Once dbt's `stg_ggr_daily` view existed as a dependent object on `analytics.ggr_daily`, Spark's overwrite-mode write (which does a `DROP TABLE` under the hood) started failing with a dependency error. Fixed by switching the Redshift write to `TRUNCATE` + append via preactions, which updates the data without dropping the table object dbt depends on — a direct interaction between Phase 7.5 and Phase 8 that only surfaced once both existed together.

**Spark/Docker Desktop resource contention under Airflow.** Running the Spark JVM and Docker Desktop's own `docker-desktop-user-distro` proxy in the same WSL2 VM meant a Spark resource spike could crash the proxy, surfacing as "WSL integration unexpectedly stopped" mid-DAG-run. Confirmed by seeing the DAG succeed once the core Docker Compose stack was stopped during the run. Long-term fix: increasing WSL2 memory/CPU via `.wslconfig` and capping Spark's own resource usage (`spark.driver.memory=2g`, `spark.master=local[2]`) so both stacks can eventually coexist reliably.

**Non-hermetic dependency list caught by CI setup.** Generating `requirements.txt` from a shared dev venv (rather than the project's own isolated one) initially produced a list that looked complete but silently relied on packages the venv happened to already have installed for unrelated reasons. Verifying the file in a genuinely fresh venv surfaced three missing transitive dependencies (`certifi`, `httpx`, `fastavro`) one at a time — all pulled in implicitly by `confluent_kafka`'s schema-registry client, not by this project's own code. The real fix wasn't adding each package individually but requesting `confluent_kafka`'s own `[avro,schema-registry]` extras, which pin those transitive dependencies correctly rather than guessing at them.

## Known limitations

Documented honestly rather than hidden:

- **LOSS_CHASING alert idempotency** — a Faust agent crash/restart can currently produce duplicate alerts for the same pattern. A partial fix was applied but not fully verified clean; flagged for revisiting if RG monitoring work resumes.
- **Bet Service dual in-memory caches can drift under multi-instance horizontal scaling** — acceptable for a single-instance portfolio deployment, not for production scale-out.
- **LOSS_CHASING is detective, not preventative** — it fires on the bet that completes the pattern, after that bet has already been placed, and its "last N bets" window has no time bound.
- **`bet_stakes` Faust table has no expiry** and grows unboundedly over the service's lifetime.
- **MinIO remains in `docker-compose.yml`** even though real AWS S3 is the actual sink target — kept as a deliberate artifact of the build process rather than removed.
- **Phase 4's VOID refund path and saga-compensation-failure path** are implemented but not exercised by an end-to-end test — deferred by choice, not by oversight.
- **Phase 10 (SASL_SSL, ACLs)** is deliberately out of scope for this iteration.
- **Airflow's SSHOperator pattern** (executing Spark/dbt on the WSL2 host rather than inside a container) is a known shortcut appropriate for a local portfolio build, not a production pattern.

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs the full pytest suite (11 tests) on every push to `master`, using Python 3.12 and Temurin JDK 17 (required for PySpark). The dependency set was verified clean across multiple fully-isolated venv installs before being committed.

As of this writing, CI runs are blocked by an account-level GitHub billing verification issue unrelated to the project itself (an international card-authorization hold failing due to regional forex restrictions, on an account with $0 usage and nothing owed) — the workflow file itself is confirmed correct and will run cleanly once that's resolved. In the interim, the same test command (`pytest tests/ -v`) is run manually before each commit as the CI gate.
