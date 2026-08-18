#!/bin/bash
# Registers Debezium CDC on player_accounts. Uses ExtractNewRecordState to
# flatten the {before, after, source, op} envelope down to just the current
# row — downstream consumers of player identity data want "current state",
# not change-type awareness (unlike wallet-events, which is deliberately
# raw events, not flattened, because direction/reference_type ARE the point).

set -e

curl -s -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "player-accounts-source-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "wagerflow",
      "database.password": "wagerflow_dev_pw",
      "database.dbname": "wagerflow",
      "topic.prefix": "wagerflow",
      "table.include.list": "public.player_accounts",
      "plugin.name": "pgoutput",
      "slot.name": "player_accounts_slot",
      "publication.autocreate.mode": "filtered",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false"
    }
  }' | python3 -m json.tool

echo ""
echo "Waiting for connector to initialize..."
sleep 3
curl -s http://localhost:8083/connectors/player-accounts-source-connector/status | python3 -m json.tool