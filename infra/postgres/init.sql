-- WagerFlow: player_accounts is the CDC source of truth for player identity/
-- account-level data (NOT balance — balance lives in the event-sourced wallet
-- ledger in Kafka, never in a mutable column here, to avoid a dual-write
-- problem between Postgres and the wallet event log).

CREATE TABLE player_accounts (
    player_id VARCHAR(20) PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    country  VARCHAR(2) NOT NULL,
    kyc_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    self_exclusion BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- REPLICA IDENTITY FULL: required so Debezium's CDC stream carries the full
-- "before" image on UPDATE/DELETE, not just the primary key. Without this,
-- update/delete events downstream would be missing every other column.

ALTER TABLE player_accounts REPLICA IDENTITY FULL;

INSERT INTO player_accounts (player_id, username, country, kyc_status, self_exclusion)
VALUES
    ('player-001', 'alice_bets',   'GB', 'VERIFIED', FALSE),
    ('player-002', 'bob_wagers',   'MT', 'VERIFIED', FALSE),
    ('player-003', 'carol_stakes', 'CY', 'PENDING',  FALSE),
    ('player-004', 'dave_gamble',  'GB', 'VERIFIED', TRUE),
    ('player-005', 'eve_punter',   'EE', 'VERIFIED', FALSE);
    