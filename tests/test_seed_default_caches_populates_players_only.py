def seed_default_caches(player_cache, balance_cache):
    """Populate the local player-profile cache with the same seeded players
    used by the demo data. Balance is deliberately NOT pre-seeded here --
    it must be built exclusively from real wallet-events, matching the
    event-sourced design used by Wallet Service. Pre-seeding it caused a
    double-counting bug where deposits were counted once as a hardcoded
    default and again as a real ledger event."""
    players = {
        'player-001': {'self_exclusion': False, 'kyc_status': 'VERIFIED'},
        'player-002': {'self_exclusion': False, 'kyc_status': 'VERIFIED'},
        'player-003': {'self_exclusion': False, 'kyc_status': 'PENDING'},
        'player-004': {'self_exclusion': True, 'kyc_status': 'VERIFIED'},
        'player-005': {'self_exclusion': False, 'kyc_status': 'VERIFIED'},
    }

    for player_id, profile in players.items():
        player_cache[player_id] = profile
        # balance_cache intentionally left unset here — populated only by
        # wallet_cache_worker consuming real wallet-events
        
def test_seed_default_caches_populates_players_only():
    player_cache = {}
    balance_cache = {}

    seed_default_caches(player_cache, balance_cache)

    assert player_cache['player-001']['self_exclusion'] is False
    assert player_cache['player-004']['self_exclusion'] is True
    # balance_cache is deliberately left empty here — it's populated only
    # by wallet_cache_worker consuming real wallet-events, to avoid the
    # double-counting bug where deposits were counted once as a hardcoded
    # default and again as a real ledger event.
    assert balance_cache == {}