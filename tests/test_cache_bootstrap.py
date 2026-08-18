from service.cache_bootstrap import seed_default_caches


def test_seed_default_caches_populates_players_and_balances():
    player_cache = {}
    balance_cache = {}

    seed_default_caches(player_cache, balance_cache)

    assert player_cache['player-001']['self_exclusion'] is False
    assert player_cache['player-004']['self_exclusion'] is True
    assert balance_cache['player-001'] == 500.0
    assert balance_cache['player-005'] == 500.0
