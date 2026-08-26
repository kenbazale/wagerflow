from tests.test_seed_default_caches_populates_players_only import seed_default_caches


def test_seed_default_caches_populates_players_and_balances():
    player_cache = {}
    balance_cache = {}

    seed_default_caches(player_cache, balance_cache)

    assert player_cache['player-001']['self_exclusion'] is False
    assert player_cache['player-004']['self_exclusion'] is True
    assert balance_cache['player-001'] == 500.0
    assert balance_cache['player-005'] == 500.0


        
def test_seed_default_caches_populates_players_and_balances():
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