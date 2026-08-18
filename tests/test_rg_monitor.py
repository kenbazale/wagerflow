import unittest

from service import rg_monitor


class RGMonitorTests(unittest.TestCase):
    def test_credit_with_deposit_reference_is_detected(self):
        txn = {
            "player_id": "player-001",
            "direction": "CREDIT",
            "reference_type": "DEPOSIT",
        }

        self.assertTrue(rg_monitor.is_deposit_event(txn))

    def test_non_deposit_reference_is_not_detected(self):
        txn = {
            "player_id": "player-001",
            "direction": "DEBIT",
            "reference_type": "BET_STAKE",
        }

        self.assertFalse(rg_monitor.is_deposit_event(txn))


if __name__ == "__main__":
    unittest.main()
