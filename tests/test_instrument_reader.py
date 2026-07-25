import unittest

import pandas as pd

from autotrade.backtest.data.reader import EquityStateReader


class InstrumentReaderTests(unittest.TestCase):
    def read(self, frame):
        return list(EquityStateReader().read(frame))

    def test_undated_instrument_without_lifecycle_remains_bootstrap(self):
        frame = pd.DataFrame(
            [{"instrument_id": "AAA", "multiplier": 1}]
        )

        result = self.read(frame)

        self.assertEqual([state.instrument_id for state in result], ["AAA"])
        self.assertIsNone(result[0].time)
        self.assertTrue(result[0].is_active)

    def test_undated_instrument_with_lifecycle_is_expanded(self):
        frame = pd.DataFrame(
            [
                {
                    "instrument_id": "A",
                    "multiplier": 10,
                    "list_date": "2015-01-01",
                    "delist_date": "2015-01-20",
                }
            ]
        )

        result = self.read(frame)

        self.assertEqual(
            [pd.Timestamp(state.time) for state in result],
            [pd.Timestamp("2015-01-01"), pd.Timestamp("2015-01-20")],
        )
        self.assertEqual([state.is_active for state in result], [True, False])
        self.assertEqual([state.multiplier for state in result], [10, 10])

    def test_dated_instrument_without_lifecycle_keeps_input_states(self):
        frame = pd.DataFrame(
            [
                {"date": "2015-01-01", "instrument_id": "A", "multiplier": 10},
                {"date": "2015-01-09", "instrument_id": "A", "multiplier": 5},
            ]
        )

        result = self.read(frame)

        self.assertEqual(len(result), 2)
        self.assertEqual([state.is_active for state in result], [True, True])
        self.assertEqual([state.multiplier for state in result], [10, 5])

    def test_dated_instrument_delisting_copies_latest_state(self):
        frame = pd.DataFrame(
            [
                {
                    "date": "2015-01-01",
                    "instrument_id": "A",
                    "list_date": "2015-01-01",
                    "delist_date": "2015-01-20",
                    "multiplier": 10,
                },
                {
                    "date": "2015-01-09",
                    "instrument_id": "A",
                    "list_date": "2015-01-01",
                    "delist_date": "2015-01-20",
                    "multiplier": 5,
                },
            ]
        )

        result = self.read(frame)

        self.assertEqual(
            [pd.Timestamp(state.time) for state in result],
            [
                pd.Timestamp("2015-01-01"),
                pd.Timestamp("2015-01-09"),
                pd.Timestamp("2015-01-20"),
            ],
        )
        self.assertEqual(
            [state.is_active for state in result],
            [True, True, False],
        )
        self.assertEqual([state.multiplier for state in result], [10, 5, 5])


if __name__ == "__main__":
    unittest.main()
