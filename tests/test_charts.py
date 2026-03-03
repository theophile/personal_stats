import unittest

import pandas as pd

from webapp.charts import sex_streaks_chart


class SexStreaksChartTest(unittest.TestCase):
    def test_bars_are_centered_over_streak_interval_without_overlap(self):
        df = pd.DataFrame(
            [
                {"start_date": "2024-01-01", "length": 2, "signed_length": 2, "type": "sex"},
                {"start_date": "2024-01-03", "length": 3, "signed_length": -3, "type": "no_sex"},
                {"start_date": "2024-01-06", "length": 1, "signed_length": 1, "type": "sex"},
            ]
        )

        fig = sex_streaks_chart(df)

        intervals = []
        for trace in fig.data:
            center = pd.Timestamp(trace.x[0])
            width_days = int(trace.width[0] / 86400000)
            left = center - pd.Timedelta(days=width_days / 2)
            right = center + pd.Timedelta(days=width_days / 2)
            intervals.append((left, right))

        intervals.sort(key=lambda it: it[0])

        self.assertEqual(len(intervals), 3)
        self.assertEqual(intervals[0][0], pd.Timestamp("2024-01-01"))
        self.assertEqual(intervals[0][1], pd.Timestamp("2024-01-03"))
        self.assertEqual(intervals[1][0], pd.Timestamp("2024-01-03"))
        self.assertEqual(intervals[1][1], pd.Timestamp("2024-01-06"))
        self.assertEqual(intervals[2][0], pd.Timestamp("2024-01-06"))
        self.assertEqual(intervals[2][1], pd.Timestamp("2024-01-07"))

    def test_chart_x_values_are_json_safe(self):
        df = pd.DataFrame(
            [{"start_date": "2024-01-01", "length": 2, "signed_length": 2, "type": "sex"}]
        )

        fig = sex_streaks_chart(df)

        self.assertIsInstance(fig.data[0].x[0], str)


if __name__ == "__main__":
    unittest.main()
