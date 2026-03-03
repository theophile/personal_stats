import unittest

import pandas as pd

from webapp.charts import duration_violin_chart, partner_orgasms_chart, position_association_chart, rolling_anomaly_chart, sex_streaks_chart


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


class AdditionalChartsTest(unittest.TestCase):
    def test_partner_chart_adds_milestone_shapes(self):
        df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-01"), "total_org_partner": 1, "trend": 1.0},
                {"date": pd.Timestamp("2024-01-02"), "total_org_partner": 2, "trend": 1.5},
            ]
        )
        fig = partner_orgasms_chart(df, milestones=[(pd.Timestamp("2024-01-02"), "Event")])
        self.assertGreaterEqual(len(fig.layout.shapes or []), 1)

    def test_duration_violin_chart_with_data(self):
        df = pd.DataFrame(
            [
                {"partner": "Alice", "duration": 30},
                {"partner": "Alice", "duration": 60},
                {"partner": "Beth", "duration": 20},
            ]
        )
        fig = duration_violin_chart(df)
        self.assertGreaterEqual(len(fig.data), 1)

    def test_rolling_anomaly_chart_marks_points(self):
        df = pd.DataFrame(
            [
                {"date": pd.Timestamp("2024-01-01"), "value": 1, "baseline": 1.0, "zscore": 0.0, "is_anomaly": 0},
                {"date": pd.Timestamp("2024-01-02"), "value": 5, "baseline": 2.0, "zscore": 2.5, "is_anomaly": 1},
            ]
        )
        fig = rolling_anomaly_chart(df)
        self.assertGreaterEqual(len(fig.data), 2)

    def test_position_association_chart(self):
        df = pd.DataFrame(
            [
                {
                    "antecedent": "A",
                    "consequent": "B",
                    "support": 0.5,
                    "confidence": 0.75,
                    "lift": 1.25,
                    "count": 3,
                }
            ]
        )
        fig = position_association_chart(df)
        self.assertGreaterEqual(len(fig.data), 1)


if __name__ == "__main__":
    unittest.main()
