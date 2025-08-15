"""
Unit tests for core.utils
"""
# Externals
import unittest

# Internals
from apps.src.core.utils import cal_partition_dt


class testCalPartitionDt(unittest.TestCase):


    def test_cal_partition_dt(self):
        self.assertEqual(
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-10-01",
                num_partitions=10
            ),
            [
                "2025-08-07",
                "2025-09-25"
            ]
        )


if __name__ == '__main__':
    unittest.main()