"""
Unit tests for utils
"""
# Externals
import logging
import unittest

# Internals
from apps.core.utils import cal_partition_dt


# Configure logging to console
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

class testCalPartitionDt(unittest.TestCase):

    def test_cal_partition_dt(self):
        logger = logging.getLogger(__name__)
        logger.info("\n\n[UNITTEST].[Utils].[function] cal_partition_dt [SUCCESS]")

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

        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-11-01",
                to_dt="2025-10-01",
                num_partitions=10
            )
        self.assertEqual(str(cm.exception), "num_partitions too large for date bounds!")


        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-11-01",
                to_dt="2025-11-01",
                num_partitions=10
            )
        self.assertEqual(str(cm.exception), "num_partitions too large for date bounds!")


        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-08-02",
                num_partitions=10
            )
        self.assertEqual(str(cm.exception), "num_partitions too large for date bounds!")


        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-08-03",
                num_partitions=2
            )
        self.assertEqual(str(cm.exception), "Cannot find valid partition dates!")


        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-08-02",
                num_partitions=1
            )
        self.assertEqual(str(cm.exception), "Cannot find valid partition dates!")


if __name__ == '__main__':
    unittest.main()