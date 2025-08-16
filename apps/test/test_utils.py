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


if __name__ == '__main__':
    unittest.main()