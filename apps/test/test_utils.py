"""
Unit tests for utils
"""
import logging
import unittest

from apps.core.utils import (
    cal_partition_dt,
    read_module_file,
)

# Configure logging to console and packages paths
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")

class TestUtils(unittest.TestCase):
    def test_cal_partition_dt(self):
        """
        Find optimal partition date range for spark parallelism
        """
        self.assertEqual(
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-10-01",
                num_partitions=10
            ),
            ["2025-08-07", "2025-09-25"],
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
                num_partitions=2)
        self.assertEqual(str(cm.exception), "Cannot find valid partition dates!")

        with self.assertRaises(ValueError) as cm:
            cal_partition_dt(
                from_dt="2025-08-01",
                to_dt="2025-08-02",
                num_partitions=1)
        self.assertEqual(str(cm.exception), "Cannot find valid partition dates!")

        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Utils].[function] cal_partition_dt [SUCCESS]\n")


    def test_read_module_file(self):
        """
        Read file from module
        """
        self.assertEqual(
            read_module_file(
                file_path="data/test_sql.sql",
                caller_path=__file__
            ).format(
                table_name="abc",
                partition_column="ccc",
                from_dt="12314",
                to_dt="31423"
            ),
            "SELECT * FROM abc WHERE ccc BETWEEN 12314 AND 31423",
        )

        """
        Read file from test module
        """
        self.assertEqual(
            len(
                read_module_file(
                    file_path="data/ep-popolo-v1.0.json",
                    caller_path=__file__
                )
            ),
            9_832_951,
        )

        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Utils].[function] read_module_file [SUCCESS]\n")


if __name__ == "__main__":
    unittest.main()
