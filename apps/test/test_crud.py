"""
Unit tests for utils
"""
# Externals
import logging
import unittest

# Internals
from apps.core.crud.minio_lake import (
    minio_write, 
    minio_read,
)

from apps.core.crud.postgres_ops import (
    ops_read, 
    ops_write,
)

# Configure logging to console
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')


class TestCRUD(unittest.TestCase):

    def test_minio_write(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[CRUD].[function] minio_write [SUCCESS]\n")

        """
        Read file from default core module
        """
        self.assertEqual(
            minio_write(
                df=df,
                path="",
                partition_cols="",
            ),
            True
        )


    def test_minio_read(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[CRUD].[function] minio_read [SUCCESS]\n")

        """
        Read file from default core module
        """
        self.assertEqual(
            minio_read(
                path="",
            ),
            True
        )

    
    def test_ops_read(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[CRUD].[function] ops_read [SUCCESS]\n")

        """
        Read file from default core module
        """
        self.assertEqual(
            ops_read(
                sql_query="",
                config="",
                parallel="",
                partition_column="",
                lower_bound="",
                upper_bound="",
                num_partitions="",
                fetch_size="",
            ),
            True
        )


    def test_ops_write(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[CRUD].[function] ops_write [SUCCESS]\n")

        """
        Read file from default core module
        """
        self.assertEqual(
            ops_write(
                dbtable="rental_gold",
                df="",
                config="",
                options={
                    "batchsize": "20000",
                    "rewriteBatchedStatements": "true",
                    "isolationLevel": "NONE",
                    "numPartitions": "5",
                    "truncate": "true",
                    "compression": "gzip",
                }
            ),
            True
        )


if __name__ == '__main__':
    unittest.main()
