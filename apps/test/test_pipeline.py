"""
Unit tests for pipeline obj
"""
# Externals
import logging
import unittest
from datetime import datetime

# Internals
from apps.core.pipeline import BaseDataPipeline


# Configure logging to console
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')

class TestDataPipeline(BaseDataPipeline):

    def __init__(self, as_of_date):
        super().__init__(as_of_date)

    def run(self):
        pass

testObjc = TestDataPipeline(as_of_date=datetime.today().strftime('%Y-%m-%d'))

class TestPipeline(unittest.TestCase):

    def test_df_shape_check(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] _df_shape_check [SUCCESS]\n")

        self.assertEqual(
            testObjc._df_shape_check(
            ),
            (False, {})
        )


    def test_df_size_check(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] _df_size_check [SUCCESS]\n")

        self.assertEqual(
            testObjc._df_size_check(
            ),
            (False, {})
        )


    def test_df_values_check(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] _df_values_check [SUCCESS]\n")

        self.assertEqual(
            testObjc._df_values_check(
            ),
            (False, {})
        )


    def test_df_keys_check(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] _df_keys_check [SUCCESS]\n")

        self.assertEqual(
            testObjc._df_keys_check(
            ),
            (False, {})
        )


    def test_df_null_check(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] _df_null_check [SUCCESS]\n")

        self.assertEqual(
            testObjc._df_null_check(
            ),
            (False, {})
        )

    
    def test_enforced_dqc_checks(self):
        logger = logging.getLogger(__name__)
        logger.info("\n[UNITTEST].[Pipeline].[dqc] enforced_dqc_checks [SUCCESS]\n")

        self.assertEqual(
            testObjc.enforced_dqc_checks(),
            (False, {})
        )
        