"""
"""
import pandas as pd

from io import StringIO
from unittest import TestCase
from pre_post_launch.data_preprocessing import DataPreprocessing
from pre_post_launch.tests.stub_data.stub_data_preprocessing import df_clean_and_generate_tags_based_on_genres

class TestDataPreProcessing(TestCase):
    """
    Tests for the data pre-processing module and class
    """

    def test_clean_and_generate_tags_based_on_genres(self):
        """
        Determine if cleaning and tag generation works as expected for genres
        """
        df = df_clean_and_generate_tags_based_on_genres

        data_processing = DataPreprocessing(df.copy())
        # data_processing.clean_and_generate_tags_based_on_genres()

        self.assertIn('comedy', df['DESCRIPTIVE_GENRE_DESC_AGG'].tolist()[0])
        self.assertIn('tag:comedy', data_processing.base.columns)
        self.assertNotIn('comedy', data_processing.base.columns)

if __name__ == '__main__':
    unittest.main()
