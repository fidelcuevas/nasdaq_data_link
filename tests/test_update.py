import unittest
from unittest.mock import patch

import pandas as pd

from src.update import Table, Dataset


class TestTable(unittest.TestCase):

    @patch('update.os')
    @patch('update.logger')
    @patch('update.s3_resource')
    @patch('update.ndl')
    @patch('update.pd')
    @patch('update.wrangler')
    def test_refresh(self, mock_wrangler, mock_pd, mock_ndl, mock_s3_resource, mock_logger, mock_os):
        mock_os.path.exists.return_value = True
        mock_os.remove.return_value = None
        mock_pd.read_csv.return_value = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        mock_ndl.get_table.return_value = pd.DataFrame({'col1': [4, 5, 6], 'col2': ['d', 'e', 'f']})
        mock_wrangler.s3.to_parquet.return_value = None

        # Test refresh with update field
        table = Table(name='test_table', update_field='last_updated', partition_cols=['date'])
        table.refresh(date='2024-03-01')
        mock_ndl.get_table.assert_called_once_with('test_table', paginate=True, last_updated='2024-03-01')
        mock_wrangler.s3.to_parquet.assert_called_once()

        # Test refresh without update field
        table = Table(name='test_table', update_field='', partition_cols=['date'])
        table.refresh(date='2024-03-01')
        mock_wrangler.s3.to_parquet.assert_called_with(df=mock_pd.read_csv.return_value,
                                                        path=f"s3://skycastle-data/nasdaq/test_table.parquet",
                                                        index=False,
                                                        compression='gzip')

class TestDataset(unittest.TestCase):

    @patch('update.os')
    @patch('update.logger')
    @patch('update.s3_resource')
    @patch('update.ndl')
    @patch('update.pd')
    @patch('update.wrangler')
    def test_update_tables(self, mock_wrangler, mock_pd, mock_ndl, mock_s3_resource, mock_logger, mock_os):
        mock_os.path.exists.return_value = True
        mock_os.remove.return_value = None
        mock_pd.read_csv.return_value = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        mock_ndl.get_table.return_value = pd.DataFrame({'col1': [4, 5, 6], 'col2': ['d', 'e', 'f']})
        mock_wrangler.s3.to_parquet.return_value = None

        # Mock Dataset object
        dataset = Dataset(file_path='/schemas/test_dataset.yaml')
        dataset.update_tables()
        self.assertEqual(mock_logger.info.call_count, 6)
        self.assertEqual(mock_logger.warning.call_count, 1)

if __name__ == '__main__':
    unittest.main()
