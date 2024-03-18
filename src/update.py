import argparse
import glob
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta

import awswrangler as wrangler
import boto3
import pandas as pd
import yaml

AWS_BUCKET = os.environ['AWS_BUCKET']
AWS_BUCKET_DIR = os.environ.get('AWS_BUCKET_DIR', 'nasdaq')
AWS_COMPRESSION_TYPE = 'gzip'
DATE_STRING_FORMAT = '%Y-%m-%d'
SPEC_FILES_PATH = '/schemas/*'


@dataclass
class Table:
    name: str
    update_field: str
    partition_cols: list[str]
    schema: dict[str: str]

    def __post_init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._s3_key = (f"{AWS_BUCKET_DIR}/{self.name}"
                        f"{'/' + self.name.split('/')[1] + '.parquet' if not self.update_field else ''}")

        # If there's an update key, check that there's an existing parquet directory to append to.
        if self.update_field:
            logger.debug(f'Found update field `{self.update_field}` for {self.name}; checking for S3 object...')
            try:
                response = s3_client.list_objects(Bucket=AWS_BUCKET, Prefix=self.s3_key, MaxKeys=1)
                if 'Contents' in response:
                    logger.debug(f'S3 resource check passed for {self.name}; using `append` mode.')
                    self._refresh_mode = 'overwrite_partitions'
                else:
                    logger.debug(f'S3 resource check failed for {self.name}; re-creating table.')
                    self._refresh_mode = 'overwrite'
            except Exception as e:
                raise e
        else:
            logger.debug(f'No update field provided for {self.name}; using `overwrite` mode.')
            self._refresh_mode = 'overwrite'

    def _get_ndl_data(self, _date: str) -> None:
        if self.refresh_mode in ['append', 'overwrite_partitions']:
            # Get only update_key=date rows.
            logger.info(f'Downloading new rows for: {self.name} ...')
            try:
                self._data = ndl.get_table(f"{self.name}", paginate=True, **{self.update_field: _date})
            except Exception as e:
                logger.error(f'Failed to get new rows for {self.name}: {e}')
                raise e
        else:
            # Download the zip extract.
            file_name = f"{self.name.replace('/', '.')}.zip"
            logger.info(f'Downloading table extract: {self.name} ...')
            try:
                ndl.export_table(f"{self.name}", filename=file_name)
                self._data = pd.read_csv(file_name)
            except Exception as e:
                logger.error(f'Failed to get new data for {self.name}: {e}')
                raise e
            finally:
                if os.path.exists(file_name):
                    os.remove(file_name)
        logger.info(f'Fetched {len(self._data)} rows: {self.name}.')

    def _apply_transforms(self) -> None:
        for column, dtype in self.schema.items():
            if self.partition_cols:
                if column in self.partition_cols and dtype == 'datetime64[ns]':
                    self._data[column] = self._data[column].astype(dtype)
                    self._data['yyyy'] = self._data[column].apply(lambda x: x.year)
                    self._data['mm'] = self._data[column].apply(lambda x: x.month)
                    self._data['dd'] = self._data[column].apply(lambda x: x.day)
                    self._data.drop(column, axis=1, inplace=True)
                    self.partition_cols.remove(column)
                    self.partition_cols = ['yyyy', 'mm', 'dd']
            elif dtype == 'datetime64[ns]':
                self._data[column] = self._data[column].astype(dtype).dt.strftime('%Y-%m-%d')
            else:
                logger.debug(f'Cast: {column} to {dtype}')
                self._data[column] = self._data[column].astype(dtype)

    def _put_to_aws(self) -> None:
        kwargs = {
            'df': self._data,
            'path': f"s3://{AWS_BUCKET}/{self.s3_key}",
            'index': False,
            'compression': AWS_COMPRESSION_TYPE,
        }
        if self.update_field:
            # Use the dataset option and 'append' mode for time series data.
            kwargs.update({
                'dataset': True,
                'mode': self.refresh_mode,
                'partition_cols': self.partition_cols,
                'concurrent_partitioning': True
            })
        logger.info(f'Uploading {self.name}...')
        try:
            wrangler.s3.to_parquet(**kwargs)
            logger.info(f'Finished updating {self.name}.')
        except Exception as e:
            logger.error(f'Upload failed for {self.name}; {e}')
            raise e

    @property
    def s3_key(self) -> str:
        """
        The path this object will write to in its target S3 bucket.
        :returns: a directory path
        """
        return self._s3_key

    @property
    def refresh_mode(self) -> str:
        """
        The mode setting that will be used by the awswrangler `to_parquet` method.
        :returns: `append` or `overwrite`
        """
        return self._refresh_mode

    def refresh(self, date: str) -> None:
        """
        Update this table in AWS S3 with new data from Nasdaq Data Link.
        :date: a YYYY-MM-DD string representing the date to update.
        """
        self._get_ndl_data(_date=date)
        self._apply_transforms()
        self._put_to_aws()


class Dataset(dict):
    """
    Creates a dict mapping for a given Nasdaq Datalink dataset from YAML specification. Each key is the name of a table,
    and each value is a dict, itself mapping keys and values to table metadata.
    """

    def __init__(self, file_path: str):
        super().__init__()
        global logger
        assert logger, 'Missing logger object; please assign.'
        with open(file_path, 'r') as specification:
            self.update(yaml.safe_load(specification))
        logger.info(f'Loaded {len(self)} tables: {', '.join(self.keys())}.')
        self._dataset_name = file_path.replace('.yaml', '').upper().split('/').pop()
        logger.info(f'Dataset name: {self._dataset_name}.')

    @property
    def dataset_name(self) -> str:
        """
        Get the name of the dataset as provided by the YAML config file.
        :returns: the uppercase dataset name
        """
        return self._dataset_name

    def update_tables(self, as_of_date: str) -> None:
        """
        Update the S3 data for all tables defined in this object.
        """
        for table_name, metadata in self.items():
            try:
                table = Table(name=f'{self.dataset_name}/{table_name}',
                              update_field=metadata['update_field'],
                              partition_cols=metadata['partition_cols'],
                              schema=metadata['schema'])
                table.refresh(as_of_date)
            except Exception as e:
                logger.error(f'Failed to refresh {self.dataset_name}/{table_name}: {e}')
                continue


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_handler = logging.StreamHandler()
    log_formatter = logging.Formatter(fmt=' %(name)s :: %(levelname)-8s :: %(message)s')

    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)

    assert os.environ.get('NASDAQ_DATA_LINK_API_KEY'), logger.error(
        'Pre-check failed: NASDAQ_DATA_LINK_API_KEY not found.')
    import nasdaqdatalink as ndl

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date',
                        default=(datetime.today() - timedelta(days=1)).strftime(DATE_STRING_FORMAT),
                        help='The date to update for, in string format. Defaults to T-1.')

    s3_client = boto3.client('s3')
    update_date: str = parser.parse_args().date

    config_file_paths = glob.glob(f"{os.path.dirname(os.path.realpath(__file__))}{SPEC_FILES_PATH}")
    for config_file_path in config_file_paths:
        dataset = Dataset(file_path=config_file_path)
        dataset.update_tables(as_of_date=update_date)
