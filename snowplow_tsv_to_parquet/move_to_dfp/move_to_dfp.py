import logging
import sys
import time
import os
from datetime import date, datetime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem


S3_INPUT_BUCKET = os.environ['S3_INPUT_BUCKET']  # 'behavior-datalake' on prod.
S3_OUTPUT_BUCKET = os.environ['S3_OUTPUT_BUCKET']  # 'jyllandsposten-upload-prod' on prod
S3_OUTPUT_PREFIX = os.environ['S3_OUTPUT_PREFIX']  # 'snowplow_pageviews' on prod


def add_dt_cols(date_to_process: date, table: pa.Table) -> pa.Table:
    def str_zfill(val: int, zfill: int):
        return pd.Series(str(val), index=range(len(table))).str.zfill(zfill)

    year = str_zfill(date_to_process.year, 4)
    month = str_zfill(date_to_process.month, 2)
    day = str_zfill(date_to_process.day, 2)
    dt = pd.Series(date_to_process.isoformat(), index=range(len(table)))

    return table\
        .append_column(pa.Column.from_array('year', year))\
        .append_column(pa.Column.from_array('month', month))\
        .append_column(pa.Column.from_array('day', day))\
        .append_column(pa.Column.from_array('dt', dt))


def add_brand_col(table: pa.Table) -> pa.Table:
    brand = pd.Categorical(np.where(table.column('site').to_pandas() == 'jyllands-posten.dk', 'jp', 'erhvervsmedier'))
    return table.append_column(pa.Column.from_array('brand', brand))


def convert_tstamps(table: pa.Table) -> pa.Table:
    # Some tstamps must be converted to ms (instead of ns) to avoid errors from Arrow.
    tstamps_to_convert = ['refr_device_tstamp', 'true_tstamp']
    new_tstamps = [table.column(c).to_pandas().astype('datetime64[ms]') for c in tstamps_to_convert]

    table = table.drop(tstamps_to_convert)
    # Add all the new tstamp columns.
    for (col_name, new_col) in zip(tstamps_to_convert, new_tstamps):
        table = table.append_column(pa.Column.from_array(col_name, new_col))

    return table


def drop_contexts(table: pa.Table) -> pa.Table:
    # 'contexts' should be dropped entirely because it contains sensitive information (namely SSOid) embedded in the
    # JSON which is somewhat difficult for the DFP to hash.
    return table.drop(['contexts'])


def read_table(date_to_process: date, read_nthreads: int, fs: S3FileSystem) -> pa.Table:
    in_date_path = date_to_process.strftime('year=%Y/month=%m/day=%d')
    in_path = 's3://{}/snowplow/event=page_view/{}'.format(S3_INPUT_BUCKET, in_date_path)
    logging.info('Reading data for input path {}.'.format(in_path))

    try:
        ds = pq.ParquetDataset(in_path, filesystem=fs, metadata_nthreads=read_nthreads)
    except OSError as ex:
        logging.exception('Unexpected error occured when reading data.' +
                          ' Make sure the path {} exists and that you have access to it.'.format(in_path))
        exit(1)

    return ds.read()


def write_dataset(table: pa.Table, fs: S3FileSystem) -> None:
    partitions = ['brand', 'year', 'month', 'dt']
    out_path = 's3://{}/{}'.format(S3_OUTPUT_BUCKET, S3_OUTPUT_PREFIX)

    logging.info('Writing data to {}...'.format(out_path))

    # Note that PyArrow does not support overwriting data so if you run this multiple times, it will create multiple files
    # for the same paritions.
    pq.write_to_dataset(table, root_path=out_path, filesystem=fs, partition_cols=partitions)


def main(date_to_process: date, fs: S3FileSystem, read_nthreads: int):
    logging.info('Reading data for {}...'.format(date_to_process.isoformat()))
    table = read_table(date_to_process, read_nthreads, fs)

    logging.info('Dropping contexts...')
    table = drop_contexts(table)

    logging.info('Converting timestamps...')
    table = convert_tstamps(table)

    logging.info('Adding brand...')
    table = add_brand_col(table)

    logging.info('Adding date columns for use in partitioning...')
    table = add_dt_cols(date_to_process, table)

    logging.info('Writing output...')
    write_dataset(table, fs)

    logging.info('Done.')


if __name__ == '__main__':
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    if len(sys.argv) < 2:
        logging.error('Need one argument: a date in format YYYY-mm-dd.')
        exit(1)

    date_to_process_str = sys.argv[1]
    try:
        date_to_process = datetime.strptime(date_to_process_str, "%Y-%m-%d").date()
    except ValueError as ex:
        logging.exception('Could not parse input date {} with format YYYY-mm-dd.'.format(date_to_process_str))

    # Construct a S3 connection using the default credentials provider chain from boto3. The ACL is required for DFP to
    # own the files when uploading them.
    fs = S3FileSystem(s3_additional_kwargs={'ACL': 'bucket-owner-full-control'})
    main(date_to_process, fs, read_nthreads=8)
