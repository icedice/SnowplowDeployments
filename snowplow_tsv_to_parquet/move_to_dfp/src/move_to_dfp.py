import logging
import os
import sys
from datetime import date, datetime, time
from time import gmtime

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
from memory_profiler import profile
from s3fs import S3FileSystem

from input import read_page_views, read_page_pings, read_scroll_reach

S3_INPUT_BUCKET = os.environ['S3_INPUT_BUCKET']  # 'behavior-datalake' on prod.
S3_OUTPUT_BUCKET = os.environ['S3_OUTPUT_BUCKET']  # 'jyllandsposten-upload-prod' on prod
S3_OUTPUT_PREFIX = os.environ['S3_OUTPUT_PREFIX']  # 'snowplow_pageviews' on prod


def add_dt_cols(date_to_process: date, hour_to_process: int, table: pa.Table) -> pa.Table:
    def str_zfill(val: int, zfill: int):
        return pd.Series(str(val), index=range(len(table))).str.zfill(zfill)

    utc_dt = datetime.combine(date_to_process, time(hour_to_process), tzinfo=pytz.utc)
    cph_dt = utc_dt.astimezone(pytz.timezone('Europe/Copenhagen'))

    logging.info(f'Converted {date_to_process.isoformat()} {hour_to_process} UTC to {cph_dt.date().isoformat()}'
                 f' {cph_dt.time().hour} Europe/Copenhagen.')

    year = str_zfill(cph_dt.year, 4)
    month = str_zfill(cph_dt.month, 2)
    day = str_zfill(cph_dt.day, 2)
    hour = str_zfill(cph_dt.hour, 2)
    dt = pd.Series(cph_dt.date().isoformat(), index=range(len(table)))

    # noinspection PyCallByClass,PyTypeChecker
    return table \
        .append_column(pa.Column.from_array('year', year)) \
        .append_column(pa.Column.from_array('month', month)) \
        .append_column(pa.Column.from_array('day', day)) \
        .append_column(pa.Column.from_array('hour', hour)) \
        .append_column(pa.Column.from_array('dt', dt))


def add_brand_col(table: pa.Table) -> pa.Table:
    brand = pd.Categorical(np.where(table.column('site').to_pandas() == 'jyllands-posten.dk', 'jp', 'erhvervsmedier'))
    # noinspection PyCallByClass,PyTypeChecker
    return table.append_column(pa.Column.from_array('brand', brand))


def add_page_pings_enabled_col(table: pa.Table) -> pa.Table:
    # Page views with page pings enabled have the 'heartbeat' context added. The context also tells us how many seconds
    # there are between each page ping. For now, we just hard code that value to 30s but it can be extracted from the
    # heartbeat context if needed.
    page_pings_enabled = table.column('contexts').to_pandas()\
        .str.contains('iglu:dk.jyllands-posten/heartbeat/jsonschema/')
    # noinspection PyCallByClass,PyTypeChecker
    return table.append_column(pa.Column.from_array('page_pings_enabled', page_pings_enabled))


def write_dataset(df: pd.DataFrame, fs: S3FileSystem, threads: int) -> None:
    logging.info('Converting output to an Arrow table...')
    # noinspection PyArgumentList,PyUnresolvedReferences
    table = pa.Table.from_pandas(df=df, schema=pa.Schema.from_pandas(df), nthreads=threads)

    partitions = ['brand', 'year', 'month', 'dt']
    out_path = f's3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PREFIX}'

    logging.info(f'Writing data to {out_path}...')

    # Note that PyArrow does not support overwriting data so if you run this multiple times, it will create multiple
    # files for the same partitions.
    pq.write_to_dataset(table, root_path=out_path, filesystem=fs, partition_cols=partitions)


def get_and_preprocess_pvs(date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int) -> pd.DataFrame:
    logging.info(f'Reading page view data for {date_to_process.isoformat()} {hour_to_process:02d}...')
    pvs = read_page_views(S3_INPUT_BUCKET, date_to_process, hour_to_process, threads, fs)

    logging.info('Adding brand...')
    pvs = add_brand_col(pvs)

    logging.info('Adding date columns for use in partitioning...')
    pvs = add_dt_cols(date_to_process, hour_to_process, pvs)

    logging.info('Adding is page ping enabled column...')
    pvs = add_page_pings_enabled_col(pvs)

    # The context column must be dropped before writing the output as it contains sensitive information (namely SSOid)
    # embedded in the JSON which is somewhat difficult for the DFP to hash.
    pvs = pvs.drop(['contexts'])

    return pvs.to_pandas(use_threads=threads > 1)


def get_pps_per_pv(date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int) -> pd.Series:
    logging.info(f'Reading page ping data for {date_to_process.isoformat()} {hour_to_process:02d}...')

    pps_table = read_page_pings(S3_INPUT_BUCKET, date_to_process, hour_to_process, threads, fs)
    pps = pps_table.to_pandas(use_threads=threads > 1)

    logging.info('Finding page pings per page view...')
    pps_per_pv = pps.groupby('web_page_id').size()

    return pps_per_pv.rename('page_pings')


def get_max_scroll_reach_values(date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int)\
        -> pd.Series:
    """
    Gets max scroll reach values for each web_page_id.
    """
    logging.info(f'Reading scroll reach data for {date_to_process.isoformat()} {hour_to_process:02d}...')

    scroll_reach = read_scroll_reach(S3_INPUT_BUCKET, date_to_process, hour_to_process, threads, fs)

    # Convert the values from string to int and return only that.
    scroll_reach['scroll_reach'] = pd.to_numeric(scroll_reach['se_value'], errors='coerce')
    return scroll_reach.groupby('web_page_id')['scroll_reach'].max()


def add_timespent(pvs: pd.DataFrame, date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int)\
        -> pd.DataFrame:
    pps_per_pv = get_pps_per_pv(date_to_process, hour_to_process, fs, threads)

    pvs_with_pps_per_pv = pvs.join(pps_per_pv, how='left')

    # Pvs with pings enabled but without any page pings will be NaN in the joined DF. Set their number of pings to 0.
    should_have_pps_but_has_not = pvs['page_pings_enabled'] & pvs_with_pps_per_pv['page_pings'].isna()
    pvs_with_pps_per_pv['page_pings'] = np.where(should_have_pps_but_has_not, 0, pvs_with_pps_per_pv['page_pings'])

    # Finally, calculate time spent. For pvs without pps, the result will be NaN.
    pvs_with_pps_per_pv['time_spent'] = pvs_with_pps_per_pv['page_pings'] * 30

    return pvs_with_pps_per_pv


def add_scroll_reach(pvs: pd.DataFrame, date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int)\
        -> pd.DataFrame:
    max_scroll_reach_per_pv = get_max_scroll_reach_values(date_to_process, hour_to_process, fs, threads)
    return pvs.join(max_scroll_reach_per_pv, how='left')


@profile
def run(date_to_process: date, hour_to_process: int, fs: S3FileSystem, threads: int):
    pvs = get_and_preprocess_pvs(date_to_process, hour_to_process, fs, threads)

    pvs = pvs.set_index('web_page_id')

    logging.info('Adding time spent to each page view...')
    pvs = add_timespent(pvs, date_to_process, hour_to_process, fs, threads)

    logging.info('Adding max scroll reach to each page view...')
    pvs = add_scroll_reach(pvs, date_to_process, hour_to_process, fs, threads)

    pvs = pvs.reset_index()

    logging.info('Writing output...')
    write_dataset(pvs, fs, threads)

    logging.info('Done.')


def main():
    logging.Formatter.converter = gmtime
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    if len(sys.argv) < 3:
        logging.error('Need two arguments: 1) a date in format YYYY-mm-dd, 2) hour in the format HH.')
        exit(1)

    date_to_process_str = sys.argv[1]
    try:
        date_to_process = datetime.strptime(date_to_process_str, "%Y-%m-%d").date()
    except ValueError:
        raise RuntimeError(f'Could not parse input date {date_to_process_str} with format YYYY-mm-dd.')

    hour_to_process_str = sys.argv[2]
    try:
        hour_to_process = int(hour_to_process_str)
        if 0 < hour_to_process > 23:
            raise ValueError()
    except ValueError:
        raise RuntimeError(f'Could not parse input hour {hour_to_process_str} with format HH.')

    # Construct a S3 connection using the default credentials provider chain from boto3. The ACL is required for DFP to
    # own the files when uploading them.
    fs = S3FileSystem(s3_additional_kwargs={'ACL': 'bucket-owner-full-control'})
    run(date_to_process, hour_to_process, fs, threads=8)


if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        logging.exception(ex)
