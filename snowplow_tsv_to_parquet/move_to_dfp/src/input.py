import logging
from datetime import date
from typing import Set, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem


def read_table(s3_bucket: str, event: str, date_to_process: date, hour_to_process: int, read_nthreads: int,
               fs: S3FileSystem, col_whitelist: Optional[Set[str]]) -> pa.Table:
    in_path = f's3://{s3_bucket}/snowplow/event={event}/date={date_to_process}/hour={hour_to_process:02d}'
    logging.info(f'Reading {event} data from input path {in_path}.')

    try:
        ds = pq.ParquetDataset(in_path, filesystem=fs, metadata_nthreads=read_nthreads)
        return ds.read(columns=col_whitelist)
    except OSError:
        raise RuntimeError(f'Unexpected error occurred when reading {event} data.' +
                           f' Make sure the path {in_path} exists and that you have access to it.')


def read_page_views(s3_bucket: str, date_to_process: date, hour_to_process: int, read_nthreads: int,
                    fs: S3FileSystem) -> pa.Table:
    # Only keep whitelisted columns that DFP are not interested in to keep the output as simple as possible.
    col_whitelist = {'collector_tstamp', 'event_type', 'user_ipaddress', 'network_id', 'geo_country', 'geo_city',
                     'geo_region_name', 'geo_zipcode', 'page_url', 'page_title', 'page_referrer', 'page_urlscheme',
                     'page_urlhost', 'page_urlpath', 'page_urlquery', 'page_urlfragment', 'refr_urlscheme',
                     'refr_urlhost', 'refr_urlpath', 'refr_urlquery', 'refr_urlfragment', 'refr_medium', 'refr_source',
                     'refr_term', 'mkt_medium', 'mkt_source', 'mkt_term', 'mkt_content', 'mkt_campaign', 'mkt_clickid',
                     'mkt_network', 'useragent', 'br_name', 'br_family', 'br_version', 'os_name', 'os_family',
                     'dvce_type', 'domain_sessionid', 'derived_tstamp', 'anon_id', 'user_id', 'user_authorized',
                     'grp_authorized', 'user_authenticated', 'grp_authenticated', 'site', 'content_id', 'section_id',
                     'section_name', 'section_path_id', 'page_restricted', 'web_page_id', 'contexts', 'app_id'}
    return read_table(s3_bucket, 'page_view', date_to_process, hour_to_process, read_nthreads, fs, col_whitelist)


def read_page_pings(s3_bucket: str, date_to_process: date, hour_to_process: int, read_nthreads: int,
                    fs: S3FileSystem) -> pa.Table:
    col_whitelist = {'web_page_id'}
    return read_table(s3_bucket, 'page_ping', date_to_process, hour_to_process, read_nthreads, fs, col_whitelist)


def read_scroll_reach(s3_bucket: str, date_to_process: date, hour_to_process: int, read_nthreads: int,
                      fs: S3FileSystem) -> pd.DataFrame:
    col_whitelist = {'se_category', 'se_action', 'se_value', 'web_page_id'}
    table = read_table(s3_bucket, 'struct', date_to_process, hour_to_process, read_nthreads, fs, col_whitelist)
    df = table.to_pandas(use_threads=read_nthreads > 1)

    filter_mask = (df['se_category'] == 'user_activity') & (df['se_action'] == 'article_scroll_reach')
    return df[filter_mask][['web_page_id', 'se_value']]
