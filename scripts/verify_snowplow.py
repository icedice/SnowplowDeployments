"""
This tool does 2 things:
1) Make downloading Snowplow files from S3 for given dates in a specific timezone easier.
2) Extract specific columns of the Snowplow events including exploding contexts into columns.

This tool is intended to be used for verifying Snowplow data against other tracking systems by
mangling Snowplow events into a format that is more easily comparable to, say, Google Analytics.

Prerequisites: pytz (pip install pytz)

Author: Asger Bachmann (asger.g.bachmann@jp.dk)
"""

from datetime import timedelta, datetime
from glob import iglob
import sys
import gzip
import json
from pytz import timezone


class Context(object):
    def __init__(self, schema_name, expected_properties):
        self.schema_name = schema_name
        self.expected_properties = expected_properties


def get_hour_deltas():
    raw_start_date = sys.argv[2]
    raw_end_date = sys.argv[3]

    start_date_format = "%Y%m%d" if len(raw_start_date) == 8 else "%Y%m%d%H"
    end_date_format = "%Y%m%d" if len(raw_end_date) == 8 else "%Y%m%d%H"

    cph_tz = timezone("Europe/Copenhagen")
    utc_tz = timezone("UTC")

    # From and including input in Europe/Copenhagen and used in UTC.
    start_date = cph_tz.localize(datetime.strptime(raw_start_date, start_date_format)).astimezone(utc_tz)
    # Up to but not including input in Europe/Copenhagen and used in UTC.
    end_date = cph_tz.localize(datetime.strptime(raw_end_date, end_date_format)).astimezone(utc_tz)

    delta = end_date - start_date
    for i in range(int(delta.seconds / 3600 + delta.days * 24)):
        delta_date = (start_date + timedelta(hours=i))
        suffix = "{}/{:02d}/{:02d}/{:02d}".format(delta_date.year, delta_date.month, delta_date.day, delta_date.hour)
        yield suffix


def print_cli_sync_cmds():
    base_s3_path = "s3://jpmedier-datalake/snowplow/"

    for s3_suffix in get_hour_deltas():
        s3_path = base_s3_path + s3_suffix
        cli_cmd = "aws s3 sync {} {}".format(s3_path, s3_suffix)
        print(cli_cmd)


def explode_contexts(contexts, contexts_to_extract):
    res = []

    for context_to_extract in contexts_to_extract:
        for context in contexts:
            if context["schema"].startswith("iglu:" + context_to_extract.schema_name):
                context_data = context["data"]
                for (expected_property, conv_fn) in context_to_extract.expected_properties:
                    if expected_property in context_data:
                        raw_property_val = context_data[expected_property]
                        try:
                            # Ensure the column has the correct type by attempting to convert it using the conv_fn.
                            converted_propery_val = conv_fn(raw_property_val)
                        except Exception as e:
                            # If this fails, use the conv_fn's default value as the property value (e.g. int() == 0).
                            converted_propery_val = conv_fn()

                        res.append(str(converted_propery_val))
                    else:
                        res.append("NOTSET")

                break
    
    return res


def quote(s):
    return "\"" + s.replace("\"", "\\\"") + "\""


def extract():
    sp_columns = ["app_id", "platform", "etl_tstamp", "collector_tstamp", "dvce_created_tstamp", "event", "event_id", "txn_id", "name_tracker", "v_tracker", "v_collector", "v_etl", "user_id", "user_ipaddress", "user_fingerprint", "domain_userid", "domain_sessionidx", "network_userid", "geo_country", "geo_region", "geo_city", "geo_zipcode", "geo_latitude", "geo_longitude", "geo_region_name", "ip_isp", "ip_organization", "ip_domain", "ip_netspeed", "page_url", "page_title", "page_referrer", "page_urlscheme", "page_urlhost", "page_urlport", "page_urlpath", "page_urlquery", "page_urlfragment", "refr_urlscheme", "refr_urlhost", "refr_urlport", "refr_urlpath", "refr_urlquery", "refr_urlfragment", "refr_medium", "refr_source", "refr_term", "mkt_medium", "mkt_source", "mkt_term", "mkt_content", "mkt_campaign", "contexts", "se_category", "se_action", "se_label", "se_property", "se_value", "unstruct_event", "tr_orderid", "tr_affiliation", "tr_total", "tr_tax", "tr_shipping", "tr_city", "tr_state", "tr_country", "ti_orderid", "ti_sku", "ti_name", "ti_category", "ti_price", "ti_quantity", "pp_xoffset_min", "pp_xoffset_max", "pp_yoffset_min", "pp_yoffset_max", "useragent", "br_name", "br_family", "br_version", "br_type", "br_renderengine", "br_lang", "br_features_pdf", "br_features_flash", "br_features_java", "br_features_director", "br_features_quicktime", "br_features_realplayer", "br_features_windowsmedia", "br_features_gears", "br_features_silverlight", "br_cookies", "br_colordepth", "br_viewwidth", "br_viewheight", "os_name", "os_family", "os_manufacturer", "os_timezone", "dvce_type", "dvce_ismobile", "dvce_screenwidth", "dvce_screenheight", "doc_charset", "doc_width", "doc_height", "tr_currency", "tr_total_base", "tr_tax_base", "tr_shipping_base", "ti_currency", "ti_price_base", "base_currency", "geo_timezone", "mkt_clickid", "mkt_network", "etl_tags", "dvce_sent_tstamp", "refr_domain_userid", "refr_device_tstamp", "derived_contexts", "domain_sessionid", "derived_tstamp", "event_vendor", "event_name", "event_format", "event_version", "event_fingerprint", "true_tstamp"]  # nopep8
    to_extract = [
        "app_id", "platform", "collector_tstamp",
        "domain_userid", "network_userid", "user_ipaddress",
        "domain_sessionid", "dvce_type",
        "os_name", "os_family", "page_url", "page_referrer",
        "br_name", "br_family"
    ]

    contexts_to_extract = [
        Context("com.google.analytics/cookies/", [("_ga", str)]),
        Context("dk.jyllands-posten/user/", [("anon_id", str), ("user_id", str), ("user_authenticated", str), ("user_authorized", str), ("ab_group", int)]),  # nopep8
        Context("dk.jyllands-posten/page_view/", [("section_id", int), ("section_name", str), ("content_id", int), ("content_type", str), ("page_name", str), ("page_type", str), ("page_restricted", str), ("site", str), ("sub_site", str), ("editorial_category", str)])  # nopep8
    ]
    contexts_cols_names = [c.schema_name + p for c in contexts_to_extract for (p, _) in c.expected_properties]

    contexts_idx = sp_columns.index("contexts")
    event_idx = sp_columns.index("event")
    to_extract_idx = [sp_columns.index(x) for x in to_extract]

    all_col_names = to_extract + contexts_cols_names
    quoted_col_names = (quote(n) for n in all_col_names)
    print(",".join(quoted_col_names))

    # Embarrassingly parallel over the dir_base values. If it takes too long, simply parallelize it.
    for dir_base in get_hour_deltas():
        for file_name in iglob(dir_base + "/*.gz"):
            with gzip.open(file_name, "rb") as f:
                for line in f:
                    cols = line.decode("utf-8").split("\t")

                    if cols[event_idx] != "page_view":
                        continue

                    extracted_cols = [cols[idx] for idx in to_extract_idx]

                    contexts = json.loads(cols[contexts_idx])["data"]
                    contexts_cols = explode_contexts(contexts, contexts_to_extract)

                    all_cols = extracted_cols + contexts_cols
                    quoted_cols = (quote(c) for c in all_cols)
                    print(",".join(quoted_cols))


def print_usage_and_exit():
    print("USAGE: python verify_snowplow.py {sync|extract} START_DATE END_DATE")
    print("")
    print("START_DATE and END_DATE must be Europe/Copenhagen dates in the format \"%Y%m%d\" or \"%Y%m%d%H\".")
    print("START_DATE is inclusive, END_DATE is exclusive to the hour.")
    print("")
    print("EXAMPLE: python generate_s3_paths.py 20170906 20170907")
    print("")
    print("EXAMPLE: python generate_s3_paths.py 2017090615 2017090710")
    exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print_usage_and_exit()
    elif sys.argv[1] == "sync":
        print_cli_sync_cmds()
    elif sys.argv[1] == "extract":
        extract()
    else:
        print_usage_and_exit()
