"""
Takes as input a number of file names containing gzipped error events from Snowplow's enricher.
Output the events in a JSON format, one event per line. The output can be parsed with e.g. jq.

Example:
    python3 error_events_to_json.py *.gz | jq 'select(.qs.url != null) | .qs.url | .[]' | grep 'IAtrackingPlugin' | wc -l
This command will find the number of error events with "IAtrackingPlugin" in the URL query parameter sent to Snowplow's
collector.

Prerequisites: thriftpy (pip install thriftpy)

Author: Asger Bachmann (asger.g.bachmann@jp.dk)
"""

import base64
import json
from urllib import parse
import sys
import gzip

import thriftpy
from thriftpy.protocol import TCyBinaryProtocolFactory
from thriftpy.utils import deserialize


def decode_event(data, collector_payload):
    json_payload = json.loads(data.decode('utf-8'))
    decoded_thrift_payload = base64.b64decode(json_payload['line'])
    thrift_payload = deserialize(collector_payload, decoded_thrift_payload, TCyBinaryProtocolFactory())

    qs = thrift_payload.querystring
    event = {
        'qs': parse.parse_qs(qs),
        'errors': json_payload['errors']
    }
    return event


def main():
    file_names = sys.argv[1:]

    collector = thriftpy.load('collector-payload.thrift')
    collector_payload = collector.CollectorPayload()

    for file_name in file_names:
        try:
            with gzip.open(file_name) as in_file:
                for event in in_file:
                    decoded_event = decode_event(event, collector_payload)

                    print(json.dumps(decoded_event))
        except Exception as e:
            print('Could not parse file {}: {}'.format(file_name, e), file=sys.stderr)


if __name__ == '__main__':
    main()
