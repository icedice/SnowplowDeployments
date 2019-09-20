import time
from datetime import datetime

import boto3
import botocore


def get_kinesis_data_iterator(stream_name, iterator_type):
    client = boto3.client('kinesis')
    kinesis_stream = client.describe_stream(StreamName=stream_name)
    shards = kinesis_stream['StreamDescription']['Shards']
    shard_ids = [shard['ShardId'] for shard in shards]

    iter_responses = [client.get_shard_iterator(StreamName=stream_name, ShardId=shard_id, ShardIteratorType=iterator_type) for shard_id in shard_ids]
    shard_iterators = [iter_response['ShardIterator'] for iter_response in iter_responses]

    last_sequences = [None for _ in shard_iterators]

    while True:
        for i in range(len(shard_iterators)):
            if shard_iterators[i] is None:
                continue

            try:
                record_response = client.get_records(ShardIterator=shard_iterators[i])
                now = datetime.now()

                for record in record_response['Records']:
                    last_sequences[i] = record['SequenceNumber']
                    yield now, record['Data']

                # Get the next iterator for the current shard from the response.
                if 'NextShardIterator' in record_response:
                    shard_iterators[i] = record_response['NextShardIterator']
                else:
                    print(f'Shard {shard_ids[i]} closed.')
                    shard_iterators[i] = None
            except botocore.exceptions.ClientError as err:
                backoff_exceptions = ['ProvisionedThroughputExceededException', 'ThrottlingException']
                if err.response['Error']['Code'] in backoff_exceptions:
                    print('Calling Kinesis too often. Backing off...')
                    time.sleep(5)
                else:
                    raise err

        time.sleep(1)


def print_thrift(timestamp, data):
    json_payload = json.loads(data.decode('utf-8'))
    decoded_thrift_payload = base64.b64decode(json_payload['line'])
    thrift_payload = deserialize(collector_payload, decoded_thrift_payload, TCyBinaryProtocolFactory())
    print('{}: {}, {}'.format(timestamp, thrift_payload, json_payload['errors']))


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    else:
        stream_name = 'Dev-enriched_good'

    if len(sys.argv) > 2:
        iterator_type = sys.argv[2]
    else:
        iterator_type = 'LATEST'

    if stream_name.endswith('_bad'):
        decode_thrift = True
    else:
        decode_thrift = False

    print('USING PARAMETERS {}, {} and {}.'.format(stream_name, iterator_type, decode_thrift))
    kinesis_data = get_kinesis_data_iterator(stream_name, iterator_type)

    if decode_thrift:
        import base64
        import thriftpy
        from thriftpy.protocol import TCyBinaryProtocolFactory
        from thriftpy.utils import deserialize
        import json
        collector = thriftpy.load('collector-payload.thrift')
        collector_payload = collector.CollectorPayload()

    for timestamp, data in kinesis_data:
        if decode_thrift:
            print_thrift(timestamp, data)
        else:
            print('{}: {}'.format(timestamp, data))
