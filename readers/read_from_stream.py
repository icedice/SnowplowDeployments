from datetime import datetime, timedelta
import json
import time

import boto3

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
            try:
                record_response = client.get_records(ShardIterator=shard_iterators[i])
                
                now = datetime.now()
                print('Time: {0}'.format(now.strftime('%Y/%m/%d %H:%M:%S')))

                for record in record_response['Records']:
                    last_sequences[i] = record['SequenceNumber']
                    yield record['Data']

                # Get the next iterator for the current shard from the response.
                shard_iterators[i] = record_response['NextShardIterator']
            except ClientError as err:
                backoffExceptions = ['ProvisionedThroughputExceededException', 'ThrottlingException']
                if err.response['Error']['Code'] in backoffExceptions:
                    print('Calling Kinesis too often. Backing off...')
                    time.sleep(5)
                else:
                    raise err
                    
            time.sleep(1)

    kinesis.close()

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        stream_name = sys.argv[1]
    else:
        stream_name = 'Dev-enriched_good'

    if len(sys.argv) > 2:
        iterator_type = sys.argv[2]
    else:
        iterator_type = "LATEST"

    print("USING PARAMETERS {} and {}".format(stream_name, iterator_type))
    kinesis_data = get_kinesis_data_iterator(stream_name, iterator_type)

    for data in kinesis_data:
        print(data)
