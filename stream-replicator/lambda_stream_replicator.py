"""
Takes an input stream and replicates a fraction of Snowplow records to an output stream, changing the records' app_id in the process.
"""
import base64, json, boto3, sys, os
from random import randint

keep_one_in_X_events = 1000
out_stream_name = os.environ["ENV_OUTPUT_STREAM_NAME"]

def handler(event, context):
    kinesis_client = boto3.client("kinesis")

    records = []
    for record in event["Records"]:
        seq_number = record["kinesis"]["sequenceNumber"]
        # With X% propbability, write a Snowplow event from prod to dev. Use the unique seq number to determine which records to keep.
        if (hash(seq_number) % keep_one_in_X_events) != 0:
            continue

        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))

            # Replace the app_id with a string containing the old app_id as well.
            index_of_first_tab = payload.find("\t")
            orig_app_id = payload[:index_of_first_tab]
            payload_excl_app_id = payload[index_of_first_tab:]
            # To easily support filtering of Kinesis events, we write the target environment first as part of the app_id.
            new_app_id = "dev.lambda_replicate_from_prod ({})".format(orig_app_id)
            new_payload = new_app_id + payload_excl_app_id
            records.push({"Data": bytes(new_payload, "UTF-8"), "PartitionKey": seq_number})
        except Exception as e:
            print("ERROR: {}\nInput: {}".format(e.message, record["kinesis"]["data"]), file=sys.stderr)

    
    kinesis_client.put_records(Records=records, StreamName=out_stream_name)
    print("Put {}/{} events on stream {}.".format(len(records), len(event["Records"]), out_stream_name))