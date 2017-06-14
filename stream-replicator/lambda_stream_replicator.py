"""
Takes an input stream and replicates a fraction of Snowplow records to an output stream, changing the records' app_id in the process.
"""
import base64, json, boto3, sys, os

keep_one_in_X_events = 100
out_stream_name = os.environ["ENV_OUTPUT_STREAM_NAME"]

assume_role_arn = os.environ["ENV_ASSUME_ROLE_ARN"]
assume_role_session_name = "snowplow-stream-replicator"

def handler(event, context):
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
            print("ERROR: {}\nInput: {}".format(str(e), record["kinesis"]["data"]), file=sys.stderr)

    if len(records) == 0:
        return
        
    sts_client = boto3.client("sts")
    sts_response = sts_client.assume_role(RoleArn=assume_role_arn, RoleSessionName=assume_role_session_name, DurationSeconds=900)

    kinesis_client = boto3.client("kinesis",
        aws_access_key_id=sts_response["Credentials"]["AccessKeyId"],
        aws_secret_access_key=sts_response["Credentials"]["SecretAccessKey"],
        aws_session_token=sts_response["Credentials"]["SessionToken"]
    )    
    kinesis_client.put_records(Records=records, StreamName=out_stream_name)
    print("Put {}/{} events on stream {}.".format(len(records), len(event["Records"]), out_stream_name))
