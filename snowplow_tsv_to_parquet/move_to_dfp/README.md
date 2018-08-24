# Transfer data to Dataforsyningsplatform (DFP)
Currently we only transfer pageviews to DFP. This is triggered by the same Airflow DAG that triggers the parent `snowplow_tsv_to_parquet` project, except that this job is only triggered every midnight instead of every hour.

# Developing locally
Create virtual environment (or do it your preferred way):
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Set your environment variables for the current shell:
```bash
export S3_INPUT_BUCKET=behavior-datalake-test
export S3_OUTPUT_BUCKET=behavior-datalake-test
export S3_OUTPUT_PREFIX=snowplow_pageviews_dfp
```

Run the script for a given date:
```bash
python3 move_to_dfp.py 2018-08-22
```

__CAUTION__: If you run the script multiple times for the same date, multiple files with duplicate data will be created so be weary of this in prod.

## Cleanup in S3
```bash
aws s3 rm --recursive s3://${S3_OUTPUT_BUCKET}/${S3_OUTPUT_PREFIX}
```

# Building and running the Docker image
```bash
docker build -t move_to_dfp .
docker run -it -e AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID' -e AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY' -e AWS_SESSION_TOKEN='$AWS_SESSION_TOKEN' move_to_dfp 2018-08-22
```
