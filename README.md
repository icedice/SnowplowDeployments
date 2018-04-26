# SnowplowDeployments
Jyllands-Posten's Snowplow setup with Docker images for the collector and stream enricher and enrich schemas.

# Building
```bash
docker build -f Dockerfile-scala-alpine -t scala-alpine .
docker build -f Dockerfile-scala-collector -t scala-collector .
docker build -f Dockerfile-stream-enrich -t stream-enrich .
```

# Running
Assuming your AWS access key is in AWS\_ACCESS\_KEY\_ID and the secret access key is in AWS\_SECRET\_ACCESS\_KEY.
```bash
docker run --rm -it -p 9090:9090 -e is_production=false -e kinesisStreamGoodName=Dev-web_good -e kinesisStreamBadName=Dev-web_bad -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY scala-collector
docker run --rm -it -e kinesis_input_good=Dev-web_good -e kinesis_output_good=Dev-enriched_good -e kinesis_output_bad=Dev-enriched_bad -e app_name=SnowplowKinesisEnrich_local -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY stream-enrich
```

If using the `with_aws_env` script, run as
```bash
with_aws_env docker run --rm -it -p 9090:9090 -e is_production=false -e kinesisStreamGoodName=Dev-web_good -e kinesisStreamBadName=Dev-web_bad -e AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID' -e AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY' -e AWS_SESSION_TOKEN='$AWS_SESSION_TOKEN' scala-collector
with_aws_env docker run --rm -it -e kinesis_input_good=Dev-web_good -e kinesis_output_good=Dev-enriched_good -e kinesis_output_bad=Dev-enriched_bad -e app_name=SnowplowKinesisEnrich_local -e AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID' -e AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY' -e AWS_SESSION_TOKEN='$AWS_SESSION_TOKEN' stream-enrich
```

# Deploying the iglu repo
The iglu repo can be added to S3 and exposed as a website. Configure _stream-enrich/configuration/resolver.json_ with the relevant URL.
