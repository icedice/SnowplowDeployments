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

# Splitting Kinesis Shards
> Used example @ https://brandur.org/kinesis-by-example
New shards can't (easily) be created. Instead you need to split up existing shards. This is done by dividing one shard into
two or more.

Example:

```
Shard01: 
  HashStart:0
  HashEnd:10
```
  
You need to devide the endhash by 2 eg. 10/2 = 5.

The new calculated hash is set as a new starting hash key for the shard with the command:

> aws kinesis split-shard --stream-name $NAME --shard-to-split Shard01 --new-starting-hash-key 5
  
AWS will now create two new shards with the old shard (Shard01) as parent.
The new shards will be:

```
Shard02: 
  HashStart:0
  HashEnd:5
Shard03: 
  HashStart:5
  HashEnd:10
```

If your want to split them further you can do so in the same manner.

Your rarely as fortunate to have the hash starting at 0. When this is not the case use the following formular:
((HashEnd-HashStart) / 2) + HashStart

If we want to split Shard03:
((10-5) / 2) + 5 = 7.5

```
Result:
Shard02: 
  HashStart:0
  HashEnd:5
Shard04: 
  HashStart:5
  HashEnd:7.5
Shard05: 
  HashStart:7.5
  HashEnd:10
```

### Tools:
Install and run irb(Interactive Ruby) to calculate the large hash numbers: docker run -it ruby irb

### Troubleshooting: 
In consumer services it is essential for the kinesiswork to be able to checkpoint on shutdown. Usually we have logic to checkpoint on gracefull shutdowns, but this is not the case when reading from a closed shard. The kinesis worker will keep crashing if a checkpoint is not set when reaching the end of a closed shard. With the worker being able to checkpoint at the end, the shard is registered as closed.

```
In scala this is done with the following code:
  override def shutdown(shutdownInput: ShutdownInput): Unit = {
    Logger.info("KinesisRecordProcessor.shutdown() called")
    if (shutdownInput.getShutdownReason == ShutdownReason.TERMINATE) {
      Logger.info("KinesisRecordProcessor.shutdown() checkpointing because of shard termination")
      shutdownInput.getCheckpointer.checkpoint()
    }
  }
```
