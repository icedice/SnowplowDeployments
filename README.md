# SnowplowDeployments
Files needed to deploy Snowplow to Docker.

# TL;DR (as of 2018-01-02)
## Building
```bash
git submodule update --init --recursive

# Use subshells to avoid changing this shell's working directory.
(cd snowplow/2-collectors/scala-stream-collector; sbt "project kinesis" assembly)
(cd snowplow/3-enrich/stream-enrich; sbt "project kinesis" assembly)

docker build -f Dockerfile-scala-alpine -t scala-alpine .
docker build -f Dockerfile-scala-collector -t scala-collector .
docker build -f Dockerfile-stream-enrich -t stream-enrich .
```

## Running
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

# Longer version
## Building
Check out and update the Snowplow submodule from GitHub using `git submodule update --init --recursive`.

### Building scala-alpine
scala-alpine is the base image for the following images. It is based on Alpine Linux meaning that the resulting images takes up around 200 MB of disk space instead of 1 GB.
* Run `docker build -f Dockerfile-scala-alpine -t scala-alpine .`.

### Building scala-collector
* Build the scala-collector by following the instructions in the Snowplow project (see _snowplow/2-collectors/scala-stream-collector/README.md_). It is a good idea to run `sbt clean` before assembling the jar file as this will ensure that no old jar files are present and accidentally used in the docker images.
* Run `docker build -f Dockerfile-scala-collector -t scala-collector .`.

### Building stream enricher
* Build the stream enricher by following the instructions in the project (see _snowplow/3-enrich/stream-enrich/README.md_). It is a good idea to run `sbt clean` before assembling the jar file as this will ensure that no old jar files are present and accidentally used in the docker images.
* Run `docker build -f Dockerfile-stream-enrich -t stream-enrich .`.

## Deploying the iglu repo
The iglu repo can be added to S3 and exposed as a website. Configure _stream-enrich/configuration/resolver.json_ with the relevant URL.

# Updating Snowplow version
In the *snowplow* directory, do a `git checkout` of a commit, branch or tag. The submodule itself will not have any changes if you run `git status`, however this repository will have changes to commit and push resulting in an update of the submodule in the remote as well.

After checking out the relevant commit, branch or tag, recursively update the submodule to pull changes from the submodule's submodules.

For example, to update to R89 using the r89-plain-of-jars tag and commit the changes, run
```bash
cd snowplow
git checkout r89-plain-of-jars
git submodule update --init --recursive
cd ..
git add .

# Follow https://github.com/snowplow/snowplow/wiki/Upgrade-Guide and make the appropriate changes.

gc -m "Update Snowplow to R89."
```

# Tips and tricks
_.dockerignore_ is set up to ignore a lot of files to make the Docker build context smaller. If you need to COPY a file or folder in a Dockerfile and it cannot find the given path, check that it is not ignored in _.dockerignore_.
