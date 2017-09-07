# SnowplowDeployments
Files needed to deploy Snowplow to Docker.

# TL;DR (as of 2017-09-07)
## Building
Assuming Oracle JDK 1.7 is in _/opt/jdk1.7.0_80_ and _sbt-extras_ is in PATH. Note: In R89, Java 8 is finally officially supported so when we upgrade, we can also upgrade to JDK 8.

```bash
git submodule update --init --recursive

# Use subshells to avoid changing this shell's working directory.
(cd snowplow/2-collectors/scala-stream-collector;
sbt -java-home /opt/jdk1.7.0_80 assembly)

(cd snowplow/3-enrich/stream-enrich;
sbt -java-home /opt/jdk1.7.0_80 assembly)

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

# Longer version
## Building
Check out and update the Snowplow submodule from GitHub using `git submodule update --init --recursive`.

### Building scala-alpine
scala-alpine is the base image for the following images. It is based on Alpine Linux meaning that the resulting images takes up around 200 MB of disk space instead of 1 GB.
* Run `docker build -f Dockerfile-scala-alpine -t scala-alpine .`.

### Building scala-collector
* Build the scala-collector by following the instructions in the Snowplow project (see _snowplow/2-collectors/scala-stream-collector/README.md_).
* Run `docker build -f Dockerfile-scala-collector -t scala-collector .`.

### Building stream enricher
* Build the stream enricher by following the instructions in the project (see _snowplow/3-enrich/stream-enrich/README.md_).
* Run `docker build -f Dockerfile-stream-enrich -t stream-enrich .`.

## Deploying the iglu repo
The iglu repo can be added to S3 and exposed as a website. Configure _stream-enrich/configuration/resolver.json_ with the relevant URL.

# Tips and tricks
_sbt-extras_ (https://github.com/paulp/sbt-extras/) allows you to specify JAVA\_HOME on the command line (so you can compile the Snowplow components using JDK 1.7). For example: `sbt -java-home /opt/jdk1.7.0_80 assembly`

_.dockerignore_ is set up to ignore a lot of files to make the Docker build context smaller. If you need to COPY a file or folder in a Dockerfile and it cannot find the given path, check that it is not ignored in _.dockerignore_.