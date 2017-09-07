#!/bin/bash
if [ "$#" -lt 1 ]; then
  echo 'please specify the following parameter: docker-tag(version)'
  exit 1
fi
version=$1
docker build -f Dockerfile-scala-alpine -t scala-alpine .
docker build  -f Dockerfile-scalacollector -t scala-collector .
docker tag stream-enrich 092102721606.dkr.ecr.eu-west-1.amazonaws.com/snowplow-scala-collector:$version
docker push 092102721606.dkr.ecr.eu-west-1.amazonaws.com/snowplow-scala-collector:$version
