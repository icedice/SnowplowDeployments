#!/bin/bash
if [ "$#" -lt 1 ]; then
  echo 'please specify the following parameter: docker-tag(version)'
  exit 1
fi
version=$1
docker build -f Dockerfile-scala-alpine -t scala-alpine .
docker build  -f Dockerfile-stream-enrich -t stream-enrich . 
docker tag stream-enrich 092102721606.dkr.ecr.eu-west-1.amazonaws.com/snowplow-stream-enrich:$version
docker push 092102721606.dkr.ecr.eu-west-1.amazonaws.com/snowplow-stream-enrich:$version
