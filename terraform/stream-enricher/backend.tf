terraform {
  backend "s3" {
    region  = "eu-west-1"
    encrypt = true
    key = "snowplow-stream-enricher"
  }
}
