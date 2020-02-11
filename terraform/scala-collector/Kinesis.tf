resource "aws_kinesis_stream" "good_stream" {
  name             = "${var.environment_name}-${var.good_stream_name}"
  shard_count      = var.no_of_shards
  retention_period = 24
  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]
  tags = {
    Environment = var.environment_name
    CostCenter  = "2691"
  }
}

resource "aws_kinesis_stream" "bad_stream" {
  name             = "${var.environment_name}-${var.bad_stream_name}"
  shard_count      = 1
  retention_period = 24
  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]
  tags = {
    Environment = var.environment_name
  }
}

