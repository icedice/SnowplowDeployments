provider "aws" {
  region  = "${var.region}"
  version = "~> 1.16"
}

variable "region" { default = "eu-west-1" }

variable "account" {}
variable "projectname" { default = "snowplow-stream-enricher" }
variable "environment_name" { description = "Environment with upper first character. Ex. 'Dev', 'Test' or 'Prod'" }

variable "clustername" {}

variable "docker_versiontag" {}

variable "ecs_task_memory" {}
variable "ecs_task_cpu" {}
variable "ecs_task_count" {}

variable "enriched_good_stream_name" { default = "enriched_good" }
variable "enriched_bad_stream_name"  { default = "enriched_bad" }
variable "web_good_stream_name"  { default = "web_good" }
variable "dynamo_db_table_SnowplowKinesisEnrich" { default = "SnowplowKinesisEnrich" }
variable "no_of_shards" {}