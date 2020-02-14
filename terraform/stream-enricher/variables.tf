provider "aws" {
  region  = "eu-west-1"
}

variable "projectname" { default = "snowplow-stream-enricher" }
variable "image_projectname" { default = "snowplow-stream-enrich" }
variable "environment" { description = "Environment with upper first character. Ex. 'Dev', 'Test' or 'Prod'" }
variable "clustername" {}

variable "desired_ecs_task_count" {}

variable "docker_versiontag" {}
variable "ecs_task_memory" {}
variable "ecs_task_cpu" {}

variable "dynamo_db_table_SnowplowKinesisEnrich" { default = "SnowplowKinesisEnrich" }
variable "web_good_stream_name"      { default = "web_good" }
variable "enriched_bad_stream_name"  { default = "enriched_bad" }
variable "enriched_good_stream_name" { default = "enriched_good" }

variable "no_of_shards" {}

locals {
  env_projectname   = "${var.environment}-${var.projectname}"
  create_repository = var.environment == "Dev"

  tags = {
    name        = var.projectname
    cost_center = "shared"
    environment = var.environment
    project     = var.projectname
    team        = "data-science"
    team_email  = "datascience@jp.dk"
  }
}

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}