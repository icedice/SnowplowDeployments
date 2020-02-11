# Provider
provider "aws" {
  region = var.region
}

variable "region" {
  default = "eu-west-1"
}

variable "projectname" {
  default = "snowplow-scala-collector"
}

variable "environment_name" {
  description = "Environment with upper first character. Ex. 'Dev', 'Test' or 'Prod'"
}

variable "account" {}
variable "clustername" {}

variable "docker_versiontag" {}

variable "ecs_task_cpu" {}
variable "ecs_task_memory" {}
variable "ecs_num_tasks" {}

variable "lb_listener_protocol" { default = "HTTPS"}

# environment variables
variable "good_stream_name" { default = "web_good" }
variable "bad_stream_name" { default = "web_bad" }
variable "no_of_shards" {}
variable "is_production" {}
