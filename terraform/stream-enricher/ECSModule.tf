module "ecs_module" {
  source = "git@github.com:Jyllands-Posten/CloudInfrastructure.git//Terraform/Common/ECS_Service_Module/"

  environmentname = "${var.environment_name}"
  ecs_cluster_id  = "${var.clustername}"

  ecs_environmentvariables_jsonarray = <<EOF
  [
  {"name":"kinesis_input_good","value":"${var.environment_name}-${var.web_good_stream_name}"},
  {"name":"kinesis_output_good","value":"${var.environment_name}-${var.enriched_good_stream_name}"},
  {"name":"kinesis_output_bad","value":"${var.environment_name}-${var.enriched_bad_stream_name}"},
  {"name":"app_name","value":"${var.environment_name}-${var.dynamo_db_table_SnowplowKinesisEnrich}"}
]
EOF

  projectname       = "${var.projectname}"
  task_role_arn     = "${aws_iam_role.task_role.arn}"
  ecs_image         = "092102721606.dkr.ecr.eu-west-1.amazonaws.com/snowplow-stream-enrich:${var.docker_versiontag}"
  ecs_service_tasks = "${var.ecs_task_count}"
  ecs_task_memory   = "${var.ecs_task_memory}"
  ecs_task_cpu      = "${var.ecs_task_cpu}"
}