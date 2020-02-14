module "stream_enricher_ecs_service" {
  source = "git@github.com:Jyllands-Posten/terraform-modules.git//modules/aws-modules/ecs-service?ref=ad97fca02ecd06c7d701e2b6720182e1d770ae00"

  ecs_cluster_arn    = var.environment
  environment        = var.environment
  name               = var.projectname
  task_definition    = aws_ecs_task_definition.stream_enricher_task_definition.arn
  desired_count      = var.desired_ecs_task_count
  ecs_load_balancers = []
}

resource "aws_ecs_task_definition" "stream_enricher_task_definition" {
  family                   = local.env_projectname
  task_role_arn            = aws_iam_role.stream_enricher_task_role.arn
  network_mode             = "bridge"
  requires_compatibilities = ["EC2"]
  container_definitions    = module.stream_enricher_container_definition.json
  tags                     = local.tags
}

module "stream_enricher_container_definition" {
  source = "../../../terraform-modules/modules/aws-modules/ecs-container-definition"

  name                         = local.env_projectname
  environment                  = var.environment
  container_image              = "092102721606.dkr.ecr.eu-west-1.amazonaws.com/${var.image_projectname}:${var.docker_versiontag}"
  container_name               = local.env_projectname
  container_memory             = var.ecs_task_memory
  container_cpu                = var.ecs_task_cpu
  container_memory_reservation = null
  port_mappings                = []
  secrets                      = []

  environment_variables = [
    {
      name  = "app_name",
      value = "${var.environment}-${var.dynamo_db_table_SnowplowKinesisEnrich}"
    },
    {
      name  = "kinesis_input_good",
      value = "${var.environment}-${var.web_good_stream_name}"
    },
    {
      name  = "kinesis_output_bad",
      value = "${var.environment}-${var.enriched_bad_stream_name}"
    },
    {
      name  = "kinesis_output_good",
      value = "${var.environment}-${var.enriched_good_stream_name}"
    }
  ]

  ulimits = [{
    name:      "nofile",
    softLimit: 1024000,
    hardLimit: 1024000
  }]

  log_configuration = {
    logDriver = "awslogs"
    options = {
      awslogs-group  = aws_cloudwatch_log_group.log_group.id,
      awslogs-region = data.aws_region.current.name
    }
    secretOptions = null
  }

}

