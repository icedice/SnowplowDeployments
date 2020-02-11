module "esc_scala_collector" {
  source = "git@github.com:Jyllands-Posten/terraform-modules.git//modules/common-modules/ecs-service?ref=6a07e1fff46d8a9d580cda37cd5a244fba62658a"

  name = var.projectname
  environment = var.environment_name
  cluster = var.clustername

  container_image = "092102721606.dkr.ecr.eu-west-1.amazonaws.com/${var.projectname}:${lower(var.docker_versiontag)}"
  container_port = 9090

  dns_name = "sp1"
  service_internal = false

  cpu_reservation = var.ecs_task_cpu
  memory_reservation = var.ecs_task_memory
  desired_count = var.ecs_num_tasks

  protocol = var.lb_listener_protocol

  create_ecr_repository = true // same as defaut, ie. delete this
  
  environment_variables = [
    {
      name  = "kinesisStreamBadName"
      value = "${var.environment_name}-${var.bad_stream_name}"
    },
    {
      name  = "kinesisStreamGoodName"
      value = "${var.environment_name}-${var.good_stream_name}"
    },
    {
      name  = "is_production"
      value = "${var.is_production}"
    }
  ]

  required_tags = local.tags
}

locals {
  tags = {
    name        = var.projectname
    cost_center =  "shared"
    environment = var.environment_name
    project     = var.projectname
    team        = "data-science"
    team_email  = "datascience@jp.dk"
  }
}