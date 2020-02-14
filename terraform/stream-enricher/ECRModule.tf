module "ecr" {
  source            = "../../../terraform-modules/modules/common-modules/ecs-ecr"
  create_repository = local.create_repository

  name = lower(var.image_projectname)
  tags = local.tags
}
