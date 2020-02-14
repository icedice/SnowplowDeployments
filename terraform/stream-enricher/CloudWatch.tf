resource "aws_cloudwatch_log_group" "log_group" {
  name              = local.env_projectname
  retention_in_days = var.environment == "Prod" ? 60 : 30
  tags              = local.tags
}
