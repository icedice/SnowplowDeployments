resource "aws_iam_role" "stream_enricher_task_role" {
  // Do not change this name on Test (arn:aws:iam::092102721606:role/Test-snowplow-stream-enricher-EcsTaskRole). It is
  // referred to by prod-stream-replicator lambda.
  name               = "${local.env_projectname}-EcsTaskRole"
  tags               = local.tags
  assume_role_policy = data.aws_iam_policy_document.instance_and_prod_stream_replicator_assume_role_policy_document.json
}

data "aws_iam_policy_document" "instance_and_prod_stream_replicator_assume_role_policy_document" {
  statement {
    actions = ["sts:AssumeRole"]
    effect = "Allow"
    principals {
      type = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
  statement {
    actions = ["sts:AssumeRole"]
    effect = "Allow"
    principals {
      type = "AWS"
      identifiers = ["arn:aws:sts::625420756232:assumed-role/prod-stream-replicator-role/prod-stream-replicator"]
    }
  }
}

resource "aws_iam_role_policy" "stream_enricher_policy" {
  name = "${var.environment}-${var.projectname}-misc"
  role =  aws_iam_role.stream_enricher_task_role.id
  policy = data.aws_iam_policy_document.stream_enricher_policy_document.json
}

# Snowplow docs doesn't really say what permissions and specific resources, the enricher needs to access so some of 
# these permissions are too broad (e.g. get from all of S3).
data "aws_iam_policy_document" "stream_enricher_policy_document" {
  statement {
    sid = "Stmt1481029582000"
    effect = "Allow"
    actions = ["kinesis:*"]
    resources = [
      "arn:aws:kinesis:eu-west-1:${data.aws_caller_identity.current.account_id}:stream/${var.environment}-${var.enriched_bad_stream_name}",
      "arn:aws:kinesis:eu-west-1:${data.aws_caller_identity.current.account_id}:stream/${var.environment}-${var.enriched_good_stream_name}",
      "arn:aws:kinesis:eu-west-1:${data.aws_caller_identity.current.account_id}:stream/${var.environment}-${var.web_good_stream_name}"]
  }
  statement {
    sid = "DescribeQueryScanBooksTable"
    effect = "Allow"
    actions = ["dynamodb:*"]
    resources = [
      "arn:aws:dynamodb:eu-west-1:${data.aws_caller_identity.current.account_id}:table/${var.environment}-${var.dynamo_db_table_SnowplowKinesisEnrich}"
    ]
  }
  statement {
    sid = "S3"
    effect = "Allow"
    actions = ["s3:Get*", "s3:List*"]
    resources = ["*"]
  }
  statement {
    sid = "CloudWatch"
    effect = "Allow"
    actions = ["cloudwatch:*"]
    resources = ["*"]
  }
}

