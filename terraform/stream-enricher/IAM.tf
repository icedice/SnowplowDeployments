# Task Role
# AllowProdReplicator allows the replicator Lambda to put things onto the Test Kinesis stream.
# It will also allow the Lambda to assume Dev and Prod roles, however, they won't have the access policy at the bottom of the file.
resource "aws_iam_role" "task_role" {
    name = "${var.environment_name}-${var.projectname}-EcsTaskRole"
    assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "ecs-tasks.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    },
    {
      "Sid": "AllowProdReplicator",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:sts::625420756232:assumed-role/prod-stream-replicator-role/prod-stream-replicator"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

# Snowplow docs doesn't really say what permissions and specific resources, the enricher needs to access so some of 
# these permissions are too broad (e.g. get from all of S3).
resource "aws_iam_role_policy" "role_policy" {
  name = "${var.environment_name}-${var.projectname}-misc"
  role = "${aws_iam_role.task_role.id}"
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1481029582000",
            "Effect": "Allow",
            "Action": [
                "kinesis:*"
            ],
            "Resource": [
                "arn:aws:kinesis:eu-west-1:${var.account}:stream/${var.environment_name}-${var.enriched_bad_stream_name}",
                "arn:aws:kinesis:eu-west-1:${var.account}:stream/${var.environment_name}-${var.enriched_good_stream_name}",
                "arn:aws:kinesis:eu-west-1:${var.account}:stream/${var.environment_name}-${var.web_good_stream_name}"
            ]
        },
        {
            "Sid": "DescribeQueryScanBooksTable",
            "Effect": "Allow",
            "Action": "dynamodb:*",
            "Resource": "arn:aws:dynamodb:eu-west-1:${var.account}:table/${var.environment_name}-${var.dynamo_db_table_SnowplowKinesisEnrich}"
        },
        {
            "Sid": "S3",
            "Effect": "Allow",
            "Action": ["s3:Get*", "s3:List*"],
            "Resource": "*"
        },
        {
            "Sid": "CloudWatch",
            "Effect": "Allow",
            "Action": "cloudwatch:*",
            "Resource": "*"
        }
    ]
}
EOF
}

