resource "aws_iam_role_policy" "kinesis_full_access" {
  name   = "${var.environment_name}-${var.projectname}-kinesisAccess"
  role   = module.esc_scala_collector.iam_role_id
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
                "arn:aws:kinesis:eu-west-1:${var.account}:stream/${var.environment_name}-${var.bad_stream_name}",
                "arn:aws:kinesis:eu-west-1:${var.account}:stream/${var.environment_name}-${var.good_stream_name}"
            ]
        }
    ]
}
EOF

}

