data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_role" {
  name               = "${local.name}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_policy" {
  statement {
    sid     = "InvokeLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.freshness.arn,
      aws_lambda_function.dq_validator.arn
    ]
  }

  statement {
    sid     = "GlueSync"
    actions = ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns"]
    resources = [
      aws_glue_job.raw_to_validated.arn,
      aws_glue_job.enrich_to_curated.arn
    ]
  }

  statement {
    sid       = "DdbPut"
    actions   = ["dynamodb:PutItem"]
    resources = [aws_dynamodb_table.audit.arn]
  }

  statement {
    sid       = "SnsPublish"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]
  }
}

resource "aws_iam_policy" "sfn_policy" {
  name   = "${local.name}-sfn-policy"
  policy = data.aws_iam_policy_document.sfn_policy.json
}

resource "aws_iam_role_policy_attachment" "sfn_attach" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}
