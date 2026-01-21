data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${local.name}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Least-ish S3 scope for lambdas:
# - freshness: list snapshot prefix
# - dq: read metrics file
data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
  }

  statement {
    sid       = "ListSnapshotPrefix"
    actions   = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.bucket_name}"]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        var.snapshot_prefix,
        "${var.snapshot_prefix}*",
        var.metrics_prefix,
        "${var.metrics_prefix}*"
      ]
    }
  }

  statement {
    sid     = "ReadMetricsAndSnapshots"
    actions = ["s3:GetObject"]
    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.snapshot_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.metrics_prefix}*"
    ]
  }

  # approval handler needs these
  statement {
    sid       = "SendTaskCallbacks"
    actions   = ["states:SendTaskSuccess", "states:SendTaskFailure"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda_policy" {
  name   = "${local.name}-lambda-policy"
  policy = data.aws_iam_policy_document.lambda_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_attach" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

resource "aws_iam_role_policy_attachment" "lambda_vpc_access" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}
