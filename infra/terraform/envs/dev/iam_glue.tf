data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "${local.name}-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

data "aws_iam_policy_document" "glue_policy" {

  # ----------------------------
  # S3 BUCKET-LEVEL (List, location, multipart list)
  # ----------------------------
  statement {
    sid    = "S3BucketOps"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:ListBucketMultipartUploads"
    ]
    resources = ["arn:aws:s3:::${var.bucket_name}"]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        # RAW
        "${var.raw_trips_prefix}",
        "${var.raw_trips_prefix}*",

        # VALIDATED
        "${var.validated_trips_prefix}",
        "${var.validated_trips_prefix}*",

        # CURATED
        "${var.curated_trips_prefix}",
        "${var.curated_trips_prefix}*",

        # MASTER SNAPSHOT
        "${var.snapshot_prefix}",
        "${var.snapshot_prefix}*",

        # METRICS
        "${var.metrics_prefix}",
        "${var.metrics_prefix}*",

        # QUARANTINE
        "${var.quarantine_prefix}",
        "${var.quarantine_prefix}*",

        # SCRIPTS
        "${local.glue_scripts_prefix}",
        "${local.glue_scripts_prefix}*",

        # TEMP
        "glue-temp/",
        "glue-temp/*"
      ]
    }
  }

  # ----------------------------
  # S3 OBJECT-LEVEL (RW + multipart parts)
  # ----------------------------
  statement {
    sid    = "S3ObjectRW"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "arn:aws:s3:::${var.bucket_name}/${var.raw_trips_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.validated_trips_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.curated_trips_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.snapshot_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.metrics_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${var.quarantine_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/${local.glue_scripts_prefix}*",
      "arn:aws:s3:::${var.bucket_name}/glue-temp/*"
    ]
  }

  # ----------------------------
  # CloudWatch Logs
  # ----------------------------
  statement {
    sid       = "GlueLogs"
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["*"]
  }

  # ----------------------------
  # Governance metrics
  # ----------------------------
  statement {
    sid       = "PutGovernanceMetrics"
    effect    = "Allow"
    actions   = ["cloudwatch:PutMetricData"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "glue_policy" {
  name   = "${local.name}-glue-policy"
  policy = data.aws_iam_policy_document.glue_policy.json
}

resource "aws_iam_role_policy_attachment" "glue_attach" {
  role       = aws_iam_role.glue_role.name
  policy_arn = aws_iam_policy.glue_policy.arn
}

# Recommended baseline Glue permissions
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
