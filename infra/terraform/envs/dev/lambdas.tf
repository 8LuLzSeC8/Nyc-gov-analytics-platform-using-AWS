data "archive_file" "freshness_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src/freshness_check"
  output_path = "${path.module}/.build/freshness.zip"
}

data "archive_file" "approval_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src/approval_handler"
  output_path = "${path.module}/.build/approval.zip"
}

data "archive_file" "dq_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_src/dq_validator"
  output_path = "${path.module}/.build/dq.zip"
}

resource "aws_lambda_function" "freshness" {
  function_name = "${local.name}-freshness-check"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.11"

  filename         = data.archive_file.freshness_zip.output_path
  source_code_hash = data.archive_file.freshness_zip.output_base64sha256

  timeout     = 30
  memory_size = 256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.workloads.id]
  }

  environment {
    variables = {
      DEFAULT_MAX_AGE_HOURS = tostring(var.max_age_hours)
    }
  }
}

resource "aws_lambda_function" "approval" {
  function_name = "${local.name}-approval-handler"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.11"

  filename         = data.archive_file.approval_zip.output_path
  source_code_hash = data.archive_file.approval_zip.output_base64sha256

  timeout     = 30
  memory_size = 256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.workloads.id]
  }
}

resource "aws_lambda_function" "dq_validator" {
  function_name = "${local.name}-dq-validator"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "app.lambda_handler"
  runtime       = "python3.11"

  filename         = data.archive_file.dq_zip.output_path
  source_code_hash = data.archive_file.dq_zip.output_base64sha256

  timeout     = 30
  memory_size = 256

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.workloads.id]
  }
}

# Allow API Gateway to invoke approval lambda
resource "aws_lambda_permission" "apigw_invoke_approval" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.approval.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.approval.execution_arn}/*/*"
}
