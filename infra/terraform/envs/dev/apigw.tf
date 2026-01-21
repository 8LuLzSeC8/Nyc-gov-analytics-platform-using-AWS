resource "aws_apigatewayv2_api" "approval" {
  name          = "${local.name}-approval-api"
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "approval_lambda" {
  api_id                 = aws_apigatewayv2_api.approval.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.approval.arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "approve" {
  api_id    = aws_apigatewayv2_api.approval.id
  route_key = "GET /approve"
  target    = "integrations/${aws_apigatewayv2_integration.approval_lambda.id}"
}

resource "aws_apigatewayv2_route" "reject" {
  api_id    = aws_apigatewayv2_api.approval.id
  route_key = "GET /reject"
  target    = "integrations/${aws_apigatewayv2_integration.approval_lambda.id}"
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.approval.id
  name        = "$default"
  auto_deploy = true
}
