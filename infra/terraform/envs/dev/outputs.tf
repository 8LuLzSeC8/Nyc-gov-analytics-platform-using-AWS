output "sns_topic_arn" {
  value = aws_sns_topic.alerts.arn
}

output "api_base_url" {
  value = aws_apigatewayv2_api.approval.api_endpoint
}

output "state_machine_arn" {
  value = aws_sfn_state_machine.pipeline.arn
}

output "dynamodb_table" {
  value = aws_dynamodb_table.audit.name
}

output "glue_job_1_name" {
  value = aws_glue_job.raw_to_validated.name
}

output "glue_job_2_name" {
  value = aws_glue_job.enrich_to_curated.name
}
