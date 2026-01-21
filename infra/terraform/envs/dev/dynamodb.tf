resource "aws_dynamodb_table" "audit" {
  name         = "${local.name}-audit"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "run_id"
  range_key    = "event_type"

  attribute {
    name = "run_id"
    type = "S"
  }

  attribute {
    name = "event_type"
    type = "S"
  }

  tags = { Name = "${local.name}-audit" }
}
