resource "aws_security_group" "workloads" {
  name        = "${local.name}-workloads-sg"
  description = "Lambda + Glue egress to VPC endpoints"
  vpc_id      = aws_vpc.main.id

  # no inbound needed
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-workloads-sg" }
}

resource "aws_security_group" "endpoints" {
  name        = "${local.name}-endpoints-sg"
  description = "Allow 443 from workloads to interface endpoints"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port       = 443
    to_port         = 443
    protocol        = "tcp"
    security_groups = [aws_security_group.workloads.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name}-endpoints-sg" }
}
