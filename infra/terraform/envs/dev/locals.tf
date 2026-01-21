data "aws_availability_zones" "available" {}

locals {
  name = var.project

  # where we upload glue scripts inside your existing bucket
  glue_scripts_prefix = "manifests/glue-scripts/"

  # CloudWatch governance metrics namespace
  governance_namespace = "GovernanceNYCTaxi"
}
