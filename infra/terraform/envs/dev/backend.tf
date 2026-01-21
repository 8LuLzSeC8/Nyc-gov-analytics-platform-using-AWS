terraform {
  backend "s3" {
    bucket         = "tfstate-398265530653-us-east-2"
    key            = "nyc-taxi-governed-platform/dev/terraform.tfstate"
    region         = "us-east-2"
    dynamodb_table = "terraform-locks"
    encrypt        = true
  }
}
