variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "project" {
  type    = string
  default = "nyc-taxi-gov"
}

# EXISTING bucket (we do not create it)
variable "bucket_name" {
  type = string
}

# Your prefixes (match your structure)
variable "snapshot_prefix" {
  type    = string
  default = "validated/master_snapshot/"
}

variable "raw_trips_prefix" {
  type    = string
  default = "raw/trips/"
}

variable "validated_trips_prefix" {
  type    = string
  default = "validated/trips_validated/"
}

variable "curated_trips_prefix" {
  type    = string
  default = "curated/trips_enriched/"
}

variable "metrics_prefix" {
  type    = string
  default = "audit/metrics/"
}

variable "max_age_hours" {
  type    = number
  default = 24
}

variable "quality_threshold" {
  type    = number
  default = 0.98
}

# Email for SNS subscription (approval notifications)
variable "alert_email" {
  type = string
}

# Optional: lock VPC CIDR
variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "private_subnet_cidrs" {
  type    = list(string)
  default = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "quarantine_prefix" {
  type        = string
  description = "S3 prefix for quarantined (bad) rows written by Glue job 1"
  default     = "validated/quarantine/"
}
