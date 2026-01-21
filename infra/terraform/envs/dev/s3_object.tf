resource "aws_s3_object" "glue_job_1" {
  bucket = var.bucket_name
  key    = "${local.glue_scripts_prefix}glue_raw_to_validated.py"
  source = "${path.module}/glue_scripts/glue_raw_to_validated.py"
  etag   = filemd5("${path.module}/glue_scripts/glue_raw_to_validated.py")
}

resource "aws_s3_object" "glue_job_2" {
  bucket = var.bucket_name
  key    = "${local.glue_scripts_prefix}glue_enrich_to_curated.py"
  source = "${path.module}/glue_scripts/glue_enrich_to_curated.py"
  etag   = filemd5("${path.module}/glue_scripts/glue_enrich_to_curated.py")
}
