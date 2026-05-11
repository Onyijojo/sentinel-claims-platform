resource "aws_glue_catalog_database" "sentinel" {
  name = "${var.project_name}_catalog"
}

resource "aws_glue_job" "extraction" {
  name         = "${var.project_name}-extraction"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.bucket}/glue-scripts/extraction.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}"
    "--TempDir"                          = "s3://${aws_s3_bucket.datalake.bucket}/tmp/"
    "--S3_BUCKET"                        = aws_s3_bucket.datalake.bucket
    "--ENVIRONMENT"                      = var.environment
    "--additional-python-modules"        = "gdown"
    "--gdrive_folder_id"                 = var.gdrive_folder_id
  }
}

resource "aws_glue_job" "transformation" {
  name         = "${var.project_name}-transformation"
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.bucket}/glue-scripts/transformation.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}"
    "--TempDir"                          = "s3://${aws_s3_bucket.datalake.bucket}/tmp/"
    "--S3_BUCKET"                        = aws_s3_bucket.datalake.bucket
    "--ENVIRONMENT"                      = var.environment
  }
}

resource "aws_glue_job" "loading" {
  name              = "${var.project_name}-loading"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  max_retries       = 0

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.bucket}/glue-scripts/loading.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = "/aws-glue/jobs/${var.project_name}"
    "--TempDir"                          = "s3://${aws_s3_bucket.datalake.bucket}/tmp/"
    "--S3_BUCKET"                        = aws_s3_bucket.datalake.bucket
    "--REDSHIFT_WORKGROUP"               = aws_redshiftserverless_workgroup.main.workgroup_name
    "--ENVIRONMENT"                      = var.environment
    "--additional-python-modules"        = "psycopg2-binary"
  }
}

resource "aws_glue_crawler" "landing_zone" {
  name          = "${var.project_name}-landing-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.sentinel.name

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/landing/"
  }

  schedule = "cron(0 6 * * ? *)"
}
