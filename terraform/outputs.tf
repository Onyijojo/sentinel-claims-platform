output "datalake_bucket_name" {
  description = "Name of the S3 datalake bucket"
  value       = aws_s3_bucket.datalake.bucket
}

output "datalake_bucket_arn" {
  description = "ARN of the S3 datalake bucket"
  value       = aws_s3_bucket.datalake.arn
}

output "glue_role_arn" {
  description = "ARN of the Glue IAM role"
  value       = aws_iam_role.glue_role.arn
}

output "redshift_role_arn" {
  description = "ARN of the Redshift IAM role"
  value       = aws_iam_role.redshift_role.arn
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint"
  value       = aws_redshiftserverless_workgroup.main.endpoint
}

output "redshift_workgroup_name" {
  description = "Redshift Serverless workgroup name"
  value       = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "redshift_namespace_name" {
  description = "Redshift Serverless namespace name"
  value       = aws_redshiftserverless_namespace.main.namespace_name
}

output "redshift_database_name" {
  description = "Redshift Serverless database name"
  value       = "dev"
}

output "glue_catalog_database" {
  description = "Name of the Glue catalog database"
  value       = aws_glue_catalog_database.sentinel.name
}

output "sns_alerts_topic_arn" {
  description = "ARN of the SNS alerts topic"
  value       = aws_sns_topic.alerts.arn
}
