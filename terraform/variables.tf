variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project identifier used in resource naming"
  type        = string
  default     = "sentinel"
}

variable "redshift_master_username" {
  description = "Master username for Redshift cluster"
  type        = string
  default     = "admin"
}

variable "redshift_master_password" {
  description = "Master password for Redshift cluster"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address for pipeline alert notifications"
  type        = string
  default     = "data-alerts@sentinel.com"
}

variable "gdrive_folder_id" {
  description = "Google Drive folder ID containing the source CSV files"
  type        = string
}
