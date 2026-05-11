locals {
  common_tags = {
    Project     = "sentinel-claims-analytics"
    Environment = var.environment
    ManagedBy   = "terraform"
  }

  data_classification_tags = {
    DataClassification = "confidential"
    Compliance         = "HIPAA"
  }
}
