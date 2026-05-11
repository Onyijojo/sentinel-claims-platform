resource "aws_s3_bucket" "datalake" {
  bucket = "sentinel-claims-data"
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "raw-zone-lifecycle"
    status = "Enabled"
    filter { prefix = "raw/" }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}
