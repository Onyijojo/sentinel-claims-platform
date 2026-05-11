# Terraform Guide — Sentinel Claims Platform

## Prerequisites
- Terraform >= 1.5.0
- AWS CLI configured with `sentinel-dev` credentials
- The `sentinel-claims-data` S3 bucket must exist (used for both data and Terraform state)

## Resources Managed

| File | Resources |
|---|---|
| `main.tf` | AWS provider, S3 backend |
| `variables.tf` | Input variables |
| `s3.tf` | S3 bucket, versioning, encryption, lifecycle rules |
| `iam.tf` | Glue and Redshift IAM roles and policies |
| `glue.tf` | Glue jobs, catalog database, landing zone crawler |
| `redshift.tf` | Redshift Serverless namespace, workgroup, security group |
| `cloudwatch.tf` | Log group, metric alarms, SNS topic, budget alert |
| `tags.tf` | Local tag values |
| `outputs.tf` | Output values (endpoints, ARNs, names) |

## First-Time Setup

```bash
cd terraform/
terraform init
terraform import aws_s3_bucket.datalake sentinel-claims-data
terraform import aws_redshiftserverless_namespace.main sentinel-namespace
terraform import aws_redshiftserverless_workgroup.main sentinel-workgroup
terraform import aws_iam_role.glue_role sentinel-glue-role
terraform import aws_iam_role.redshift_role sentinel-redshift-role
terraform import aws_glue_job.extraction sentinel-extraction
terraform import aws_glue_job.transformation sentinel-transformation
terraform import aws_glue_job.loading sentinel-loading
terraform plan -var="redshift_master_password=<password>" -var="gdrive_folder_id=<id>"
terraform apply -var="redshift_master_password=<password>" -var="gdrive_folder_id=<id>"
```

## Day-to-Day Commands

```bash
# Preview changes
terraform plan -var="redshift_master_password=<password>" -var="gdrive_folder_id=<id>"

# Apply changes
terraform apply -var="redshift_master_password=<password>" -var="gdrive_folder_id=<id>"

# View current state
terraform state list

# Show a specific resource
terraform state show aws_glue_job.extraction

# View outputs
terraform output
```

## Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `aws_region` | No | `us-east-1` | AWS region |
| `environment` | No | `dev` | Deployment environment |
| `project_name` | No | `sentinel` | Resource name prefix |
| `redshift_master_username` | No | `admin` | Redshift admin username |
| `redshift_master_password` | Yes | — | Redshift admin password |
| `gdrive_folder_id` | Yes | — | Google Drive folder ID for source CSVs |
| `alert_email` | No | `data-alerts@sentinel.com` | Email for pipeline alerts |

## Important Notes

- The Redshift workgroup uses `ignore_changes = all` — modify workgroup settings directly in the AWS console, not via Terraform.
- The Redshift namespace uses `prevent_destroy = true` — run `terraform destroy` will require removing this flag first.
- Glue job `--additional-python-modules` are passed via both default arguments and Airflow `script_args` to ensure they are applied on every run.
