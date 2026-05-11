data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_security_groups" "redshift_sg" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project_name}-namespace"
  db_name             = "dev"
  admin_username      = var.redshift_master_username
  admin_user_password = var.redshift_master_password
  iam_roles           = [aws_iam_role.redshift_role.arn]

  tags = {
    Name = "${var.project_name}-namespace"
  }

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [admin_user_password, admin_username, db_name, iam_roles]
  }
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name      = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name      = "${var.project_name}-workgroup"
  base_capacity       = 4
  publicly_accessible = true
  subnet_ids          = data.aws_subnets.default.ids
  security_group_ids  = data.aws_security_groups.redshift_sg.ids

  tags = {
    Name = "${var.project_name}-workgroup"
  }

  lifecycle {
    ignore_changes = all
  }
}
