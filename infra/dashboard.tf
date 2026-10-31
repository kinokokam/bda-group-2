# Streamlit analyst dashboard on one EC2 instance, reading the gold table
# through Athena with a read-only instance role (no keys on the box).
#
# Exists only when dashboard_allowed_cidr is set in terraform.tfvars, so a
# teammate's provision without it neither fails nor opens the port to anyone.
# It sits in a public subnet because the private ones have no NAT and it needs
# the Athena API, SSM and PyPI. teardown destroys the instance; the rest is free.

locals {
  deploy_dashboard = var.dashboard_allowed_cidr != ""
  athena_workgroup = aws_athena_workgroup.main.arn
  glue_database    = aws_glue_catalog_database.main.name
}

data "aws_ssm_parameter" "al2023" {
  count = local.deploy_dashboard ? 1 : 0
  name  = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# Its etag goes into user_data, so uploading a new package replaces the
# instance on the next apply.
data "aws_s3_object" "dashboard_package" {
  count  = local.deploy_dashboard ? 1 : 0
  bucket = var.bucket
  key    = var.dashboard_zip_key
}

resource "aws_security_group" "dashboard" {
  count       = local.deploy_dashboard ? 1 : 0
  name        = "${var.prefix}-dashboard"
  description = "Streamlit from the analyst address only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Streamlit; it has no login, so never open this to 0.0.0.0/0"
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = [var.dashboard_allowed_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.prefix}-dashboard" }
}

data "aws_iam_policy_document" "dashboard_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "dashboard" {
  count              = local.deploy_dashboard ? 1 : 0
  name               = "${var.prefix}-dashboard"
  assume_role_policy = data.aws_iam_policy_document.dashboard_trust.json
}

resource "aws_iam_role_policy" "dashboard_read" {
  count = local.deploy_dashboard ? 1 : 0
  name  = "read-gold"
  role  = aws_iam_role.dashboard[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = local.athena_workgroup
      },
      {
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
        Resource = [
          "arn:aws:glue:${var.region}:${local.account}:catalog",
          "arn:aws:glue:${var.region}:${local.account}:database/${local.glue_database}",
          "arn:aws:glue:${var.region}:${local.account}:table/${local.glue_database}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = [
          "${local.bucket}/gold/*",
          "${local.bucket}/model/*",
          "${local.bucket}/evaluation/*",
          "${local.bucket}/${var.dashboard_zip_key}",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload"]
        Resource = "${local.bucket}/athena-results/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
        Resource = local.bucket
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "dashboard_ssm" {
  count      = local.deploy_dashboard ? 1 : 0
  role       = aws_iam_role.dashboard[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "dashboard" {
  count = local.deploy_dashboard ? 1 : 0
  name  = "${var.prefix}-dashboard"
  role  = aws_iam_role.dashboard[0].name
}

resource "aws_instance" "dashboard" {
  count = local.deploy_dashboard ? 1 : 0

  ami                    = data.aws_ssm_parameter.al2023[0].value
  instance_type          = var.dashboard_instance_type
  subnet_id              = aws_subnet.public[0].id
  vpc_security_group_ids = [aws_security_group.dashboard[0].id]
  iam_instance_profile   = aws_iam_instance_profile.dashboard[0].name

  metadata_options {
    http_tokens = "required"
  }

  # replace() strips the carriage returns a Windows checkout adds; bash on
  # the instance would otherwise fail on the first line.
  user_data_replace_on_change = true
  user_data = replace(<<-EOF
    #!/bin/bash
    # package etag ${data.aws_s3_object.dashboard_package[0].etag}
    set -euxo pipefail
    dnf install -y python3.11 python3.11-pip unzip
    mkdir -p /opt/dashboard
    aws s3 cp "s3://${var.bucket}/${var.dashboard_zip_key}" /tmp/dashboard.zip --region ${var.region}
    unzip -o /tmp/dashboard.zip -d /opt/dashboard
    python3.11 -m venv /opt/dashboard/venv
    /opt/dashboard/venv/bin/pip install --quiet ${var.dashboard_packages}
    chown -R ec2-user:ec2-user /opt/dashboard

    cat > /etc/systemd/system/aml-dashboard.service <<'UNIT'
    [Unit]
    Description=AML BQ1 Streamlit dashboard
    After=network-online.target
    Wants=network-online.target

    [Service]
    User=ec2-user
    WorkingDirectory=/opt/dashboard/frontend
    Environment=HOME=/opt/dashboard
    Environment=AWS_REGION=${var.region}
    Environment=ATHENA_WORKGROUP=${aws_athena_workgroup.main.name}
    Environment=GLUE_DATABASE=${local.glue_database}
    Environment=BQ1_BUCKET=${var.bucket}
    ExecStart=/opt/dashboard/venv/bin/streamlit run app.py --server.port 8501 --server.address 0.0.0.0 --server.headless true --browser.gatherUsageStats false
    Restart=on-failure

    [Install]
    WantedBy=multi-user.target
    UNIT

    systemctl daemon-reload
    systemctl enable --now aml-dashboard
  EOF
  , "\r", "")

  # A newer AMI is published every few weeks; picking it up should not
  # replace the instance mid-demo.
  lifecycle {
    ignore_changes = [ami]
  }

  # The policies hang off the role, not the profile, so nothing else ties
  # them to the instance: `apply -target aws_instance.dashboard` would skip
  # them and boot a box that cannot read the package or query Athena.
  depends_on = [
    aws_iam_role_policy.dashboard_read,
    aws_iam_role_policy_attachment.dashboard_ssm,
  ]

  tags = { Name = "${var.prefix}-dashboard" }
}
