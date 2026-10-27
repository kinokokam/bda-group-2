locals {
  kafka_prefix    = "arn:aws:kafka:${var.region}:${local.account}"
  kafka_cluster   = "${local.kafka_prefix}:cluster/${var.prefix}/*"
  kafka_topic_arn = "${local.kafka_prefix}:topic/${var.prefix}/*"
  kafka_group_arn = "${local.kafka_prefix}:group/${var.prefix}/*"
}

data "aws_iam_policy_document" "kafka_access" {
  statement {
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:AlterCluster"]
    resources = [local.kafka_cluster]
  }
  statement {
    actions = [
      "kafka-cluster:CreateTopic",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData",
      "kafka-cluster:ReadData",
      "kafka-cluster:DeleteTopic",
      "kafka-cluster:AlterTopic",
    ]
    resources = ["${local.kafka_topic_arn}/*"]
  }
  statement {
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = ["${local.kafka_group_arn}/*"]
  }
}

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.prefix}-replay"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

resource "aws_iam_role_policy" "lambda_kafka" {
  name   = "kafka"
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.kafka_access.json
}

resource "aws_iam_role_policy" "lambda_discover_cluster" {
  name = "discover-cluster"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["kafka:ListClustersV2", "kafka:DescribeClusterV2", "kafka:GetBootstrapBrokers"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "lambda_read_data" {
  name = "read-data"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:GetObject"], Resource = "${local.bucket}/*" },
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = local.bucket },
    ]
  })
}

data "aws_iam_policy_document" "flink_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["kinesisanalytics.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flink" {
  name               = "${var.prefix}-flink"
  assume_role_policy = data.aws_iam_policy_document.flink_trust.json
}

resource "aws_iam_role_policy" "flink_kafka" {
  name   = "kafka"
  role   = aws_iam_role.flink.id
  policy = data.aws_iam_policy_document.kafka_access.json
}

resource "aws_iam_role_policy" "flink_access" {
  name = "access"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${local.bucket}/*"
      },
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation"], Resource = local.bucket },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
        ]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeDhcpOptions", "ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface", "ec2:CreateNetworkInterfacePermission"]
        Resource = "*"
      },
    ]
  })
}
