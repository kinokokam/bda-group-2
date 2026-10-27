

resource "aws_glue_catalog_database" "main" {
  name = replace("${var.prefix}_catalog", "-", "_")
}

resource "aws_athena_workgroup" "main" {
  name          = var.prefix
  force_destroy = true

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket}/athena-results/"
    }
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.prefix}-replay"
  retention_in_days = 3
}

resource "aws_lambda_function" "replay" {
  count = var.deploy_lambda ? 1 : 0

  function_name = "${var.prefix}-replay"
  role          = aws_iam_role.lambda.arn
  runtime       = "python3.12"
  handler       = "handler.handler"
  s3_bucket     = var.bucket
  s3_key        = var.lambda_zip_key

  timeout     = 900
  memory_size = 1024

  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.clients.id]
  }

  environment {
    variables = {
      BOOTSTRAP     = "pending"
      BUCKET        = var.bucket
      HOURLY_PREFIX = "bronze/hourly/"
      REGION        = var.region
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_vpc,
    aws_cloudwatch_log_group.lambda,
  ]
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = "${var.prefix}-flink-logs"
  retention_in_days = 3
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "app"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

resource "aws_kinesisanalyticsv2_application" "scorer" {
  count = var.deploy_flink ? 1 : 0

  name                   = "${var.prefix}-scorer"
  runtime_environment    = var.flink_runtime
  service_execution_role = aws_iam_role.flink.arn

  application_configuration {
    application_code_configuration {
      code_content {
        s3_content_location {
          bucket_arn = local.bucket
          file_key   = var.flink_zip_key
        }
      }
      code_content_type = "ZIPFILE"
    }

    application_snapshot_configuration {
      snapshots_enabled = true
    }

    environment_properties {
      property_group {
        property_group_id = "kinesis.analytics.flink.run.options"
        property_map = {
          python     = "main.py"
          pyArchives = "deps.zip"
          pyFiles    = "features.py"
          jarfile    = "lib/aml-bq1-kafka-iam.jar"
        }
      }

      property_group {
        property_group_id = "BQ1"
        property_map = {
          "bootstrap.servers" = "pending"
          "source.topic"      = "transactions"
          "alerts.topic"      = "alerts"
          "rejected.topic"    = "rejected"
          "bucket"            = var.bucket
          "run.id"            = "manual"
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = 60000
        min_pause_between_checkpoints = 5000
      }

      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = 1
        parallelism_per_kpu  = 1
        auto_scaling_enabled = false
      }

      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "TASK"
      }
    }

    vpc_configuration {
      subnet_ids         = aws_subnet.private[*].id
      security_group_ids = [aws_security_group.clients.id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = "${aws_cloudwatch_log_group.flink.arn}:log-stream:${aws_cloudwatch_log_stream.flink.name}"
  }
}
