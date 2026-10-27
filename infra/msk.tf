resource "aws_msk_configuration" "main" {
  name              = "${var.prefix}-config"
  kafka_versions    = ["3.8.x"]
  server_properties = <<-PROPS
    auto.create.topics.enable=true
    delete.topic.enable=true
    default.replication.factor=2
    min.insync.replicas=1
    num.partitions=1
    log.retention.hours=24
  PROPS
}

resource "aws_cloudwatch_log_group" "msk" {
  name              = "${var.prefix}-msk-logs"
  retention_in_days = 3
}

resource "aws_msk_cluster" "main" {
  cluster_name           = var.prefix
  kafka_version          = "3.8.x"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = aws_subnet.public[*].id
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
      }
    }

    connectivity_info {
      public_access {
        type = var.enable_msk_public_access ? "SERVICE_PROVIDED_EIPS" : "DISABLED"
      }
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.main.arn
    revision = aws_msk_configuration.main.latest_revision
  }

  client_authentication {
    sasl {
      iam = true
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
      in_cluster    = true
    }
  }

  logging_info {
    broker_logs {
      cloudwatch_logs {
        enabled   = true
        log_group = aws_cloudwatch_log_group.msk.name
      }
    }
  }
}
