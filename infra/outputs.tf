output "bootstrap_brokers" {
  description = "Private IAM endpoint, for Lambda and Flink inside the VPC."
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
}

output "bootstrap_brokers_public" {
  description = "Public IAM endpoint on port 9198, for the local frontend. Empty until enable_msk_public_access is applied."
  value       = try(aws_msk_cluster.main.bootstrap_brokers_public_sasl_iam, "")
}

output "cluster_arn" {
  value = aws_msk_cluster.main.arn
}

output "private_subnet_ids" {
  value = aws_subnet.private[*].id
}

output "flink_log_group" {
  value = aws_cloudwatch_log_group.flink.name
}

output "env_file_block" {
  description = "Paste into .env."
  value       = <<-ENV
    AWS_PROFILE=${var.profile}
    AWS_REGION=${var.region}
    S3_BUCKET_NAME=${var.bucket}
    MSK_BOOTSTRAP=${aws_msk_cluster.main.bootstrap_brokers_sasl_iam}
    MSK_BOOTSTRAP_PUBLIC=${try(aws_msk_cluster.main.bootstrap_brokers_public_sasl_iam, "")}
    MSK_CLUSTER_ARN=${aws_msk_cluster.main.arn}
    FLINK_APP=${var.prefix}-scorer
    REPLAY_LAMBDA=${var.prefix}-replay
    ATHENA_WORKGROUP=${var.prefix}
  ENV
}
