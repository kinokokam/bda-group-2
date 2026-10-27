variable "region" {
  description = "Region holding the aml-bq1 bucket and every resource here."
  type        = string
  default     = "ap-southeast-1"
}

variable "bucket" {
  description = "Existing data bucket. Never created here; it holds finished work."
  type        = string
  default     = "aml-bq1"
}

variable "profile" {
  description = "Local AWS credential profile."
  type        = string
  default     = "bda"
}

variable "prefix" {
  description = "Name prefix for every created resource."
  type        = string
  default     = "aml-bq1"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.t3.small"
}

variable "broker_storage_gb" {
  type    = number
  default = 20
}

variable "flink_runtime" {
  type    = string
  default = "FLINK-1_20"
}

variable "lambda_zip_key" {
  description = "S3 key of the replay Lambda package."
  type        = string
  default     = "artifacts/replay.zip"
}

variable "flink_zip_key" {
  description = "S3 key of the Flink application package."
  type        = string
  default     = "artifacts/flink.zip"
}

variable "deploy_lambda" {
  description = "Create the Lambda. Requires lambda_zip_key to exist in the bucket."
  type        = bool
  default     = false
}

variable "deploy_flink" {
  description = "Create the Flink application. Requires flink_zip_key to exist."
  type        = bool
  default     = false
}

variable "enable_msk_public_access" {
  description = <<-EOT
    Expose the brokers on the internet, IAM-authenticated, so the local frontend
    can consume alerts without a bastion. MSK rejects this at creation time, so
    the first apply must leave it false and a second apply turns it on.
  EOT
  type        = bool
  default     = false
}

variable "deploy_glue_endpoint" {
  description = <<-EOT
    Interface endpoint for the Glue API. The Flink application runs in private
    subnets with no NAT, so the Iceberg Glue catalog cannot reach glue's public
    endpoint without it. Billed per AZ-hour, so it is torn down with the
    cluster rather than left standing.
  EOT
  type        = bool
  default     = false
}
