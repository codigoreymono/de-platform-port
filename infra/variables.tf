variable "aws_region" {
  description = "Region for every resource in this stack."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Short project name used in resource names."
  type        = string
  default     = "nyc311"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,20}$", var.project))
    error_message = "project must be lowercase alphanumeric text with single hyphens."
  }
}

variable "bucket_name" {
  description = "Project-owned S3 bucket that stores Bronze, Silver, Gold and control objects."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be a valid S3 bucket name."
  }
}

variable "data_prefix" {
  description = "Relative POSIX prefix under the bucket that scopes every pipeline object."
  type        = string
  default     = "nyc311"

  validation {
    condition = (
      length(var.data_prefix) > 0
      && can(regex("^[0-9A-Za-z!._-]+(/[0-9A-Za-z!._-]+)*$", var.data_prefix))
    )
    error_message = "data_prefix must be a canonical relative POSIX prefix."
  }
}

variable "mode" {
  description = "Storage namespace. AWS execution only writes the official published namespace."
  type        = string
  default     = "published"

  validation {
    condition     = var.mode == "published"
    error_message = "The deployed pipeline publishes only the official published namespace."
  }
}

variable "ingest_zip_key" {
  description = "S3 key of the built ingest ZIP, relative to the bucket."
  type        = string
  default     = "lambda-code/nyc311-ingest.zip"
}

variable "curate_zip_key" {
  description = "S3 key of the built curate ZIP, relative to the bucket."
  type        = string
  default     = "lambda-code/nyc311-curate.zip"
}

variable "gold_zip_key" {
  description = "S3 key of the built gold ZIP, relative to the bucket."
  type        = string
  default     = "lambda-code/nyc311-gold.zip"
}

variable "ingest_source_hash" {
  description = "Base64 SHA-256 of the ingest ZIP, for Lambda deployment change detection."
  type        = string
  default     = ""
}

variable "curate_source_hash" {
  description = "Base64 SHA-256 of the curate ZIP, for Lambda deployment change detection."
  type        = string
  default     = ""
}

variable "gold_source_hash" {
  description = "Base64 SHA-256 of the gold ZIP, for Lambda deployment change detection."
  type        = string
  default     = ""
}

variable "glue_database_name" {
  description = "Glue Catalog database that holds the Silver and Gold tables."
  type        = string
  default     = "nyc311"

  validation {
    condition     = can(regex("^[A-Za-z0-9_]{1,255}$", var.glue_database_name))
    error_message = "glue_database_name must contain only letters, numbers and underscores."
  }
}

variable "athena_result_prefix" {
  description = "Bucket prefix for Athena query results, outside every table location."
  type        = string
  default     = "athena-results"

  validation {
    condition     = can(regex("^[0-9A-Za-z!._-]+(/[0-9A-Za-z!._-]+)*$", var.athena_result_prefix))
    error_message = "athena_result_prefix must be a canonical relative POSIX prefix."
  }
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention for each stage function."
  type        = number
  default     = 14

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653], var.log_retention_days)
    error_message = "log_retention_days must be a value accepted by CloudWatch Logs."
  }
}

variable "tags" {
  description = "Additional tags merged into every resource."
  type        = map(string)
  default     = {}
}
