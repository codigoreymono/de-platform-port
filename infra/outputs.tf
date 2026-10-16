output "bucket_name" {
  description = "Bucket that holds every pipeline object."
  value       = aws_s3_bucket.data.id
}

output "mode_namespace" {
  description = "Bucket-relative namespace that scopes all stage objects."
  value       = local.mode_namespace
}

output "function_names" {
  description = "Stage Lambda function names."
  value       = { for name, function in aws_lambda_function.stage : name => function.function_name }
}

output "state_machine_arn" {
  description = "ARN of the sequential pipeline state machine."
  value       = aws_sfn_state_machine.pipeline.arn
}

output "glue_database" {
  description = "Glue Catalog database that holds the Silver and Gold tables."
  value       = aws_glue_catalog_database.data.name
}

output "athena_workgroup" {
  description = "Athena workgroup for bounded Gold queries."
  value       = aws_athena_workgroup.queries.name
}
