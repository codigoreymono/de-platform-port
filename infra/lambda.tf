resource "aws_cloudwatch_log_group" "stage" {
  for_each = local.lambda_profiles

  name              = "/aws/lambda/${var.project}-${each.key}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "stage" {
  for_each = local.lambda_profiles

  function_name = "${var.project}-${each.key}"
  role          = aws_iam_role.stage[each.key].arn
  runtime       = "python3.12"
  handler       = each.value.handler
  architectures = ["x86_64"]
  memory_size   = each.value.memory_mib
  timeout       = each.value.timeout

  s3_bucket        = aws_s3_bucket.data.id
  s3_key           = each.value.zip_key
  source_code_hash = each.value.zip_hash != "" ? each.value.zip_hash : null

  ephemeral_storage {
    size = 512
  }

  dynamic "environment" {
    for_each = length(each.value.environment) > 0 ? [each.value.environment] : []
    content {
      variables = environment.value
    }
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.stage[each.key].name
  }

  depends_on = [
    aws_iam_role_policy.data,
    aws_iam_role_policy.logs,
    aws_s3_bucket_public_access_block.data,
  ]
}
