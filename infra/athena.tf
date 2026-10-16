resource "aws_athena_workgroup" "queries" {
  name        = "${var.project}-queries"
  description = "Bounded analytical queries over the four Gold products."

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/${var.athena_result_prefix}/"
    }
  }
}
