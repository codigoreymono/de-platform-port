data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "stage" {
  for_each = local.lambda_profiles

  name               = "${var.project}-${each.key}"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

data "aws_iam_policy_document" "data" {
  statement {
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.data.arn}/${local.mode_namespace}/*"]
  }
}

resource "aws_iam_role_policy" "data" {
  for_each = local.lambda_profiles

  name   = "${var.project}-${each.key}-data"
  role   = aws_iam_role.stage[each.key].id
  policy = data.aws_iam_policy_document.data.json
}

resource "aws_iam_role_policy" "logs" {
  for_each = local.lambda_profiles

  name = "${var.project}-${each.key}-logs"
  role = aws_iam_role.stage[each.key].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.stage[each.key].arn}:*"
      },
    ]
  })
}

data "aws_iam_policy_document" "gold_glue" {
  statement {
    actions = ["glue:GetTable", "glue:GetTables", "glue:UpdateTable"]

    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${var.glue_database_name}",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*",
    ]
  }
}

resource "aws_iam_role_policy" "gold_glue" {
  name   = "${var.project}-gold-glue"
  role   = aws_iam_role.stage["gold"].id
  policy = data.aws_iam_policy_document.gold_glue.json
}

data "aws_iam_policy_document" "states_assume" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "states" {
  name               = "${var.project}-states"
  assume_role_policy = data.aws_iam_policy_document.states_assume.json
}

data "aws_iam_policy_document" "states_invoke" {
  statement {
    actions = ["lambda:InvokeFunction"]
    resources = [
      for function in aws_lambda_function.stage : function.arn
    ]
  }
}

resource "aws_iam_role_policy" "states_invoke" {
  name   = "${var.project}-states-invoke"
  role   = aws_iam_role.states.id
  policy = data.aws_iam_policy_document.states_invoke.json
}
