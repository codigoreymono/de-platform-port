locals {
  state_machine_definition = {
    Comment = "Manually started, strictly sequential September 2026 NYC 311 pipeline."
    StartAt = "Prepare"
    States = {
      Prepare = {
        Type = "Pass"
        Parameters = {
          bucket     = var.bucket_name
          prefix     = local.prefix
          mode       = var.mode
          config     = {}
          days       = local.cohort_days
          task_days  = local.task_days
          catalog    = local.catalog
          "run_id.$" = "$.run_id"
        }
        Next = "DailyIngestCurate"
      }
      DailyIngestCurate = {
        Type           = "Map"
        MaxConcurrency = 1
        ItemsPath      = "$.task_days"
        ResultPath     = "$.daily_results"
        ItemSelector = {
          "bucket.$"     = "$$.Map.Item.Value.bucket"
          "prefix.$"     = "$$.Map.Item.Value.prefix"
          "mode.$"       = "$$.Map.Item.Value.mode"
          "config.$"     = "$$.Map.Item.Value.config"
          "start_date.$" = "$$.Map.Item.Value.start_date"
          "end_date.$"   = "$$.Map.Item.Value.end_date"
          "run_id.$"     = "$$.Execution.Input.run_id"
        }
        ItemProcessor = {
          ProcessorConfig = { Mode = "INLINE" }
          StartAt         = "Ingest"
          States = {
            Ingest = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = aws_lambda_function.stage["ingest"].arn
                "Payload.$"  = "$"
              }
              ResultSelector = { "Payload.$" = "$.Payload" }
              ResultPath     = "$.ingest"
              Next           = "Curate"
            }
            Curate = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = aws_lambda_function.stage["curate"].arn
                "Payload.$"  = "$.ingest.Payload"
              }
              ResultSelector = { "Payload.$" = "$.Payload" }
              ResultPath     = "$.curate"
              End            = true
            }
          }
        }
        Next = "Gold"
      }
      Gold = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.stage["gold"].arn
          Payload = {
            "bucket.$"  = "$.bucket"
            "prefix.$"  = "$.prefix"
            "mode.$"    = "$.mode"
            "run_id.$"  = "$.run_id"
            "days.$"    = "$.days"
            "catalog.$" = "$.catalog"
          }
        }
        ResultSelector = { "Payload.$" = "$.Payload" }
        ResultPath     = "$.gold"
        End            = true
      }
    }
  }
}

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.project}-pipeline"
  type       = "STANDARD"
  role_arn   = aws_iam_role.states.arn
  definition = jsonencode(local.state_machine_definition)
}
