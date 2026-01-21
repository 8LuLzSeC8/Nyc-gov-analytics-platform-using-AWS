locals {
  approve_url = "${aws_apigatewayv2_api.approval.api_endpoint}/approve"
  reject_url  = "${aws_apigatewayv2_api.approval.api_endpoint}/reject"
}

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${local.name}-pipeline"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    Comment       = "Governed NYC Taxi pipeline (JSONata version for 1-click approval links)"
    QueryLanguage = "JSONata"
    StartAt       = "AUDIT_RUN_START"

    States = {

      AUDIT_RUN_START = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"

        Arguments = {
          TableName = aws_dynamodb_table.audit.name
          Item = {
            run_id     = { S = "{% $states.context.Execution.Name %}" }
            event_type = { S = "START" }
            status     = { S = "STARTED" }
            started_at = { S = "{% $states.context.State.EnteredTime %}" }
            input      = { S = "{% $string($states.input) %}" }
          }
        }

        # pass through input unchanged
        Output = "{% $states.input %}"
        Next   = "MASTER_FRESHNESS_CHECK"
      }

      MASTER_FRESHNESS_CHECK = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"

        Arguments = {
          FunctionName = aws_lambda_function.freshness.arn
          Payload = {
            bucket          = "{% $states.input.bucket %}"
            snapshot_prefix = "{% $states.input.snapshot_prefix %}"
            max_age_hours   = "{% $states.input.max_age_hours %}"
          }
        }

        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
            IntervalSeconds = 1
            MaxAttempts     = 3
            BackoffRate     = 2
            JitterStrategy  = "FULL"
          }
        ]

        # Build the state output object (keep your inputs + embed lambda payload)
        Output = {
          bucket            = "{% $states.input.bucket %}"
          snapshot_prefix   = "{% $states.input.snapshot_prefix %}"
          max_age_hours     = "{% $states.input.max_age_hours %}"
          raw_trips_prefix  = "{% $states.input.raw_trips_prefix %}"
          validated_prefix  = "{% $states.input.validated_trips_prefix %}"
          curated_prefix    = "{% $states.input.curated_trips_prefix %}"
          metrics_prefix    = "{% $states.input.metrics_prefix %}"
          quality_threshold = "{% $states.input.quality_threshold %}"
          freshness         = "{% $states.result.Payload %}"
        }

        Next = "FRESHNESS_OK?"
      }

      "FRESHNESS_OK?" = {
        Type = "Choice"
        Choices = [
          {
            Next      = "WAIT_FOR_APPROVAL"
            Condition = "{% ($states.input.freshness.freshnessOk) = (true) %}"
          }
        ]
        Default = "ALERT_MASTER_DATA_STALE"
      }

      ALERT_MASTER_DATA_STALE = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"

        Arguments = {
          TopicArn = aws_sns_topic.alerts.arn
          Subject  = "Master data snapshot stale - approval required"
          Message  = "{% 'Master snapshot is STALE. Approval required to proceed.\\n\\n' & 'RunId: ' & $states.context.Execution.Name & '\\n' & 'LastModified: ' & $states.input.freshness.lastModified & '\\n' & 'AgeHours: ' & $string($states.input.freshness.ageHours) & ' (max ' & $string($states.input.freshness.maxAgeHours) & ')\\n' & 'SnapshotPrefix: ' & $states.input.snapshot_prefix %}"
        }

        Output = "{% $states.input %}"
        Next   = "WAIT_FOR_APPROVAL"
      }

      WAIT_FOR_APPROVAL = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish.waitForTaskToken"

        Arguments = {
          TopicArn = aws_sns_topic.alerts.arn
          Subject  = "Approve pipeline run"

          # ✅ This is the exact pattern from your demo:
          # build URL with encoded token inside Message using JSONata.
          Message = "{% 'Pipeline requires approval\\n\\n' & 'RunId: ' & $states.context.Execution.Name & '\\n\\n' & 'Approve: ${local.approve_url}?taskToken=' & $encodeUrlComponent($states.context.Task.Token) & '\\n' & 'Reject:  ${local.reject_url}?taskToken=' & $encodeUrlComponent($states.context.Task.Token) & '\\n\\n' & 'FreshnessOK: ' & $string($states.input.freshness.freshnessOk) & '\\n' & 'LastModified: ' & $states.input.freshness.lastModified & '\\n' & 'AgeHours: ' & $string($states.input.freshness.ageHours) & ' (max ' & $string($states.input.freshness.maxAgeHours) & ')\\n' & 'SnapshotPrefix: ' & $states.input.snapshot_prefix %}"
        }

        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SET_REJECTION"
          }
        ]

        Output = "{% $states.input %}"
        Next   = "RUN_GLUE_RAW_TO_VALIDATED"
      }

      SET_REJECTION = {
        Type   = "Pass"
        Output = { errorMessage = "APPROVAL_REJECTED" }
        Next   = "AUDIT_RUN_FAILED"
      }

      RUN_GLUE_RAW_TO_VALIDATED = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Arguments = {
          JobName = aws_glue_job.raw_to_validated.name
          Arguments = {
            "--bucket"                 = "{% $states.input.bucket %}"
            "--raw_trips_prefix"       = "{% $states.input.raw_trips_prefix %}"
            "--validated_trips_prefix" = "{% $states.input.validated_prefix %}"
            "--run_id"                 = "{% $states.context.Execution.Name %}"
          }
        }

        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SET_GLUE_FAILURE"
          }
        ]

        Output = "{% $states.input %}"
        Next   = "RUN_GLUE_ENRICH_TO_CURATED"
      }

      RUN_GLUE_ENRICH_TO_CURATED = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"

        Arguments = {
          JobName = aws_glue_job.enrich_to_curated.name
          Arguments = {
            "--bucket"                 = "{% $states.input.bucket %}"
            "--validated_trips_prefix" = "{% $states.input.validated_prefix %}"
            "--curated_trips_prefix"   = "{% $states.input.curated_prefix %}"
            "--snapshot_prefix"        = "{% $states.input.snapshot_prefix %}"
            "--metrics_prefix"         = "{% $states.input.metrics_prefix %}"
            "--run_id"                 = "{% $states.context.Execution.Name %}"
          }
        }

        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SET_GLUE_FAILURE"
          }
        ]

        Output = "{% $states.input %}"
        Next   = "DQ_VALIDATION"
      }

      SET_GLUE_FAILURE = {
        Type   = "Pass"
        Output = { errorMessage = "GLUE_JOB_FAILED" }
        Next   = "AUDIT_RUN_FAILED"
      }

      DQ_VALIDATION = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"

        Arguments = {
          FunctionName = aws_lambda_function.dq_validator.arn
          Payload = {
            bucket            = "{% $states.input.bucket %}"
            output_prefix     = "{% $states.input.metrics_prefix %}"
            metrics_prefix    = "{% $states.input.metrics_prefix %}"
            quality_threshold = "{% $states.input.quality_threshold %}"
            run_id            = "{% $states.context.Execution.Name %}"
          }
        }

        Retry = [
          {
            ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
            IntervalSeconds = 1
            MaxAttempts     = 3
            BackoffRate     = 2
            JitterStrategy  = "FULL"
          }
        ]

        Output = "{% $states.result.Payload %}"
        Next   = "DQ_PASSED?"
      }

      "DQ_PASSED?" = {
        Type = "Choice"
        Choices = [
          {
            Next      = "AUDIT_RUN_SUCCESS"
            Condition = "{% ($states.input.qualityPassed) = (true) %}"
          }
        ]
        Default = "SET_QUALITY_FAILURE"
      }

      SET_QUALITY_FAILURE = {
        Type   = "Pass"
        Output = { errorMessage = "QUALITY_CHECK_FAILED" }
        Next   = "AUDIT_RUN_FAILED"
      }

      AUDIT_RUN_SUCCESS = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"

        Arguments = {
          TableName = aws_dynamodb_table.audit.name
          Item = {
            run_id     = { S = "{% $states.context.Execution.Name %}" }
            event_type = { S = "END" }
            status     = { S = "SUCCESS" }
            ended_at   = { S = "{% $states.context.State.EnteredTime %}" }
            dq         = { S = "{% $string($states.input) %}" }
          }
        }

        End    = true
        Output = null
      }

      AUDIT_RUN_FAILED = {
        Type     = "Task"
        Resource = "arn:aws:states:::dynamodb:putItem"

        Arguments = {
          TableName = aws_dynamodb_table.audit.name
          Item = {
            run_id     = { S = "{% $states.context.Execution.Name %}" }
            event_type = { S = "END" }
            status     = { S = "FAILED" }
            ended_at   = { S = "{% $states.context.State.EnteredTime %}" }
            reason     = { S = "{% $states.input.errorMessage ? $states.input.errorMessage : 'FAILED' %}" }
            details    = { S = "{% $string($states.input) %}" }
          }
        }

        Next   = "PIPELINE_FAIL_ALERT"
        Output = "{% $states.input %}"
      }

      PIPELINE_FAIL_ALERT = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"

        Arguments = {
          TopicArn = aws_sns_topic.alerts.arn
          Subject  = "Pipeline FAILED"
          Message  = "{% 'Pipeline failed. RunId: ' & $states.context.Execution.Name & '\\nReason: ' & ($states.input.errorMessage ? $states.input.errorMessage : 'FAILED') %}"
        }

        End = true
      }
    }
  })
}
