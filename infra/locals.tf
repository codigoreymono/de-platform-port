locals {
  tags = merge(
    {
      Project   = var.project
      ManagedBy = "terraform"
      Cohort    = "2026-09"
    },
    var.tags,
  )

  prefix         = trimsuffix(var.data_prefix, "/")
  cohort_start   = "2026-09-01"
  cohort_end     = "2026-10-01"
  cohort_month   = "2026-09"
  cohort_days    = [for day in range(1, 31) : format("%s-%02d", local.cohort_month, day)]
  catalog        = { database = var.glue_database_name }
  mode_namespace = "${local.prefix}/${var.mode}"

  task_days = [
    for day in range(1, 31) : {
      bucket     = var.bucket_name
      prefix     = local.prefix
      mode       = var.mode
      config     = {}
      start_date = format("%s-%02d", local.cohort_month, day)
      end_date   = day == 30 ? local.cohort_end : format("%s-%02d", local.cohort_month, day + 1)
    }
  ]

  # Silver Parquet already carries a physical created_date column; the table
  # exposes it only as a Hive partition key so Athena does not see two columns.
  silver_columns = [
    { name = "unique_key", type = "string" },
    { name = "agency", type = "string" },
    { name = "agency_name", type = "string" },
    { name = "complaint_type", type = "string" },
    { name = "descriptor", type = "string" },
    { name = "borough", type = "string" },
    { name = "incident_zip", type = "string" },
    { name = "status", type = "string" },
    { name = "open_data_channel_type", type = "string" },
    { name = "created_date_local", type = "timestamp" },
    { name = "closed_date_local", type = "timestamp" },
    { name = "resolution_action_updated_date_local", type = "timestamp" },
    { name = "created_date_utc", type = "timestamp" },
    { name = "closed_date_utc", type = "timestamp" },
    { name = "resolution_action_updated_date_utc", type = "timestamp" },
    { name = "source_updated_at_utc", type = "timestamp" },
    { name = "observed_at_utc", type = "timestamp" },
    { name = "lifecycle_class", type = "string" },
    { name = "duration_eligible", type = "boolean" },
    { name = "quality_flags", type = "array<string>" },
    { name = "source_run_id", type = "string" },
    { name = "source_page_key", type = "string" },
    { name = "source_row_number", type = "bigint" },
    { name = "transformation_version", type = "string" },
  ]

  gold_columns = {
    demand_daily = [
      { name = "created_date", type = "date" },
      { name = "borough", type = "string" },
      { name = "complaint_type", type = "string" },
      { name = "request_count", type = "bigint" },
      { name = "category_share", type = "double" },
    ]
    closure_duration = [
      { name = "agency", type = "string" },
      { name = "complaint_type", type = "string" },
      { name = "valid_closed_count", type = "bigint" },
      { name = "median_closure_hours", type = "double" },
      { name = "p90_closure_hours", type = "double" },
    ]
    outstanding_requests = [
      { name = "agency", type = "string" },
      { name = "borough", type = "string" },
      { name = "lifecycle_class", type = "string" },
      { name = "age_band", type = "string" },
      { name = "outstanding_count", type = "bigint" },
      { name = "unknown_status_count", type = "bigint" },
    ]
    channel_usage = [
      { name = "created_date", type = "date" },
      { name = "borough", type = "string" },
      { name = "channel", type = "string" },
      { name = "request_count", type = "bigint" },
      { name = "channel_share", type = "double" },
    ]
  }

  lambda_profiles = {
    ingest = {
      memory_mib  = 512
      timeout     = 900
      handler     = "nyc311.lambda_ingest.handler"
      zip_key     = var.ingest_zip_key
      zip_hash    = var.ingest_source_hash
      environment = {}
    }
    curate = {
      memory_mib  = 1024
      timeout     = 900
      handler     = "nyc311.lambda_curate.handler"
      zip_key     = var.curate_zip_key
      zip_hash    = var.curate_source_hash
      environment = { PYTHONTZPATH = "/var/task/timezone" }
    }
    gold = {
      memory_mib  = 2048
      timeout     = 900
      handler     = "nyc311.lambda_gold.handler"
      zip_key     = var.gold_zip_key
      zip_hash    = var.gold_source_hash
      environment = {}
    }
  }
}
