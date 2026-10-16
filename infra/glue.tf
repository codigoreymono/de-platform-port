resource "aws_glue_catalog_database" "data" {
  name        = var.glue_database_name
  description = "NYC 311 September 2026 Silver and Gold products."
}

resource "aws_glue_catalog_table" "silver" {
  name          = "silver"
  database_name = aws_glue_catalog_database.data.name
  table_type    = "EXTERNAL_TABLE"
  description   = "September-created NYC 311 requests at recorded observation times."

  parameters = {
    classification        = "parquet"
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${var.bucket_name}/${local.mode_namespace}/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.silver_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  partition_keys {
    name = "created_date"
    type = "date"
  }
}

resource "aws_glue_partition" "silver" {
  for_each = toset(local.cohort_days)

  table_name       = aws_glue_catalog_table.silver.name
  database_name    = aws_glue_catalog_database.data.name
  partition_values = [each.value]

  storage_descriptor {
    location      = "s3://${var.bucket_name}/${local.mode_namespace}/silver/created_date=${each.value}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = local.silver_columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_glue_catalog_table" "gold" {
  for_each = local.gold_columns

  name          = each.key
  database_name = aws_glue_catalog_database.data.name
  table_type    = "EXTERNAL_TABLE"
  description   = "Gold product ${each.key} for the selected published run."

  parameters = {
    classification        = "parquet"
    EXTERNAL              = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    # The Gold Lambda repoints this location to the selected run after success.
    location      = "s3://${var.bucket_name}/${local.mode_namespace}/gold/run=unpublished/${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }

  lifecycle {
    # The runtime catalog update owns the location; Terraform must not revert it.
    ignore_changes = [storage_descriptor]
  }
}
