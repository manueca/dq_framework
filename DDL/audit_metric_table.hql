CREATE EXTERNAL TABLE `audit_metric_table`(
  `id` string,
  `condition_to_check` string,
  `kpi` string,
  `kpi_val` decimal(38,5),
  `status` string,
  `region_cd` string,
  `cv` string,
  `reason` string,
  `average` decimal(38,5),
  `kpi_forecast` decimal(38,5),
  `pid_val` string,
  `partition_col_nm` string,
  `actual_variance` decimal(38,5))
PARTITIONED BY (
  `team_name` string,
  `process_dt` string,
  `table_name` string,
  `environment` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://<bucket>/prod/qa-framework/audit_metric_table'
