open Types

let schema_registry_url = "http://svc-schema-registry.vinted.net:45001"

let warehouse_schemas_url = Printf.sprintf "%s/events.warehouse.json" schema_registry_url

let environment_to_string =
  function
  | Production -> "production"
  | Development -> "development"
  | Smoke -> "smoke"
  | Sampled -> "sampled"

let pipeline_to_string =
  function
  | AfterWork -> "after_work"
  | Hourly -> "hourly"
  | InfraHourly -> "infra_hourly"
  | Nightly -> "nightly"
  | InfraNightly -> "infra_nightly"
  | InfraHdfsBackup -> "infra_hdfs_backup"
  | Sampled -> "sampled"
  | Smoke -> "smoke"

let parquet_events_path = "/user/hive/warehouse/events2.db/#{environment}/output"

let parquet_events_source_path =
  "/user/hive/warehouse/events2.db/${environment}/partitioned_event_buffer"

let parquet_events_temp_path_hourly =
  "/user/hive/warehouse/events2.db/${environment}/temp_hourly_consolidator_output"

let parquet_events_temp_path_late =
  "/user/hive/warehouse/events2.db/${environment}/temp_late_consolidator_output"

let event_extractor_base_path = "/etl/events-raw-kafka2"

(* TODO: read from environment variable *)
let spark_version = "2.3.2"

let spark_dist_path = "${spark_jobs_app_path}/spark"

let spark_dist_path_real = "/opt/spark-" ^ spark_version

let spark_dist_jar = "spark-dist.jar"

let spark_dist_jar_path = Printf.sprintf "%s/%s" spark_dist_path_real spark_dist_jar

let spark_dist_jar_relative = Printf.sprintf "%s/%s" spark_dist_path spark_dist_jar

let spark_job_class_path = "/etc/hadoop/conf:/etc/hive/conf:."

let spark_default_app_conf = "${spark_jobs_app_path}/spark-default-application.conf"

let vinted_oozie_slack_username = "Oozie"

let oozie_wf_url = "${oozie_hue_url}list_oozie_workflow/${wf:id()}"

let vinted_slack_webhook_url =
  print_endline "Using fake slack url change pls";
  "https://hooks.slack.com/trololololololo"
