open Types
open Job_builder
open Vinted

module Vinted_jobs = struct
  include Job_builder

  let vinted_jobs = build_jobs (fun () ->
      job "LateEventParquetConsolidator" SparkJob begin
        spark_class_name "EventParquetConsolidator";
        spark_tier L;
        retry_max 0;

        pipeline Sampled;
        pipeline Smoke;
        pipeline AfterWork;

        let since_days_ago = int 7 in
        let until_days_ago = int 1 in

        job_configs [
          "event_consolidator.warehouse_schemas_url", warehouse_schemas_url;
          "event_consolidator.since_days_ago", since_days_ago;
          "event_consolidator.until_days_ago", until_days_ago;
          "event_consolidator.parquet_database", "events";
          "event_consolidator.events_blacklist", "list.show_item";
          "event_consolidator.extra_provision_days", int 7;
          "event_consolidator.parallelism", int 2;
          "event_consolidator.target_file_size", "64MB";
          (* TODO: interpolate config paths *)
          "event_consolidator.events_destination_path", parquet_events_path;
          "event_consolidator.events_source_path", parquet_events_source_path;
          "event_consolidator.events_temp_path", parquet_events_temp_path_late;
        ];

        env_configs Sampled [
          "event_consolidator.events", "user.click";
          "event_consolidator.since_days_ago", int 0;
          "event_consolidator.until_days_ago", int 0;
        ];

        env_configs Smoke [
          "event_consolidator.events", "user.click";
          "event_consolidator.since_days_ago", int 0;
          "event_consolidator.until_days_ago", int 0;
        ];

        spark_config "spark.eventLog.enabled" (bool false);

        env_spark_configs Production [
          "spark.executor.cores", int 2;
          "spark.executor.instances", int 32;
          "spark.executor.memory", "29G";
          "spark.driver.memory", "8G";
          "spark.dynamicAllocation.enabled", bool false;
          "spark.yarn.executor.memoryOverhead", "1G";
          "spark.sql.shuffle.partitions", int 8192;
        ];
      end;

      job "LateEventParquetConsolidatorListShowItem" SparkJob begin
        spark_class_name "EventParquetConsolidator";
        spark_tier L;
        retry_max 0;

        pipeline AfterWork;

        let since_days_ago = int 7 in
        let until_days_ago = int 1 in

        job_configs [
          "event_consolidator.warehouse_schemas_url", warehouse_schemas_url;
          "event_consolidator.since_days_ago", since_days_ago;
          "event_consolidator.until_days_ago", until_days_ago;
          "event_consolidator.parquet_database", "events";
          "event_consolidator.events", "list.show_item";
          "event_consolidator.extra_provision_days", int 7;
          "event_consolidator.parallelism", int 2;
          "event_consolidator.target_file_size", "64MB";
          "event_consolidator.events_destination_path", "/user/hive/warehouse/events2.db/#{environment}/output";
          "event_consolidator.events_source_path", "/user/hive/warehouse/events2.db/#{environment}/partitioned_event_buffer";
          "event_consolidator.events_temp_path", "/user/hive/warehouse/events2.db/#{environment}/temp_late_consolidator_output";
        ];

        env_configs Sampled [
          "event_consolidator.events", "user.click";
          "event_consolidator.since_days_ago", int 0;
          "event_consolidator.until_days_ago", int 0;
        ];

        env_configs Smoke [
          "event_consolidator.events", "user.click";
          "event_consolidator.since_days_ago", int 0;
          "event_consolidator.until_days_ago", int 0;
        ];

        env_spark_configs Production [
          "spark.executor.cores", int 2;
          "spark.executor.instances", int 64;
          "spark.executor.memory", "11G";
          "spark.driver.memory", "8G";
          "spark.dynamicAllocation.enabled", bool false;
          "spark.yarn.executor.memoryOverhead", "1G";
          "spark.sql.shuffle.partitions", int 15360;
        ];
      end;

      job "HistoricShadowSaleFact" SparkJob begin
        spark_class_name "HistoricShadowSaleFact";
        spark_tier XL;

        pipeline AfterWork;
        pipeline Sampled;
        pipeline Smoke;

        (* doesn't exist yet *)
        (* depends_on "ShadowSaleFact"; *)

        sla_kill_after default_kill_after;

        env_spark_configs Production [
          "spark.executor.cores", int 4;
          "spark.dynamicAllocation.maxExecutors", int 128;
          "spark.default.parallelism", int 8192;
          "spark.sql.shuffle.partitions", int 8192;
        ];

        env_spark_configs Development [
          "spark.executor.cores", int 4;
          "spark.dynamicAllocation.maxExecutors", int 128;
          "spark.default.parallelism", int 8192;
          "spark.sql.shuffle.partitions", int 8192;
        ];

        job_config "input.coalesce" (int 8192);
      end;

      job "AccountingLedgerBatchView" SparkJob begin
        spark_class_name "AccountingLedgerBatchView";
        spark_tier M;

        pipeline Sampled;
        pipeline Smoke;
        pipeline AfterWork;

        (* depends_on "ImportMysqlMorningAmericaForJobs"; *)

        sla_kill_after default_kill_after;
      end;

      job "MobileUserRetentionBatchView" SparkJob begin
        spark_class_name "MobileUserRetentionBatchView";
        spark_tier XL;

        pipeline Sampled;
        pipeline Smoke;
        pipeline AfterWork;

        (* depends_on "DailyActiveUsersBatchView";
         * depends_on "AnonIdMapBatchView";
         * depends_on "AppInstallFact"; *)

        sla_kill_after default_kill_after;
      end;

      job "InvalidEventEnrichedFact" SparkJob begin
        spark_class_name "InvalidEventEnrichedFact";
        spark_tier L;

        (* input can't be easily sampled, but specification covers integration
         * with EventExtractor and Kafka, so it's OK to skip PIPELINE_SAMPLED *)
        pipeline AfterWork;

        (* depends_on "AppVersionDimension"; *)
        (* depends_on "InvalidEventErrorsExtractor"; *)

        sla_kill_after default_kill_after;

        job_config "event_extractor.base_path" event_extractor_base_path;
      end;

      job "MessagesEnrichedFact" SparkJob begin
        spark_class_name "MessagesEnrichedFact";
        spark_tier XL;

        pipeline Sampled;
        pipeline Smoke;
        (* pipeline AfterWork; *)

        (* depends_on "UserDimension"; *)
        (* depends_on "UserTemporalDimension"; *)
        (* depends_on "ItemDimension"; *)
        (* depends_on "ImportMysqlNightlyEuropeForJobs";
         * depends_on "ImportMysqlNightlyDelayedEuropeForJobs"; *)

        sla_kill_after default_kill_after;

        spark_config "spark.sql.shuffle.partitions" (int 2000);
      end;

      job "SizeRecommendationsBatchView" SparkJob begin
        spark_class_name "SizeRecommendationsBatchView";
        spark_tier XL;

        pipeline AfterWork;
        pipeline Sampled;
        pipeline Smoke;

        (* depends_on "ItemSaleFact"; *)
        (* depends_on "ImportMysqlNightlyEuropeForJobs"; *)

        sla_kill_after default_kill_after;

        job_config "silent_sizes_model_path" "/user/hive/models/silentSizesGBT.model";
      end;
    )
end
