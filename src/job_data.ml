open Core
open Types
open Vinted

let rec spark_tier_data tier environment =
  let common_tier_ops tier = spark_tier_data tier None in
  match tier, environment with
  | XS, None -> [
      "spark.driver.memory", "512M";
      "spark.executor.memory", "512M";
      "spark.executor.cores", "1";
      "spark.dynamicAllocation.enabled", "false";
      "spark.executor.instances", "1";
    ]
  | S, None -> [
      "spark.driver.memory", "512M";
      "spark.executor.memory", "1024M";
      "spark.executor.cores", "1";
      "spark.dynamicAllocation.maxExecutors", "16";
    ]
  | M, None -> [
      "spark.driver.memory", "1024M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
      "spark.dynamicAllocation.maxExecutors", "32";
    ]
  | M, Some Smoke -> (
      List.append [
        "spark.driver.memory", "1024M";
        "spark.executor.memory", "4096M";
        "spark.executor.cores", "2";
      ] (common_tier_ops M)
    )
  | M, Some Sampled -> List.append [
      "spark.driver.memory", "1024M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
    ] (common_tier_ops M)
  | L, None -> [
      "spark.driver.memory", "4096M";
      "spark.executor.memory", "8192M";
      "spark.executor.cores", "2";
      "spark.dynamicAllocation.maxExecutors", "64";
    ]
  | L, Some Smoke -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
    ] (common_tier_ops L)
  | L, Some Sampled -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
    ] (common_tier_ops L)
  | XL, None -> [
      "spark.driver.memory", "16000M"; (* much RAM for driver to collect data *)
      "spark.executor.memory", "8000M";
      "spark.executor.cores", "2";
      "spark.yarn.executor.memoryOverhead", "8192";
      "spark.driver.maxResultSize", "8g";
      "spark.dynamicAllocation.minExecutors", "2";
      "spark.dynamicAllocation.maxExecutors", "64";
      "spark.sql.shuffle.partitions", "1024";
    ]
  | XL, Some Smoke -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
    ] (common_tier_ops XL)
  | XL, Some Sampled -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "4096M";
      "spark.executor.cores", "2";
    ] (common_tier_ops XL)
  | XXL, None -> [
      "spark.driver.memory", "28000M";
      "spark.executor.memory", "25000M";
      "spark.executor.cores", "4";
      "spark.yarn.executor.memoryOverhead", "6144";
      "spark.driver.maxResultSize", "16g";
      "spark.dynamicAllocation.minExecutors", "10";
      "spark.dynamicAllocation.initialExecutors", "20";
      "spark.dynamicAllocation.maxExecutors", "64";
      "spark.default.parallelism", "512";
      "spark.sql.shuffle.partitions", "2048";
    ]
  | XXL, Some Smoke -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "8192M";
      "spark.executor.cores", "2";
      "spark.dynamicAllocation.minExecutors", "5";
    ] (common_tier_ops XXL)
  | XXL, Some Sampled -> List.append [
      "spark.driver.memory", "2048M";
      "spark.executor.memory", "8192M";
      "spark.executor.cores", "2";
      "spark.dynamicAllocation.minExecutors", "5";
    ] (common_tier_ops XXL)
  | other, Some _ -> common_tier_ops other

let rec spark_common_options =
  function
  | None -> [
      "spark.master", "yarn-client";

      "spark.sql.warehouse.dir", "hdfs://warehouse:8020/user/hive/warehouse";

      "spark.executor.extraJavaOptions", "-XX:+UseCompressedOops -XX:MaxPermSize=512m";

      "spark.serializer", "org.apache.spark.serializer.KryoSerializer";
      "spark.kryo.registrator", "vinted.warehouse.serializers.VintedKryoRegistrator";
      "spark.rdd.compress", "true";

      "spark.memory.offHeap.enabled", "false";
      "spark.memory.offHeap.size", "4G";

      "spark.eventLog.enabled", "true";
      "spark.yarn.historyServer.address", "spark.vinted.net";
      "spark.eventLog.dir", "hdfs:///user/spark/applicationHistory";

      "spark.dynamicAllocation.enabled", "true";
      "spark.shuffle.service.enabled", "true"; (* required for dynamic allocation *)
      "spark.dynamicAllocation.minExecutors", "1";
      "spark.dynamicAllocation.initialExecutors", "10";
      "spark.dynamicAllocation.maxExecutors", "16";

      "spark.sql.streaming.metricsEnabled", "true";
    ]
  | Some Smoke -> List.append [
      "spark.executor.extraJavaOptions",
      "-XX:+UseCompressedOops -XX:MaxPermSize=512m -Dhadoop.dfs.replication=1";
      "spark.ui.enabled", "false";
      "spark.yarn.executor.memoryOverhead", "";
      "spark.dynamicAllocation.minExecutors", "1";
      "spark.dynamicAllocation.initialExecutors", "5";
      "spark.dynamicAllocation.maxExecutors", "16";
    ] (spark_common_options None)
  | Some Sampled -> List.append [
      "spark.executor.extraJavaOptions",
      "-XX:+UseCompressedOops -XX:MaxPermSize=512m -Dhadoop.dfs.replication=1";
      "spark.ui.enabled", "false";
      "spark.yarn.executor.memoryOverhead", "";
      "spark.dynamicAllocation.minExecutors", "1";
      "spark.dynamicAllocation.initialExecutors", "5";
      "spark.dynamicAllocation.maxExecutors", "16";
    ] (spark_common_options None)
  | Some _ -> spark_common_options None

let dedup_settings_first_only settings =
  let final = ref [] in
  let setting_doesnt_exist k =
    List.find !final ~f:(fun (k2, _) -> k = k2) |> Option.is_none
  in
  List.iter settings ~f:(fun (k, v) ->
      if setting_doesnt_exist k then
        final := (k, v) :: !final
    );
  List.rev !final

let find_config (conf_list: setting list) ~key ~default =
  List.filter_map conf_list ~f:(fun (k, v) ->
      if k = key then
        Some v
      else
        None
    ) |> List.hd |> Option.value ~default

let config_exists conf_list key =
  List.find conf_list ~f:(fun (k, _) -> k = key) |> Option.is_some

let dashed_job_name job =
  let repl_regex = Str.regexp "\\(.\\)\\([A-Z]\\)" in
  Str.global_replace repl_regex "\\1-\\2" job.job_name |> String.lowercase

let add_java_options_to_string currentops ~newopts =
  let current =
    if currentops = "" then
      []
    else
      String.split ~on:' ' currentops
  in
  List.append newopts current
  |> String.concat ~sep:" "

let escape_bash str = Str.global_replace (Str.regexp "\\$") "\\\\$" str

let custom_spark_options environment curr_settings job =
  let extra_opts = ref [] in
  let add_opt k v = extra_opts := (k, v) :: !extra_opts in
  let dashed_job_name = dashed_job_name job in
  let max_executors =
    if find_config curr_settings ~key:"spark.dynamicAllocation.enabled" ~default:"false"
       |> bool_of_string then (
      (* we should always find this key at this point *)
      find_config curr_settings
        ~key:"spark.dynamicAllocation.maxExecutors" ~default:"not a number"
      |> int_of_string_opt
    ) else (
      find_config curr_settings
        ~key:"spark.dynamicAllocation.maxExecutors" ~default:"not a number"
      |> int_of_string_opt
    ) in
  let executor_cores = find_config curr_settings
      ~key:"spark.executor.cores" ~default:"2" |> int_of_string in
  Option.iter max_executors ~f:(fun max_executors ->
      let parallelism = max_executors * executor_cores * job.job_parallelism_multiplier
                        |> string_of_int in
      add_opt "spark.default.parallelism" parallelism;
      add_opt "spark.sql.shuffle.partitions" parallelism;
    );

  add_opt "spark.metrics.namespace"
    (Printf.sprintf "%s.%s"
       (environment_to_string environment)
       dashed_job_name);

  (* add oozie job information *)
  let curr = find_config curr_settings ~key:"spark.executor.extraJavaOptions" ~default:"" in
  let extra_java_opts =
    add_java_options_to_string curr ~newopts:[
      "-Doozie.job.id=${wf:id()}";
      (Printf.sprintf
         "-Doozie.action.id=${wf:id()}@%s"
         dashed_job_name);
    ] in
  add_opt "spark.driver.extraJavaOptions" extra_java_opts;
  add_opt "spark.executor.extraJavaOptions" extra_java_opts;

  !extra_opts

let final_spark_job_options environment pipeline job =
  let static_options =
    List.concat
    [
      (List.filter_map
         job.job_spark_environment_options
         ~f:(function (env, kv) ->
             if env = environment
             then Some kv
             else None));
      job.job_spark_options;
      (spark_tier_data
         (Option.value_exn job.job_spark_tier)
         (Some pipeline));
      (spark_common_options (Some pipeline));
    ] in
  let custom_options = custom_spark_options environment static_options job in
  (* custom code can override static options as it appears first *)
  List.concat [custom_options; static_options] |> dedup_settings_first_only

let job_spark_default_driver_memory = "512M"

let job_default_map_memory_mb = 32 * 1024 |> string_of_int

let job_spark_driver_memory environment pipeline job =
  final_spark_job_options environment pipeline job
  |> find_config ~key:"spark.driver.memory" ~default:job_spark_default_driver_memory

let job_map_memory_mb pipeline =
  if pipeline = Sampled then
    "6144"
  else
    job_default_map_memory_mb
