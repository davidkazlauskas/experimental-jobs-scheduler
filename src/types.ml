
type cluster_job_type =
  | SparkJob
  | DistcpJob
[@@deriving yojson]

type environment =
  | Production
  | Development
  | Smoke
  | Sampled
[@@deriving yojson]

type spark_tier =
  | XS
  | S
  | M
  | L
  | XL
  | XXL
[@@deriving yojson]

type cluster_job_pipeline =
  | Hourly
  | InfraHourly
  | Nightly
  | InfraNightly
  | InfraHdfsBackup
  | AfterWork
  | Sampled
  | Smoke
[@@deriving yojson]

type time_unit =
  | Hours of int
  | Minutes of int
  | Seconds of int
[@@deriving yojson]

type setting = (string * string)
  [@@deriving yojson]

type cluster_job = {
  job_sequence_id: int;
  job_type: cluster_job_type;
  job_name: string;
  job_pipelines: cluster_job_pipeline list;
  job_hard_dependencies: string list;
  job_soft_dependencies: string list;
  job_options: setting list;
  job_environment_options: (environment * setting) list;
  job_retry_max: int;
  job_sla_kill_after_seconds: int;
  job_spark_tier: spark_tier option;
  job_spark_class_name: string;
  job_spark_options: setting list;
  job_spark_environment_options: (environment * setting) list;
  job_parallelism_multiplier: int;
} [@@deriving yojson]

type cluster_jobs_list = cluster_job list
