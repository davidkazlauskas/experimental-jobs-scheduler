open Core
open Types
open Job_static_analysis

let time_unit_to_seconds =
  function
  | Hours h -> h * 3600
  | Minutes m -> m * 60
  | Seconds s -> s

let default_kill_after = Hours 4

module Job_builder : sig
  val build_jobs : (unit -> unit) -> cluster_jobs_list
  val job : string -> cluster_job_type -> unit -> unit
  val pipeline : cluster_job_pipeline -> unit
  val job_config : string -> string -> unit
  val job_configs : (string * string) list -> unit
  val env_config : environment -> string -> string -> unit
  val env_configs : environment -> (string * string) list -> unit
  val depends_on : string -> unit
  val soft_depends_on : string -> unit
  val retry_max : int -> unit
  val sla_kill_after : time_unit -> unit
  val spark_tier : spark_tier -> unit
  val spark_class_name : string -> unit
  val spark_config : string -> string -> unit
  val spark_configs : (string * string) list -> unit
  val env_spark_config : environment -> string -> string -> unit
  val env_spark_configs : environment -> (string * string) list -> unit
  val int : int -> string
  val bool : bool -> string
end = struct
  let job_default_retry_max = 2
  let job_default_kill_after = Hours 3 |> time_unit_to_seconds

  type resettable_fields = {
    mutable job_options: setting list;
    mutable job_env_options: (environment * setting) list;
    mutable job_hard_dependencies: string list;
    mutable job_soft_dependencies: string list;
    mutable job_pipelines: cluster_job_pipeline list;
    mutable job_retry_max: int;
    mutable job_sla_kill_after_seconds: int;
    (* spark *)
    mutable job_spark_tier: spark_tier option;
    mutable job_spark_class: string;
    mutable job_spark_config: setting list;
    mutable job_spark_env_config: (environment * setting) list;
  }

  let mk_default_fields () = {
    job_options = [];
    job_env_options = [];
    job_hard_dependencies = [];
    job_soft_dependencies = [];
    job_pipelines = [];
    job_retry_max = job_default_retry_max;
    job_sla_kill_after_seconds = job_default_kill_after;
    job_spark_tier = None;
    job_spark_class = "";
    job_spark_config = [];
    job_spark_env_config = [];
  }

  let buffer = ref (mk_default_fields ())
  let jobs_list = ref []
  let job_sequence_id = ref 0

  let next_job_sequence_id () =
    job_sequence_id := !job_sequence_id + 1;
    !job_sequence_id

  let reset_job_builder_state () =
    buffer := mk_default_fields ()

  let job (name: string) (job_type: cluster_job_type) (_: unit) =
    let b = !buffer in
    let new_job = {
      job_sequence_id = next_job_sequence_id ();
      job_type = job_type;
      job_name = name;
      job_options = List.rev b.job_options;
      job_hard_dependencies = List.rev b.job_hard_dependencies;
      job_soft_dependencies = List.rev b.job_soft_dependencies;
      job_pipelines = List.rev b.job_pipelines;
      job_environment_options = List.rev b.job_env_options;
      job_retry_max = b.job_retry_max;
      job_sla_kill_after_seconds = b.job_sla_kill_after_seconds;
      job_spark_tier = b.job_spark_tier;
      job_spark_class_name = b.job_spark_class;
      job_spark_options = List.rev b.job_spark_config;
      job_spark_environment_options = List.rev b.job_spark_env_config;
      job_parallelism_multiplier = 1;
    } in
    job_post_build_static_analysis new_job;
    jobs_list := new_job :: !jobs_list;
    reset_job_builder_state ()

  let job_config key value =
    !buffer.job_options <- (key, value) :: !buffer.job_options

  let job_configs (the_list: (string * string) list) =
    List.iter the_list ~f:(fun (k, v) -> job_config k v)

  let env_config environment key value =
    !buffer.job_env_options <- (environment, (key, value)) :: !buffer.job_env_options

  let env_configs environment kv_list =
    List.iter kv_list ~f:(fun (k, v) ->
        env_config environment k v
      )

  let build_jobs (the_func: unit -> unit) : cluster_jobs_list =
    the_func ();
    let result = List.rev !jobs_list in
    jobs_list := [];
    job_list_static_analysis result;
    result

  let depends_on job =
    !buffer.job_hard_dependencies <- job :: !buffer.job_hard_dependencies

  let soft_depends_on job =
    !buffer.job_soft_dependencies <- job :: !buffer.job_soft_dependencies

  let pipeline (pipeline: cluster_job_pipeline) =
    !buffer.job_pipelines <- pipeline :: !buffer.job_pipelines

  let retry_max num =
    !buffer.job_retry_max <- num

  let sla_kill_after tu =
    !buffer.job_sla_kill_after_seconds <- time_unit_to_seconds tu

  let spark_tier (tier: spark_tier) =
    !buffer.job_spark_tier <- Some tier

  let spark_class_name class_name =
    !buffer.job_spark_class <- class_name

  let spark_config key value =
    !buffer.job_spark_config <- (key, value) :: !buffer.job_spark_config

  let spark_configs kv_list =
    List.iter kv_list ~f:(fun (key, value) -> spark_config key value)

  let env_spark_config env key value =
    !buffer.job_spark_env_config <- (env, (key, value)) :: !buffer.job_spark_env_config

  let env_spark_configs env kv_list =
    List.iter kv_list ~f:(fun (key, value) -> env_spark_config env key value)

  let int i = string_of_int i

  let bool b = string_of_bool b
end
