open Core
open Types

let job_list_check_pipelines jobs =
  List.iter jobs ~f:(fun job ->
      if List.is_empty job.job_pipelines then (
        raise (Failure (Printf.sprintf "Job %s has no pipelines assigned to it." job.job_name));
      )
    )

let jobs_check_duplicate_names jobs =
  let job_names = List.map jobs ~f:(fun job -> job.job_name)
                  |> List.sort ~compare:String.compare in
  let unique_names_len = job_names
                         |> List.dedup_and_sort ~compare:String.compare
                         |> List.length in
  if unique_names_len < List.length jobs then (
    let last_name = ref "" in
    List.iter job_names ~f:(fun job_name ->
        if job_name = !last_name then (
          prerr_endline ("Repeating job name: " ^ job_name);
        );
        last_name := job_name
      );
    raise (Failure "Jobs have duplicate names. Repeating names printed.")
  )

let job_by_name job_list name =
  List.find job_list ~f:(fun job -> job.job_name = name)

let job_check_dependencies_exist jobs =
  List.iter jobs ~f:(fun job ->
      List.iter (List.append job.job_soft_dependencies job.job_hard_dependencies) ~f:(fun dep ->
          if job_by_name jobs dep = None then (
            raise (Failure (Printf.sprintf
                              "Dependency %s for job %s does not exist" dep job.job_name))
          )
        )
    )

let rec job_list_check_single_circular_dependency current_names job all_jobs =
  let curr_len = List.length current_names in
  let unique_len = List.dedup_and_sort ~compare:String.compare current_names |> List.length in
  if unique_len < curr_len then (
    List.iter (List.rev current_names)
      ~f:(fun job_name ->
          print_endline (job_name ^ " depends on ->")
        );
    print_endline "...";
    raise (Failure "Jobs have circular dependencies. Circular job path printed above.")
  );
  List.iter (List.append job.job_soft_dependencies job.job_hard_dependencies)
    ~f:(fun dep_job_name ->
        job_list_check_single_circular_dependency
          (dep_job_name :: current_names)
          (* we already checked that all dependencies exist with earlier check *)
          (Option.value_exn (job_by_name all_jobs dep_job_name))
          all_jobs
      )

let job_list_check_circular_dependencies jobs =
  List.iter jobs ~f:(fun job -> job_list_check_single_circular_dependency
                        [job.job_name] job jobs)

let spark_job_must_have_tier job =
  if job.job_type = SparkJob && Option.is_none job.job_spark_tier then (
    raise (Failure (Printf.sprintf "Spark job %s has no tier assigned to it" job.job_name))
  )

let spark_job_must_have_class job =
  if job.job_type = SparkJob && job.job_spark_class_name = "" then (
    raise (Failure (Printf.sprintf "Spark job %s must have spark class assigned to it"
                      job.job_name))
  )

let retry_max_non_negative job =
  if job.job_retry_max < 0 then raise (Failure "retry_max must be non negative")

let job_unique_dependency_names job =
  let all_jobs =
    List.append
      job.job_hard_dependencies
      job.job_soft_dependencies
  in
  let uniques = List.dedup_and_sort ~compare:String.compare all_jobs in
  if List.length uniques < List.length all_jobs then (
    raise (Failure (Printf.sprintf "Job %s has duplicate dependency names." job.job_name));
  )


let job_static_analysis_functions = [
  job_list_check_pipelines;
  jobs_check_duplicate_names;
  job_check_dependencies_exist;
  job_list_check_circular_dependencies;
]

let job_post_build_analysis_functions = [
  job_unique_dependency_names;
  retry_max_non_negative;
  spark_job_must_have_tier;
  spark_job_must_have_class;
]

(* TODO: check naming convetions for config values and job names etc. *)

let job_list_static_analysis (jobs: cluster_job list) =
  List.iter job_static_analysis_functions ~f:(fun func -> func jobs)

let job_post_build_static_analysis job =
  List.iter job_post_build_analysis_functions ~f:(fun func -> func job)
