open Core
open Types
open Jobs.Vinted_jobs
open Oozie_xml_builder

let () =
  List.iter vinted_jobs ~f:(fun job -> job
                                       |> cluster_job_to_yojson
                                       |> Yojson.Safe.pretty_to_string
                                       |> print_endline);
  build_bundle AfterWork Production vinted_jobs "./test_bundle";
  print_endline "All good"
