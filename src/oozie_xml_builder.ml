open Core
open Yojson
open Xml
open Types
open Job_data
open Vinted

type track_progress_json_fields = {
  title: string;
  value: string;
  short: bool;
} [@@deriving yojson]

type track_progress_json_attachment = {
  mrkdwn_in: string list;
  title: string;
  title_link: string;
  color: string;
  fields: track_progress_json_fields list;
} [@@deriving yojson]

type track_progress_json_payload = {
  channel: string;
  username: string;
  icon_emoji: string;
  attachments: track_progress_json_attachment list;
} [@@deriving yojson]

let slack_status_color arg =
  match arg with
  | "start" -> "#439FE0"
  | "success" -> "good"
  | "retrying" -> "warning"
  | "failed" -> "danger"
  | _ -> arg

let track_progress_json_payload job job_suffix job_status =
  let notification_properties = ref [] in
  let app_notif_property k v = notification_properties := (k, v) :: !notification_properties in
  app_notif_property "Environment" "${job_name_suffix}";
  app_notif_property "Pipeline" "${job}";
  app_notif_property "Started at" "${pipeline_start_time}";
  if job_status = "retrying" then
    app_notif_property "Retry at" "${timestamp()}";
  if job_status = "failed" || job_status = "success" then
    app_notif_property "Ended at" "${timestamp()}";
  {
    (* TODO: pick channel by user *)
    channel = "#dwh-monitoring";
    username = vinted_oozie_slack_username;
    icon_emoji = "gun";
    attachments = [{
        mrkdwn_in = ["fields"];
        title = Printf.sprintf "%s [%s]" (job.job_name ^ job_suffix) job_status;
        title_link = oozie_wf_url;
        color = slack_status_color job_status;
        fields =
          !notification_properties
          |> List.rev
          |> List.map ~f:(fun (k, v) -> { title = k; value = v; short = true });
      }]
  }

let inside_xml_tag buf ~start_tag ~end_tag func =
  Buffer.add_string buf start_tag;
  func ();
  Buffer.add_string buf end_tag

let pretty_xml inp_xml =
  try
  let xml = Xml.parse_string inp_xml |> Xml.to_string_fmt in
  (* not preserved after parsing *)
  "<?xml version=\"1.0\"?>\n" ^ xml
  with
  | (Xml.Error _) as exn -> (
      prerr_endline "Xml produced is not parseable. Please check your xml.";
      raise exn
    )
  | other -> raise other

let bundle_name pipeline environment =
  Printf.sprintf "pipeline-%s-%s"
    (pipeline_to_string pipeline)
    (environment_to_string environment)

let dashed_job_name_wenv pipeline environment job =
  let pipeline = pipeline_to_string pipeline in
  let environment = environment_to_string environment in
  let dashed_job_name = dashed_job_name job in
  Printf.sprintf "%s-%s-%s" pipeline environment dashed_job_name

let coordinator_name pipeline environment job =
  "pipeline-" ^ dashed_job_name_wenv pipeline environment job ^ "-coord"

let workflow_name pipeline environment job =
  "pipeline-" ^ dashed_job_name_wenv pipeline environment job ^ "-wf"

let oozie_next_kick_off_time (_jobs: cluster_job list) =
  (* TODO: implement kick off time logic *)
  "2019-03-13T20:00Z"

let to_retry_job job =
  { job with job_name = job.job_name ^ "Retry";
             job_parallelism_multiplier = 2 * job.job_parallelism_multiplier }

let build_bundle_str pipeline environment (jobs: cluster_job list) =
  let buf = Buffer.create 512 in
  let app str = Buffer.add_string buf str in
  let bundle_name = bundle_name pipeline environment in
  inside_xml_tag buf
    ~start_tag:(
      Printf.sprintf {|
        <bundle-app xmlns="uri:oozie:bundle:0.1" name="%s">
      |} bundle_name
    )
    ~end_tag:{|</bundle-app>|}
    (fun () ->
       Printf.sprintf {|
          <controls>
            <kick-off-time>%s</kick-off-time>
          </controls>
         |} (oozie_next_kick_off_time jobs) |> app;
       List.iter jobs ~f:(fun job ->
           let coordinator_name = coordinator_name pipeline environment job in
           Printf.sprintf
             {|
               <coordinator name="%s">
                 <app-path>${application_path}/%s.xml</app-path>
               </coordinator>
              |}
             coordinator_name coordinator_name |> app;
         )
    );
  Buffer.contents buf |> pretty_xml

let job_yarn_queue pipeline =
  Printf.sprintf "dwh-jobs-%s"
    (String.tr ~target:'_' ~replacement:'-' (pipeline_to_string pipeline))

let spark_job_oozie_shell_action pipeline environment buf
    ~job
    ~ok_go_to ~error_go_to
    ~retry_max ~retry_interval =
  (* let dashed_job_name_wenv = dashed_job_name_wenv pipeline environment job in *)
  let map_memory = job_map_memory_mb pipeline in
  let driver_memory = job_spark_driver_memory environment pipeline job in
  let dashed_name = dashed_job_name job in
  let shell_exec_name = Printf.sprintf "%s-start-spark.sh" dashed_name in
  let shell_properties = [
    "oozie.launcher.mapreduce.map.memory.mb", map_memory;
    "mapreduce.map.memory.mb", map_memory;
    "oozie.launcher.mapred.child.java.opts", Printf.sprintf "-Xmx%s" driver_memory;
    "mapred.child.java.opts", Printf.sprintf "-Xmx%s" driver_memory;
  ] in
  let workflow_env_variables = [
    "SPARK_DEFAULT_APP_CONF", "${spark_jobs_app_path}/spark-default-application.conf";
    "HADOOP_USER_NAME", "${user_name}";
    "LD_LIBRARY_PATH", "/usr/lib/hadoop/lib/native/";
    "SPARK_APP_NAME", "${job_name_suffix}";
    "JOB_ID", job.job_name;
    "JOB_ENVIRONMENT", environment_to_string environment;
    "JOB_PIPELINE", pipeline_to_string pipeline;
  ] in
  let files = [
    Printf.sprintf "${application_path}/%s" shell_exec_name, shell_exec_name;
    spark_dist_jar_relative, spark_dist_jar;
    spark_default_app_conf, "application.conf";
  ] in
  let app str = Buffer.add_string buf str in
  inside_xml_tag buf
    ~start_tag:(Printf.sprintf {|
      <action name="%s" retry-max="%d" retry-interval="%d">
    |} dashed_name retry_max retry_interval)
    ~end_tag:"</action>"
    (fun () ->
       inside_xml_tag buf
         ~start_tag:{|<shell xmlns="uri:oozie:shell-action:0.2">|}
         ~end_tag:"</shell>"
         (fun () ->
            app {|
              <job-tracker>${job_tracker}</job-tracker>
              <name-node>${name_node}</name-node>
            |};
            inside_xml_tag buf
              ~start_tag:"<configuration>"
              ~end_tag:"</configuration>"
              (fun () ->
                 List.iter shell_properties ~f:(fun (k, v) ->
                     Printf.sprintf {|
                       <property>
                         <name>%s</name>
                         <value>%s</value>
                       </property>
                     |} k v |> app;
                   );
              );

            Printf.sprintf "<exec>%s</exec>" shell_exec_name |> app;

            List.iter workflow_env_variables ~f:(fun (k, v) ->
                Printf.sprintf "<env-var>%s=%s</env-var>" k v |> app;
              );

            List.iter files ~f:(fun (k, v) ->
                Printf.sprintf "<file>%s#%s</file>" k v |> app;
              )

         );

       Printf.sprintf {|<ok to="%s"/>|} ok_go_to |> app;
       Printf.sprintf {|<error to="%s"/>|} error_go_to |> app;
    )

let slack_notification_action buf ~name ~webhook_url ~json_payload ~ok_go_to ~error_go_to =
  let app str = Buffer.add_string buf str in
  inside_xml_tag buf
    ~start_tag:(Printf.sprintf {|<action name="%s" retry-max="2" retry-interval="1">|} name)
    ~end_tag:"</action>"
    (fun () ->
       inside_xml_tag buf
         ~start_tag:{|<shell xmlns="uri:oozie:shell-action:0.1">|}
         ~end_tag:"</shell>"
         (fun () ->
            let payload_as_str = track_progress_json_payload_to_yojson json_payload
                                 |> Yojson.Safe.to_string in
            let payload_sanitized = String.tr ~target:'\'' ~replacement:' ' payload_as_str in
            app {|
              <job-tracker>${job_tracker}</job-tracker>
              <name-node>${name_node}</name-node>
              <exec>shell_exec.sh</exec>
              <argument>/usr/bin/curl</argument>
              <argument>-v</argument>
            |};
            Printf.sprintf "<argument>-d 'payload=%s'</argument>" payload_sanitized |> app;
            Printf.sprintf {|
              <argument>%s</argument>
              <file>${application_path}/shell_exec.sh</file>
              <capture-output/>
            |} webhook_url |> app;
         );
        Printf.sprintf {|<ok to="%s"/>|} ok_go_to |> app;
        Printf.sprintf {|<error to="%s"/>|} error_go_to |> app;
    )

let build_workflow_str pipeline environment job =
  let buf = Buffer.create 512 in
  let app str = Buffer.add_string buf str in
  let dashed_name = dashed_job_name job in
  (* let dashed_job_name_wenv = dashed_job_name_wenv pipeline environment job in *)
  let workflow_name = workflow_name pipeline environment job in
  inside_xml_tag buf
    ~start_tag:(Printf.sprintf
      {|<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="%s">|}
      workflow_name)
    ~end_tag:"</workflow-app>"
    (fun () ->
       Printf.sprintf {|
         <global>
             <job-tracker>${job_tracker}</job-tracker>
             <name-node>${name_node}</name-node>
             <configuration>
               <property>
                 <name>hive.metastore.uris</name>
                 <value>${hive_metastore_uri}</value>
               </property>
               <property>
                 <name>oozie.launcher.mapred.job.queue.name</name>
                 <value>%s</value>
               </property>
             </configuration>
         </global>
         |}
         (job_yarn_queue pipeline) |> app;

       Printf.sprintf {|
         <start to="avoid-duplicate-runs"/>
         <decision name="avoid-duplicate-runs">
           <switch>
             <case to="track-fail">${workflowAlreadyRunning("name=%s;status=RUNNING")}</case>
             <default to="track-start"/>
           </switch>
         </decision>
       |} workflow_name |> app;

       Printf.sprintf {|
         <action name="track-start">
           <!-- noop -->
           <fs/>
           <ok to="%s"/>
           <error to="%s"/>
         </action>
       |} dashed_name dashed_name |> app;

       let retry_job = to_retry_job job in
       let retry_job_name = dashed_job_name retry_job in
       let retry_wrapper_job_name = dashed_name ^ "-retry-wrapper" in
       spark_job_oozie_shell_action pipeline environment buf
         ~job:job
         ~ok_go_to:"track-success"
         ~error_go_to:retry_wrapper_job_name
         ~retry_max:job.job_retry_max ~retry_interval:5;

       slack_notification_action buf
         ~name:retry_wrapper_job_name
         ~webhook_url:vinted_slack_webhook_url
         ~json_payload:(track_progress_json_payload job "RetryWrapper" "retrying")
         ~ok_go_to:retry_job_name
         ~error_go_to:retry_job_name;

       spark_job_oozie_shell_action pipeline environment buf
         ~job:retry_job
         ~ok_go_to:"track-success"
         ~error_go_to:"track-fail"
         ~retry_max:0 ~retry_interval:5;

       app {|
          <action name="track-success">
            <!-- noop -->
            <fs/>
            <ok to="mark-success"/>
            <error to="mark-success"/>
          </action>
       |};

       slack_notification_action buf
         ~name:"track-fail"
         ~webhook_url:vinted_slack_webhook_url
         ~json_payload:(track_progress_json_payload job "" "failed")
         ~ok_go_to:"mark-fail"
         ~error_go_to:"mark-fail";

       app {|
         <action name="mark-success">
           <fs>
             <mkdir path="${wf_success_marker}"/>
             <mkdir path="${wf_finished_marker}"/>
           </fs>
           <ok to="end"/>
           <error to="fail"/>
         </action>
         <action name="mark-fail">
           <fs>
             <mkdir path="${wf_finished_marker}"/>
           </fs>
           <ok to="fail"/>
           <error to="fail"/>
         </action>
         <kill name="fail">
           <message>Task failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
         </kill>
         <end name="end"/>
       |};

       app {|
         <sla:info>
           <sla:nominal-time>${pipeline_start_time}</sla:nominal-time>
           <sla:should-end>480</sla:should-end>
           <sla:max-duration>240</sla:max-duration>
         </sla:info>
       |};
    );
  Buffer.contents buf |> pretty_xml

type pipeline_frequency =
  | Once
  | Hourly
  | ThreeHourly
  | Daily
  | Every28H
  | Every42H
  | Weekly

let dataset_template =
  function
  | Once -> "once"
  | Hourly -> "${YEAR}/${MONTH}/${DAY}/${HOUR}"
  | ThreeHourly -> "${YEAR}/${MONTH}/${DAY}/${HOUR}"
  | Daily -> "${YEAR}/${MONTH}/${DAY}"
  | Every28H -> "${YEAR}/${MONTH}/${DAY}"
  | Every42H -> "${YEAR}/${MONTH}/${DAY}"
  | Weekly -> "${YEAR}/${MONTH}/${DAY}"

let build_coordinator_str pipeline environment job =
  let buf = Buffer.create 512 in
  let app str = Buffer.add_string buf str in
  let coordinator_name = coordinator_name pipeline environment job in
  let pipeline_frequency = Daily in
  let frequency_minutes = 1440 in
  let start_time = "2019-03-13T20:00Z" in
  let end_time = "2038-06-28T23:00Z" in
  let dependency_wait_timeout_minutes = 480 in
  let workflow_name = workflow_name pipeline environment job in
  let datasets = List.concat [
      List.map job.job_hard_dependencies
        ~f:(fun job -> Printf.sprintf "succeeded-%s" job);
      List.map job.job_soft_dependencies
        ~f:(fun job -> Printf.sprintf "finished-%s" job);
      ["succeeded-" ^ job.job_name; "finished-" ^ job.job_name];
    ] in
  inside_xml_tag buf
    ~start_tag:(Printf.sprintf
      {|<coordinator-app xmlns="uri:oozie:coordinator:0.4"
        name="%s" frequency="%d" start="%s" end="%s" timezone="UTC">|}
      coordinator_name frequency_minutes start_time end_time
    )
    ~end_tag:"</coordinator-app>"
    (fun () ->
       Printf.sprintf {|
        <controls>
          <timeout>%d</timeout>
          <execution>LAST_ONLY</execution>
          <throttle>1</throttle>
        </controls>
       |} dependency_wait_timeout_minutes |> app;

       inside_xml_tag buf
         ~start_tag:"<datasets>"
         ~end_tag:"</datasets>"
         (fun () ->
            List.iter datasets ~f:(fun dataset ->
                let template = dataset_template pipeline_frequency in
                let template_joined = String.concat ~sep:"/" [
                    "${job_dependencies_dir}";
                    "job_success_markers";
                    template;
                    dataset;
                  ] in
                Printf.sprintf {|
                  <dataset name="%s" frequency="%d" initial-instance="%s" timezone="UTC">
                    <uri-template>%s</uri-template>
                    <done-flag/>
                  </dataset>
                |} dataset frequency_minutes start_time template_joined |> app;
              );
         );

       Printf.sprintf {|
         <output-events>
           <data-out name="outputSuccess" dataset="succeeded-%s">
             <instance>${coord:current(0)}</instance>
           </data-out>
           <data-out name="outputFinished" dataset="finished-%s">
             <instance>${coord:current(0)}</instance>
           </data-out>
         </output-events>
       |} job.job_name job.job_name |> app;

       Printf.sprintf {|
         <action>
           <workflow>
             <app-path>${application_path}/%s.xml</app-path>
             <configuration>
               <property>
                 <name>wf_success_marker</name>
                 <value>${coord:dataOut('outputSuccess')}</value>
               </property>
               <property>
                 <name>wf_finished_marker</name>
                 <value>${coord:dataOut('outputFinished')}</value>
               </property>
               <property>
                 <name>pipeline_start_time</name>
                 <value>${coord:nominalTime()}</value>
               </property>
             </configuration>
           </workflow>
         </action>
      |} workflow_name |> app;
    );
  Buffer.contents buf |> pretty_xml

let yarn_job_queue pipeline =
  "dwh-jobs-" ^ String.tr ~target:'_' ~replacement:'-' (pipeline_to_string pipeline)

let str_ends_with str postfix =
  match String.substr_index str ~pattern:postfix with
  | Some pos -> String.length postfix = String.length str - pos
  | None -> false

let derive_spark_job_class_name job =
  let builder = ref "vinted.warehouse.jobs" in
  let app str = builder := !builder ^ str in
  (* TODO: more rules for other things *)
  if str_ends_with job.job_spark_class_name "BatchView" then (
    app ".batchviews";
  );
  app ".";
  app job.job_spark_class_name;
  !builder

let build_start_spark_str pipeline environment job =
  let buf = Buffer.create 512 in
  let app str = Buffer.add_string buf str in
  let yarn_queue = yarn_job_queue pipeline in
  let spark_job_opts = final_spark_job_options environment pipeline job in
  let class_name = derive_spark_job_class_name job in
  Printf.sprintf
{|#!/bin/bash

set -x

export SPARK_APP_NAME

%s/bin/spark-submit \
  --verbose \
  --class vinted.warehouse.jobs.JobRunner \
  --files $SPARK_DEFAULT_APP_CONF \
  --driver-class-path %s \
  --conf "spark.executor.extraLibraryPath=%s" \
  --conf "spark.yarn.queue=%s" \
  --packages qubole:sparklens:0.2.0-s_2.11 \
  --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
  |} spark_dist_path_real spark_job_class_path spark_job_class_path yarn_queue |> app;
  List.iter spark_job_opts ~f:(fun (k, v) ->
      let escaped_v = escape_bash v in
      Printf.sprintf
        {|--conf "%s=%s" \
  |}
        k escaped_v |> app
    );
  Printf.sprintf
{|%s \
  %s \
  2>&1|} spark_dist_jar class_name |> app;
  Buffer.contents buf

let build_bundle pipeline environment jobs dir =
  let relevant_jobs = List.filter jobs
      ~f:(fun job ->
         List.filter job.job_pipelines ~f:(fun p -> p = pipeline)
         |> List.hd |> Option.is_some) in
  let bundle_str = build_bundle_str pipeline environment relevant_jobs in
  Unix.mkdir_p dir;
  Out_channel.write_all
    ~data:bundle_str
    (Printf.sprintf "%s/bundle.xml" dir);
  List.iter jobs ~f:(fun job ->
      let xml_str = build_workflow_str pipeline environment job in
      Out_channel.write_all ~data:xml_str
      (Printf.sprintf "%s/%s.xml" dir (workflow_name pipeline environment job))
    );
  List.iter jobs ~f:(fun job ->
      let xml_str = build_coordinator_str pipeline environment job in
      Out_channel.write_all ~data:xml_str
      (Printf.sprintf "%s/%s.xml" dir (coordinator_name pipeline environment job))
    );
  List.iter jobs ~f:(fun job ->
      let spark_sh = build_start_spark_str pipeline environment job in
      let dashed_job_name = dashed_job_name job in
      Out_channel.write_all ~data:spark_sh
      (Printf.sprintf "%s/%s" dir (Printf.sprintf "%s-start-spark.sh" dashed_job_name))
    );
  List.iter jobs ~f:(fun job ->
      let job = to_retry_job job in
      let spark_sh = build_start_spark_str pipeline environment job in
      let dashed_job_name = dashed_job_name job in
      Out_channel.write_all ~data:spark_sh
      (Printf.sprintf "%s/%s" dir (Printf.sprintf "%s-start-spark.sh" dashed_job_name))
    )
