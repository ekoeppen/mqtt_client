open Lwt
open Mqtt_lwt
open Printf
open Logs
open Logs_fmt
open Logs_lwt
open Tls_lwt
open Cmdliner

let tls_connect ~broker ~port ~ca_file ~cert_file ~key_file =
  let%lwt authenticator = X509_lwt.authenticator (`Ca_file ca_file) in
  let%lwt certificate = X509_lwt.private_of_pems ~cert:cert_file ~priv_key:key_file in
  let%lwt (ic, oc) = Tls_lwt.connect_ext Tls.Config.(
    client ~authenticator ~certificates:(`Single certificate)
    ~ciphers:Ciphers.supported ()) (broker, port) in
  Lwt.return {ic; oc}

let std_connect ~broker ~port =
  let%lwt socket = Mqtt_lwt.connect ~host:broker ~port:port in
  Lwt.return (of_socket socket)

let start_client ~broker ~port ~ca_file ~cert_file ~key_file =
  let%lwt conn = (if ca_file <> "" && cert_file <> "" && key_file <> "" then
      tls_connect ~broker ~port ~ca_file ~cert_file ~key_file
    else
      std_connect ~broker ~port) in
  mqtt_client conn ~opts:default_conn_opts

let display_topic _ topic payload id =
  Logs_lwt.app (fun m -> m "Topic: %s Payload: %s Msg_id: %d" topic payload id)

let sub ~broker ~port ~topic ~ca_file ~cert_file ~key_file =
  Logs.info (fun m -> m "Subscribing");
  let%lwt client = start_client ~broker ~port ~ca_file ~cert_file ~key_file in
  let%lwt () = subscribe ~topics:[topic] client.oc in
  let%lwt () = process_publish_pkt client display_topic in
  Lwt.return ()

let pub ~broker ~port ~topic ~message ~ca_file ~cert_file ~key_file =
  Logs.info (fun m -> m "Publishing");
  let%lwt client = start_client ~broker ~port ~ca_file ~cert_file ~key_file in
  publish ~topic:topic ~payload:message client.oc

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (Logs_fmt.reporter ());
  ()

let logging =
  let env = Arg.env_var "MQTT_CLIENT_VERBOSITY" in
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ~env ())

let sub_client _ broker port ca_file cert_file key_file topic =
  Lwt_main.run (sub ~broker ~port ~topic ~ca_file ~cert_file ~key_file)

let pub_client _ broker port ca_file cert_file key_file topic message =
  Lwt_main.run (pub ~broker ~port ~topic ~message ~ca_file ~cert_file ~key_file)

let broker =
  let doc = "MQTT broker" in
  Arg.(value & opt string "localhost" & info ["h"; "host"] ~doc)
let port =
  let doc = "Port number" in
  Arg.(value & opt int 1883 & info ["p"; "port"] ~doc)
let ca_file =
  let doc = "CA file" in
  Arg.(value & opt non_dir_file "" & info ["ca"] ~doc)
let cert_file =
  let doc = "Client certificate file" in
  Arg.(value & opt non_dir_file "" & info ["cert"] ~doc)
let key_file =
  let doc = "Client key" in
  Arg.(value & opt non_dir_file "" & info ["key"] ~doc)

let topic =
  let doc = "Topic" in
  Arg.(required & pos 0 (some string) None & info [] ~docv:"TOPIC" ~doc)

let message =
  let doc = "Messsage" in
  Arg.(required & pos 1 (some string) None & info [] ~docv:"MESSAGE" ~doc)

let sub_cmd =
  let doc = "Subscribe to a topic" in
  Term.(const sub_client $ logging $ broker $ port $ ca_file $ cert_file $ key_file $ topic),
  Term.info "sub" ~exits:Term.default_exits ~doc

let pub_cmd =
  let doc = "Publish message to a topic" in
  Term.(const pub_client $ logging $ broker $ port $ ca_file $ cert_file $ key_file $ topic $ message),
  Term.info "pub" ~exits:Term.default_exits ~doc

let cmd =
  let doc = "MQTT client" in
  let exits = Term.default_exits in
  Term.(ret (const (fun _ -> `Help (`Pager, None)) $ logging)),
  Term.info "mqtt_client" ~doc ~exits

let () = Term.(eval_choice cmd [sub_cmd; pub_cmd] |> exit)
