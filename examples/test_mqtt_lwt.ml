open Lwt
open Mqtt_lwt
open Printf
open Logs
open Logs_fmt
open Logs_lwt
open Tls_lwt

let tls_connect ~broker ~port =
  let%lwt authenticator = X509_lwt.authenticator `No_authentication_I'M_STUPID in
  let%lwt (ic, oc) = Tls_lwt.connect_ext Tls.Config.(client ~authenticator ~ciphers:Ciphers.supported ()) (broker, port) in
  Lwt.return {ic; oc}

let std_connect ~broker ~port =
  let%lwt socket = Mqtt_lwt.connect ~host:broker ~port:port in
  Lwt.return (of_socket socket)

let start_client ~broker ~port =
  let%lwt conn = (if port = 8883 then tls_connect else std_connect) ~broker ~port in
  mqtt_client conn ~opts:default_conn_opts

let display_topic _ topic payload id =
  Logs_lwt.info (fun m -> m "Topic: %s Payload: %s Msg_id: %d" topic payload id)

let sub ~broker ~port ~topic =
  Logs.info (fun m -> m "Subscribing");
  let%lwt client = start_client ~broker ~port in
  let%lwt () = subscribe ~topics:[topic] client.oc in
  let%lwt () = process_publish_pkt client display_topic in
  Lwt.return ()

let pub ~broker ~port ~topic ~message =
  Logs.info (fun m -> m "Publishing");
  let%lwt client = start_client ~broker ~port in
  publish ~topic:topic ~payload:message client.oc

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug);
  match Sys.argv with
  | [| _ ; host ; port ; topic ; msg |] -> Lwt_main.run (pub ~broker:host ~port:(int_of_string port) ~topic:topic ~message:msg)
  | [| _ ; host ; port ; topic |]       -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:topic)
  | [| _ ; host ; port |]               -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:"#")
  | [| _ ; host |]                      -> Lwt_main.run (sub ~broker:host ~port:1883 ~topic:"#")
  | args                                -> Printf.eprintf "%s <host> <port> <topic> <message>\n%!" args.(0)
