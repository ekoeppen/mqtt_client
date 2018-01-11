open Lwt
open Mqtt_lwt
open Printf
open Logs
open Logs_fmt
open Logs_lwt

let display_topic topic payload id =
  Logs_lwt.info (fun m -> m "Topic: %s Payload: %s Msg_id: %d" topic payload id)

let sub ~broker ~port ~topic =
  Logs.info (fun m -> m "Subscribing");
  connect_to_broker ~broker ~port (fun conn ->
     let%lwt () = subscribe ~topics:[topic] conn.write_chan in
     let%lwt () = process_publish_pkt conn display_topic in
     Lwt.return ()
   )

let pub ~broker ~port ~topic ~message =
  Logs.info (fun m -> m "Publishing");
  connect_to_broker ~broker ~port (fun conn -> (publish ~topic:topic ~payload:message conn.write_chan))

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug);
  match Sys.argv with
  | [| _ ; host ; port ; topic ; msg |] -> Lwt_main.run (pub ~broker:host ~port:(int_of_string port) ~topic:topic ~message:msg)
  | [| _ ; host ; port ; topic |]       -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:topic)
  | [| _ ; host ; port |]               -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:"#")
  | [| _ ; host |]                      -> Lwt_main.run (sub ~broker:host ~port:1883 ~topic:"#")
  | args                                -> Printf.eprintf "%s <host> <port> <topic> <message>\n%!" args.(0)
