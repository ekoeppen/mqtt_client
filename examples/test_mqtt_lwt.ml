(** test_mqtt.ml:
  * Example useage of Mqtt_async module
  * Subscribes to every topic (#) then waits for a message to
  * topic "PING" which doesn't start with 'A' at which point it
  * unsubscribes from topic and continues to send ping's to keep alive
*)

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
     (subscribe ~topics:[topic] conn.write_chan) >>
     (process_publish_pkt conn display_topic >> Lwt.return ())
   )

let pub ~broker ~port ~topic ~message =
  Logs.info (fun m -> m "Publishing");
  connect_to_broker ~broker ~port (fun conn -> (publish ~topic:topic ~payload:message conn.write_chan))

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug);
  match Sys.argv with
  | [| _ ; host ; port ; topic |] -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:topic)
  | [| _ ; host ; port |]         -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:"#")
  | [| _ ; host |]                -> Lwt_main.run (sub ~broker:host ~port:1883 ~topic:"#")
  | args                          -> Printf.eprintf "%s <host> <port> <topic>\n%!" args.(0)
