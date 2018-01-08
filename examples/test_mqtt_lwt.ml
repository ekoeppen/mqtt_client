(** test_mqtt.ml:
  * Example useage of Mqtt_async module
  * Subscribes to every topic (#) then waits for a message to
  * topic "PING" which doesn't start with 'A' at which point it
  * unsubscribes from topic and continues to send ping's to keep alive
*)

open Lwt
open Mqtt_lwt
open Printf

let display_topic topic payload id =
  puts (sprintf "Topic: %s\nPayload: %s\nMsg_id: %d\n" topic payload id)

let sub ~broker ~port ~topic =
  puts "Starting...\n" >>
  connect_to_broker ~broker ~port (fun conn ->
     (subscribe ~topics:[topic] conn.write_chan) >>
     (process_publish_pkt conn display_topic >> Lwt.return ())
   )

let pub ~broker ~port ~topic ~message =
  puts "Starting...\n" >>
  connect_to_broker ~broker ~port (fun conn -> (publish ~topic:topic ~payload:message conn.write_chan))

let () =
    match Sys.argv with
    | [| _ ; host ; port ; topic |] -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:topic)
    | [| _ ; host ; port |]         -> Lwt_main.run (sub ~broker:host ~port:(int_of_string port) ~topic:"#")
    | [| _ ; host |]                -> Lwt_main.run (sub ~broker:host ~port:1883 ~topic:"#")
    | args                          -> Printf.eprintf "%s <host> <port> <topic>\n%!" args.(0)
