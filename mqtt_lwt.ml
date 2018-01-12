(** mqtt_async.ml: MQTT client library written in OCaml
 * Based on spec at:
 * http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html
 * *)
open Lwt
open Printf
open Logs
open Logs_lwt

(* constants *)
let keep_alive_timer_interval_default = 10.0  (*seconds*)
let keepalive       = 15                      (*seconds*)
let version         = 3                (* MQTT version *)

let msg_id = ref 0

(* "private" Helper funcs *)
(** int_to_str2:  pass in an int and get a two-char string back *)
let string_of_char c = String.make 1 c

(** int_to_str2: integer (up to 65535) to 2 byte encoded string *)
(* TODO: should probably throw exception if i is greater than 65535 *)
let int_to_str2 i =
  (string_of_char (char_of_int ((i lsr 8) land 0xFF))) ^
  (string_of_char (char_of_int (i land 0xFF)))

(** encode_string: Given a string returns two-byte length follwed by string *)
let encode_string str =
  (int_to_str2 (String.length str)) ^ str

(** increment the current msg_id and return it as a 2 byte string *)
let get_msg_id_bytes =
  incr msg_id;
  int_to_str2 !msg_id

let charlist_to_str l =
  let buf = Buffer.create (List.length l) in
  List.iter (Buffer.add_char buf) l;
  Buffer.contents buf

(* end of "private" helper funcs *)

type conn_t = {
  socket: Lwt_unix.file_descr; (* probably don't need socket *)
  read_chan: Lwt_io.input Lwt_io.channel;
  write_chan: Lwt_io.output Lwt_io.channel;
  istream: char Lwt_stream.t
}

type msg_type =
    CONNECT
  | CONNACK
  | PUBLISH
  | PUBACK
  | PUBREC
  | PUBREL
  | PUBCOMP
  | SUBSCRIBE
  | SUBACK
  | UNSUBSCRIBE
  | UNSUBACK
  | PINGREQ
  | PINGRESP
  | DISONNECT
  | RESERVED

type header_t = {
  msg:            msg_type;
  dup:            bool;
  qos:            int;
  retain:         bool;
  remaining_len:  int;
  mutable buffer: string;
}

type packet_t = {
  header:         header_t;
  topic:          string ;
  msg_id:         int;
  payload:        string option
}

let msg_type_to_int msg = match msg with
    CONNECT     -> 1
  | CONNACK     -> 2
  | PUBLISH     -> 3
  | PUBACK      -> 4
  | PUBREC      -> 5
  | PUBREL      -> 6
  | PUBCOMP     -> 7
  | SUBSCRIBE   -> 8
  | SUBACK      -> 9
  | UNSUBSCRIBE -> 10
  | UNSUBACK    -> 11
  | PINGREQ     -> 12
  | PINGRESP    -> 13
  | DISONNECT   -> 14
  | RESERVED    -> 15

let msg_type_to_str msg = match msg with
    CONNECT     -> "CONNECT"
  | CONNACK     -> "CONNACK"
  | PUBLISH     -> "PUBLISH"
  | PUBACK      -> "PUBACK"
  | PUBREC      -> "PUBREC"
  | PUBREL      -> "PUBREL"
  | PUBCOMP     -> "PUBCOMP"
  | SUBSCRIBE   -> "SUBSCRIBE"
  | SUBACK      -> "SUBACK"
  | UNSUBSCRIBE -> "UNSUBSCRIBE"
  | UNSUBACK    -> "UNSUBACK"
  | PINGREQ     -> "PINGREQ"
  | PINGRESP    -> "PINGRESP"
  | DISONNECT   -> "DISCONNECT"
  | RESERVED    -> "RESERVED"

let int_to_msg_type b = match b with
    1 -> Some CONNECT
  | 2 -> Some CONNACK
  | 3 -> Some PUBLISH
  | 4 -> Some PUBACK
  | 5 -> Some PUBREC
  | 6 -> Some PUBREL
  | 7 -> Some PUBCOMP
  | 8 -> Some SUBSCRIBE
  | 9 -> Some SUBACK
  | 10 -> Some UNSUBSCRIBE
  | 11 -> Some UNSUBACK
  | 12 -> Some PINGREQ
  | 13 -> Some PINGRESP
  | 14 -> Some DISONNECT
  | 15 -> Some RESERVED
  | _  -> None


(*let pr,pw = Lwt_io.pipe ~buffer_size:256 ()*)
let (pr,push_pw) = Lwt_stream.create ()

(* get_remaining_len: find the number of remaining bytes needed
 * for this packet *)
let get_remaining_len istream =
  let rec aux multiplier value =
    let%lwt c = Lwt_stream.next istream in
    let digit = Char.code c in
    match (digit land 128) with
    | 0 ->  return (value + ((digit land 127) * multiplier))
    | _ ->  aux (multiplier * 128) ( value + ((digit land 127)*
                                              multiplier)) in
  aux 1 0

let msg_header msg dup qos retain =
  char_of_int ((((msg_type_to_int msg) land 0xFF) lsl 4) lor
               ((if dup then 0x1 else 0x0)        lsl 3) lor
               ((qos land 0x03)                   lsl 1) lor
               (if retain then 0x1 else 0x0))

let get_header istream =
  let%lwt c = Lwt_stream.next istream in
  let byte_1 = Char.code c in
  let mtype = match (int_to_msg_type ((byte_1 lsr 4) land 0xFF)) with
    | None -> failwith "Not a legal msg type!"
    | Some msg -> msg in
  let%lwt remaining_bytes = get_remaining_len istream in
  let dup    = if ((byte_1 lsr 3) land 0x01) = 1 then true else false in
  let qos    = (byte_1 lsr 1) land 0x3 in
  let retain = if (byte_1 land 0x01) = 1 then true else false in
  return {
    msg = mtype;
    dup = dup;
    qos = qos;
    retain = retain;
    remaining_len = remaining_bytes;
    buffer = ""
  }

(* try to read a packet from the broker *)
let receive_packet istream =
  let%lwt header = get_header istream in
  let%lwt clist = Lwt_stream.nget header.remaining_len istream in
  let buffer = charlist_to_str clist in
  header.buffer <- buffer;
  return header

let send_puback w msg_idstr =
  let puback_str = charlist_to_str [
      (msg_header PUBACK false 0 false);
      Char.chr 2;
      (* remaining length *)
    ] ^ msg_idstr in
  Logs.debug (fun m -> m "PUBACK >>>");
  Lwt_io.write w puback_str

let rec receive_packets istream write_chan =
  let%lwt header = receive_packet istream in
  let%lwt () = (Logs.debug (fun m -> m ">>> %s" (msg_type_to_str header.msg));
    match header.msg with
    | PUBLISH ->
     (let msg_id_len = (if header.qos = 0 then 0 else 2) in
       let topic_len = ( (Char.code header.buffer.[0]) lsl 8) lor
                       (0xFF land (Char.code header.buffer.[1])) in
       let topic = String.sub header.buffer 2 topic_len in
       let msg_id =
         ( if header.qos = 0 then 0
           else ((Char.code header.buffer.[topic_len+2]) lsl 8) lor
                (0xFF land (Char.code header.buffer.[topic_len+3])) ) in
       let payload_len=header.remaining_len - topic_len - 2 - msg_id_len in
       let payload = Some (String.sub header.buffer (topic_len + 2 + msg_id_len) payload_len) in
       let%lwt () = return (push_pw (Some { header ; topic ; msg_id; payload })) in
       send_puback write_chan (int_to_str2 msg_id))
   | _ ->
     (return ())
  ) in
  receive_packets istream write_chan

(** process_publish_pkt f:
  *  when a PUBLISH packet is received back from the broker, call the
  *  passed-in function f with topic, payload and message id.
  *  User supplies the function f which will process the publish packet
  *  as the user sees fit. This function is called asynchronously whenever
  *  a PUBLISH packet is received.
*)
let process_publish_pkt conn f =
  let rec process' () =
    (let%lwt pkt = Lwt_stream.get pr in
      match pkt with
      | None -> Logs_lwt.err (fun m -> m "None packet!!")
      | Some { payload = None; _ } -> Logs_lwt.err (fun m -> m "No Payload!")
      | Some { topic = t; payload = Some p; msg_id = m; _ } -> let%lwt () = (f t p m) in process' ()) in
    process' ()

(** recieve_connack: wait for the CONNACT (Connection achnowledgement packet) *)
let receive_connack istream =
  let%lwt header = get_header istream in
  if (header.msg <> CONNACK) then begin
    failwith "did not receive a CONNACK"
  end;
  let%lwt clist = Lwt_stream.nget header.remaining_len istream in
  let buffer = charlist_to_str clist in
  if (int_of_char buffer.[header.remaining_len-1]) <> 0 then
    Lwt.fail (Failure ( "Connection was not established\n " ))
  else return ()

(** connect: estabishes the Tcp socket connection to the broker *)
let connect ~host ~port =
  let socket = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_STREAM 0 in
  let%lwt host_info = Lwt_unix.gethostbyname host in
  let server_address = host_info.Lwt_unix.h_addr_list.(0) in
  let%lwt () = Lwt_unix.connect socket (Lwt_unix.ADDR_INET (server_address, port)) in
  let read_chan = Lwt_io.of_fd ~mode:Lwt_io.input  socket in
  let write_chan = Lwt_io.of_fd ~mode:Lwt_io.output socket in
  let istream = Lwt_io.read_chars read_chan in
  Logs.info (fun m -> m "Connected to %s, port %d" host port);
  return {socket; read_chan; write_chan; istream}


(** send_ping_req: sents a PINGREQ packet to the broker to keep the connetioon alive *)
let send_ping_req w_chan =
  let ping_str = charlist_to_str [(msg_header PINGREQ false 0 false); Char.chr 0  (* remaining length *)] in
  let%lwt () = Lwt_io.write w_chan ping_str in
  Logs.debug (fun m -> m "PINGREQ >>>");
  Lwt_io.flush w_chan

(** ping_loop: send a PINGREQ at regular intervals *)
let rec ping_loop ?(interval=keep_alive_timer_interval_default) conn w =
  let%lwt () = Lwt_unix.sleep interval in
  let%lwt () = send_ping_req w in
  ping_loop conn w

(** multi_byte_len: The algorithm for encoding a decimal number into the
 * variable length encoding scheme (see section 2.1 of MQTT spec
*)
let multi_byte_len len =
  let rec aux bodylen acc = match bodylen with
    | 0 -> List.rev acc
    | _ -> let bodylen' = bodylen/128 in
      let digit = (bodylen mod 0x80) lor (if bodylen' > 0 then 0x80 else 0x00) in
      aux bodylen' (digit::acc) in
  aux len []

(** subscribe to topics *)
let subscribe ?(qos=1) ~topics w =
  let payload =
    (if qos > 0 then get_msg_id_bytes else "") ^
    List.fold_left (fun a topic -> a ^ (encode_string topic) ^ string_of_char (char_of_int qos)) "" topics in
  let remaining_len = List.map (fun i -> char_of_int i) (multi_byte_len (String.length payload)) |> charlist_to_str in
  let subscribe_str = (string_of_char (msg_header SUBSCRIBE false 1 false)) ^ remaining_len ^ payload in
  let%lwt () = Lwt_io.write w subscribe_str in
  Lwt_io.flush w

(* TODO unsubscribe and subscribe are almost exactly identical. Refactor *)
let unsubscribe ?(qos=1) ~topics w =
  let payload =
    (if qos > 0 then get_msg_id_bytes else "") ^
    List.fold_left (fun a topic -> a ^ encode_string topic) "" topics in
  let remaining_len = List.map (fun i -> char_of_int i) (multi_byte_len (String.length payload)) |> charlist_to_str in
  let unsubscribe_str = (string_of_char (msg_header UNSUBSCRIBE false 1 false)) ^ remaining_len ^ payload in
  let%lwt () = Lwt_io.write w unsubscribe_str in
  Lwt_io.flush w

(** publish message to topic *)
let publish ?(dup=false) ?(qos=0) ?(retain=false) ~topic ~payload w =
  let msg_id_str = if qos > 0 then get_msg_id_bytes else "" in
  let var_header = (encode_string topic) ^ msg_id_str in
  let publish_str' = var_header ^ payload in
  let remaining_bytes = List.map (fun i -> char_of_int i) (multi_byte_len (String.length publish_str')) |> charlist_to_str in
  let publish_str = (string_of_char (msg_header PUBLISH dup qos retain)) ^ remaining_bytes ^ publish_str' in
  let%lwt () = Lwt_io.write w publish_str in
  Lwt_io.flush w

(** publish_periodically: periodically publish a message to
 * a topic, period specified by period in seconds (float)
*)
let publish_periodically ?(qos=0) ?(period=1.0) ~topic f w =
  let rec publish_periodically' () =
    let pub_str = f () in
    let%lwt () = Lwt_unix.sleep period in
    let%lwt () = publish ~qos ~topic ~payload:pub_str w in
    publish_periodically' () in
  (publish_periodically' () : (unit Lwt.t) )

(** connect_to_broker *)
let connect_to_broker ?(keep_alive_interval=keep_alive_timer_interval_default+.0.5)
    ?(dup=false) ?(qos=0) ?(retain=false) ?(username="") ?(password="")
    ?(will_message="") ?(will_topic="") ?(clean_session=true) ?(will_qos=0)
    ?(will_retain=false) ~broker ~port f =
  let connect_flags =
    ((if clean_session then 0x02 else 0) lor
     (if (String.length will_topic ) > 0 then 0x04 else 0) lor
     ((will_qos land 0x03) lsl 3) lor
     (if will_retain then 0x20 else 0) lor
     (if (String.length password) > 0 then 0x40 else 0) lor
     (if (String.length username) > 0 then 0x80 else 0)) in
  (* keepalive timer, adding 1 below just to make the interval 1 sec longer than
     the ping_loop for safety sake *)
  let ka_timer_str = int_to_str2( int_of_float keep_alive_interval+1) in
  let variable_header = (encode_string "MQIsdp") ^
                        (string_of_char (char_of_int version)) ^
                        (string_of_char (char_of_int connect_flags)) ^
                        ka_timer_str in
  (* clientid string should be no longer that 23 chars *)
  let clientid = "OCaml_12345678901234567" in
  let payload =
    (encode_string clientid) ^
    (if (String.length will_topic)> 0 then encode_string will_topic else "")^
    (if (String.length will_message)>0 then encode_string will_message else "")^
    (if (String.length username) >0 then encode_string username else "") ^
    (if (String.length password) >0 then encode_string password else "") in
  let vheader_payload = variable_header ^ payload in
  let remaining_len = (multi_byte_len (String.length vheader_payload) )
                      |> List.map (fun i -> char_of_int i)
                      |> charlist_to_str in
  let connect_str = (string_of_char (msg_header CONNECT dup qos retain)) ^ remaining_len ^ vheader_payload in
  let%lwt t = connect ~host:broker ~port:port in
  let%lwt () = Lwt_io.write t.write_chan connect_str in
  let%lwt () = receive_connack t.istream in
  ( (ping_loop  ~interval:keep_alive_interval t t.write_chan ) <&>
    (f t) <&>
    (receive_packets t.istream t.write_chan) )
