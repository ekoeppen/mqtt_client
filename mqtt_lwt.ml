open Lwt

(* constants *)
let keep_alive_interval_default = 10.0 (*seconds*)
let version = 3 (* MQTT version *)

let msg_id = ref 0
let ping_req = ref 0

(* "private" Helper funcs *)
let string_of_char c = String.make 1 c

(* int_to_str2: integer (up to 65535) to 2 byte encoded string *)
(* TODO: should probably throw exception if i is greater than 65535 *)
let int_to_str2 i =
  (string_of_char (char_of_int ((i lsr 8) land 0xFF))) ^
  (string_of_char (char_of_int (i land 0xFF)))

(* encode_string: Given a string returns two-byte length follwed by string *)
let encode_string str =
  (int_to_str2 (String.length str)) ^ str

(* increment the current msg_id and return it as a 2 byte string *)
let get_msg_id_bytes =
  incr msg_id;
  int_to_str2 !msg_id

let charlist_to_str l =
  let buf = Buffer.create (List.length l) in
  List.iter (Buffer.add_char buf) l;
  Buffer.contents buf
(* end of "private" helper funcs *)

type t = {
  oc: Lwt_io.output Lwt_io.channel;
  ic: Lwt_io.input Lwt_io.channel;
}

type conn_opts_t = {
  clean_session : bool;
  keep_alive_interval : float;
  dup : bool;
  qos : int;
  retain : bool;
  client_id : string;
  username : string;
  password : string;
  will_message: string;
  will_topic : string;
  will_qos : int;
  will_retain : bool
}

let default_conn_opts = {
  clean_session = true;
  keep_alive_interval = 10.0;
  dup = false;
  qos = 0;
  retain = false;
  client_id = "mqtt_lwt_00000000";
  username = "";
  password = "";
  will_message = "";
  will_topic = "";
  will_qos = 0;
  will_retain = false;
}

let to_int opts =
  ((if opts.clean_session then 0x02 else 0) lor
   (if (String.length opts.will_topic ) > 0 then 0x04 else 0) lor
   ((opts.will_qos land 0x03) lsl 3) lor
   (if opts.will_retain then 0x20 else 0) lor
   (if (String.length opts.password) > 0 then 0x40 else 0) lor
   (if (String.length opts.username) > 0 then 0x80 else 0))

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


let (pr, push_pw) = Lwt_stream.create ()

(* get_remaining_len: find the number of remaining bytes needed
 * for this packet
 *)
let get_remaining_len istream =
  let rec aux multiplier value =
    let%lwt c = Lwt_stream.next istream in
    let digit = Char.code c in
    match (digit land 128) with
    | 0 ->  return (value + ((digit land 127) * multiplier))
    | _ ->  aux (multiplier * 128) ( value + ((digit land 127)* multiplier)) in
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

let rec receive_packets ic oc =
  let istream = Lwt_io.read_chars ic in
  let%lwt header = receive_packet istream in
  let%lwt () = (Logs.debug (fun m -> m ">>> %s" (msg_type_to_str header.msg));
    match header.msg with
    | PUBLISH ->
     (let msg_id_len = (if header.qos = 0 then 0 else 2) in
       let topic_len = ((Char.code header.buffer.[0]) lsl 8) lor
         (0xFF land (Char.code header.buffer.[1])) in
       let topic = String.sub header.buffer 2 topic_len in
       let msg_id =
         (if header.qos = 0 then 0
           else ((Char.code header.buffer.[topic_len + 2]) lsl 8) lor
                (0xFF land (Char.code header.buffer.[topic_len + 3]))) in
       let payload_len = header.remaining_len - topic_len - 2 - msg_id_len in
       let payload = Some (String.sub header.buffer (topic_len + 2 + msg_id_len) payload_len) in
       let%lwt () = return (push_pw (Some { header ; topic ; msg_id; payload })) in
       send_puback oc (int_to_str2 msg_id))
    | PINGRESP ->
      decr ping_req; return ()
   | _ ->
     (return ())
  ) in
  receive_packets ic oc

(* process_publish_pkt f:
 * when a PUBLISH packet is received back from the broker, call the
 * passed-in function f with topic, payload and message id.
 * User supplies the function f which will process the publish packet
 * as the user sees fit. This function is called asynchronously whenever
 * a PUBLISH packet is received.
 *)
let process_publish_pkt conn f =
  let rec process' () =
    (let%lwt pkt = Lwt_stream.get pr in
      match pkt with
      | None -> Logs_lwt.err (fun m -> m "None packet!!")
      | Some { payload = None; _ } -> Logs_lwt.err (fun m -> m "No Payload!")
      | Some { topic = t; payload = Some p; msg_id = m; _ } -> let%lwt () = (f conn t p m) in process' ()) in
    process' ()

(* recieve_connack: wait for the CONNACT (Connection achnowledgement packet) *)
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

(* send_ping_req: sents a PINGREQ packet to the broker to keep the connetioon alive *)
let send_ping_req w_chan =
  let ping_str = charlist_to_str [(msg_header PINGREQ false 0 false); Char.chr 0  (* remaining length *)] in
  let%lwt () = Lwt_io.write w_chan ping_str in
  Logs.debug (fun m -> m "PINGREQ >>>");
  Lwt_io.flush w_chan

(* ping_loop: send a PINGREQ at regular intervals *)
let rec ping_loop ?(interval=keep_alive_interval_default) w =
  if !ping_req > 1 then begin
    raise (Failure ("Failed to receive PINGRESP for earlier PINGREQ"));
  end;
  incr ping_req;
  let%lwt () = Lwt_unix.sleep interval in
  let%lwt () = send_ping_req w in
  ping_loop w

(* multi_byte_len: The algorithm for encoding a decimal number into the
 * variable length encoding scheme (see section 2.1 of MQTT spec
 *)
let multi_byte_len len =
  let rec aux bodylen acc = match bodylen with
    | 0 -> List.rev acc
    | _ -> let bodylen' = bodylen / 128 in
      let digit = (bodylen mod 0x80) lor (if bodylen' > 0 then 0x80 else 0x00) in
      aux bodylen' (digit::acc) in
  aux len []

(* subscribe to topics *)
let subscribe ?(qos=1) ~topics w =
  let payload =
    (if qos > 0 then get_msg_id_bytes else "") ^
    List.fold_left (fun a topic -> a ^ (encode_string topic) ^ string_of_char (char_of_int qos)) "" topics in
  let remaining_len = List.map (fun i -> char_of_int i) (multi_byte_len (String.length payload)) |> charlist_to_str in
  let subscribe_str = (string_of_char (msg_header SUBSCRIBE false 1 false)) ^ remaining_len ^ payload in
  let%lwt () = Lwt_io.write w.oc subscribe_str in
  Lwt_io.flush w.oc

let unsubscribe ?(qos=1) ~topics w =
  let payload =
    (if qos > 0 then get_msg_id_bytes else "") ^
    List.fold_left (fun a topic -> a ^ encode_string topic) "" topics in
  let remaining_len = List.map (fun i -> char_of_int i) (multi_byte_len (String.length payload)) |> charlist_to_str in
  let unsubscribe_str = (string_of_char (msg_header UNSUBSCRIBE false 1 false)) ^ remaining_len ^ payload in
  let%lwt () = Lwt_io.write w.oc unsubscribe_str in
  Lwt_io.flush w.oc

(* publish message to topic *)
let publish ?(dup=false) ?(qos=0) ?(retain=false) ~topic ~payload w =
  let msg_id_str = if qos > 0 then get_msg_id_bytes else "" in
  let var_header = (encode_string topic) ^ msg_id_str in
  let publish_str' = var_header ^ payload in
  let remaining_bytes = List.map (fun i -> char_of_int i) (multi_byte_len (String.length publish_str')) |> charlist_to_str in
  let publish_str = (string_of_char (msg_header PUBLISH dup qos retain)) ^ remaining_bytes ^ publish_str' in
  let%lwt () = Lwt_io.write w.oc publish_str in
  Lwt_io.flush w.oc

(* publish_periodically: periodically publish a message to
 * a topic, period specified by period in seconds (float)
 *)
let publish_periodically ?(qos=1) ?(period=1.0) ~topic f w =
  let rec publish_periodically' () =
    let pub_str = f () in
    let%lwt () = Lwt_unix.sleep period in
    let%lwt () = publish ~qos ~topic ~payload:pub_str w in
    publish_periodically' () in
  (publish_periodically' () : (unit Lwt.t) )

(* connect_socket: estabishes the Tcp socket connection to the broker *)
let connect_socket ~host ~port =
  let socket = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_STREAM 0 in
  let%lwt host_info = Lwt_unix.gethostbyname host in
  let server_address = host_info.Lwt_unix.h_addr_list.(0) in
  let%lwt () = Lwt_unix.connect socket (Lwt_unix.ADDR_INET (server_address, port)) in
  Logs.info (fun m -> m "Connected to %s, port %d" host port);
  return (socket)

let of_socket socket =
  let oc = Lwt_io.of_fd ~mode:Lwt_io.output socket in
  let ic = Lwt_io.of_fd ~mode:Lwt_io.input socket in
  {oc; ic}

let connect_str opts =
  (* keepalive timer, adding 1 below just to make the interval 1 sec longer than
     the ping_loop for safety sake *)
  let ka_timer_str = int_to_str2(int_of_float opts.keep_alive_interval + 1) in
  let variable_header = (encode_string "MQIsdp") ^
    (version |> char_of_int |> string_of_char) ^
    (to_int opts |> char_of_int |> string_of_char) ^
    ka_timer_str in
  (* clientid string should be no longer that 23 chars *)
  let payload =
    (encode_string opts.client_id) ^
    (if (String.length opts.will_topic) > 0 then encode_string opts.will_topic else "")^
    (if (String.length opts.will_message) > 0 then encode_string opts.will_message else "")^
    (if (String.length opts.username) > 0 then encode_string opts.username else "") ^
    (if (String.length opts.password) > 0 then encode_string opts.password else "") in
  let vheader_payload = variable_header ^ payload in
  let remaining_len = (multi_byte_len (String.length vheader_payload))
    |> List.map (fun i -> char_of_int i)
    |> charlist_to_str in
  (string_of_char (msg_header CONNECT opts.dup opts.qos opts.retain)) ^ remaining_len ^ vheader_payload

let connect t ~opts =
  let%lwt () = Lwt_io.write t.oc (connect_str opts) in
  let%lwt () = receive_connack (Lwt_io.read_chars t.ic) in
  Logs.debug (fun m -> m "Connect handshake complete");
  return t

let run t =
  Lwt.pick [
    ping_loop ~interval:keep_alive_interval_default t.oc;
    receive_packets t.ic t.oc
  ]

(* connect_to_broker *)
let connect_to_broker ~opts ~broker ~port =
  let%lwt socket = connect_socket ~host:broker ~port:port in
  let t = of_socket socket in
  connect t ~opts
