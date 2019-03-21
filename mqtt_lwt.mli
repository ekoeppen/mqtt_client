type t = {
  oc : Lwt_io.output Lwt_io.channel;
  ic : Lwt_io.input Lwt_io.channel;
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
  will_message : string;
  will_topic : string;
  will_qos : int;
  will_retain : bool;
}
val default_conn_opts : conn_opts_t

val process_publish_pkt : 'a -> ('a -> string -> string -> int -> unit Lwt.t) -> unit Lwt.t
val subscribe : ?qos:int -> topics:string list -> t -> unit Lwt.t
val unsubscribe : ?qos:int -> topics:string list -> t -> unit Lwt.t
val publish : ?dup:bool -> ?qos:int -> ?retain:bool -> topic:string ->
  payload:string -> t -> unit Lwt.t
val publish_periodically : ?qos:int -> ?period:float -> topic:string ->
  (unit -> string) -> t -> unit Lwt.t

val connect_socket : host:string -> port:int -> Lwt_unix.file_descr Lwt.t
val of_socket : Lwt_unix.file_descr -> t
val connect : t -> opts:conn_opts_t -> t Lwt.t
val run : t -> unit Lwt.t
val connect_to_broker : opts:conn_opts_t -> broker:string -> port:int -> t Lwt.t
