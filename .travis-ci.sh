sudo apt-get install ocaml opam
export OPAMYES=1
opam init 
opam install ${OPAM_DEPENDS}
eval `opam config env`
opam install lwt logs tls
corebuild -pkg lwt,lwt.ppx,lwt.unix,logs,logs.fmt,logs.lwt,tls,tls.lwt examples/test_mqtt_lwt.native
