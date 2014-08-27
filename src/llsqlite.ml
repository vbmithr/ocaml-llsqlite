open Batteries
open Lwt

let section = Lwt_log.Section.make "llsqlite"

let masteraddr = Re_pcre.regexp "(.*)/(.*)/(.*)/(.*)/(.*)"
let clientaddr = Re_pcre.regexp "(.*)/(.*)"

let client_ops ?conn_wrapper ~addr ~port ops =
  let open Llsqlite3.Client in
  let c = make ?conn_wrapper ~id:(string_of_int (Unix.getpid ())) () in
  Lwt_unix.handle_unix_error (fun () -> connect c ~addr ~port) ()  >>
  Lwt_list.iter_s (fun op ->
      match_lwt execute c op with
      | `OK s -> Lwt_io.printf "+OK %s\n" s
      | `Error s -> Lwt_io.printf "-ERR %s\n" s
    ) ops

let mode = ref `Help
let tls = ref false
let anon_args = ref []

let specs =
  Arg.align
    [
      "-tls", Arg.Set tls, " Use TLS";
      "-server", Arg.String (fun n -> mode := `Master n), "";
      "-client", Arg.String (fun addr -> mode := `Client addr), "";
      "-v", Arg.String (fun s -> Lwt_log.(add_rule s Info)), "<section> Put <section> to Info level";
      "-vv", Arg.String (fun s -> Lwt_log.(add_rule s Debug)), "<section> Put <section> to Debug level"
    ]

let usage_msg =
  Printf.sprintf
    "Usage:\n\
     %s [options...] -server <serv_saddr> <id> <db_file>\n\
     %s [options...] -client <saddr> <sql> [<sql>...]\n\n\
     serv_saddr ::== node_addr/node_port/group_addr/group_port/iface\n\
     saddr ::== addr/port\n\n\
     Options are:"
    Sys.argv.(0) Sys.argv.(0)

let usage () =
  Arg.usage specs usage_msg;
  exit 1


let x509_cert = "certs/server.crt"
let x509_pk   = "certs/server.key"

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs (fun a -> anon_args := a::!anon_args) usage_msg;
  anon_args := List.rev !anon_args;
  match !mode with
  | `Help -> usage ()
  | `Master addr ->
    (
      match Re.(exec masteraddr addr |> get_all) with
      | [|_; addr; node_port; group_addr; group_port; iface|] ->
        let node_saddr = Unix.(ADDR_INET (inet_addr_of_string addr,
                                          int_of_string node_port)) in
        let client_saddr = Unix.(ADDR_INET (inet_addr_of_string addr,
                                            int_of_string node_port + 1)) in
        let group_saddr = Unix.(ADDR_INET (inet_addr_of_string group_addr,
                                           int_of_string group_port)) in

        (* starting server *)
        Lwt_main.run
          (
            lwt conn_wrapper = if !tls then
                lwt () = Tls_lwt.rng_init () in
                lwt certificate =
                  X509_lwt.private_of_pems ~cert:x509_cert ~priv_key:x509_pk in
                Some (Tls.Config.(Oraft_lwt_tls.make_server_wrapper
                                    ~server_config:(server ~certificate ())
                                    ~client_config:(client ()) ())) |> return
              else return None
            in
            match !anon_args with
            | [id; db] ->
              let set_template () =
                let file_name = "llsqlite-" ^ id ^ ".log" in
                let template = "$(date).$(milliseconds) ($(pid)|$(section)|$(level)) $(message)" in
                let chan_log =
                  Lwt_log.channel ~template ~close_mode:`Keep ~channel:Lwt_io.stdout () in
                lwt file_log = Lwt_log.file ~template ~mode:`Truncate ~file_name () in
                Lwt_log.default := Lwt_log.broadcast [chan_log; file_log];
                Lwt.return_unit in
              lwt () = set_template () in
              let db = Sqlite3.db_open db in
              Llsqlite3.Server.distribute ?conn_wrapper ~id ~iface
                ~node_saddr ~client_saddr ~group_saddr db
            | _ ->
              usage ()
          )
      | _ -> usage ()
    )
  | `Client addr ->
    (
      match Re.(exec clientaddr addr |> get_all) with
      | [|_; addr; port;|] ->
        let addr = Unix.inet_addr_of_string addr in
        let port = int_of_string port + 1 in
          Lwt_main.run
            (
              if !tls then
                lwt () = Tls_lwt.rng_init () in
                let client_config = Tls.Config.client () in
                let conn_wrapper = Oraft_lwt_tls.make_client_wrapper ~client_config () in
                client_ops ~conn_wrapper ~addr ~port !anon_args
              else
                client_ops ~addr ~port !anon_args
            )
      | _ -> usage ()
    )

