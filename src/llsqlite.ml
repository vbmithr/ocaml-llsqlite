open Batteries
open Lwt

let section = Lwt_log.Section.make "llsqlite"

let masteraddr = Re_pcre.regexp "(.*)/(.*)/(.*)/(.*)/(.*)"
let clientaddr = Re_pcre.regexp "(.*)/(.*)"

let client_op ?tls ~addr ~port op =
  let open Llsqlite3.Client in
  let c = make ~id:(string_of_int (Unix.getpid ())) () in
  Lwt_unix.handle_unix_error (fun () -> connect ?tls c ~addr ~port) ()  >>
    match_lwt execute c op with
    | `OK s -> Printf.printf "+OK %s\n" s; return ()
    | `Error s -> Printf.printf "-ERR %s\n" s; return ()

let mode = ref `Help
let sql = ref None
let tls = ref false

let specs =
  Arg.align
    [
      "-tls", Arg.Set tls, " Use TLS";
      "-server", Arg.String (fun n -> mode := `Master n),
      "<node_addr>/<node_port>/<group_addr>/<group_port>/<iface> Start a node";
      "-client", Arg.String (fun addr -> mode := `Client addr),
      "<addr>/<port> Client mode";
      "-sql", Arg.String (fun s -> sql := Some s),
      "STRING Execute the given SQL statement";
      "-v", Arg.String (fun s -> Lwt_log.(add_rule s Info)), " Be verbose";
      "-vv", Arg.String (fun s -> Lwt_log.(add_rule s Debug)), " Be more verbose"
    ]

let usage () =
  print_endline (Arg.usage_string specs "Usage:");
  exit 1

let set_template () =
  let template = "$(date).$(milliseconds) ($(pid)|$(section)|$(level)) $(message)" in
  let std_logger =
    Lwt_log.channel ~template ~close_mode:`Keep ~channel:Lwt_io.stdout () in
  Lwt_log.default := std_logger

let () = set_template ()

let x509_cert = "certs/server.crt"
let x509_pk   = "certs/server.key"

let tls_create = function
  | false -> return (None, None)
  | true  ->
      lwt () = Tls_lwt.rng_init () in
      lwt certificate =
        X509_lwt.private_of_pems ~cert:x509_cert ~priv_key:x509_pk in
      return Tls.Config.(Some (server ~certificate ()), Some (client ()))

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  match !mode with
  | `Help -> usage ()
  | `Master addr ->
    (
      match Re.(exec masteraddr addr |> get_all) with
      | [|_; node_addr; node_port; group_addr; group_port; iface|] ->
        let node_addr = Unix.inet_addr_of_string node_addr in
        let node_port = int_of_string node_port in
        let group_addr = Unix.inet_addr_of_string group_addr in
        let group_port = int_of_string group_port in
        let db = Sqlite3.db_open ("/tmp/llsqlite." ^ (string_of_int (Unix.getpid ()))) in

        (* starting server *)
        Lwt_main.run
          (
            lwt (tls, client_tls) = tls_create !tls in
            Llsqlite3.Server.distribute ?tls ?client_tls ~node_addr ~node_port
              ~client_port:(node_port + 1) ~group_addr ~group_port ~iface db
          )
      | _ -> usage ()
    )
    | `Client addr ->
      (
        match Re.(exec clientaddr addr |> get_all), !sql with
        | [|_; addr; port;|], Some sql ->
          let addr = Unix.inet_addr_of_string addr in
          let port = int_of_string port + 1 in
          let tls = if !tls then Some (Tls.Config.client ()) else None in
          Lwt_main.run (client_op ?tls ~addr ~port sql)
        | _ -> usage ()
      )

