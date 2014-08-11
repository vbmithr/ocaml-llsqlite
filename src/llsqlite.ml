open Batteries
open Lwt


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


let urandom buf off len =
  let ic = open_in "/dev/urandom" in
  try
    let nb_read = input ic buf off len in
    close_in ic; nb_read
  with exn ->
    close_in ic; raise exn

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

        (* init TLS stuff *)
        let privkey = File.with_file_in "certs/server.key" IO.read_all in
        let cert = File.with_file_in "certs/server.crt" IO.read_all in
        let entropy = File.with_file_in "/dev/urandom" (fun ic -> IO.nread ic 2048) in
        Nocrypto.Rng.reseed (entropy |> Cstruct.of_string);
        let privkey = Cstruct.of_string privkey in
        let privkey = X509.PK.of_pem_cstruct1 privkey in
        let cert = Cstruct.of_string cert in
        let cert = X509.Cert.of_pem_cstruct cert in
        let certificate = cert, privkey in
        let tls, client_tls = if !tls
          then  Some (Tls.Config.server ~certificate ()), Some (Tls.Config.client ())
          else None, None in

        (* starting server *)
        Llsqlite3.Server.distribute ?tls ?client_tls ~node_addr ~node_port
          ~client_port:(node_port + 1) ~group_addr ~group_port ~iface db |> Lwt_main.run
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

