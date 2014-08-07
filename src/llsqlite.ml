open Printf
open Lwt

let masteraddr = Re_pcre.regexp "(.*)/(.*)/(.*)/(.*)/(.*)"
let clientaddr = Re_pcre.regexp "(.*)/(.*)"

let client_op ~addr ~port op =
  let open Llsqlite3.Client in
  let c = make ~id:(string_of_int (Unix.getpid ())) () in
  connect c ~addr ~port >>
    match_lwt execute c op with
    | `OK s -> printf "+OK %s\n" s; return ()
    | `Error s -> printf "-ERR %s\n" s; return ()

let mode         = ref `Help
let sql          = ref None

let specs =
  Arg.align
    [
      "-server", Arg.String (fun n -> mode := `Master n),
      "<node_addr>/<node_port>/<group_addr>/<group_port>/<iface> Start a node";
      "-client", Arg.String (fun addr -> mode := `Client addr),
      "<addr>/<port> Client mode";
      "-sql", Arg.String (fun s -> sql := Some s),
      "STRING Execute the given SQL statement";
      "-v", Arg.Unit (fun () -> Lwt_log.(add_rule "llsqlite3" Info)), " Be verbose";
      "-vv", Arg.Unit (fun () -> Lwt_log.(add_rule "llsqlite3" Debug)), " Be more verbose"
    ]

let usage () =
  print_endline (Arg.usage_string specs "Usage:");
  exit 1

let set_template () =
  let template = "$(date).$(milliseconds) [$(pid)]: $(message)" in
  let std_logger =
    Lwt_log.channel ~template ~close_mode:`Keep ~channel:Lwt_io.stdout () in
  Lwt_log.default := std_logger

let () = set_template ()


let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  match !mode with
  | `Help -> usage ()
  | `Master addr ->
    printf "Launching server %d\n%!" (Unix.getpid ());
    (
      match Re.(exec masteraddr addr |> get_all) with
      | [|_; node_addr; node_port; group_addr; group_port; iface|] ->
        let node_addr = Unix.inet_addr_of_string node_addr in
        let node_port = int_of_string node_port in
        let group_addr = Unix.inet_addr_of_string group_addr in
        let group_port = int_of_string group_port in
        let db = Sqlite3.db_open ("/tmp/llsqlite." ^ (string_of_int (Unix.getpid ()))) in
        Llsqlite3.Server.distribute ~node_addr ~node_port
          ~client_port:(node_port + 1) ~group_addr ~group_port ~iface ~db |> Lwt_main.run
      | _ -> usage ()
    )
    | `Client addr ->
      printf "Launching client %d\n%!" (Unix.getpid ());
      (
        match Re.(exec clientaddr addr |> get_all), !sql with
        | [|_; addr; port;|], Some sql ->
          let addr = Unix.inet_addr_of_string addr in
          let port = int_of_string port + 1 in
          Lwt_main.run (client_op ~addr ~port sql)
        | _ -> usage ()
      )

