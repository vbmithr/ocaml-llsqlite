open Printf
open Lwt

module String  = BatString
module Hashtbl = BatHashtbl

let section = Lwt_log.Section.make "dict"
let ipv6addr = Re_pcre.regexp "\\[(.*)\\]:([0-9]*)%?(.*)?"

type op = string (* SQL statement *)

exception SQLite_Error_Code of Sqlite3.Rc.t

let make_exec db =
  fun _ sql ->
    let rc = Sqlite3.exec db sql in
    match rc with
    | Sqlite3.Rc.OK -> `Sync (`OK "" |> Lwt.return)
    | rc -> `Sync (`Error (SQLite_Error_Code rc) |> Lwt.return)

module CONF =
struct
  type op_ = op
  type op = op_

  let string_of_op op = op
  let op_of_string str = str

  let sockaddr s =
    let open Unix in
    (* Identify the type of address *)
    if Re.execp ipv6addr s
    then (* IPv6 address *)
      match
        Re.(exec ipv6addr s |> get_all)
      with
      | [|_;host;port;""|] ->
        ADDR_INET (inet_addr_of_string host, int_of_string port)
      | [|_;host;port;zone|] ->
        ADDR_INET (inet_addr_of_string host, int_of_string port)
      | _ ->
        raise (Invalid_argument "invalid IPv6 address")
    else (* not an IPv6 address *)
    try
      let host, service = String.split ~by:":" s in
      match getaddrinfo host service [] with
      | [] -> raise (Invalid_argument "getaddrinfo returned no results")
      | h::_ -> h.ai_addr
    with Not_found ->
      Lwt_log.ign_warning_f ~section "Using UNIX domain socket %s" s;
      ADDR_UNIX s

  let node_sockaddr s = String.split ~by:"," s |> fst |> sockaddr
  let app_sockaddr  s = String.split ~by:"," s |> snd |> sockaddr
end

module SERVER = RSM.Make_server(CONF)
module CLIENT = RSM.Make_client(CONF)

let run_server ~db ~addr ?join ~id () =
  let exec = make_exec db in
  lwt server = SERVER.make exec addr ?join id in
    SERVER.run server

let client_op ~addr op =
  let c    = CLIENT.make ~id:(string_of_int (Unix.getpid ())) () in
  let exec = CLIENT.execute
  in
    CLIENT.connect c ~addr >>
    match_lwt exec c op with
        `OK s -> printf "+OK %s\n" s; return ()
      | `Error s -> printf "-ERR %s\n" s; return ()

let mode         = ref `Help
let cluster_addr = ref None
let mcast_addr   = ref None
let sql          = ref None

let initialized  = ref false

let specs =
  Arg.align
    [
      "-master", Arg.String (fun n -> mode := `Master n),
        "ADDR Launch master at given address (<node_addr>:<node_port>,<app_addr:app_port>)";
      "-join", Arg.String (fun p -> cluster_addr := Some p),
        "ADDR Join cluster at given address (<node_addr>:<node_port>,<app_addr:app_port>)";
      "-mcast", Arg.String (fun p -> mcast_addr := Some p),
        "ADDR Join mcast group at given address (<mcast_addr>:<port>%<iface>)";
      "-client", Arg.String (fun addr -> mode := `Client addr), "ADDR Client mode";
      "-sql", Arg.String (fun s -> sql := Some s), "STRING Execute the given SQL statement";
      "-v", Arg.Unit (fun () -> Lwt_log.(add_rule "dict" Info)), " Be verbose";
      "-vv", Arg.Unit (fun () -> Lwt_log.(add_rule "dict" Debug)), " Be more verbose"
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

let get_peer_info = function
  | Unix.ADDR_UNIX _ -> raise_lwt (Invalid_argument "get_peer_info")
  | Unix.ADDR_INET (a, p) as sa ->
    (* Found one neighbour, asking his oraft node port *)
    let sa_domain = Unix.domain_of_sockaddr sa in
    let s = Lwt_unix.(socket sa_domain SOCK_STREAM 0) in
    Lwt_unix.connect s sa >>= fun () ->
    let buf = String.make 5 '\000' in
    Lwt_unix.recv s buf 0 5 [] >>= fun nb_recv ->
    if nb_recv <> 5
    then raise_lwt (Failure "get_peer_info: failed to obtain info")
    else
      let remote_node_port = EndianString.BigEndian.get_int16 buf 0 in
      let remote_app_port = EndianString.BigEndian.get_int16 buf 2 in
      let uint16_of_int16 i16 = if i16 < 0 then i16 + 65535 else i16 in
      let remote_node_port = uint16_of_int16 remote_node_port in
      let remote_app_port = uint16_of_int16 remote_app_port in
      let cluster_addr =
        match sa_domain with
        | Unix.PF_UNIX -> assert false
        | Unix.PF_INET ->
          Printf.sprintf "%s:%d,%s:%d"
            (Unix.string_of_inet_addr a) remote_node_port
            (Unix.string_of_inet_addr a) remote_app_port
        | Unix.PF_INET6 ->
          Printf.sprintf "[%s]:%d,[%s]:%d"
            (Unix.string_of_inet_addr a) remote_node_port
            (Unix.string_of_inet_addr a) remote_app_port
      in
      Lwt.return (cluster_addr, (buf.[4] <> '\000'))

let db = Sqlite3.db_open ("/tmp/llsqlite." ^ (string_of_int (Unix.getpid ())))

let () =
  ignore (Sys.set_signal Sys.sigpipe Sys.Signal_ignore);
  Arg.parse specs ignore "Usage:";
  match !mode with
      `Help -> usage ()
    | `Master addr ->
      let my_node_sockaddr = CONF.node_sockaddr addr in
      let my_app_sockaddr = CONF.app_sockaddr addr in
      let my_ports = match my_node_sockaddr, my_app_sockaddr with
        | Unix.ADDR_INET (a, p),  Unix.ADDR_INET (a2, p2) -> p, p2
        | _ -> failwith "my_node_port" in
      (match !mcast_addr with
       | None -> Lwt_main.run (run_server ~db ~addr ?join:!cluster_addr ~id:addr ())
       | Some mcast_addr ->
         if not (Re.execp ipv6addr mcast_addr) then
           raise (Invalid_argument "Invalid multicast address");
         match Re.(exec ipv6addr mcast_addr |> get_all) with
         | [|_;v6addr;port;iface|] ->
           let port = int_of_string port in
           let return_oraft_ports _ fd saddr =
             let buf = String.make 5 '\000' in
             EndianString.BigEndian.set_int16 buf 0 (fst my_ports);
             EndianString.BigEndian.set_int16 buf 2 (snd my_ports);
             if !initialized then
               buf.[4] <- '\001';
             Lwt_unix.send fd buf 0 5 [] >>= fun nb_sent ->
             if nb_sent <> 5 then
               Lwt_log.warning_f ~section "Could not send all info to %s"
                 (Llnet.Helpers.string_of_saddr saddr)
             else
               Lwt.return_unit
           in
           let main_thread () =
             let open Llnet in
             Llnet.connect
               ~tcp_reactor:return_oraft_ports
               ~iface (Ipaddr.of_string_exn v6addr) port >>= fun h ->
             (* Waiting for other peers to manifest themselves *)
             Lwt_log.info_f ~section "I am %s, now detecting peers..."
               Llnet.(Helpers.string_of_saddr h.tcp_in_saddr)
             >>= fun () ->
             Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
             let neighbours = Llnet.neighbours_nonblock h in
             let nb_neighbours = List.length neighbours in
             match nb_neighbours,
                   Llnet.order h,
                   Llnet.neighbours_nonblock h with
             | 0, _, _ ->
               (* We are alone, run server without joining a cluster *)
               initialized := true;
               run_server ~db ~addr ?join:!cluster_addr ~id:addr ()
             | _, o, ns ->
               let rec try_joining_cluster () =
                 Lwt_list.map_p (fun n -> get_peer_info n) ns >>= fun p_infos ->
                 let initialized_peers =
                   List.fold_left (fun a (addr, init) ->
                       if init then addr::a else a) [] p_infos in
                 match initialized_peers with
                 | [] ->
                   (* No peers initialized, and I'm the lowest IP, run
                        server without joining a cluster *)
                   if o = 0 then
                     (
                       initialized := true;
                       Lwt_log.ign_info ~section "Found 0 peers initialized, running standalone";
                       run_server ~db ~addr ?join:!cluster_addr ~id:addr ()
                     )
                   else Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
                     try_joining_cluster ()
                 | peers ->
                   (* Some peers initialized, connecting to the first one *)
                   Lwt_list.iter_s (fun p ->
                       try_lwt
                         initialized := true;
                         Lwt_log.ign_info_f ~section "Connecting to %s" p;
                         run_server ~db ~addr ?join:(Some p) ~id:addr ()
                       with exn ->
                         initialized := false;
                         Lwt_log.warning_f ~exn ~section
                           "Exn raised when trying to sync to peer %s, trying others"
                           p
                     ) peers >>= fun () ->
                   Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
                   try_joining_cluster ()
               in try_joining_cluster ()
           in Lwt_main.run (main_thread ())
         | _ -> failwith "Invalid multicast address: zone id missing"

      )
    | `Client addr ->
      printf "Launching client %d\n" (Unix.getpid ());
      match !sql with
      | None -> usage ()
      | Some sql -> Lwt_unix.run (client_op ~addr sql)


