let (>>=) = Lwt.(>>=)

let section = Lwt_log.Section.make "llsqlite3"

type mutable_state = {
  initialized: bool;
}

(* The type of a distributed db *)
type lldb = {
  llnet: mutable_state Llnet.t;
  db: Sqlite3.db;
}

(* Raised when SQLite returns a non-OK code *)
exception SQLite_Error_Code of Sqlite3.Rc.t

module CONF =
struct
  type op = string
  let string_of_op op = op
  let op_of_string str = str

  let string_of_sockaddr = function
    | Unix.ADDR_UNIX s -> s
    | Unix.ADDR_INET (h, p) ->
      let port = "\000\000" in
      EndianString.BigEndian.set_int16 port 0 p;
      (match Ipaddr_unix.of_inet_addr h with
       | Ipaddr.V4 v4addr ->
         "\004" ^ port ^ Ipaddr.V4.to_bytes v4addr
       | Ipaddr.V6 v6addr ->
         "\006" ^ port ^ Ipaddr.V6.to_bytes v6addr
      )

  let sockaddr_of_string str =
    if String.length str < 7
    then Unix.ADDR_UNIX str (* minimum size of IPv4 packing *)
    else
      let p = EndianString.BigEndian.get_int16 str 1 in
      let p = if p < 0 then p + 65535 else p in
      match str.[0] with
      | '\004' ->
        let a = Ipaddr.V4.of_bytes_raw str 3 |> Ipaddr_unix.V4.to_inet_addr in
        Unix.ADDR_INET (a, p)
      | '\006' ->
        let a = Ipaddr.V6.of_bytes_raw str 3 |> Ipaddr_unix.V6.to_inet_addr in
        Unix.ADDR_INET (a, p)
      | _ ->
        Unix.ADDR_UNIX str

  let make_addr_unix u1 u2 =
    let open Unix in
    match u1, u2 with
    | ADDR_UNIX s1, ADDR_UNIX s2 ->
      let s1_len = String.length s1 in
      String.make 1 (Char.chr s1_len) ^ s1 ^ s2
    | _ -> failwith "make_addr_unix"

  let make_addr_inet addr server_port client_port =
    let open Unix in
    let sa1 = ADDR_INET (addr, server_port) |> string_of_sockaddr in
    let sa2 = ADDR_INET (addr, client_port) |> string_of_sockaddr in
    String.(make 1 (Char.chr (length sa1))) ^ sa1 ^ sa2

  let make_addr_inet_gai addr service1 service2 =
    let open Unix in
    match getaddrinfo addr service1 [],
          getaddrinfo addr service2 [] with
    | h1::_, h2::_ ->
      let sa1 = string_of_sockaddr h1.ai_addr in
      let sa2 = string_of_sockaddr h2.ai_addr in
      String.(make 1 (Char.chr (length sa1))) ^ sa1 ^ sa2
    | _ -> failwith "make_addr_inet_gai"

  let node_sockaddr s =
    let first_len = Char.code s.[0] in
    String.sub s 1 first_len |> sockaddr_of_string

  let app_sockaddr s =
    let len = String.length s in
    let first_len = Char.code s.[0] in
    String.sub s (1+first_len) (len - first_len - 1) |> sockaddr_of_string
end

module SERVER = RSM.Make_server(CONF)
module CLIENT = RSM.Make_client(CONF)

let run_server ~db ~addr ?join ~id () =
  let make_exec db =
    fun _ sql ->
      let rc = Sqlite3.exec db sql in
      match rc with
      | Sqlite3.Rc.OK -> `Sync (`OK "" |> Lwt.return)
      | rc -> `Sync (`Error (SQLite_Error_Code rc) |> Lwt.return)
  in
  let exec = make_exec db in
  lwt server = SERVER.make exec addr ?join id in
    SERVER.run server

let get_peer_info sa =
  (* Found one neighbour, asking his oraft node port *)
  let sa_domain = Unix.domain_of_sockaddr sa in
  let s = Lwt_unix.(socket sa_domain SOCK_STREAM 0) in
  Lwt_unix.connect s sa >>= fun () ->
  let buf = String.make 256 '\000' in
  Lwt_unix.recv s buf 0 256 [] >>= fun nb_recv ->
  let cluster_addr = String.sub buf 1 (nb_recv - 1) in
  Lwt.return (cluster_addr, (buf.[0] <> '\000'))

let db_serve ~iface ~(addr:Unix.inet_addr) ~mcast_port ~server_port ~client_port db =
  let oraft_addr = CONF.make_addr_inet addr server_port client_port in
  let oraft_addr_len = String.length oraft_addr in
  let oraft_id = Unix.string_of_inet_addr addr ^ "/" ^ string_of_int server_port in
  let return_oraft_addr h fd saddr =
    let buf = String.make 1 '\000' ^ oraft_addr in
    (match h.Llnet.user_data with
     | None -> Lwt.fail (Failure "return_oraft_addr")
     | Some { initialized } when initialized ->
       buf.[0] <- '\001'; Lwt.return_unit
     | _ -> Lwt.return_unit
    ) >>= fun () ->
    Lwt_unix.send fd buf 0 (1 + oraft_addr_len) [] >>= fun nb_sent ->
    if nb_sent <> 1 + oraft_addr_len then
      Lwt_log.warning_f ~section "Could not send all info to %s"
        (Llnet.Helpers.string_of_saddr saddr)
    else Lwt.return_unit
  in
  let open Llnet in
  Llnet.connect
    ~tcp_reactor:return_oraft_addr
    ~user_data:{initialized=false}
    ~iface (Ipaddr_unix.of_inet_addr addr) mcast_port >>= fun h ->
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
    h.user_data <- Some {initialized = true};
    run_server ~db ~addr:oraft_addr ~id:oraft_id ()
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
            h.user_data <- Some {initialized = true};
            Lwt_log.ign_info ~section "Found 0 peers initialized, running standalone";
            run_server ~db ~addr:oraft_addr ~id:oraft_id ()
          )
        else Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
          try_joining_cluster ()
      | peers ->
        (* Some peers initialized, connecting to the first one *)
        Lwt_list.iter_s (fun p ->
            try_lwt
              h.user_data <- Some {initialized = true};
              Lwt_log.ign_info_f ~section "Connecting to %s" p;
              run_server ~db ~addr:oraft_addr ?join:(Some p) ~id:oraft_id ()
            with exn ->
              h.user_data <- Some {initialized = false};
              Lwt_log.warning_f ~exn ~section
                "Exn raised when trying to sync to peer %s, trying others"
                p
          ) peers >>= fun () ->
        Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
        try_joining_cluster ()
    in try_joining_cluster ()

