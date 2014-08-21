let (>>=) = Lwt.(>>=)

let section = Lwt_log.Section.make "llsqlite3"

type sql = string

type mutable_state = {
  initialized: bool;
}

module Conf =
struct
  type op = string
  let string_of_op op = op
  let op_of_string str = str

  let port_of_string_raw s off =
    let p = EndianString.BigEndian.get_int16 s off in
    if p < 0 then p + 65535 else p

  let bytes_of_sockaddr = function
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

  let sockaddr_of_bytes str =
    if String.length str < 7
    then Unix.ADDR_UNIX str (* minimum size of IPv4 packing *)
    else
      let p = port_of_string_raw str 1 in
      match str.[0] with
      | '\004' ->
        let a = Ipaddr.V4.of_bytes_raw str 3 |> Ipaddr_unix.V4.to_inet_addr in
        Unix.ADDR_INET (a, p)
      | '\006' ->
        let a = Ipaddr.V6.of_bytes_raw str 3 |> Ipaddr_unix.V6.to_inet_addr in
        Unix.ADDR_INET (a, p)
      | _ ->
        Lwt_log.ign_warning_f ~section "Using UNIX socket %s" str;
        Unix.ADDR_UNIX str

  let make_addr_unix u1 u2 =
    let open Unix in
    match u1, u2 with
    | ADDR_UNIX s1, ADDR_UNIX s2 ->
      let s1_len = String.length s1 in
      String.make 1 (Char.chr s1_len) ^ s1 ^ s2
    | _ -> failwith "make_addr_unix"

  let address_of_saddrs sa1 sa2 =
    let sa1 = bytes_of_sockaddr sa1 in
    let sa2 = bytes_of_sockaddr sa2 in
    String.(make 1 (Char.chr (length sa1))) ^ sa1 ^ sa2

  let make_addr_inet addr server_port client_port =
    let open Unix in
    let sa1 = ADDR_INET (addr, server_port) |> bytes_of_sockaddr in
    let sa2 = ADDR_INET (addr, client_port) |> bytes_of_sockaddr in
    String.(make 1 (Char.chr (length sa1))) ^ sa1 ^ sa2

  let make_addr_inet_gai addr service1 service2 =
    let open Unix in
    match getaddrinfo addr service1 [],
          getaddrinfo addr service2 [] with
    | h1::_, h2::_ ->
      let sa1 = bytes_of_sockaddr h1.ai_addr in
      let sa2 = bytes_of_sockaddr h2.ai_addr in
      String.(make 1 (Char.chr (length sa1))) ^ sa1 ^ sa2
    | _ -> failwith "make_addr_inet_gai"

  let node_sockaddr s =
    let first_len = Char.code s.[0] in
    String.sub s 1 first_len |> sockaddr_of_bytes

  let app_sockaddr s =
    let len = String.length s in
    let first_len = Char.code s.[0] in
    String.sub s (1+first_len) (len - first_len - 1) |> sockaddr_of_bytes

  let string_of_address addr =
    let a1_len = Char.code addr.[0] in
    let a1 = String.sub addr 1 a1_len in
    let a2 = String.sub addr (1 + a1_len) (String.length addr - a1_len - 1) in

    match a1.[0] with
    | '\004' ->
      let ipaddr = Ipaddr.V4.(of_bytes_raw a1 3 |> to_string) in
      let node_port = port_of_string_raw a1 1 in
      let app_port = port_of_string_raw a2 1 in
      Printf.sprintf "%s/%d/%d" ipaddr node_port app_port
    | '\006' ->
      let ipaddr = Ipaddr.V6.(of_bytes_raw a1 3 |> to_string) in
      let node_port = port_of_string_raw a1 1 in
      let app_port = port_of_string_raw a2 1 in
      Printf.sprintf "%s/%d/%d" ipaddr node_port app_port
    | _ -> Printf.sprintf "%s/%s" a1 a2

  let saddrs_of_address addr =
    let open Unix in
    let a1_len = Char.code addr.[0] in
    let a1 = String.sub addr 1 a1_len in
    let a2 = String.sub addr (1 + a1_len) (String.length addr - a1_len - 1) in

    match a1.[0] with
    | '\004' ->
      let ipaddr = Ipaddr.V4.(of_bytes_raw a1 3 |> Ipaddr_unix.V4.to_inet_addr) in
      let node_port = port_of_string_raw a1 1 in
      let app_port = port_of_string_raw a2 1 in
      ADDR_INET (ipaddr, node_port), ADDR_INET (ipaddr, app_port)
    | '\006' ->
      let ipaddr = Ipaddr.V6.(of_bytes_raw a1 3 |> Ipaddr_unix.V6.to_inet_addr) in
      let node_port = port_of_string_raw a1 1 in
      let app_port = port_of_string_raw a2 1 in
      ADDR_INET (ipaddr, node_port), ADDR_INET (ipaddr, app_port)
    | _ -> ADDR_UNIX a1, ADDR_UNIX a2
end

module Client = struct
  include RSM.Make_client(Conf)

  let connect h ~addr ~port = connect h ~addr:(Conf.make_addr_inet addr port port)
end


module Server = struct
  include RSM.Make_server(Conf)

let run_server ?tls ?client_tls ~db ~addr ?join ~id () =
  let make_exec db =
    fun _ sql ->
      try
        let rc = Sqlite3.(exec db sql |> Rc.to_string) in
        `Sync (`OK rc |> Lwt.return)
      with exn ->
        `Sync (`Error exn |> Lwt.return)

  in
  let exec = make_exec db in
  lwt server = make ?tls ?client_tls exec addr ?join id in
  run server

let get_peer_info sa =
  (* Found one neighbour, asking his oraft node port *)
  let sa_domain = Unix.domain_of_sockaddr sa in
  let s = Lwt_unix.(socket sa_domain SOCK_STREAM 0) in
  Lwt_unix.connect s sa >>= fun () ->
  let buf = String.make 256 '\000' in
  Lwt_unix.recv s buf 0 256 [] >>= fun nb_recv ->
  let peer_initialized = (buf.[0] <> '\000') in
  let oraft_addr = String.sub buf 1 (nb_recv-1) in
  Lwt_log.debug_f ~section "Got peer oraft node addr %s, initialized = %b"
    (Conf.string_of_address oraft_addr) peer_initialized >>= fun () ->
  Lwt.return (oraft_addr, peer_initialized)

let distribute
    ?tls
    ?client_tls
    ~id ~iface
    ~node_saddr
    ~client_saddr
    ~group_saddr
    db =

  let node_port = Llnet.Helpers.port_of_saddr node_saddr in

  let return_oraft_addr h fd saddr =
    let open Llnet in
    let node_saddr = Helpers.(saddr_with_port h.tcp_in_saddr node_port) in
    let oraft_addr = Conf.address_of_saddrs node_saddr client_saddr in
    let oraft_addr_len = String.length oraft_addr in
    let buf = String.make 1 '\000' ^ oraft_addr in
    (match h.Llnet.user_data with
     | None -> Lwt.fail (Failure "return_oraft_addr")
     | Some { initialized } when initialized ->
       buf.[0] <- '\001'; Lwt.return_unit
     | _ -> Lwt.return_unit
    ) >>= fun () ->
    Lwt_unix.send fd buf 0 (1+oraft_addr_len) [] >>= fun nb_sent ->
    if nb_sent <> (1+oraft_addr_len) then
      Lwt_log.warning_f ~section "Could not send all info to %s"
        (Llnet.Helpers.string_of_saddr saddr)
    else Lwt.return_unit
  in

  let open Llnet in
  Llnet.connect
    ~tcp_reactor:return_oraft_addr
    ~user_data:{initialized=false}
    ~iface
    group_saddr >>= fun h ->

  let node_saddr = Helpers.(saddr_with_port h.tcp_in_saddr
                              (port_of_saddr node_saddr)) in
  let oraft_addr = Conf.address_of_saddrs node_saddr client_saddr in

  (* Waiting for other peers to manifest themselves *)
  Lwt_log.info_f ~section "%s (ORaft: %d) starting"
    Llnet.(Helpers.string_of_saddr h.tcp_in_saddr) node_port
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
    run_server ?tls ?client_tls ~db ~addr:oraft_addr ~id ()
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
            Lwt_log.info ~section
              "Found 0 peers initialized, running standalone" >>= fun () ->
            run_server ?tls ?client_tls ~db ~addr:oraft_addr ~id ()
          )
        else Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
          try_joining_cluster ()
      | peers ->
        (* Some peers initialized, connecting to the first one *)
        let nb_peers = List.length peers in
        Lwt_list.iteri_s (fun i oaddr ->
            let oaddr_str = oaddr |> Conf.string_of_address in
            try_lwt
              h.user_data <- Some {initialized = true};
              Lwt_log.info_f ~section "Connecting to (%d/%d) %s"
               (i+1) nb_peers oaddr_str >>= fun () ->
              run_server ?tls ?client_tls
                ~db ~addr:oraft_addr ?join:(Some oaddr) ~id ()
            with exn ->
              h.user_data <- Some {initialized = false};
              Lwt_log.warning_f ~exn ~section
                "Impossible to connect to peer %d/%d (%s)"
                i nb_peers oaddr_str
          ) peers >>= fun () ->
        Lwt_unix.sleep (2. *. h.ival) >>= fun () ->
        try_joining_cluster ()
    in try_joining_cluster ()

end
