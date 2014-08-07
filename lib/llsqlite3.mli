type sql = string
(* The type of SQL queries. *)

module Client : sig
  type t
  val make : id:string -> unit -> t
  val connect : t -> addr:Unix.inet_addr -> port:int -> unit Lwt.t
  val execute : t -> sql -> [ `Error of string | `OK of string ] Lwt.t
  val execute_ro : t -> sql -> [ `Error of string | `OK of string ] Lwt.t
end

module Server : sig
  val distribute :
    iface:string ->
    node_addr:Unix.inet_addr ->
    node_port:int ->
    client_port:int ->
    group_addr:Unix.inet_addr ->
    group_port:int ->
    db:Sqlite3.db -> unit Lwt.t
    (** [distribute ~iface ~node_addr ~node_port ~client_port
        ~group_addr ~group_port ~db] transforms the Sqlite3 database
        [db] to a distributed database using RAFT over llnet. [db] can
        still be read locally. *)
end
