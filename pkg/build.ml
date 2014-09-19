open Topkg

let () =
  Pkg.describe "llsqlite" ~builder:`OCamlbuild [
    Pkg.lib "pkg/META";
    Pkg.lib ~exts:Exts.module_library "lib/llsqlite3";
    Pkg.bin ~auto:true "src/llsqlite";
  ]
