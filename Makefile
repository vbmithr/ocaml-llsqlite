PKG=llsqlite
PREFIX=`opam config var prefix`
BUILDOPTS=native=true native-dynlink=true

all: build

build:
	ocamlbuild -use-ocamlfind pkg/build.native
	./build.native $(BUILDOPTS)

install: build
	opam-installer --prefix=$(PREFIX) $(PKG).install

uninstall: $(PKG).install
	opam-installer -u --prefix=$(PREFIX) $(PKG).install

PHONY: clean

clean:
	ocamlbuild -clean
