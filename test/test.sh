#!/bin/bash

if [[ $# -lt 3 ]]
then
    printf "Usage: ${0} <iface> <node_port> <group_port>\n"
    exit 1
else
    ../llsqlite.native -server ::/${2}/ff02::dead:beaf/${3}/${1} -v &
    ../llsqlite.native -server ::/$(($2 + 2))/ff02::dead:beaf/${3}/${1} -v &
    sleep 3
    ../llsqlite.native -client ::1/10000 -sql "CREATE TABLE STORE (KEY STRING, VALUE STRING)"
    for i in {0..3}; do
	K=`pwgen -1`
	V=`pwgen -1`
	printf "Insterting $K -> $V into the DB\n"
	../llsqlite.native -client ::1/10000 -sql "INSERT INTO STORE (KEY, VALUE) VALUES (\"`pwgen -1`\", \"`pwgen -1`\")"
	sleep 1
    done
    printf "Cleaning up...\n"
    killall llsqlite.native
    exit 0
fi
