#!/bin/bash

control_c ()
{
    printf "Cleaning up...\n"
    killall llsqlite.native
    exit 0
}

if [[ $# -lt 6 ]]
then
    printf "Usage: ${0} <node_addr> <node_port> <group_addr> <group_port> <iface> <connect_addr> \n"
    exit 1
else
    trap control_c SIGINT
    ./llsqlite.native -tls -server ${1}/${2}/${3}/${4}/${5} -vv llsqlite3 -vv llsqlite "0" "llsqlite.db" &
    ./llsqlite.native -tls -server ${1}/$(($2 + 2))/${3}/${4}/${5} -vv llsqlite3 -vv llsqlite "1" "llsqlite.db2" &
    ./llsqlite.native -tls -server ${1}/$(($2 + 4))/${3}/${4}/${5} -vv llsqlite3 -vv llsqlite "2" "llsqlite.db3" &
    sleep 3
    RC=1
    while [[ $RC -ne 0 ]]; do
    	printf "Trying to create table STORE\n"
	./llsqlite.native -tls -client ${6}/10000 "CREATE TABLE STORE (KEY STRING, VALUE STRING)"
    	RC=$?
    	sleep 1
    done
    for i in {0..10}; do
    	K=`pwgen -1`
    	V=`pwgen -1`
    	RC=1
    	while [[ $RC -ne 0 ]]; do
    	    printf "Trying to insert $K -> $V into the DB\n"
	    ./llsqlite.native -tls -client ${6}/10000 "INSERT INTO STORE (KEY, VALUE) VALUES (\"`pwgen -1`\", \"`pwgen -1`\")"
    	    RC=$?
    	    sleep 1
    	done
    	sleep 1
    done
    sleep 1000
    control_c
fi
