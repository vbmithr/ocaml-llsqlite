#!/bin/bash

control_c ()
{
    printf "Cleaning up...\n"
    killall llsqlite.native
    exit 0
}

if [[ $# -lt 3 ]]
then
    printf "Usage: ${0} <iface> <node_port> <group_port>\n"
    exit 1
else
    trap control_c SIGINT
    ./llsqlite.native -tls -server ::/${2}/ff02::dead:beaf/${3}/${1} -vv RSM -vv oraft_lwt -vv llsqlite3 -vv llsqlite &
    ./llsqlite.native -tls -server ::/$(($2 + 2))/ff02::dead:beaf/${3}/${1} -vv RSM -vv oraft_lwt -vv llsqlite3 -vv llsqlite &
    # ./llsqlite.native -tls -server ::/$(($2 + 4))/ff02::dead:beaf/${3}/${1} -vv llsqlite3 &
    # ./llsqlite.native -tls -server ::/$(($2 + 6))/ff02::dead:beaf/${3}/${1} -vv llsqlite3 &
    # ./llsqlite.native -tls -server ::/$(($2 + 8))/ff02::dead:beaf/${3}/${1} -vv llsqlite3 &
    # ./llsqlite.native -tls -server ::/$(($2 + 10))/ff02::dead:beaf/${3}/${1} -vv llsqlite3 &
    sleep 3
    RC=1
    while [[ $RC -ne 0 ]]; do
    	printf "Trying to create table STORE\n"
    	./llsqlite.native -tls -client ::1/10000 -sql "CREATE TABLE STORE (KEY STRING, VALUE STRING)"
    	RC=$?
    	sleep 1
    done
    for i in {0..10}; do
    	K=`pwgen -1`
    	V=`pwgen -1`
    	RC=1
    	while [[ $RC -ne 0 ]]; do
    	    printf "Trying to insert $K -> $V into the DB\n"
    	    ./llsqlite.native -tls -client ::1/10000 -sql "INSERT INTO STORE (KEY, VALUE) VALUES (\"`pwgen -1`\", \"`pwgen -1`\")"
    	    RC=$?
    	    sleep 1
    	done
    	sleep 1
    done
    sleep 1000
    control_c
fi
