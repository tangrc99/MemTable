#! /bin/bash

(printf "set 111 1\r\nget 111\r\nping\r\nping\r\n" ; sleep 3) | nc localhost 6379