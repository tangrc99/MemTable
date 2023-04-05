#! /bin/bash

(printf "set 111 222\r\nget 111\r\nping\r\nping\r\n" ; sleep 3) | nc localhost 6379