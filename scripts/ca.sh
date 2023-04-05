#! /bin/bash
mkdir -p tests/tls
openssl genrsa -out tests/tls/ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key tests/tls/ca.key \
    -days 3650 \
    -subj '/CN=CertificateAuthority' \
    -outform PEM \
    -out tests/tls/ca.crt
openssl genrsa -out tests/tls/redis.key 2048
openssl req \
    -new -sha256 \
    -key tests/tls/redis.key \
    -subj '/CN=Server' | \
openssl x509 \
    -req -sha256 \
   	-CA tests/tls/ca.crt \
    -CAkey tests/tls/ca.key \
    -CAserial tests/tls/ca.txt \
    -CAcreateserial \
    -days 365 \
    -outform PEM \
    -out tests/tls/redis.crt
openssl dhparam -out tests/tls/redis.dh 2048
