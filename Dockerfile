FROM golang:latest as build

ADD . /etc/MemTable
WORKDIR /etc/MemTable

ENV GOPROXY=https://goproxy.cn,direct
RUN make

FROM ubuntu:latest

WORKDIR /etc/MemTable
RUN mkdir -p /etc/MemTable/conf /etc/MemTable/logs /etc/MemTable/bin
COPY --from=build /etc/MemTable/conf/* /etc/MemTable/conf
COPY --from=build /etc/MemTable/logs/* /etc/MemTable/logs
COPY --from=build /etc/MemTable/bin/* /etc/MemTable/bin/
EXPOSE 6380
CMD ["./bin/memtable","--host","0.0.0.0"," --port","6380"]

