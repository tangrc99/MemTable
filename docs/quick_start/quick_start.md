# Quick Start

直接使用 Makefile 对项目进行构建：

```shell
git clone https://github.com/tangrc99/MemTable
cd MemTable
# build server and client
make
# run server with config
./bin/memtable conf/default.conf
```

## Dokcer Image

 使用 Dockerfile 构建一个 Docker 镜像并运行：

```shell
# pwd: MemTable
docker build -o memtable .
# run container with port 6380
docker run --name memtable -p 6380:6380 memtable
```

## Run Tests

使用 Makefile 进行单元测试与覆盖率测试：

```shell
# run unit-tests
make test

# run coverage-tests
make coverage
```

## Issues

如果在项目的使用中遇到任何问题，可以联系作者，或[提交 issue](https://github.com/tangrc99/MemTable/issues/new)