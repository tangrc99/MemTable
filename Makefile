BINARY=memtable
VERSION="0.0.0-beta"
CLIENT=memtable-cli
CLIENT_VERSION="0.0.0"

INSTALL_DIR=/usr/local/bin

# green
HINTER_COLOR="\033[32;1m"
# pink
BIN_COLOR="\033[35;1m"
# blue
TARGET_COLOR="\033[36;1m"
END_COLOR="\033[0m"

.PHONY: default
default: server client
	@printf "%bbuild finished %b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: server
server:
	@printf "target: %b%s: v%s%b\n" $(TARGET_COLOR) $(BINARY) $(VERSION) $(END_COLOR)
	@go build -x -o ./bin/$(BINARY) -ldflags "-s -w -X 'main.Version=$(VERSION)'" .
	@printf "build finished: bin/%b\n" $(BIN_COLOR)$(BINARY)$(END_COLOR)
	@printf "%bbuild server finished %b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: client
client:
	@printf "target: %b%s: v%s%b\n" $(TARGET_COLOR) $(CLIENT) $(CLIENT_VERSION) $(END_COLOR)
	@cd client && go build -x -o ../bin/$(CLIENT) -ldflags "-s -w -X 'main.Version=$(CLIENT_VERSION)'" .
	@printf "build finished: bin/%b\n" $(BIN_COLOR)$(CLIENT)$(END_COLOR)
	@printf "%bbuild client finished %b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: test
test:
	@go test -v ./...

.PHONY: coverage
coverage:
	@go test -v -cover ./...

.PHONY: clean
clean:
	@if [ -e "./bin/$(BINARY)" ]; then rm ./bin/$(BINARY) ; fi
	@if [ -e "./bin/$(CLIENT)" ]; then rm ./bin/$(CLIENT) ; fi
	@printf "%bmake clean finished%b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: install
install:
	@if [ ! -e "$(INSTALL_DIR)/$(BINARY)" ]; then make server ; fi
	@if [ ! -e "$(INSTALL_DIR)/$(CLIENT)" ]; then make client ; fi
	@printf "%binstall directory: %s%b\n" $(HINTER_COLOR) $(INSTALL_DIR) $(END_COLOR)
	cp bin/$(BINARY) bin/$(CLIENT) $(INSTALL_DIR)
	@printf "%bmake install finished%b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: uninstall
uninstall:
	@if [ -e "$(INSTALL_DIR)/$(BINARY)" ]; then rm $(INSTALL_DIR)/$(BINARY) ; fi
	@if [ -e "$(INSTALL_DIR)/$(CLIENT)" ]; then rm $(INSTALL_DIR)/$(CLIENT) ; fi
	@printf "%bmake uninstall finished%b\n" $(HINTER_COLOR) $(END_COLOR)

.PHONY: help
help:
	@echo "Usages: "
	@printf "  %-15s: %s\n" "make [all]" "build server and client"
	@printf "  %-15s: %s\n" "make server" "build server only"
	@printf "  %-15s: %s\n" "make client" "build client only"
	@printf "  %-15s: %s\n" "make test" "build and run tests"
	@printf "  %-15s: %s\n" "make clean" "remove built files"
	@printf "  %-15s: %s\n" "make install" "install built files"
	@printf "  %-15s: %s\n" "make uninstall" "remove installed built files"
	@echo "ARGS: "
	@printf "  %-15s: %s\n" "BINARY" "output file name of server"
	@printf "  %-15s: %s\n" "CLIENT" "output file name of client"
	@printf "  %-15s: %s\n" "INSTALL_DIR" "directory to install built files"

