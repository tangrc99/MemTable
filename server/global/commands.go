package global

// ExecStatus 标识一个 command 是否为写操作
type ExecStatus int

const (
	// RD 标识 command 为只读操作
	RD ExecStatus = iota
	// WR 标识 command 为写操作
	WR
)

type CommandType int

const (
	CTServer CommandType = iota
	CTDatabase
)

type Command struct {
	id int
	es ExecStatus
	ct CommandType
	f  any
}

func (c *Command) GetId() int {
	return c.id
}

func (c *Command) Type() CommandType {
	return c.ct
}

func (c *Command) IsWriteCommand() bool {
	return c.es == WR
}

func (c *Command) Function() any {
	return c.f
}

var id = 0
var commandTable = make(map[string]Command)

func registerCommand(name string, cmd Command) {
	cmd.id = id
	id++
	commandTable[name] = cmd
}

func RegisterDatabaseCommand(name string, cmd any, status ExecStatus) {
	c := Command{
		es: status,
		ct: CTDatabase,
		f:  cmd,
	}
	registerCommand(name, c)
}

func RegisterServerCommand(name string, cmd any, status ExecStatus) {
	c := Command{
		es: status,
		ct: CTServer,
		f:  cmd,
	}
	registerCommand(name, c)
}

func FindCommand(name string) (cmd Command, exist bool) {
	cmd, exist = commandTable[name]
	return cmd, exist
}

func GetCommandId(name string) int {
	cmd, exist := commandTable[name]
	if !exist {
		return -1
	}
	return cmd.id
}

func IsCommandExist(name string) bool {
	_, exist := commandTable[name]
	return exist
}

func IsRandCommand(cmd string) bool {
	return cmd == "randomkey" || cmd == "srandmember"
}

func IsWriteCommand(cmd string) bool {
	f, exist := commandTable[cmd]
	if !exist {
		return false
	}
	return f.IsWriteCommand()
}

func IsDatabaseCommand(cmd string) bool {
	f, exist := commandTable[cmd]
	if !exist {
		return false
	}
	return f.ct == CTDatabase
}

func IsServerCommand(cmd string) bool {
	f, exist := commandTable[cmd]
	if !exist {
		return false
	}
	return f.ct == CTServer
}
