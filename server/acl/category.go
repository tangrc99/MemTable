package acl

import (
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/server/global"
)

// category 是一个权限控制组
type category struct {
	name    string
	allowed *structure.BitMap
}

func newCategory(name string) *category {
	return &category{
		name:    name,
		allowed: structure.NewBitMap(1024),
	}
}

func (c *category) addPermittedCommand(command string) {
	id := global.GetCommandId(command)
	c.allowed.Set(id, 1)
}

func (c *category) addForbiddenCommand(command string) {
	id := global.GetCommandId(command)
	c.allowed.Set(id, 0)
}

func (c *category) permitAll() {
	c.allowed.RangeSet(1, 0, 1023)
}

func (c *category) forbidAll() {
	c.allowed.RangeSet(0, 0, 1023)
}

func (c *category) IsPermitted(pos int) bool {
	return c.allowed.Get(pos) == 1
}

var categoryAll *category
var categoryWrite *category
var categoryRead *category

func initCategory() {
	categoryAll = newCategory("all")
	categoryAll.permitAll()

	categoryWrite = newCategory("write")
	global.ForAnyCommands(func(cmdName string, cmd global.Command) {
		if cmd.IsWriteCommand() {
			categoryWrite.allowed.Set(cmd.GetId(), 1)
		}
	})

	categoryRead = newCategory("read")
	global.ForAnyCommands(func(cmdName string, cmd global.Command) {
		if !cmd.IsWriteCommand() {
			categoryRead.allowed.Set(cmd.GetId(), 1)
		}
	})
}
