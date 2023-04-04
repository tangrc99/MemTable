package acl

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestUserPassword(t *testing.T) {

	u1 := NewUser("test")

	assert.True(t, u1.IsPasswordMatch("123"))

	u2 := NewUser("test")
	u2.WithPasswords([]string{"123"})
	assert.True(t, u2.IsPasswordMatch("123"))

	u3 := NewUser("test")
	u3.WithPasswords([]string{"123", "234", "456"})
	assert.True(t, u3.IsPasswordMatch("123"))
	assert.True(t, u3.IsPasswordMatch("234"))
	assert.True(t, u3.IsPasswordMatch("456"))

}

func TestUserKey(t *testing.T) {

	u1 := NewUser("test")
	assert.False(t, u1.IsKeyAccessible("1"))

	u2 := NewUser("test")
	u2.WithPatterns([]string{".*"})
	assert.True(t, u2.IsKeyAccessible("1234"))
	assert.True(t, u2.IsKeyAccessible("4563342fgdhfghgf546ygdghf"))
}

func TestUserOff(t *testing.T) {
	u1 := NewUser("test")
	u1.WithUserOff()
	assert.False(t, u1.IsOn())

	u2 := NewUser("test")
	assert.True(t, u2.IsOn())
}

func TestUserAuthority(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)

	u1 := NewUser("test")
	u1.WithPermittedCommand([]string{"set", "get"})
	assert.True(t, u1.IsCommandAllowed("set"))
	assert.True(t, u1.IsCommandAllowed("get"))
	assert.False(t, u1.IsCommandAllowed("del"))

	assert.False(t, u1.IsCommandAllowed("124"))
}

func TestCategory(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	id := global.GetCommandId("set")

	assert.True(t, categoryAll.IsPermitted(id+0))
	assert.True(t, categoryAll.IsPermitted(id+1))
	assert.True(t, categoryAll.IsPermitted(id+2))
	assert.True(t, categoryAll.IsPermitted(id+3))
	assert.True(t, categoryAll.IsPermitted(1023))

	assert.True(t, categoryWrite.IsPermitted(id+0))
	assert.False(t, categoryWrite.IsPermitted(id+1))
	assert.True(t, categoryWrite.IsPermitted(id+2))
	assert.False(t, categoryWrite.IsPermitted(id+3))

	assert.False(t, categoryRead.IsPermitted(id+0))
	assert.True(t, categoryRead.IsPermitted(id+1))
	assert.False(t, categoryRead.IsPermitted(id+2))
	assert.False(t, categoryRead.IsPermitted(id+3))

	c := newCategory("test")
	c.addPermittedCommand("set")
	c.addPermittedCommand("del")

	assert.True(t, c.IsPermitted(id+0))
	assert.False(t, c.IsPermitted(id+1))
	assert.True(t, c.IsPermitted(id+2))

	c.addForbiddenCommand("del")

	assert.False(t, c.IsPermitted(id+2))

	c.permitAll()

	assert.True(t, c.IsPermitted(id+2))
	assert.True(t, c.IsPermitted(id+34))

	c.forbidAll()

	assert.False(t, c.IsPermitted(id+0))
	assert.False(t, c.IsPermitted(id+1))
	assert.False(t, c.IsPermitted(id+2))
	assert.False(t, c.IsPermitted(id+34))
	assert.False(t, c.IsPermitted(1000))

}

func TestUserAuthorityWithCategory(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	u1 := NewUser("test")
	u1.WithPermittedCategory(categoryAll)
	assert.True(t, u1.IsCommandAllowed("set"))
	assert.True(t, u1.IsCommandAllowed("get"))
	assert.True(t, u1.IsCommandAllowed("del"))
	assert.False(t, u1.IsCommandAllowed("124"))

	u2 := NewUser("test")
	u2.WithPermittedCategory(categoryAll).WithForbiddenCategory(categoryWrite)

	assert.False(t, u2.IsCommandAllowed("set"))
	assert.True(t, u2.IsCommandAllowed("get"))
	assert.False(t, u2.IsCommandAllowed("del"))
	assert.False(t, u2.IsCommandAllowed("124"))

	u3 := NewUser("test")
	u3.WithPermittedCategory(categoryAll).WithForbiddenCategory(categoryWrite).WithPermittedCommand([]string{"del"})

	assert.False(t, u3.IsCommandAllowed("set"))
	assert.True(t, u3.IsCommandAllowed("get"))
	assert.True(t, u3.IsCommandAllowed("del"))
	assert.False(t, u3.IsCommandAllowed("124"))
}

/* ---------------------------------------------------------------------------
* ACL 函数实现
* ------------------------------------------------------------------------- */
