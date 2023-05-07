package acl

import (
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils"
	"os"
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

	u1.WithUserOn()
	assert.True(t, u1.IsOn())

	u2 := NewUser("test")
	assert.True(t, u2.IsOn())

	u2.WithUserOff()
	assert.False(t, u2.IsOn())
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

func TestUserToResp(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)

	u1 := NewUser("test")
	u1.WithPermittedCommand([]string{"set", "get"}).WithPassword("123456")
	assert.Equal(t, "*8\r\n$5\r\nflags\r\n*1\r\n$2\r\non\r\n$9\r\npasswords\r\n*1\r\n$32\r\n"+string(utils.Sha256([]byte("123456")))+"\r\n$8\r\ncommands\r\n*0\r\n$4\r\nkeys\r\n*0\r\n",
		string(u1.ToResp().ToBytes()))
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

func TestACLUserBasic(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	ok := acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"), []byte("~.*"), []byte("+@all")})
	assert.Nil(t, ok)

	user, exist := acl.FindUser("user")
	assert.True(t, exist)

	assert.Equal(t, "user", user.name)
	assert.True(t, user.IsOn())
	assert.True(t, user.IsPasswordMatch("123456"))
	assert.False(t, user.IsPasswordMatch("not match"))
	assert.True(t, user.IsKeyAccessible("some key"))
	assert.True(t, user.IsCommandAllowed("set"))
	assert.True(t, user.IsCommandAllowed("get"))
	assert.True(t, user.IsCommandAllowed("del"))
	assert.False(t, user.IsCommandAllowed("dsf"))

}

func TestACLUserBasic2(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	ok := acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"), []byte("~.*"), []byte("+@write")})
	assert.Nil(t, ok)

	user, exist := acl.FindUser("user")
	assert.True(t, exist)

	assert.Equal(t, "user", user.name)
	assert.True(t, user.IsOn())
	assert.True(t, user.IsPasswordMatch("123456"))
	assert.False(t, user.IsPasswordMatch("not match"))
	assert.True(t, user.IsKeyAccessible("some key"))
	assert.True(t, user.IsCommandAllowed("set"))
	assert.False(t, user.IsCommandAllowed("get"))
	assert.True(t, user.IsCommandAllowed("del"))
	assert.False(t, user.IsCommandAllowed("dsf"))
}

func TestACLUserBasic3(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	ok := acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"), []byte("~.*"), []byte("+@read")})
	assert.Nil(t, ok)

	user, exist := acl.FindUser("user")
	assert.True(t, exist)

	assert.Equal(t, "user", user.name)
	assert.True(t, user.IsOn())
	assert.True(t, user.IsPasswordMatch("123456"))
	assert.False(t, user.IsPasswordMatch("not match"))
	assert.True(t, user.IsKeyAccessible("some key"))
	assert.False(t, user.IsCommandAllowed("set"))
	assert.True(t, user.IsCommandAllowed("get"))
	assert.False(t, user.IsCommandAllowed("del"))
	assert.False(t, user.IsCommandAllowed("dsf"))

	assert.Equal(t, "user user on #"+utils.Sha256String([]byte("123456"))+" ~.* +@read", user.ToString())
}

func TestACLUserBasic4(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	ok := acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"),
		[]byte("~.*"), []byte("+set"), []byte("+DEL")})
	assert.Nil(t, ok)

	user, exist := acl.FindUser("user")
	assert.True(t, exist)

	assert.Equal(t, "user", user.name)
	assert.True(t, user.IsOn())
	assert.True(t, user.IsPasswordMatch("123456"))
	assert.False(t, user.IsPasswordMatch("not match"))
	assert.True(t, user.IsKeyAccessible("some key"))
	assert.True(t, user.IsCommandAllowed("set"))
	assert.False(t, user.IsCommandAllowed("get"))
	assert.True(t, user.IsCommandAllowed("del"))
	assert.False(t, user.IsCommandAllowed("dsf"))

	assert.Equal(t, "user user on #"+utils.Sha256String([]byte("123456"))+" ~.* +set +del", user.ToString())
}

func TestACLUserBasic5(t *testing.T) {

	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	_ = acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"),
		[]byte("~.*"), []byte("+set"), []byte("+DEL")})

	_ = acl.SetupUser("user", [][]byte{[]byte("-del")})

	user, exist := acl.FindUser("user")
	assert.True(t, exist)

	assert.False(t, user.IsCommandAllowed("del"))

	assert.False(t, acl.DeleteUser("default"))
	assert.True(t, acl.DeleteUser("user"))

}

func TestACLGetUserAndCategories(t *testing.T) {
	global.RegisterDatabaseCommand("set", nil, global.WR)
	global.RegisterDatabaseCommand("get", nil, global.RD)
	global.RegisterDatabaseCommand("del", nil, global.WR)
	initCategory()

	acl := NewAccessControlList("")
	_ = acl.CreateUser([][]byte{[]byte("user"), []byte("on"), []byte(">123456"),
		[]byte("~.*"), []byte("+set"), []byte("+DEL")})

	users := []string{"user", "default"}

	us := acl.GetAllUserNames()

	assert.Subset(t, users, us)
	assert.Equal(t, len(users), len(us))

	us = []string{}

	for _, u := range acl.GetAllUsers() {
		us = append(us, u.name)
	}

	assert.Subset(t, users, us)
	assert.Equal(t, len(users), len(us))

	categories := []string{"all", "write", "read"}
	cs := acl.GetCategoryNames()
	assert.Subset(t, categories, cs)
	assert.Equal(t, len(categories), len(cs))

	_, ok := acl.FindCategory("all")
	assert.True(t, ok)
	_, ok = acl.FindCategory("write")
	assert.True(t, ok)
	_, ok = acl.FindCategory("read")
	assert.True(t, ok)
}

func TestACLParseFile(t *testing.T) {

	const file = "aclfile.tmp"

	tmpFile, err := os.Create(file)
	t.Cleanup(func() {
		err = os.Remove(file)
	})
	assert.Nil(t, err)

	_, err = tmpFile.WriteString("user default on nopass ~.* +@all")
	assert.Nil(t, err)

	acl := NewAccessControlList(file)

	user, exist := acl.FindUser("default")
	assert.True(t, exist)

	assert.Equal(t, "user default on nopass ~.* +@all", user.ToString())

}

func TestACLWriteFile(t *testing.T) {

	const file = "aclfile"

	tmpFile, err := os.Create(file)
	t.Cleanup(func() {
		err = os.Remove(file)
	})
	assert.Nil(t, err)

	_, err = tmpFile.WriteString("user default on nopass ~.* +@all")
	assert.Nil(t, err)

	tmpFile.Close()

	acl := NewAccessControlList(file)

	_ = acl.SetupUser("default", [][]byte{[]byte(">123456")})

	acl.DumpToFile()

	bytes, err := os.ReadFile(file)
	assert.Nil(t, err)
	assert.Equal(t, []byte("user default on #123456 ~.* +@all\n"), bytes)
}
