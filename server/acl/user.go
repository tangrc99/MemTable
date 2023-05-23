package acl

import (
	"fmt"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"github.com/tangrc99/MemTable/utils"
	"regexp"
	"strings"
)

const (
	// TODO: Redis 的实现中，是根据这些标志位来确定用户的一些信息的。

	UserOn          = 0x00000001
	UserAllKeys     = 0x00000010
	UserAllCommands = 0x00000100
	UserNoPass      = 0x00001000
)

// User 代表一个视角下的控制权限组
type User struct {
	name      string            // 用户名称
	flag      int               // 标识位
	allowed   *structure.BitMap // 记录命令权限的 bitmap
	passwords []string          // 用户密码
	patterns  []*regexp.Regexp  // 键空间访问权限
	profiles  []string          // 记录生成 allowed bitmap 的操作
}

func NewUser(name string) *User {
	usr := &User{
		name:    name,
		flag:    0x0000001,
		allowed: structure.NewBitMap(1024),
	}

	return usr
}

func (user *User) WithPassword(password string) *User {
	user.passwords = append(user.passwords, password)
	return user
}

func (user *User) WithPasswords(passwords []string) *User {
	user.passwords = append(user.passwords, passwords...)
	return user
}

func (user *User) DeletePassword(password string) bool {
	for i := range user.passwords {
		if password == user.passwords[i] {
			user.passwords = append(user.passwords[:i], user.passwords[i+1:]...)
			return true
		}
	}
	return false
}

func (user *User) WithPattern(pattern string) *User {
	regex, err := regexp.Compile(pattern)
	if err != nil {
		logger.Errorf("Regex format error: %s", err.Error())
	}
	user.patterns = append(user.patterns, regex)
	return user
}

func (user *User) WithPatterns(patterns []string) *User {

	for i := range patterns {
		user.WithPattern(patterns[i])
	}
	return user

}

func (user *User) WithUserOn() *User {
	user.flag |= 0x00000001
	return user
}

func (user *User) WithUserOff() *User {
	user.flag &= 0x11111110
	return user
}

func (user *User) WithPermittedCommand(commands []string) *User {
	for i := range commands {
		id := global.GetCommandId(commands[i])
		user.allowed.Set(id, 1)
	}
	return user

}

func (user *User) WithForbiddenCommand(commands []string) *User {
	for i := range commands {
		id := global.GetCommandId(commands[i])
		user.allowed.Set(id, 0)
	}
	return user

}

func (user *User) WithPermittedCategory(c *category) *User {
	for i := 0; i < 1023; i++ {
		if c.IsPermitted(i) {
			user.allowed.Set(i, 1)
		}
	}
	return user
}

func (user *User) WithForbiddenCategory(c *category) *User {
	for i := 0; i < 1023; i++ {
		if c.IsPermitted(i) {
			user.allowed.Set(i, 0)
		}
	}
	return user
}

func (user *User) WithProfile(profile string) *User {
	user.profiles = append(user.profiles, profile)
	return user
}

func (user *User) IsOn() bool {
	return (user.flag & UserOn) == 1
}

func (user *User) IsPasswordMatch(password string) bool {

	if len(user.passwords) == 0 {
		return true
	}

	for i := range user.passwords {
		if password == user.passwords[i] {
			return true
		}
	}
	return false
}

func (user *User) IsCommandAllowed(command string) bool {

	c, exist := global.FindCommand(command)
	if !exist {
		return false
	}

	if user.allowed.Get(c.GetId()) == 1 {
		return true
	}

	return false
}

func (user *User) IsKeyAccessible(key string) bool {
	for i := range user.patterns {
		if user.patterns[i].MatchString(key) {
			return true
		}
	}
	return false
}

func (user *User) HasPassword() bool {
	return len(user.passwords) > 0
}

func (user *User) Name() string {
	return user.name
}

func (user *User) ToResp() resp.RedisData {

	ret := make([]resp.RedisData, 0)

	ret = append(ret, resp.MakeBulkData([]byte("flags")))
	flags := make([]resp.RedisData, 0, 4)
	if user.flag&0x00000001 != 0 {
		flags = append(flags, resp.MakeBulkData([]byte("on")))
	} else {
		flags = append(flags, resp.MakeBulkData([]byte("off")))
	}
	if user.flag&0x00000010 != 0 {
		flags = append(flags, resp.MakeBulkData([]byte("allkeys")))
	}
	if user.flag&0x00000100 != 0 {
		flags = append(flags, resp.MakeBulkData([]byte("allcommands")))
	}
	if user.flag&0x00001000 != 0 {
		flags = append(flags, resp.MakeBulkData([]byte("nopass")))
	}
	ret = append(ret, resp.MakeArrayData(flags))

	ret = append(ret, resp.MakeBulkData([]byte("passwords")))
	passwords := make([]resp.RedisData, 0, len(user.passwords))
	for i := range user.passwords {
		pwd := []byte(user.passwords[i])
		passwords = append(passwords, resp.MakeBulkData(utils.Sha256(pwd)))
	}
	ret = append(ret, resp.MakeArrayData(passwords))

	ret = append(ret, resp.MakeBulkData([]byte("commands")))
	commands := make([]resp.RedisData, 0, len(user.profiles))
	for i := range user.profiles {
		commands = append(commands, resp.MakeBulkData([]byte(user.profiles[i])))
	}
	ret = append(ret, resp.MakeArrayData(commands))

	ret = append(ret, resp.MakeBulkData([]byte("keys")))
	keys := make([]resp.RedisData, 0, len(user.patterns))
	for i := range user.patterns {
		keys = append(keys, resp.MakeBulkData([]byte(user.patterns[i].String())))
	}
	ret = append(ret, resp.MakeArrayData(keys))

	return resp.MakeArrayData(ret)
}

// ToString 将当前用户序列化，输出格式为："user default on nopass ~.* +@all"
func (user *User) ToString() string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("user %s", user.name))
	// user status
	if user.IsOn() {
		b.WriteString(" on")
	} else {
		b.WriteString(" off")
	}
	// passwords
	if len(user.passwords) == 0 {
		b.WriteString(" nopass")
	} else {
		for i := range user.passwords {
			pwd := []byte(user.passwords[i])
			b.WriteString(" #" + utils.Sha256String(pwd))
		}
	}
	// patterns
	for _, regex := range user.patterns {
		b.WriteString(" ~" + regex.String())
	}
	// commands
	if len(user.profiles) == 0 {
		b.WriteString(" -@all")
	} else {
		for i := range user.profiles {
			b.WriteString(" " + user.profiles[i])
		}
	}

	return b.String()
}

// ToStringWithoutSha256 将当前用户序列化，输出格式为："user default on nopass ~.* +@all"
func (user *User) ToStringWithoutSha256() string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("user %s", user.name))
	// user status
	if user.IsOn() {
		b.WriteString(" on")
	} else {
		b.WriteString(" off")
	}
	// passwords
	if len(user.passwords) == 0 {
		b.WriteString(" nopass")
	} else {
		for i := range user.passwords {
			b.WriteString(" #" + user.passwords[i])
		}
	}
	// patterns
	for _, regex := range user.patterns {
		b.WriteString(" ~" + regex.String())
	}
	// commands
	if len(user.profiles) == 0 {
		b.WriteString(" -@all")
	} else {
		for i := range user.profiles {
			b.WriteString(" " + user.profiles[i])
		}
	}

	return b.String()
}

// Reset 重置用户的各种参数
func (user *User) Reset() {
	user.allowed = structure.NewBitMap(1024)
	user.flag = 0x00000001
	user.patterns = []*regexp.Regexp{}
	user.passwords = []string{}
	user.profiles = []string{}
}

/* ---------------------------------------------------------------------------
* global variable
* ------------------------------------------------------------------------- */

var defaultUser *User
var manageUser *User

func initUser() {
	defaultUser = NewUser("default")
	defaultUser.WithPattern(".*").WithPermittedCategory(categoryAll).WithProfile("+@all")
	manageUser = NewUser("")
	manageUser.WithPattern(".*").WithPermittedCategory(categoryAll).WithProfile("+@all")
}

// DefaultUser 返回默认设置权限的用户
func DefaultUser() *User {
	return defaultUser
}

// ManageUser 返回一个具有全能权限的用户
func ManageUser() *User {
	return manageUser
}
