package acl

import (
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/resp"
	"github.com/tangrc99/MemTable/server/global"
	"regexp"
)

const (
	// TODO:

	UserOn        = 0x0000001
	UserReadOnly  = 0x0000010
	UserWriteOnly = 0x0000100
	UserReadWrite = UserWriteOnly | UserReadOnly
)

// User 代表一个视角下的控制权限组
type User struct {
	name      string
	flag      int
	allowed   *structure.BitMap
	passwords []string
	patterns  []*regexp.Regexp
	profile   string
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
	user.profile = profile
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

func (user *User) ToResp() resp.RedisData {
	// TODO
	return nil
}
