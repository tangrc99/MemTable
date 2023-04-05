package acl

import (
	"bufio"
	"bytes"
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/logger"
	"github.com/tangrc99/MemTable/server/errors"
	"io"
	"os"
	"regexp"
	"strings"
)

// ACL 是服务器的访问控制列表，用于管理命令组以及用户权限
type ACL struct {
	file       string
	users      map[string]*User
	categories map[string]*category
}

func NewAccessControlList(file string) *ACL {
	acl := &ACL{
		file:       file,
		users:      make(map[string]*User),
		categories: make(map[string]*category),
	}

	// 必须要确保初始化顺序
	initCategory()
	initUser()

	// default categories
	acl.categories["all"] = categoryAll
	acl.categories["write"] = categoryWrite
	acl.categories["read"] = categoryRead

	// default user
	acl.users["default"] = defaultUser

	if !acl.ParseFromFile() {
		// 如果解析文件失败
		acl.users = map[string]*User{"default": defaultUser}
	}

	return acl
}

// ParseFromFile 将会从 acl 文件中读取已经持久化的配置文件
func (a *ACL) ParseFromFile() bool {
	if a.file == "" {
		return false
	}
	file, err := os.OpenFile(a.file, os.O_RDWR, 666)
	if err != nil {
		logger.Errorf("Open aclfile fail: %s", err.Error())
		return false
	}
	reader := bufio.NewReader(file)

	// 先将结果解析到临时结构体中，防止解析失败
	tmp := &ACL{
		file:       a.file,
		users:      a.users,
		categories: a.categories,
	}

	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return false
		}
		args := bytes.Split(line, []byte{' '})

		head := string(args[0])
		if head != "user" {
			logger.Panicf("Error parsing acl file line start with %s", head)
		}

		e := tmp.CreateUser(args[1:])
		if e != nil {
			logger.Panicf("Error parsing acl file %s", e.Error())
		}
		if err == io.EOF {
			break
		}
	}

	*a = *tmp
	return true
}

func (a *ACL) DumpToFile() bool {
	if a.file == "" {
		return false
	}

	tmp, err := os.Create(a.file + ".tmp")
	if err != nil {
		logger.Errorf("Create tmp aclfile fail: %s", err.Error())
		return false
	}

	for _, user := range a.users {
		_, err = tmp.WriteString(user.ToStringWithoutSha256() + "\n")
		if err != nil {
			logger.Errorf("Open aclfile fail: %s", err.Error())
			return false
		}
	}

	err = os.Rename(a.file+".tmp", a.file)
	if err != nil {
		logger.Errorf("Write aclfile fail: %s", err.Error())
		return false
	}
	return true
}

// CreateUser 使用所给参数创建一个用户
func (a *ACL) CreateUser(args [][]byte) error {

	name := strings.ToLower(string(args[0]))
	user := NewUser(name)

	err := a.setupUser(user, args[1:])
	if err != nil {
		return err
	}

	if oldUser, exist := a.users[name]; exist {
		*oldUser = *user
	} else {
		a.users[name] = user
	}

	return nil
}

// FindUser 寻找用户，用户必须处于可用状态
func (a *ACL) FindUser(name string) (*User, bool) {
	user, exist := a.users[name]
	if exist && !user.IsOn() {
		return nil, false
	}
	return user, exist
}

func (a *ACL) DeleteUser(name string) bool {
	_, exist := a.users[name]
	if exist {
		delete(a.users, name)
		return true
	}
	return false
}

func (a *ACL) SetupUser(name string, options [][]byte) error {

	user, exist := a.users[name]
	if !exist {
		user = NewUser(name)
	}
	oldVal := *user
	err := a.setupUser(user, options)
	if err != nil {
		*user = oldVal
	}
	return err
}

func (a *ACL) setupUser(user *User, options [][]byte) error {
	for _, s := range options {
		seg := strings.ToLower(string(s))
		prefix := seg[1:]

		switch seg[0] {
		case byte('>'):
			// password
			user.WithPassword(prefix)
		case byte('<'):
			if ok := user.DeletePassword(prefix); !ok {
				return errors.ErrorPasswordNotExist(prefix)
			}

		case byte('~'):
			// pattern
			user.WithPattern(prefix)

		case byte('+'):
			// permit command
			if prefix[0] != '@' {
				user.WithPermittedCommand([]string{prefix})

			} else {
				c, exist := a.categories[prefix[1:]]
				if !exist {
					return errors.ErrorCategoryNotExist(prefix[1:])
				}
				user.WithPermittedCategory(c)
			}
			user.WithProfile(seg)

		case byte('-'):
			// forbid command
			if prefix[0] != '@' {
				user.WithForbiddenCommand([]string{prefix})

			} else {
				c, exist := a.categories[prefix[1:]]
				if !exist {
					return errors.ErrorCategoryNotExist(prefix[1:])
				}
				user.WithForbiddenCategory(c)
			}
			user.WithProfile(seg)

		default:

			if seg == "on" {
				user.WithUserOn()
			} else if seg == "off" {
				user.WithUserOff()
			} else if seg == "allkeys" {
				user.patterns = []*regexp.Regexp{regexp.MustCompile(".*")}
			} else if seg == "resetkeys" {
				user.patterns = []*regexp.Regexp{}
			} else if seg == "allcommands" {
				user.WithPermittedCategory(categoryAll)
				user.WithProfile("+@all")
			} else if seg == "nocommands" {
				user.allowed = structure.NewBitMap(1024)
				user.WithProfile("-@all")
			} else if seg == "reset" {
				user.Reset()
			} else if seg == "nopass" {
				user.passwords = []string{}
			} else {
				return errors.ErrorUnKnownSubCommand(seg)
			}
		}
	}
	return nil
}

func (a *ACL) GetAllUserNames() []string {
	users := make([]string, 0, len(a.users))
	for name := range a.users {
		users = append(users, name)
	}
	return users
}

func (a *ACL) GetAllUsers() []*User {
	users := make([]*User, 0, len(a.users))
	for _, user := range a.users {
		users = append(users, user)
	}
	return users
}

// FindCategory 根据名字找到 category
func (a *ACL) FindCategory(name string) (*category, bool) {
	c, exist := a.categories[name]
	return c, exist
}

// GetCategoryNames 获取所有已经注册的 category 名字
func (a *ACL) GetCategoryNames() []string {
	names := make([]string, 0, len(a.categories))
	for name := range a.categories {
		names = append(names, name)
	}
	return names
}
