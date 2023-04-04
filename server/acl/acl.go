package acl

type ACL struct {
	file string

	users      map[string]*User
	categories map[string]*category
}

func NewAccessControlList(file string) *ACL {
	acl := &ACL{
		file:       file,
		users:      make(map[string]*User),
		categories: make(map[string]*category),
	}
	if file == "" {

		// 必须要确保初始化顺序
		initCategory()
		initUser()

		// default categories
		acl.categories["all"] = categoryAll
		acl.categories["write"] = categoryWrite
		acl.categories["read"] = categoryRead

		// default user
		acl.users["default"] = defaultUser

	} else {
		acl.parseACLFile()
	}

	return acl
}

// parseACLFile 将会从 acl 文件中读取已经持久化的配置文件
func (a *ACL) parseACLFile() {

}

func (a *ACL) dumpToFile() {
	if a.file == "" {
		return
	}
}

func (a *ACL) CreateUser(cmd [][]byte) bool {

	if len(cmd) < 2 {
		return false
	}

	user := NewUser(string(cmd[0]))

	if string(cmd[1]) == "off" {
		user.WithUserOff()
	}

	for i := range cmd[2:] {

		seg := string(cmd[i][1:])

		switch cmd[i][1] {
		case '>':
			// password
			user.WithPassword(seg)
		case '~':
			// pattern
			user.WithPattern(seg)

		case '+':
			// permit command
			if seg[0] != '@' {
				user.WithPermittedCommand([]string{seg})

			} else {
				c, exist := a.categories[seg[1:]]
				if !exist {
					//TODO:
					return false
				}
				user.WithPermittedCategory(c)
			}

		case '-':
			// forbid command
			if seg[0] != '@' {
				user.WithForbiddenCommand([]string{seg})

			} else {
				c, exist := a.categories[seg[1:]]
				if !exist {
					//TODO:
					return false
				}
				user.WithForbiddenCategory(c)
			}
		}

	}

	return true
}

func (a *ACL) CreateCategory() {

}

func (a *ACL) FindUser(name string) (*User, bool) {
	user, exist := a.users[name]
	return user, exist
}

func (a *ACL) DeleteUser(name string) {
	delete(a.users, name)
}

func (a *ACL) SetupUser(name string, options [][]byte) {

}
