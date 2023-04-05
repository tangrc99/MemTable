package errors

import (
	"errors"
	"fmt"
)

func ErrorUnKnownSubCommand(args string) error {
	return errors.New(fmt.Sprintf("Err unknown subcommand '%s'", args))
}

func ErrorCategoryNotExist(args string) error {
	return errors.New(fmt.Sprintf("Err category not exists '%s'", args))
}

func ErrorUserNotExist(args string) error {
	return errors.New(fmt.Sprintf("Err user not exists '%s'", args))
}

func ErrorPasswordNotExist(args string) error {
	return errors.New(fmt.Sprintf("Err password not exists '%s'", args))
}
