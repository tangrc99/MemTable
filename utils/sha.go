package utils

import (
	"crypto/sha1"
	"fmt"
)

func Sha1(str []byte) string {
	h := sha1.New()
	h.Write(str)
	return fmt.Sprintf("%x", h.Sum(nil))
}
