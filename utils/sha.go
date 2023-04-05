package utils

import (
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
)

func Sha1(str []byte) string {
	h := sha1.New()
	h.Write(str)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Sha256String(str []byte) string {
	h := sha256.New()
	h.Write(str)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Sha256(str []byte) []byte {
	h := sha256.New()
	h.Write(str)
	return h.Sum(nil)
}
