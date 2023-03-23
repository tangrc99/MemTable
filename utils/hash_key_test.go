package utils

import "testing"

func TestHashKeyPrefix(t *testing.T) {

	key1 := "{12444432}222"
	key2 := "{12444432}333"
	hash1 := HashKey(key1)
	hash2 := HashKey(key2)
	if hash1 != hash2 {
		t.Errorf("prefix hash: hash1 %d != hash2 %d", hash1, hash2)
	}

	key3 := "{}111"
	key4 := "{}222"

	hash3 := HashKey(key3)
	hash4 := HashKey(key4)
	if hash3 == hash4 {
		t.Errorf("prefix hash: hash3 %d == hash4 %d", hash3, hash4)
	}
}
