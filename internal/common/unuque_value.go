package common

import (
	"math/rand"
	"time"
)

func InitUniqueValue() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func UniqueValue(len_n int) string {
	var bytes_array []byte

	for i := 0; i < len_n; i++ {
		bytes := rand.Intn(35)
		if bytes > 9 {
			bytes = bytes + 7
		}
		bytes_array = append(bytes_array, byte(bytes+16*3))
	}
	str := string(bytes_array)
	return str
}
