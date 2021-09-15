package xrand

import (
	"encoding/base64"
	"math/rand"
)

const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// StringN generates a random string of a fixed length.
func StringN(size int) string {
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return base64.StdEncoding.EncodeToString(b)
}
