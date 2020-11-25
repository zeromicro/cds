package cryptox

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"  // #nosec
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/tal-tech/go-zero/core/logx"
)

func createHash(key string) string {
	hasher := md5.New() // #nosec
	_, err := hasher.Write([]byte(key))
	if err != nil {
		logx.Error(err)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func EncryptAES(data []byte, passphrase string) []byte {
	block, _ := aes.NewCipher([]byte(createHash(passphrase)))
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		fmt.Println("encrypt ", string(data), "failed", err)
		return nil
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		fmt.Println("encrypt ", string(data), "failed", err)
		return nil
	}
	ciphertext := gcm.Seal(nonce, nonce, data, nil)
	return ciphertext
}

func DecryptAES(data []byte, passphrase string) ([]byte, error) {
	if len(data) < 12 {
		return nil, errors.New("data too short")
	}
	key := []byte(createHash(passphrase))
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := data[:nonceSize], data[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return plaintext, nil
}
