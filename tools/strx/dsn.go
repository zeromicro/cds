package strx

import (
	"encoding/base64"
	"strings"

	"github.com/tal-tech/cds/tools/cryptox"
)

const (
	dsnPrefix = "dsn://"
)

var dsnKey string

func SetDsnKey(key string) {
	dsnKey = key
}

func EncryptDsn(dsn string) string {
	if strings.HasPrefix(dsn, dsnPrefix) {
		return dsn
	}
	if dsn == "" {
		return ""
	}
	b := cryptox.EncryptAES([]byte(dsn), dsnKey)
	return dsnPrefix + base64.StdEncoding.EncodeToString(b)
}

func DecryptDsn(dsn string) (string, error) {
	if !strings.HasPrefix(dsn, dsnPrefix) {
		return dsn, nil
	}
	b, e := base64.StdEncoding.DecodeString(dsn[len(dsnPrefix):])
	if e != nil {
		return "", e
	}
	dec, e := cryptox.DecryptAES(b, dsnKey)
	if e != nil {
		return "", e
	}
	return string(dec), nil
}

func Encrypt(b []byte) string {
	res := cryptox.EncryptAES(b, dsnKey)

	return base64.StdEncoding.EncodeToString(res)
}

func Decrypt(b []byte) ([]byte, error) {
	dbuf := make([]byte, base64.StdEncoding.DecodedLen(len(b)))

	empty := []byte{}
	n, e := base64.StdEncoding.Decode(dbuf, b)
	if e != nil {
		return empty, e
	}
	dec, e := cryptox.DecryptAES(dbuf[:n], dsnKey)
	if e != nil {
		return empty, e
	}
	return dec, nil
}
