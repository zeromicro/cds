package strx

import "math/rand" // #nosec

func RandomRune() rune {
	i := 'a' + rand.Intn(26) // #nosec
	return rune(i)
}

func RandomString(length int) string {
	s := ""
	for i := 0; i < length; i++ {
		s += string(RandomRune())
	}
	return s
}
