package numx

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func Rand63n(i int64) int64 {
	return rand.Int63n(i)
}

func Randn(i int) int {
	return rand.Intn(i)
}
