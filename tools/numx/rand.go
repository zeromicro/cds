package numx

import (
	"math/rand" // #nosec
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

func Rand63n(i int64) int64 {
	return rand.Int63n(i) // #nosec
}

func Randn(i int) int {
	return rand.Intn(i) // #nosec
}
