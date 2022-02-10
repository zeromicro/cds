package util

import (
	"fmt"
	"log"
	"runtime/debug"

	"github.com/zeromicro/go-zero/core/logx"
)

// Recover 捕获 panic 并发送错误
func Recover(fs ...func()) {
	if err := recover(); err != nil {
		logx.Error(err)
		logx.Error(string(debug.Stack()))
		log.Println(string(debug.Stack()))
		if err != nil {
			fmt.Println(err)
			logx.Error(err)
		}
		for _, f := range fs {
			f()
		}
	}
}
