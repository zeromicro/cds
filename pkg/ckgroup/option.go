package ckgroup

import (
	"golang.org/x/time/rate"
)

type option struct {
	RetryNum           int
	GroupInsertLimiter *rate.Limiter
}
type OptionFunc func(*option)

func newOptions(opts ...OptionFunc) option {
	opt := option{
		RetryNum:           1,
		GroupInsertLimiter: rate.NewLimiter(rate.Inf, 0),
	}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

func WithRetryNum(retryNum int) OptionFunc {
	return func(o *option) {
		o.RetryNum = retryNum
	}
}

func WithGroupInsertLimiter(limit rate.Limit, burst int) OptionFunc {
	return func(o *option) {
		o.GroupInsertLimiter = rate.NewLimiter(limit, burst)
	}
}
