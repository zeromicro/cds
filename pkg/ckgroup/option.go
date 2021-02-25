package ckgroup

type option struct {
	RetryNum int
}
type OptionFunc func(*option)

func newOptions(opts ...OptionFunc) option {
	opt := option{
		RetryNum: 1,
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
