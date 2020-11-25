package timex

import "time"

const (
	StandardLayout = "2006-01-02 15:04:05"
)

var (
	ShanghaiLocation = time.FixedZone("Asia/Shanghai", int((time.Hour * 8).Seconds()))
	ZeroTime         = time.Time{}
)
