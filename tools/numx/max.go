package numx

const (
	MaxUint8  = ^uint8(0)
	MaxUint16 = ^uint16(0)
	MaxUint32 = ^uint32(0)
	MaxUint   = ^uint(0)
	MaxUint64 = ^uint64(0)
	MaxInt8   = int8(MaxUint8 >> 1)
	MaxInt16  = int16(MaxUint16 >> 1)
	MaxInt32  = int32(MaxUint32 >> 1)
	MaxInt    = int(MaxUint >> 1)
	MaxInt64  = int64(MaxUint64 >> 1)
	MinUint8  = 0
	MinUint16 = 0
	MinUint32 = 0
	MinUint   = 0
	MinUint64 = 0
	MinInt8   = -MaxInt8 - 1
	MinInt16  = -MaxInt16 - 1
	MinInt32  = -MaxInt32 - 1
	MinInt    = -MaxInt - 1
	MinInt64  = -MaxInt64 - 1
)

const (
	MaxTimeUnix = 2051222400 //time.Date(2180, 1, 1, 0, 0, 0, 0, time.UTC)
	MinTimeUnix = 0          // time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)
