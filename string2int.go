package csv2avro

import (
	"strconv"
)

func StringToLong(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func StringToInt(s string) (int32, error) {
	i, e := strconv.ParseInt(s, 10, 32)
	return int32(i), e
}
