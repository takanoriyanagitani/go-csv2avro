package csv2avro

import (
	"strconv"
)

func StringToDouble(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func StringToFloat(s string) (float32, error) {
	f, e := strconv.ParseFloat(s, 32)
	return float32(f), e
}
