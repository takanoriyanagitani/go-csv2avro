package csv2avro

import (
	"strconv"
)

var StringToBoolean func(string) (bool, error) = strconv.ParseBool
