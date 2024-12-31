package csv2avro

import (
	"time"
)

func StringToTimeNew(layout string) func(string) (time.Time, error) {
	return func(s string) (time.Time, error) {
		return time.Parse(layout, s)
	}
}

var StringToTimestamp3339 func(string) (time.Time, error) = StringToTimeNew(
	time.RFC3339Nano,
)
