package csv2avro

import (
	gu "github.com/google/uuid"
)

var StringToUuid func(string) (gu.UUID, error) = gu.Parse
