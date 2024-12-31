package csv2avro

import (
	"errors"
)

var (
	ErrInvalidConv error = errors.New("invalid converter")
)

//go:generate go run ./internal/gen/str2any/main.go
//go:generate gofmt -s -w .
type StringToAny func(string) (any, error)
