package csv

import (
	"iter"

	. "github.com/takanoriyanagitani/go-csv2avro/util"
)

type CsvSource interface {
	Header() IO[[]string]
	Rows() IO[iter.Seq2[[]string, error]]
}
