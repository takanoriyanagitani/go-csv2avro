package dec

import (
	"context"
	"encoding/csv"
	"io"
	"iter"
	"os"

	. "github.com/takanoriyanagitani/go-csv2avro/util"

	c "github.com/takanoriyanagitani/go-csv2avro/csv"
)

type CsvReader struct {
	rdr *csv.Reader
}

func (r CsvReader) Header() IO[[]string] {
	return func(_ context.Context) ([]string, error) {
		return r.rdr.Read()
	}
}

func (r CsvReader) Rows() IO[iter.Seq2[[]string, error]] {
	return func(_ context.Context) (iter.Seq2[[]string, error], error) {
		return func(yield func([]string, error) bool) {
			for {
				row, e := r.rdr.Read()
				if io.EOF == e {
					return
				}

				if !yield(row, e) {
					return
				}
			}
		}, nil
	}
}

func (r CsvReader) AsCsvSource() c.CsvSource { return r }

func CsvReaderNew(r io.Reader) CsvReader {
	return CsvReader{rdr: csv.NewReader(r)}
}

func CsvReaderStdin() IO[CsvReader] {
	return OfFn(func() CsvReader { return CsvReaderNew(os.Stdin) })
}
