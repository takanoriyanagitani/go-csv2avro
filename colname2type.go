package csv2avro

import (
	"errors"
	"fmt"
	"iter"
	"maps"
)

var (
	ErrNoTypeInfo   error = errors.New("no type info found")
	ErrNoColumnInfo error = errors.New("no column info found")
)

type ColumnNameToType func(string) (PrimitiveType, error)

type ColumnNameToTypeMap map[string]PrimitiveType

func (m ColumnNameToTypeMap) ToConverter(
	onMissing func(string) error,
) ColumnNameToType {
	return MapToConverter(onMissing, m)
}

func (m ColumnNameToTypeMap) ToConverterDefault() ColumnNameToType {
	return m.ToConverter(func(key string) error {
		return fmt.Errorf("%w: %s", ErrNoTypeInfo, key)
	})
}

type CsvHeader []string
type CsvRows iter.Seq2[[]string, error]

func (r CsvRows) ToMaps(
	ix2name ColumnIndexToColumnName,
	ix2type IndexToType,
) iter.Seq2[map[string]any, error] {
	return func(yield func(map[string]any, error) bool) {
		buf := map[string]any{}

		for raw, e := range r {
			clear(buf)

			if nil != e {
				yield(buf, e)
				return
			}

			for ix, s := range raw {
				var i int16 = int16(ix)

				typ, te := ix2type(i)
				if nil != te {
					yield(buf, te)
					return
				}

				var str2any StringToAny = TypeToConverter(typ)

				name, ne := ix2name(i)
				conv, ce := str2any(s)
				buf[name] = conv
				e := errors.Join(ne, ce)
				if nil != e {
					yield(buf, e)
					return
				}
			}

			if !yield(buf, nil) {
				return
			}
		}
	}
}

type ColumnIndexToColumnNameMap map[int16]string
type ColumnIndexToColumnName func(int16) (string, error)

type CsvInfo struct {
	Header []string
	Rows   iter.Seq2[[]string, error]
}

func (i CsvInfo) ToConverterDefault() ColumnIndexToColumnName {
	return CsvHeader(i.Header).ToConverterDefault()
}

func (h CsvHeader) ToMap() ColumnIndexToColumnNameMap {
	var pairs iter.Seq2[int16, string] = func(
		yield func(int16, string) bool,
	) {
		for ix, name := range h {
			yield(int16(ix), name)
		}
	}
	return maps.Collect(pairs)
}

func (h CsvHeader) ToConverter(
	onMissing func(int16) error,
) ColumnIndexToColumnName {
	return MapToConverter(
		onMissing,
		h.ToMap(),
	)
}

func (h CsvHeader) ToConverterDefault() ColumnIndexToColumnName {
	return h.ToConverter(
		func(key int16) error {
			return fmt.Errorf("%w: %v", ErrNoColumnInfo, key)
		},
	)
}

type Resolver struct {
	ColumnNameToType
	ColumnIndexToColumnName
}

func (r Resolver) ToIndexToType() IndexToType {
	return ComposeErr(
		r.ColumnIndexToColumnName,
		r.ColumnNameToType,
	)
}
