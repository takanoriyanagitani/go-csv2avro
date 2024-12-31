package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	ca "github.com/takanoriyanagitani/go-csv2avro"
	. "github.com/takanoriyanagitani/go-csv2avro/util"

	ac "github.com/takanoriyanagitani/go-csv2avro/csv"
	cs "github.com/takanoriyanagitani/go-csv2avro/csv/std"

	eh "github.com/takanoriyanagitani/go-csv2avro/avro/enc/hamba"
	ah "github.com/takanoriyanagitani/go-csv2avro/typeinfo/avsc/hamba"
)

var EnvVarByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var schemaFilename IO[string] = EnvVarByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return func(filename string) IO[string] {
		return func(_ context.Context) (string, error) {
			f, e := os.Open(filename)
			if nil != e {
				return "", e
			}
			defer f.Close()
			var buf strings.Builder
			limited := &io.LimitedReader{
				R: f,
				N: limit,
			}
			_, e = io.Copy(&buf, limited)
			return buf.String(), e
		}
	}
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var colname2typMap IO[map[string]ca.PrimitiveType] = Bind(
	schemaContent,
	Lift(func(s string) (map[string]ca.PrimitiveType, error) {
		return ah.SchemaToTypeMap(s), nil
	}),
)

var colname2typ IO[ca.ColumnNameToType] = Bind(
	colname2typMap,
	Lift(func(m map[string]ca.PrimitiveType) (ca.ColumnNameToType, error) {
		return ca.ColumnNameToTypeMap(m).ToConverterDefault(), nil
	}),
)

var csvReader IO[cs.CsvReader] = cs.CsvReaderStdin()
var csvSource IO[ac.CsvSource] = Bind(
	csvReader,
	Lift(func(r cs.CsvReader) (ac.CsvSource, error) {
		return r.AsCsvSource(), nil
	}),
)

var csvInfo IO[ca.CsvInfo] = Bind(
	csvSource,
	func(s ac.CsvSource) IO[ca.CsvInfo] {
		return func(ctx context.Context) (ca.CsvInfo, error) {
			var ret ca.CsvInfo

			hdr, e := s.Header()(ctx)
			if nil != e {
				return ret, e
			}

			rows, e := s.Rows()(ctx)
			return ca.CsvInfo{
				Header: hdr,
				Rows:   rows,
			}, e
		}
	},
)

type ConvRows struct {
	ca.ColumnIndexToColumnName
	Rows iter.Seq2[[]string, error]
}

var convRows IO[ConvRows] = Bind(
	csvInfo,
	Lift(func(i ca.CsvInfo) (ConvRows, error) {
		return ConvRows{
			ColumnIndexToColumnName: ca.CsvHeader(i.Header).
				ToConverterDefault(),
			Rows: i.Rows,
		}, nil
	}),
)

type ResolverRows struct {
	ca.ColumnIndexToColumnName
	ca.Resolver
	Rows iter.Seq2[[]string, error]
}

var resolverRows IO[ResolverRows] = Bind(
	convRows,
	func(cr ConvRows) IO[ResolverRows] {
		return Bind(
			colname2typ,
			Lift(func(c2t ca.ColumnNameToType) (ResolverRows, error) {
				return ResolverRows{
					ColumnIndexToColumnName: cr.ColumnIndexToColumnName,
					Resolver: ca.Resolver{
						ColumnNameToType:        c2t,
						ColumnIndexToColumnName: cr.ColumnIndexToColumnName,
					},
					Rows: cr.Rows,
				}, nil
			}),
		)
	},
)

type IxToTypeRows struct {
	ca.ColumnIndexToColumnName
	ca.IndexToType
	Rows iter.Seq2[[]string, error]
}

var ix2typRows IO[IxToTypeRows] = Bind(
	resolverRows,
	Lift(func(r ResolverRows) (IxToTypeRows, error) {
		return IxToTypeRows{
			ColumnIndexToColumnName: r.ColumnIndexToColumnName,
			IndexToType:             r.Resolver.ToIndexToType(),
			Rows:                    r.Rows,
		}, nil
	}),
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	ix2typRows,
	Lift(func(i IxToTypeRows) (iter.Seq2[map[string]any, error], error) {
		return ca.CsvRows(i.Rows).ToMaps(
			i.ColumnIndexToColumnName,
			i.IndexToType,
		), nil
	}),
)

var stdin2csv2raws2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(s string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(s),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	return stdin2csv2raws2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
