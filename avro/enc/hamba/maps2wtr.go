package val2avro

import (
	"context"
	"io"
	"iter"
	"os"

	ha "github.com/hamba/avro/v2"
	ho "github.com/hamba/avro/v2/ocf"

	. "github.com/takanoriyanagitani/go-csv2avro/util"

	ca "github.com/takanoriyanagitani/go-csv2avro"
)

var codecMap map[ca.Codec]ho.CodecName = map[ca.Codec]ho.CodecName{
	ca.CodecNull:    ho.Null,
	ca.CodecDeflate: ho.Deflate,
	ca.CodecSnappy:  ho.Snappy,
	ca.CodecZstd:    ho.ZStandard,
}

func CodecConv(c ca.Codec) ho.CodecName {
	mapd, found := codecMap[c]
	switch found {
	case true:
		return mapd
	default:
		return ho.Null
	}
}

func ConfigToOpts(cfg ca.OutputConfig) []ho.EncoderFunc {
	var blockLen int = cfg.BlockLength
	var codec ca.Codec = cfg.Codec
	var converted ho.CodecName = CodecConv(codec)
	return []ho.EncoderFunc{
		ho.WithBlockLength(blockLen),
		ho.WithCodec(converted),
	}
}

func MapsToWriterHamba(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	s ha.Schema,
	opts ...ho.EncoderFunc,
) error {
	enc, e := ho.NewEncoderWithSchema(
		s,
		w,
		opts...,
	)
	if nil != e {
		return e
	}
	defer enc.Close()

	for row, e := range m {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if nil != e {
			return e
		}

		e = enc.Encode(row)
		if nil != e {
			return e
		}

		e = enc.Flush()
		if nil != e {
			return e
		}
	}

	return enc.Flush()
}

func MapsToWriter(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	w io.Writer,
	schema string,
	cfg ca.OutputConfig,
) error {
	parsed, e := ha.Parse(schema)
	if nil != e {
		return e
	}
	var opts []ho.EncoderFunc = ConfigToOpts(cfg)
	return MapsToWriterHamba(
		ctx,
		m,
		w,
		parsed,
		opts...,
	)
}

func MapsToStdout(
	ctx context.Context,
	m iter.Seq2[map[string]any, error],
	schema string,
	cfg ca.OutputConfig,
) error {
	return MapsToWriter(ctx, m, os.Stdout, schema, cfg)
}

func ConfigToSchemaToMapsToStdout(
	cfg ca.OutputConfig,
) func(schema string) func(iter.Seq2[map[string]any, error]) IO[Void] {
	return func(schema string) func(iter.Seq2[map[string]any, error]) IO[Void] {
		return func(m iter.Seq2[map[string]any, error]) IO[Void] {
			return func(ctx context.Context) (Void, error) {
				return Empty, MapsToStdout(
					ctx,
					m,
					schema,
					cfg,
				)
			}
		}
	}
}

var SchemaToMapsToStdoutDefault func(
	string,
) func(
	iter.Seq2[map[string]any, error],
) IO[Void] = ConfigToSchemaToMapsToStdout(ca.OutputConfigDefault)
