package csv2avro

type PrimitiveType string

const (
	PrimitiveUnspecified PrimitiveType = "UNSPECIFIED"

	PrimitiveString  PrimitiveType = "string"
	PrimitiveInt     PrimitiveType = "int"
	PrimitiveLong    PrimitiveType = "long"
	PrimitiveFloat   PrimitiveType = "float"
	PrimitiveDouble  PrimitiveType = "double"
	PrimitiveBoolean PrimitiveType = "boolean"

	PrimitiveUuid      PrimitiveType = "uuid"
	PrimitiveTimestamp PrimitiveType = "timestamp-micros"
)

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

type OutputConfig struct {
	BlockLength int
	Codec
}

const BlockLengthDefault int = 100

var OutputConfigDefault OutputConfig = OutputConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
