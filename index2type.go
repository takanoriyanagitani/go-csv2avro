package csv2avro

type IndexToType func(int16) (PrimitiveType, error)
