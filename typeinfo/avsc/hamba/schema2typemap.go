package avsc2type

import (
	"iter"
	"log"
	"maps"

	ha "github.com/hamba/avro/v2"

	ca "github.com/takanoriyanagitani/go-csv2avro"
)

func LogicalTypeToPrimitiveType(l ha.LogicalType) ca.PrimitiveType {
	switch l {
	case ha.UUID:
		return ca.PrimitiveUuid
	case ha.TimestampMicros:
		return ca.PrimitiveTimestamp
	default:
		return ca.PrimitiveUnspecified
	}
}

func LogicalSchemaToPrimitiveType(l ha.LogicalSchema) ca.PrimitiveType {
	return LogicalTypeToPrimitiveType(l.Type())
}

func PrimitiveSchemaToPrimitiveType(p *ha.PrimitiveSchema) ca.PrimitiveType {
	var ls ha.LogicalSchema = p.Logical()
	if nil != ls {
		return LogicalSchemaToPrimitiveType(ls)
	}

	var typ ha.Type = p.Type()

	switch typ {
	case ha.String:
		return ca.PrimitiveString
	case ha.Int:
		return ca.PrimitiveInt
	case ha.Long:
		return ca.PrimitiveLong
	case ha.Float:
		return ca.PrimitiveFloat
	case ha.Double:
		return ca.PrimitiveDouble
	case ha.Boolean:
		return ca.PrimitiveBoolean
	default:
		return ca.PrimitiveUnspecified
	}
}

func FixedSchemaToPrimitiveType(f *ha.FixedSchema) ca.PrimitiveType {
	var l ha.LogicalSchema = f.Logical()
	if nil != l {
		return LogicalSchemaToPrimitiveType(l)
	}

	if "uuid" == f.Name() && 16 == f.Size() {
		return ca.PrimitiveUuid
	}

	return ca.PrimitiveUnspecified
}

func SchemaToPrimitiveType(s ha.Schema) ca.PrimitiveType {
	switch t := s.(type) {
	case *ha.PrimitiveSchema:
		return PrimitiveSchemaToPrimitiveType(t)
	case *ha.FixedSchema:
		return FixedSchemaToPrimitiveType(t)
	default:
		return ca.PrimitiveUnspecified
	}
}

func FieldsToTypeMap(fields []*ha.Field) map[string]ca.PrimitiveType {
	var i iter.Seq2[string, ca.PrimitiveType] = func(
		yield func(string, ca.PrimitiveType) bool,
	) {
		for _, field := range fields {
			var name string = field.Name()
			var s ha.Schema = field.Type()
			var typ ca.PrimitiveType = SchemaToPrimitiveType(s)
			yield(name, typ)
		}
	}
	return maps.Collect(i)
}

func RecordSchemaToTypeMap(r *ha.RecordSchema) map[string]ca.PrimitiveType {
	return FieldsToTypeMap(r.Fields())
}

func SchemaToTypeMapHamba(s ha.Schema) map[string]ca.PrimitiveType {
	switch t := s.(type) {
	case *ha.RecordSchema:
		return RecordSchemaToTypeMap(t)
	default:
		return map[string]ca.PrimitiveType{}
	}
}

func SchemaToTypeMap(schema string) map[string]ca.PrimitiveType {
	parsed, e := ha.Parse(schema)
	switch e {
	case nil:
		return SchemaToTypeMapHamba(parsed)
	default:
		log.Printf("invalid schema: %v\n", e)
		return map[string]ca.PrimitiveType{}
	}
}
