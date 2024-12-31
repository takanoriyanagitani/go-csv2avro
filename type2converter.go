package csv2avro

// This file is generated using str2any.tmpl. NEVER EDIT.

var StringToAnyInvalid StringToAny = func(
	_ string,
) (any, error) {
	return nil, ErrInvalidConv
}

func TypeToConverter(typ PrimitiveType) StringToAny {
	switch typ {

	case PrimitiveString:
		return func(s string) (any, error) {
			return StringToString(s)
		}

	case PrimitiveInt:
		return func(s string) (any, error) {
			return StringToInt(s)
		}

	case PrimitiveLong:
		return func(s string) (any, error) {
			return StringToLong(s)
		}

	case PrimitiveFloat:
		return func(s string) (any, error) {
			return StringToFloat(s)
		}

	case PrimitiveDouble:
		return func(s string) (any, error) {
			return StringToDouble(s)
		}

	case PrimitiveBoolean:
		return func(s string) (any, error) {
			return StringToBoolean(s)
		}

	case PrimitiveUuid:
		return func(s string) (any, error) {
			return StringToUuid(s)
		}

	case PrimitiveTimestamp:
		return func(s string) (any, error) {
			return StringToTimestamp3339(s)
		}

	default:
		return StringToAnyInvalid

	}
}
