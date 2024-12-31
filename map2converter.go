package csv2avro

func MapToConverter[K comparable, V any](
	onMissing func(K) error,
	m map[K]V,
) func(K) (V, error) {
	return func(key K) (V, error) {
		val, found := m[key]
		switch found {
		case true:
			return val, nil
		default:
			return val, onMissing(key)
		}
	}
}
