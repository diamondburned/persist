package persist

const valueKey valueKeyT = 0

type valueKeyT = int

// Value is a type-safe value that persists to disk.
type Value[K, V any] struct {
	m Map[K, V]
	k K
}

// NewValue returns a new Value using the default CBOR encoder and a provided
// driver with sane defaults.
func NewValue[V any](driverOpener DriverOpenFunc, path string) (Value[valueKeyT, V], error) {
	m, err := NewMap[valueKeyT, V](driverOpener, path)
	if err != nil {
		return Value[valueKeyT, V]{}, err
	}
	return Value[valueKeyT, V]{m, valueKey}, nil
}

// NewValueFromMap returns a new Value using the provided map and key.
func NewValueFromMap[K, V any](m Map[K, V], key K) Value[K, V] {
	return Value[K, V]{m, key}
}

// Store sets the value.
func (m Value[K, V]) Store(value V) error {
	return m.m.Store(m.k, value)
}

// Load gets the value.
func (m Value[K, V]) Load() (V, bool, error) {
	return m.m.Load(m.k)
}

// LoadOrStore gets the value, or stores the value if it doesn't exist.
func (m Value[K, V]) LoadOrStore(value V) (actual V, loaded bool, err error) {
	return m.m.LoadOrStore(m.k, value)
}

// LoadAndDelete gets the value and deletes it.
func (m Value[K, V]) LoadAndDelete() (V, bool, error) {
	return m.m.LoadAndDelete(m.k)
}

// Delete deletes the value.
func (m Value[K, V]) Delete() error {
	return m.m.Delete(m.k)
}

// Close closes the map.
func (m Value[K, V]) Close() error {
	return m.m.Close()
}
