package persist

const valueKey valueKeyT = 0

type valueKeyT = int

// Value is a type-safe value that persists to disk.
type Value[V any] interface {
	// Store sets the value.
	Store(value V) error
	// Load gets the value.
	Load() (V, bool, error)
	// LoadOrStore gets the value, or stores the value if it doesn't exist.
	LoadOrStore(value V) (actual V, loaded bool, err error)
	// LoadAndDelete gets the value and deletes it.
	LoadAndDelete() (V, bool, error)
	// Delete deletes the value.
	Delete() error
	// Close closes the value.
	Close() error
}

// NewValue returns a new [Value] using the default CBOR encoder and a provided
// driver with sane defaults.
func NewValue[V any](driverOpener DriverOpenFunc, path string) (Value[V], error) {
	m, err := NewMap[valueKeyT, V](driverOpener, path)
	if err != nil {
		return nil, err
	}
	return mappedValue[valueKeyT, V]{m, valueKey}, nil
}

// NewMappedValue returns a new [Value] using the provided map and key.
func NewMappedValue[K, V any](m Map[K, V], key K) Value[V] {
	return mappedValue[K, V]{m, key}
}

// mappedValue is a type-safe value with a custom key that persists to disk.
type mappedValue[K, V any] struct {
	m Map[K, V]
	k K
}

// Store sets the value.
func (m mappedValue[K, V]) Store(value V) error {
	return m.m.Store(m.k, value)
}

// Load gets the value.
func (m mappedValue[K, V]) Load() (V, bool, error) {
	return m.m.Load(m.k)
}

// LoadOrStore gets the value, or stores the value if it doesn't exist.
func (m mappedValue[K, V]) LoadOrStore(value V) (actual V, loaded bool, err error) {
	return m.m.LoadOrStore(m.k, value)
}

// LoadAndDelete gets the value and deletes it.
func (m mappedValue[K, V]) LoadAndDelete() (V, bool, error) {
	return m.m.LoadAndDelete(m.k)
}

// Delete deletes the value.
func (m mappedValue[K, V]) Delete() error {
	return m.m.Delete(m.k)
}

// Close closes the map.
func (m mappedValue[K, V]) Close() error {
	return m.m.Close()
}
