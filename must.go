package persist

import "fmt"

// MustMap wraps a map and guarantees that no errors will be returned from
// the map's methods, with the exception of Get, which now returns a bool.
type MustMap[K comparable, V any] struct {
	Map[K, V]
}

// NewMustMap returns a new MustMap. It has the same exact signature as
// NewMap, and the user must still handle errors as they would with NewMap.
func NewMustMap[K ~string, V any](driverOpener DriverOpenFunc, path string) (MustMap[K, V], error) {
	m, err := NewMap[K, V](driverOpener, path)
	if err != nil {
		return MustMap[K, V]{}, err
	}
	return MustMap[K, V]{m}, nil
}

// Get returns the value associated with the key, or false if the key is not
// found.
func (m MustMap[K, V]) Get(key K) (V, bool) {
	v, err := m.Map.Get(key)
	return v, err == nil
}

// GetOr returns the value associated with the key, or the default value if
// the key is not found.
func (m MustMap[K, V]) GetOr(key K, def V) V {
	v, err := m.Map.Get(key)
	if err != nil {
		return def
	}
	return v
}

// Getz returns the value associated with the key, or the zero value if the
// key is not found.
func (m MustMap[K, V]) Getz(key K) V {
	v, _ := m.Map.Get(key)
	return v
}

// Set sets the value associated with the key. If an error occurs, the function
// panics.
func (m MustMap[K, V]) Set(key K, value V) {
	if err := m.Map.Set(key, value); err != nil {
		panic(fmt.Sprintf("MustMap cannot set: %v", err))
	}
}

// Delete deletes the key-value pair. If an error occurs, the function panics.
func (m MustMap[K, V]) Delete(key K) {
	if err := m.Map.Delete(key); err != nil {
		panic(fmt.Sprintf("MustMap cannot delete: %v", err))
	}
}
