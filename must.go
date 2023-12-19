package persist

import "fmt"

// MustMap wraps a map and guarantees that no errors will be returned from
// the map's methods, with the exception of Get, which now returns a bool.
type MustMap[K, V any] struct {
	Map[K, V]
}

// NewMustMap returns a new MustMap. It has the same exact signature as
// NewMap, and the user must still handle errors as they would with NewMap.
func NewMustMap[K, V any](driverOpener DriverOpenFunc, path string) (MustMap[K, V], error) {
	m, err := NewMap[K, V](driverOpener, path)
	if err != nil {
		return MustMap[K, V]{}, err
	}
	return MustMap[K, V]{m}, nil
}

// Load returns the value associated with the key, or false if the key is not
// found.
func (m MustMap[K, V]) Load(key K) (V, bool) {
	v, ok, err := m.Map.Load(key)
	if err != nil {
		panic(fmt.Sprintf("MustMap cannot load: %v", err))
	}
	return v, ok
}

// Store sets the value associated with the key. If an error occurs, the
// function panics.
func (m MustMap[K, V]) Store(key K, value V) {
	if err := m.Map.Store(key, value); err != nil {
		panic(fmt.Sprintf("MustMap cannot set: %v", err))
	}
}

// LoadAndDelete returns the value associated with the key, or false if the key
// is not found. If an error occurs, the function panics.
func (m MustMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, loaded, err := m.Map.LoadAndDelete(key)
	if err != nil {
		panic(fmt.Sprintf("MustMap cannot load and delete: %v", err))
	}
	return v, loaded
}

// LoadOrStore returns the existing value associated with the key if one exists,
// or stores and returns the given value. If an error occurs, the function
// panics.
func (m MustMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	v, loaded, err := m.Map.LoadOrStore(key, value)
	if err != nil {
		panic(fmt.Sprintf("MustMap cannot load or store: %v", err))
	}
	return v, loaded
}

// Delete deletes the key-value pair. If an error occurs, the function panics.
func (m MustMap[K, V]) Delete(key K) {
	if err := m.Map.Delete(key); err != nil {
		panic(fmt.Sprintf("MustMap cannot delete: %v", err))
	}
}
