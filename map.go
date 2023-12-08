package persist

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when a key is not found.
var ErrNotFound = errors.New("not found")

// Seq2 is an iterator over a map that yields key-value pairs.
// It is inspired by https://github.com/golang/go/issues/61897.
type Seq2[K, V any] func(yield func(K, V) bool) bool

// Seq is an iterator over a map that yields values.
// It is inspired by https://github.com/golang/go/issues/61897.
type Seq[V any] func(yield func(V) bool) bool

// EncoderPair is a pair of encoders.
type EncoderPair[K, V any] struct {
	Key   Encoder[K]
	Value Encoder[V]
}

// Map is a type-safe map that persists to disk.
type Map[K, V any] struct {
	driver   Driver
	kencoder Encoder[K]
	vencoder Encoder[V]
}

// NewMap returns a new Map using the default CBOR encoder and a provided
// driver with sane defaults.
func NewMap[K, V any](driverOpener DriverOpenFunc, path string) (Map[K, V], error) {
	driver, err := driverOpener(path)
	if err != nil {
		return Map[K, V]{}, err
	}
	return Map[K, V]{
		driver:   driver,
		kencoder: CBOREncoder[K](),
		vencoder: CBOREncoder[V](),
	}, nil
}

// NewMapFromEncoders returns a new Map from a pair of encoders.
func NewMapFromEncoders[K, V any](driver Driver, encs EncoderPair[K, V]) *Map[K, V] {
	return &Map[K, V]{
		driver:   driver,
		kencoder: encs.Key,
		vencoder: encs.Value,
	}
}

// Encoder returns the encoder pair used by the map.
func (m Map[K, V]) Encoder() EncoderPair[K, V] {
	return EncoderPair[K, V]{
		Key:   m.kencoder,
		Value: m.vencoder,
	}
}

// Set sets a key-value pair.
func (m Map[K, V]) Set(k K, v V) error {
	bk, err := m.kencoder.Encode(k, nil)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	bv, err := m.vencoder.Encode(v, nil)
	if err != nil {
		return fmt.Errorf("encode value: %w", err)
	}

	return m.driver.AcquireRW(func(tx DriverReadWriteTx) error {
		return tx.Set(bk, bv)
	})
}

// Get gets a value by key.
func (m Map[K, V]) Get(k K) (V, error) {
	var v V

	bk, err := m.kencoder.Encode(k, nil)
	if err != nil {
		return v, fmt.Errorf("encode key: %w", err)
	}

	err = m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
		bv, err := tx.Get(bk)
		if err != nil {
			return fmt.Errorf("get value: %w", err)
		}

		v, err = m.vencoder.Decode(bv)
		if err != nil {
			return fmt.Errorf("decode value: %w", err)
		}

		return nil
	})
	return v, err
}

// Delete deletes a key-value pair.
func (m Map[K, V]) Delete(k K) error {
	bk, err := m.kencoder.Encode(k, nil)
	if err != nil {
		return fmt.Errorf("encode key: %w", err)
	}

	return m.driver.AcquireRW(func(tx DriverReadWriteTx) error {
		return tx.Delete(bk)
	})
}

// Close closes the map. The user must call this function to ensure that the
// map is properly closed.
func (m Map[K, V]) Close() error {
	return m.driver.Close()
}

// All returns an iterator over all key-value pairs in the map.
func (m Map[K, V]) All() Seq2[K, V] {
	return func(yield func(K, V) bool) bool {
		err := m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
			return tx.Each(func(bk, bv []byte) error {
				k, err := m.kencoder.Decode(bk)
				if err != nil {
					return fmt.Errorf("decode key: %w", err)
				}
				v, err := m.vencoder.Decode(bv)
				if err != nil {
					return fmt.Errorf("decode value: %w", err)
				}
				if !yield(k, v) {
					return driverStopIteration
				}
				return nil
			})
		})
		if err != nil && !errors.Is(err, driverStopIteration) {
			return false
		}
		return true
	}
}

// Keys returns an iterator over all keys in the map.
func (m Map[K, V]) Keys() Seq[K] {
	return func(yield func(K) bool) bool {
		err := m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
			return tx.EachKey(func(bk []byte) error {
				k, err := m.kencoder.Decode(bk)
				if err != nil {
					return fmt.Errorf("decode key: %w", err)
				}
				if !yield(k) {
					return driverStopIteration
				}
				return nil
			})
		})
		if err != nil && !errors.Is(err, driverStopIteration) {
			return false
		}
		return true
	}
}
