package persist

import (
	"fmt"
)

// Seq2 is an iterator over a map that yields key-value pairs.
// It is inspired by https://github.com/golang/go/issues/61897.
type Seq2[K, V any] func(yield func(K, V) bool)

// Seq is an iterator over a map that yields values.
// It is inspired by https://github.com/golang/go/issues/61897.
type Seq[V any] func(yield func(V) bool)

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

// Store sets a key-value pair.
func (m Map[K, V]) Store(k K, v V) error {
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

// Load gets a value by key.
func (m Map[K, V]) Load(k K) (V, bool, error) {
	var v V
	var ok bool

	bk, err := m.kencoder.Encode(k, nil)
	if err != nil {
		return v, false, fmt.Errorf("encode key: %w", err)
	}

	err = m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
		var bv []byte
		bv, ok, err = tx.Get(bk)
		if err != nil {
			return fmt.Errorf("get value: %w", err)
		}
		if ok {
			v, err = m.vencoder.Decode(bv)
			if err != nil {
				return fmt.Errorf("decode value: %w", err)
			}
		}

		return nil
	})
	return v, ok, err
}

// LoadOrStore gets a value by key, or stores a value if the key is not found.
func (m Map[K, V]) LoadOrStore(k K, v V) (value V, loaded bool, err error) {
	var bk []byte
	bk, err = m.kencoder.Encode(k, nil)
	if err != nil {
		return
	}

	err = m.driver.AcquireRW(func(tx DriverReadWriteTx) error {
		bv, ok, err := tx.Get(bk)
		if err != nil {
			return fmt.Errorf("get value: %w", err)
		}
		if !ok {
			bv, err := m.vencoder.Encode(v, nil)
			if err != nil {
				return fmt.Errorf("encode value: %w", err)
			}
			return tx.Set(bk, bv)
		}

		v, err = m.vencoder.Decode(bv)
		if err != nil {
			return fmt.Errorf("decode value: %w", err)
		}

		value = v
		loaded = true
		return nil
	})
	return
}

// LoadAndDelete gets a value by key, or deletes the key if it is not found.
func (m Map[K, V]) LoadAndDelete(k K) (v V, loaded bool, err error) {
	var bk []byte
	bk, err = m.kencoder.Encode(k, nil)
	if err != nil {
		return
	}

	err = m.driver.AcquireRW(func(tx DriverReadWriteTx) error {
		bv, ok, err := tx.Get(bk)
		if err != nil {
			return fmt.Errorf("get value: %w", err)
		}
		if !ok {
			return nil
		}
		loaded = true

		v, err = m.vencoder.Decode(bv)
		if err != nil {
			return fmt.Errorf("decode value: %w", err)
		}

		return tx.Delete(bk)
	})
	return
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
	return func(yield func(K, V) bool) {
		m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
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
	}
}

// Keys returns an iterator over all keys in the map.
func (m Map[K, V]) Keys() Seq[K] {
	return func(yield func(K) bool) {
		m.driver.AcquireRO(func(tx DriverReadOnlyTx) error {
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
	}
}
