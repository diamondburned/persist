package persist

import (
	"fmt"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

// CBORDriver is a driver that stores data in a CBOR file.
var CBORDriver DriverOpenFunc = openCBORDriver

type cborDriver struct {
	path string
	mu   sync.RWMutex
	m    map[cbor.ByteString]cbor.RawMessage
}

func openCBORDriver(path string) (Driver, error) {
	d := &cborDriver{
		path: path,
		m:    make(map[cbor.ByteString]cbor.RawMessage),
	}

	f, err := os.Open(d.path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("persist: read file: %w", err)
		}

		if err := d.AcquireRW(func(DriverReadWriteTx) error { return nil }); err != nil {
			return nil, err
		}
	} else {
		defer f.Close()

		if err := cbor.NewDecoder(f).Decode(&d.m); err != nil {
			return nil, fmt.Errorf("persist: decode CBOR: %w", err)
		}

		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("persist: close file: %w", err)
		}
	}

	return d, nil
}

func (d *cborDriver) Close() error { return nil }

func (d *cborDriver) AcquireRO(f func(DriverReadOnlyTx) error) error {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return f(d)
}

func (d *cborDriver) AcquireRW(f func(DriverReadWriteTx) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if err := f(d); err != nil {
		return err
	}

	b, err := cbor.Marshal(d.m)
	if err != nil {
		return fmt.Errorf("persist: marshal CBOR: %w", err)
	}

	if err := os.WriteFile(d.path, b, 0666); err != nil {
		return fmt.Errorf("persist: write file: %w", err)
	}

	return nil
}

func (d *cborDriver) Get(k []byte) ([]byte, bool, error) {
	v, ok := d.m[cbor.ByteString(k)]
	return v, ok, nil
}

func (d *cborDriver) Each(f func(k, v []byte) error) error {
	for k, v := range d.m {
		if err := f([]byte(k), v); err != nil {
			return err
		}
	}
	return nil
}

func (d *cborDriver) EachKey(f func(k []byte) error) error {
	for k := range d.m {
		if err := f([]byte(k)); err != nil {
			return err
		}
	}
	return nil
}

func (d *cborDriver) Set(k, v []byte) error {
	d.m[cbor.ByteString(k)] = v
	return nil
}

func (d *cborDriver) Delete(k []byte) error {
	delete(d.m, cbor.ByteString(k))
	return nil
}
