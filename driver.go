package persist

import (
	"errors"
	"io"
)

// driverStopIteration is returned by an iterator to stop the driver from
// iterating.
var driverStopIteration = errors.New("stop iteration")

// DriverOpenFunc is a function that opens a driver.
// It assumes that drivers already have a sane default configuration, so the
// user is not provided with any configuration options.
//
// There is one exception: if the path exactly matches the string ":memory:",
// then the driver must be non-persistent. If the driver is unable to satisfy
// this requirement, it must return an error.
type DriverOpenFunc func(path string) (Driver, error)

// Driver is a driver for a persistent map. It exposes the ability to acquire
// read-only and read-write transactions. Note that transactions are assumed to
// have the properties of a database transaction, i.e. they are atomic and
// isolated.
type Driver interface {
	io.Closer
	AcquireRO(func(DriverReadOnlyTx) error) error
	AcquireRW(func(DriverReadWriteTx) error) error
}

// DriverReadOnlyTx is a read-only transaction.
type DriverReadOnlyTx interface {
	Get(k []byte) ([]byte, bool, error)
	Each(func(k, v []byte) error) error
	EachKey(func(k []byte) error) error
}

// DriverReadWriteTx is a read-write transaction.
type DriverReadWriteTx interface {
	DriverReadOnlyTx
	Set(k, v []byte) error
	Delete(k []byte) error
}
