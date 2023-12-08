package badgerdb

import (
	"github.com/dgraph-io/badger/v4"
	"libdb.so/persist"
)

// Open opens a badger database and returns it as a driver.
func Open(path string) (persist.Driver, error) {
	var opts badger.Options
	if path == ":memory:" {
		opts = badger.DefaultOptions("").WithInMemory(true)
	} else {
		opts = badger.DefaultOptions(path)
	}

	// Quiet the logs unless it's really important.
	opts = opts.WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return NewDriver(db), nil
}

var _ persist.DriverOpenFunc = Open

// Driver is a driver for a persistent map.
type Driver struct {
	db *badger.DB
}

var _ persist.Driver = (*Driver)(nil)

// NewDriver returns a new Driver.
func NewDriver(db *badger.DB) *Driver {
	return &Driver{db: db}
}

func (d *Driver) Close() error {
	return d.db.Close()
}

func (d *Driver) AcquireRO(f func(persist.DriverReadOnlyTx) error) error {
	return d.db.View(func(tx *badger.Txn) error {
		return f(roTx{db: d.db, tx: tx})
	})
}

func (d *Driver) AcquireRW(f func(persist.DriverReadWriteTx) error) error {
	return d.db.Update(func(tx *badger.Txn) error {
		return f(rwTx{roTx{db: d.db, tx: tx}})
	})
}

type roTx struct {
	db *badger.DB
	tx *badger.Txn
}

var _ persist.DriverReadOnlyTx = roTx{}

func (tx roTx) Get(k []byte) ([]byte, error) {
	item, err := tx.tx.Get(k)
	if err != nil {
		return nil, err
	}
	return yoinkItemValue(item)
}

func (tx roTx) Each(f func(k, v []byte) error) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = true

	it := tx.tx.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		v, err := yoinkItemValue(item)
		if err != nil {
			return err
		}

		if err := f(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (tx roTx) EachKey(f func(k []byte) error) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := tx.tx.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()

		if err := f(k); err != nil {
			return err
		}
	}
	return nil
}

func yoinkItemValue(item *badger.Item) ([]byte, error) {
	// !!! SCARY !!! spoopy code
	var v []byte
	err := item.Value(func(val []byte) error {
		v = val
		return nil
	})
	return v, err
}

type rwTx struct {
	roTx
}

var _ persist.DriverReadWriteTx = rwTx{}

func (tx rwTx) Set(k, v []byte) error {
	return tx.tx.Set(k, v)
}

func (tx rwTx) Delete(k []byte) error {
	return tx.tx.Delete(k)
}
