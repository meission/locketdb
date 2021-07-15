package pebble

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/pebble"
	"github.com/meission/locketdb"
)

type pebbleDB struct {
	db *pebble.DB
}

var _ locketdb.DB = (*pebbleDB)(nil)

func init() {
	locketdb.RegisterEngine(locketdb.Pebble, NewDB)
}

func NewDB(name string, dir string) (locketdb.DB, error) {
	return NewDBWithOpts(name, dir, nil)
}

func NewDBWithOpts(name string, dir string, o *pebble.Options) (*pebbleDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	db, err := pebble.Open(dbPath, o)
	if err != nil {
		return nil, err
	}
	return &pebbleDB{
		db: db,
	}, nil
}

// Get implements DB.
func (db *pebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, locketdb.ErrKeyEmpty
	}
	res, _, err := db.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	return res, err
}

// Has implements DB.
func (db *pebbleDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *pebbleDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return db.db.Set(key, value, nil)
}

// SetSync implements DB.
func (db *pebbleDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return db.db.Set(key, value, pebble.Sync)
}

// Delete implements DB.
func (db *pebbleDB) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return db.db.Delete(key, nil)
}

// DeleteSync implements DB.
func (db *pebbleDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return db.db.Delete(key, pebble.Sync)
}

// Close implements DB.
func (db *pebbleDB) Close() error {
	return db.db.Close()
}

// Print implements DB.
func (db *pebbleDB) Print() error {
	fmt.Printf("%v\n", db.db.Metrics().String())

	iter := db.db.NewIter(nil)
	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Printf("%s:%s\n", iter.Key(), iter.Value())
	}
	return nil
}

// Stats implements DB.
func (db *pebbleDB) Stats() map[string]string {
	return nil
}

// NewBatch implements DB.
func (db *pebbleDB) NewBatch() locketdb.Batch {
	return newPebbleDBBatch(db)
}

// Iterator implements DB.
func (db *pebbleDB) Iterator(start, end []byte) (locketdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, locketdb.ErrKeyEmpty
	}
	itr := db.db.NewIter(nil)
	return newpebbleDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *pebbleDB) ReverseIterator(start, end []byte) (locketdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, locketdb.ErrKeyEmpty
	}
	itr := db.db.NewIter(nil)
	return newpebbleDBIterator(itr, start, end, true), nil
}
