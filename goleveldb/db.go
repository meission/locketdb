package goleveldb

import (
	"fmt"
	"path/filepath"

	"github.com/meission/locketdb"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type goLevelDB struct {
	db *leveldb.DB
}

var _ locketdb.DB = (*goLevelDB)(nil)

func init() {
	locketdb.RegisterEngine(locketdb.GoLevelDB, NewDB)
}
func NewDB(name string, dir string) (locketdb.DB, error) {
	return NewDBWithOpts(name, dir, nil)
}

func NewDBWithOpts(name string, dir string, o *opt.Options) (*goLevelDB, error) {
	dbPath := filepath.Join(dir, name+".db")
	db, err := leveldb.OpenFile(dbPath, o)
	if err != nil {
		return nil, err
	}
	return &goLevelDB{db: db}, nil
}

// Get implements DB.
func (db *goLevelDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, locketdb.ErrKeyEmpty
	}
	res, err := db.db.Get(key, nil)
	if err == errors.ErrNotFound {
		return nil, nil
	}
	return res, err
}

// Has implements DB.
func (db *goLevelDB) Has(key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return bytes != nil, nil
}

// Set implements DB.
func (db *goLevelDB) Set(key []byte, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return db.db.Put(key, value, nil)
}

// SetSync implements DB.
func (db *goLevelDB) SetSync(key []byte, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return db.db.Put(key, value, &opt.WriteOptions{Sync: true})
}

// Delete implements DB.
func (db *goLevelDB) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return db.db.Delete(key, nil)
}

// DeleteSync implements DB.
func (db *goLevelDB) DeleteSync(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return db.db.Delete(key, &opt.WriteOptions{Sync: true})
}

func (db *goLevelDB) DB() *leveldb.DB {
	return db.db
}

// Close implements DB.
func (db *goLevelDB) Close() error {
	return db.db.Close()
}

// Print implements DB.
func (db *goLevelDB) Print() error {
	str, err := db.db.GetProperty("leveldb.stats")
	if err != nil {
		return err
	}
	fmt.Printf("%v\n", str)

	itr := db.db.NewIterator(nil, nil)
	for itr.Next() {
		key := itr.Key()
		value := itr.Value()
		fmt.Printf("[%X]:\t[%X]\n", key, value)
	}
	return nil
}

// Stats implements DB.
func (db *goLevelDB) Stats() map[string]string {
	keys := []string{
		"leveldb.num-files-at-level{n}",
		"leveldb.stats",
		"leveldb.sstables",
		"leveldb.blockpool",
		"leveldb.cachedblock",
		"leveldb.openedtables",
		"leveldb.alivesnaps",
		"leveldb.aliveiters",
	}

	stats := make(map[string]string)
	for _, key := range keys {
		str, err := db.db.GetProperty(key)
		if err == nil {
			stats[key] = str
		}
	}
	return stats
}

// NewBatch implements DB.
func (db *goLevelDB) NewBatch() locketdb.Batch {
	return newGoLevelDBBatch(db)
}

// Iterator implements DB.
func (db *goLevelDB) Iterator(start, end []byte) (locketdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, locketdb.ErrKeyEmpty
	}
	itr := db.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	return newGoLevelDBIterator(itr, start, end, false), nil
}

// ReverseIterator implements DB.
func (db *goLevelDB) ReverseIterator(start, end []byte) (locketdb.Iterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, locketdb.ErrKeyEmpty
	}
	itr := db.db.NewIterator(&util.Range{Start: start, Limit: end}, nil)
	return newGoLevelDBIterator(itr, start, end, true), nil
}
