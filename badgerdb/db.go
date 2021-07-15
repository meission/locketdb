package badgerdb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/dgraph-io/badger/v3"
	"github.com/meission/locketdb"
)

type badgerDB struct {
	db *badger.DB
}

var _ locketdb.DB = (*badgerDB)(nil)

func init() {
	locketdb.RegisterEngine(locketdb.BadgerDB, NewDB)
}

// NewDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, it will be created.
func NewDB(dbName, dir string) (locketdb.DB, error) {
	// Since Badger doesn't support database names, we join both to obtain
	// the final directory to use for the database.
	path := filepath.Join(dir, dbName)

	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(path)
	opts.SyncWrites = false // note that we have Sync methods
	opts.Logger = nil       // badger is too chatty by default
	return NewDBWithOptions(opts)
}

// NewDBWithOptions creates a BadgerDB key value store
// gives the flexibility of initializing a database with the
// respective options.
func NewDBWithOptions(opts badger.Options) (*badgerDB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &badgerDB{db: db}, nil
}

func (b *badgerDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, locketdb.ErrKeyEmpty
	}
	var val []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err == nil && val == nil {
			val = []byte{}
		}
		return err
	})
	return val, err
}

func (b *badgerDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, locketdb.ErrKeyEmpty
	}
	var found bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil && err != badger.ErrKeyNotFound {
			return err
		}
		found = (err != badger.ErrKeyNotFound)
		return nil
	})
	return found, err
}

func (b *badgerDB) Set(key, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func withSync(db *badger.DB, err error) error {
	if err != nil {
		return err
	}
	return db.Sync()
}

func (b *badgerDB) SetSync(key, value []byte) error {
	return withSync(b.db, b.Set(key, value))
}

func (b *badgerDB) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func (b *badgerDB) DeleteSync(key []byte) error {
	return withSync(b.db, b.Delete(key))
}

func (b *badgerDB) Close() error {
	return b.db.Close()
}

func (b *badgerDB) Print() error {
	return nil
}

func (b *badgerDB) iteratorOpts(start, end []byte, opts badger.IteratorOptions) (*badgerDBIterator, error) {
	if (start != nil && len(start) == 0) || (end != nil && len(end) == 0) {
		return nil, locketdb.ErrKeyEmpty
	}
	txn := b.db.NewTransaction(false)
	iter := txn.NewIterator(opts)
	iter.Rewind()
	iter.Seek(start)
	if opts.Reverse && iter.Valid() && bytes.Equal(iter.Item().Key(), start) {
		// If we're going in reverse, our starting point was "end",
		// which is exclusive.
		iter.Next()
	}
	return &badgerDBIterator{
		reverse: opts.Reverse,
		start:   start,
		end:     end,

		txn:  txn,
		iter: iter,
	}, nil
}

func (b *badgerDB) Iterator(start, end []byte) (locketdb.Iterator, error) {
	opts := badger.DefaultIteratorOptions
	return b.iteratorOpts(start, end, opts)
}

func (b *badgerDB) ReverseIterator(start, end []byte) (locketdb.Iterator, error) {
	opts := badger.DefaultIteratorOptions
	opts.Reverse = true
	return b.iteratorOpts(end, start, opts)
}

func (b *badgerDB) Stats() map[string]string {
	return nil
}

func (b *badgerDB) NewBatch() locketdb.Batch {
	wb := &badgerDBBatch{
		db:         b.db,
		wb:         b.db.NewWriteBatch(),
		firstFlush: make(chan struct{}, 1),
	}
	wb.firstFlush <- struct{}{}
	return wb
}

var _ locketdb.Batch = (*badgerDBBatch)(nil)

type badgerDBBatch struct {
	db *badger.DB
	wb *badger.WriteBatch

	// Calling db.Flush twice panics, so we must keep track of whether we've
	// flushed already on our own. If Write can receive from the firstFlush
	// channel, then it's the first and only Flush call we should do.
	//
	// Upstream bug report:
	// https://github.com/dgraph-io/badger/issues/1394
	firstFlush chan struct{}
}

func (b *badgerDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	return b.wb.Set(key, value)
}

func (b *badgerDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	return b.wb.Delete(key)
}

func (b *badgerDBBatch) Write() error {
	select {
	case <-b.firstFlush:
		return b.wb.Flush()
	default:
		return fmt.Errorf("batch already flushed")
	}
}

func (b *badgerDBBatch) WriteSync() error {
	return withSync(b.db, b.Write())
}

func (b *badgerDBBatch) Close() error {
	select {
	case <-b.firstFlush: // a Flush after Cancel panics too
	default:
	}
	b.wb.Cancel()
	return nil
}

type badgerDBIterator struct {
	reverse    bool
	start, end []byte

	txn  *badger.Txn
	iter *badger.Iterator

	lastErr error
}

func (i *badgerDBIterator) Close() error {
	i.iter.Close()
	i.txn.Discard()
	return nil
}

func (i *badgerDBIterator) Domain() (start, end []byte) { return i.start, i.end }
func (i *badgerDBIterator) Error() error                { return i.lastErr }

func (i *badgerDBIterator) Next() {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	i.iter.Next()
}

func (i *badgerDBIterator) Valid() bool {
	if !i.iter.Valid() {
		return false
	}
	if len(i.end) > 0 {
		key := i.iter.Item().Key()
		if c := bytes.Compare(key, i.end); (!i.reverse && c >= 0) || (i.reverse && c < 0) {
			// We're at the end key, or past the end.
			return false
		}
	}
	return true
}

func (i *badgerDBIterator) Key() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	// Note that we don't use KeyCopy, so this is only valid until the next
	// call to Next.
	return i.iter.Item().KeyCopy(nil)
}

func (i *badgerDBIterator) Value() []byte {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	val, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		i.lastErr = err
	}
	return val
}
