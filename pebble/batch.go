package pebble

import (
	"github.com/cockroachdb/pebble"
	"github.com/meission/locketdb"
)

type pebbleDBBatch struct {
	db    *pebbleDB
	batch *pebble.Batch
}

var _ locketdb.Batch = (*pebbleDBBatch)(nil)

func newPebbleDBBatch(db *pebbleDB) *pebbleDBBatch {
	return &pebbleDBBatch{
		db:    db,
		batch: new(pebble.Batch),
	}
}

// Set implements Batch.
func (b *pebbleDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	if b.batch == nil {
		return locketdb.ErrBatchClosed
	}
	b.batch.Set(key, value, nil)
	return nil
}

// Delete implements Batch.
func (b *pebbleDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if b.batch == nil {
		return locketdb.ErrBatchClosed
	}
	b.batch.Delete(key, nil)
	return nil
}

// Write implements Batch.
func (b *pebbleDBBatch) Write() error {
	return b.write(false)
}

// WriteSync implements Batch.
func (b *pebbleDBBatch) WriteSync() error {
	return b.write(true)
}

func (b *pebbleDBBatch) write(sync bool) error {
	if b.batch == nil {
		return locketdb.ErrBatchClosed
	}
	err := b.db.db.Flush()
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// Close implements Batch.
func (b *pebbleDBBatch) Close() error {
	if b.batch != nil {
		b.batch.Reset()
		b.batch = nil
	}
	return nil
}
