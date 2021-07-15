package boltdb

import (
	"github.com/meission/locketdb"

	"go.etcd.io/bbolt"
)

type opType int

const (
	opTypeSet opType = iota + 1
	opTypeDelete
)

type operation struct {
	opType
	key   []byte
	value []byte
}

// boltDBBatch stores operations internally and dumps them to BoltDB on Write().
type boltDBBatch struct {
	db  *boltDB
	ops []operation
}

var _ locketdb.Batch = (*boltDBBatch)(nil)

func newBoltDBBatch(db *boltDB) *boltDBBatch {
	return &boltDBBatch{
		db:  db,
		ops: []operation{},
	}
}

// Set implements Batch.
func (b *boltDBBatch) Set(key, value []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if value == nil {
		return locketdb.ErrValueNil
	}
	if b.ops == nil {
		return locketdb.ErrBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeSet, key, value})
	return nil
}

// Delete implements Batch.
func (b *boltDBBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return locketdb.ErrKeyEmpty
	}
	if b.ops == nil {
		return locketdb.ErrBatchClosed
	}
	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
	return nil
}

// Write implements Batch.
func (b *boltDBBatch) Write() error {
	if b.ops == nil {
		return locketdb.ErrBatchClosed
	}
	err := b.db.db.Batch(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(bucket)
		for _, op := range b.ops {
			switch op.opType {
			case opTypeSet:
				if err := bkt.Put(op.key, op.value); err != nil {
					return err
				}
			case opTypeDelete:
				if err := bkt.Delete(op.key); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Make sure batch cannot be used afterwards. Callers should still call Close(), for errors.
	return b.Close()
}

// WriteSync implements Batch.
func (b *boltDBBatch) WriteSync() error {
	return b.Write()
}

// Close implements Batch.
func (b *boltDBBatch) Close() error {
	b.ops = nil
	return nil
}
