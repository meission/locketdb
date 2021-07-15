package boltdb

import (
	"bytes"

	"github.com/meission/locketdb"
	"go.etcd.io/bbolt"
)

// boltDBIterator allows you to iterate on range of keys/values given some
// start / end keys (nil & nil will result in doing full scan).
type boltDBIterator struct {
	tx *bbolt.Tx

	iter  *bbolt.Cursor
	start []byte
	end   []byte

	currentKey   []byte
	currentValue []byte

	isInvalid bool
	isReverse bool
}

var _ locketdb.Iterator = (*boltDBIterator)(nil)

// newBoltDBIterator creates a new boltDBIterator.
func newBoltDBIterator(tx *bbolt.Tx, start, end []byte, isReverse bool) *boltDBIterator {
	iter := tx.Bucket(bucket).Cursor()

	var ck, cv []byte
	if isReverse {
		switch {
		case end == nil:
			ck, cv = iter.Last()
		default:
			_, _ = iter.Seek(end) // after key
			ck, cv = iter.Prev()  // return to end key
		}
	} else {
		switch {
		case start == nil:
			ck, cv = iter.First()
		default:
			ck, cv = iter.Seek(start)
		}
	}

	return &boltDBIterator{
		tx:           tx,
		iter:         iter,
		start:        start,
		end:          end,
		currentKey:   ck,
		currentValue: cv,
		isReverse:    isReverse,
		isInvalid:    false,
	}
}

// Domain implements Iterator.
func (iter *boltDBIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid implements Iterator.
func (iter *boltDBIterator) Valid() bool {
	if iter.isInvalid {
		return false
	}

	if iter.Error() != nil {
		iter.isInvalid = true
		return false
	}

	// iterated to the end of the cursor
	if iter.currentKey == nil {
		iter.isInvalid = true
		return false
	}

	if iter.isReverse {
		if iter.start != nil && bytes.Compare(iter.currentKey, iter.start) < 0 {
			iter.isInvalid = true
			return false
		}
	} else {
		if iter.end != nil && bytes.Compare(iter.end, iter.currentKey) <= 0 {
			iter.isInvalid = true
			return false
		}
	}
	return true
}

// Next implements Iterator.
func (iter *boltDBIterator) Next() {
	iter.assertIsValid()
	if iter.isReverse {
		iter.currentKey, iter.currentValue = iter.iter.Prev()
	} else {
		iter.currentKey, iter.currentValue = iter.iter.Next()
	}
}

// Key implements Iterator.
func (iter *boltDBIterator) Key() []byte {
	iter.assertIsValid()
	return append([]byte{}, iter.currentKey...)
}

// Value implements Iterator.
func (iter *boltDBIterator) Value() []byte {
	iter.assertIsValid()
	var value []byte
	if iter.currentValue != nil {
		value = append([]byte{}, iter.currentValue...)
	}
	return value
}

// Error implements Iterator.
func (iter *boltDBIterator) Error() error {
	return nil
}

// Close implements Iterator.
func (iter *boltDBIterator) Close() error {
	return iter.tx.Rollback()
}

func (iter *boltDBIterator) assertIsValid() {
	if !iter.Valid() {
		panic("iterator is invalid")
	}
}
