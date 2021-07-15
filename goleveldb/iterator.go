package goleveldb

import (
	"bytes"

	"github.com/meission/locketdb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type goLevelDBIterator struct {
	iter      iterator.Iterator
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
}

var _ locketdb.Iterator = (*goLevelDBIterator)(nil)

func newGoLevelDBIterator(iter iterator.Iterator, start, end []byte, isReverse bool) *goLevelDBIterator {
	if isReverse {
		if end == nil {
			iter.Last()
		} else {
			valid := iter.Seek(end)
			if valid {
				eoakey := iter.Key() // end or after key
				if bytes.Compare(end, eoakey) <= 0 {
					iter.Prev()
				}
			} else {
				iter.Last()
			}
		}
	} else {
		if start == nil {
			iter.First()
		} else {
			iter.Seek(start)
		}
	}
	return &goLevelDBIterator{
		iter:      iter,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (iter *goLevelDBIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid implements Iterator.
func (iter *goLevelDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	// If source errors, invalid.
	if err := iter.Error(); err != nil {
		iter.isInvalid = true
		return false
	}

	// If source is invalid, invalid.
	if !iter.iter.Valid() {
		iter.isInvalid = true
		return false
	}

	key := iter.iter.Key()
	if iter.isReverse {
		start := iter.start
		if start != nil && bytes.Compare(key, start) < 0 {
			iter.isInvalid = true
			return false
		}
	} else {
		end := iter.end
		if end != nil && bytes.Compare(end, key) <= 0 {
			iter.isInvalid = true
			return false
		}
	}
	return true
}

// Key implements Iterator.
func (iter *goLevelDBIterator) Key() []byte {
	iter.assertIsValid()
	return cp(iter.iter.Key())
}

// Value implements Iterator.
func (iter *goLevelDBIterator) Value() []byte {
	iter.assertIsValid()
	return cp(iter.iter.Value())
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Next implements Iterator.
func (iter *goLevelDBIterator) Next() {
	iter.assertIsValid()
	if iter.isReverse {
		iter.iter.Prev()
	} else {
		iter.iter.Next()
	}
}

// Error implements Iterator.
func (iter *goLevelDBIterator) Error() error {
	return iter.iter.Error()
}

// Close implements Iterator.
func (iter *goLevelDBIterator) Close() error {
	iter.iter.Release()
	return nil
}

func (iter goLevelDBIterator) assertIsValid() {
	if !iter.Valid() {
		panic("iterator is invalid")
	}
}
