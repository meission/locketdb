package pebble

import (
	"bytes"

	"github.com/cockroachdb/pebble"
	"github.com/meission/locketdb"
)

type pebbleDBIterator struct {
	iter      *pebble.Iterator
	start     []byte
	end       []byte
	isReverse bool
	isInvalid bool
}

var _ locketdb.Iterator = (*pebbleDBIterator)(nil)

func newpebbleDBIterator(iter *pebble.Iterator, start, end []byte, isReverse bool) *pebbleDBIterator {
	if isReverse {
		if end == nil {
			iter.Last()
		} else {
			valid := iter.SeekGE(end)
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
			iter.SeekGE(start)
		}
	}
	return &pebbleDBIterator{
		iter:      iter,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

// Domain implements Iterator.
func (iter *pebbleDBIterator) Domain() ([]byte, []byte) {
	return iter.start, iter.end
}

// Valid implements Iterator.
func (iter *pebbleDBIterator) Valid() bool {

	// Once invalid, forever invalid.
	if iter.isInvalid {
		return false
	}

	// If iter errors, invalid.
	if err := iter.Error(); err != nil {
		iter.isInvalid = true
		return false
	}
	// If iter is invalid, invalid.
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
		if end != nil && bytes.Compare(end, key) < 0 {
			iter.isInvalid = true
			return false
		}
	}

	// Valid
	return true
}

// Key implements Iterator.
func (iter *pebbleDBIterator) Key() []byte {
	// Key returns a copy of the current key.
	iter.assertIsValid()
	return cp(iter.iter.Key())
}

// Value implements Iterator.
func (iter *pebbleDBIterator) Value() []byte {
	// Value returns a copy of the current value.
	iter.assertIsValid()
	return cp(iter.iter.Value())
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Next implements Iterator.
func (iter *pebbleDBIterator) Next() {
	iter.assertIsValid()
	if iter.isReverse {
		iter.iter.Prev()
	} else {
		iter.iter.Next()
	}
}

// Error implements Iterator.
func (iter *pebbleDBIterator) Error() error {
	return iter.iter.Error()
}

// Close implements Iterator.
func (iter *pebbleDBIterator) Close() error {
	iter.iter.Close()
	return nil
}

func (iter pebbleDBIterator) assertIsValid() {
	if !iter.Valid() {
		panic("iterator is invalid")
	}
}
