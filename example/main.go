package main

import (
	"fmt"

	"github.com/meission/locketdb"
	_ "github.com/meission/locketdb/badgerdb"
	_ "github.com/meission/locketdb/boltdb"
	_ "github.com/meission/locketdb/goleveldb"
	_ "github.com/meission/locketdb/pebble"
)

func main() {
	key := []byte("key")
	value := []byte("value:hello world")

	// db, err := locketdb.NewDB("boltdb_test", locketdb.BoltDB, "./")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// db.Set(key, value)
	// // db.Print()
	// db.Close()

	// db, err = locketdb.NewDB("badgerdb_test", locketdb.BadgerDB, "./")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// db.Set(key, value)
	// // db.Print()
	// db.Close()

	// db, err = locketdb.NewDB("goleveldb_test", locketdb.GoLevelDB, "./")
	// if err != nil {
	// 	fmt.Println(err)
	// 	return
	// }
	// db.Set(key, value)
	// db.Stats()
	// db.Close()

	db, err := locketdb.NewDB("pebble_test", locketdb.Pebble, "./")
	if err != nil {
		fmt.Println(err)
		return
	}
	db.Set(key, value)
	db.Set([]byte("key1"), value)
	db.Set([]byte("mmmm"), value)
	db.Stats()
	iter, _ := db.Iterator([]byte("key"), []byte("mmmmm"))

	for ; iter.Valid(); iter.Next() {
		fmt.Printf("%s:%s\n", iter.Key(), iter.Value())
	}

	db.Close()

}
