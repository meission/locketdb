## locketdb


### 项目介绍

> 1. 包装pure Go kv系统 统一接口
> 2. 使用方式调整,类似于"database/sql"


### 使用方式

```
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

        db, err := locketdb.NewDB("pebble_test", locketdb.Pebble, "./")
        if err != nil {
            fmt.Println(err)
            return
        }
        db.Set(key, value)
        db.Set([]byte("key1"), value)
        iter, _ := db.Iterator([]byte("key"), []byte("key1"))

        for ; iter.Valid(); iter.Next() {
            fmt.Printf("%s:%s\n", iter.Key(), iter.Value())
        }

        db.Close()

    }

    
```
