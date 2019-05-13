package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
var mega = 1024 * 1024
var seqWriteCount = 1000
var truncateToLength = 10

func main() {
	db := openDB()
	defer db.Close()
	rand.Seed(time.Now().UnixNano())

	args := os.Args
	key := args[1]

	switch key {
	case "write":
		num := args[2]
		testWriteThenRead(db, num)
	case "delete":
		testDelete(db)
	case "list":
		listDB(db)
	case "dw":
		num := args[2]
		testDelete(db)
		testWriteThenRead(db, num)
	case "ww":
		num := args[2]
		testWriteThenRead(db, num)
	default:
		getValueFromKey(db, key)
	}
}

func getKeys(db *leveldb.DB) []string {
	keys := []string{}
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		fmt.Printf("key: %s\n", string(iter.Key()))
		keys = append(keys, string(iter.Key()))
	}
	return keys
}

func testDelete(db *leveldb.DB) {
	keys := getKeys(db)

	batch := new(leveldb.Batch)
	opts := new(opt.WriteOptions)
	opts.NoWriteMerge = true

	for _, k := range keys {
		batch.Delete([]byte(k))
	}
	fmt.Printf("finish list keys")

	fmt.Printf("start deleting...")
	db.Write(batch, opts)
	fmt.Printf("finish deleting...")
}

func compareByteSlice(a, b []byte) bool {
	if len(a) != len(b) {
		fmt.Printf("length different\n")
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func testData() [][]byte {
	arr := make([][]byte, seqWriteCount)
	for i := 0; i < seqWriteCount; i++ {
		str := randomString(1 * mega)
		arr[i] = []byte(str)
	}
	return arr
}

func testWriteThenRead(db *leveldb.DB, key string) {
	fmt.Printf("start random at %s\n", time.Now())
	fixture := testData()
	fmt.Printf("end random at %s\n", time.Now())

	batch := new(leveldb.Batch)
	opts := new(opt.WriteOptions)
	opts.NoWriteMerge = true

	val1, _ := db.Get([]byte(key), nil)
	printPartial(val1)

	fmt.Printf("first element:\n")
	printPartial(fixture[0])
	for i, b := range fixture {
		num := strings.Split(key, "-")[0]
		base, _ := strconv.Atoi(num)
		newKey := fmt.Sprintf("%d-long", base+i)
		batch.Put([]byte(newKey), b)
	}
	fmt.Printf("last element:\n")
	printPartial(fixture[len(fixture)-1])

	val2, _ := db.Get([]byte(key), nil)
	fmt.Printf("val2: \n")
	printPartial(val2)
	if compareByteSlice(val1, val2) {
		fmt.Printf("val2 same as val1\n")
	} else {
		fmt.Printf("val2 different from val1\n")
	}

	fmt.Printf("strat writing at %s\n", time.Now())
	_ = db.Write(batch, opts)
	fmt.Printf("end writing at %s\n", time.Now())

	batch.Reset()
}

func truncateByte(b []byte) []byte {
	if len(b) > truncateToLength {
		return b[:truncateToLength]
	}
	return b
}

func printPartial(b []byte) {
	size := len(b)
	if size > 5 {
		size = 5
	}
	for i := 0; i < size; i++ {
		fmt.Printf("%s ", string(b[i]))
	}
	fmt.Printf("\n")
}

func dumpBatch(batch *leveldb.Batch) {
	fmt.Printf("batch:\n")
	fmt.Printf("%v\n", batch.Dump())
}

func openDB() *leveldb.DB {
	db, _ := leveldb.OpenFile("./test_level_db", nil)
	return db
}

func listDB(db *leveldb.DB) {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		val := truncateByte(iter.Value())
		fmt.Printf("key: %s, val: %s\n", iter.Key(), val)
	}
}

func getValueFromKey(db *leveldb.DB, key string) {
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	ok := iter.Seek([]byte(key))
	if ok {
		printPartial(iter.Value())
	} else {
		fmt.Printf("key not exist")
	}
}

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
