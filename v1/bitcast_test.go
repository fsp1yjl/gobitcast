package v1

import (
	"fmt"
	"os"
	"testing"
)

func TestOpenDB(t *testing.T) {

	//go test  -v -timeout 30s gobitcast/v1 -run TestOpenDB

	//pre
	dbPath := "./v1db"
	os.RemoveAll(dbPath)

	b := OpenDB(dbPath)
	if b.name != dbPath {
		t.Error("new db test error")
	}

	if _, err := os.Stat(dbPath); err != nil {
		t.Error("open db test error")
	}

	//after
	os.RemoveAll(dbPath)

}

func TestLoadDataEmpty(t *testing.T) {
	//go test  -v -timeout 30s gobitcast/v1 -run TestLoadDataEmpty

	dbPath := "./v1db"
	os.RemoveAll(dbPath)

	b := OpenDB(dbPath)

	b.LoadData()

	// t.Log("success")
}

func TestLoadDataWithFiles(t *testing.T) {
	//go test  -v -timeout 30s gobitcast/v1 -run TestLoadDataWithFiles

	dbPath := "./v1db"
	os.RemoveAll(dbPath)

	b := OpenDB(dbPath)

	b.LoadData()

	// t.Log("success")
}

func TestPut(t *testing.T) {
	// go test  -v -timeout 30s gobitcast/v1 -run TestPut

	dbPath := "./v1db"
	os.RemoveAll(dbPath)
	b := OpenDB(dbPath)
	b.LoadData()

	err := b.Put("eric", "feng")
	if err != nil {
		t.Error("test put error")
	}
	os.RemoveAll(dbPath)

}

func TestGet(t *testing.T) {
	// go test  -v -timeout 30s gobitcast/v1 -run TestGet
	dbPath := "./v1db"
	os.RemoveAll(dbPath)
	b := OpenDB(dbPath)
	b.LoadData()

	err, _ := b.Get("eri")
	if err != ErrKeyNotExist {
		t.Error("get not exist key error")
	}

	b.Put("yjl", "hello world")

	err, value := b.Get("yjl")
	if err != nil {
		t.Error("get  exist key error", err)
	}
	if value != "hello world" {
		t.Error("value wrong")
	}

	os.RemoveAll(dbPath)

	// fmt.Println("STR2:", value)

}
func TestPutAndGet(t *testing.T) {
	// go test  -v -timeout 30s gobitcast/v1 -run TestPutAndGet
	dbPath := "./v1db"
	os.RemoveAll(dbPath)
	b := OpenDB(dbPath)
	b.LoadData()

	b.Put("eric", "feng")
	b.Put("eric1", "feng1")
	b.Put("eric", "ddddddddd")

	err, value := b.Get("eri")
	if err != ErrKeyNotExist {
		t.Error("get not exist key error")
	}

	err, value = b.Get("eric")
	if err != nil {
		t.Error("get  exist key error")
	}

	// if value != "feng" {
	// 	t.Error("get value of key error")
	// }
	fmt.Println("STR2:", value)

	os.RemoveAll(dbPath)
}

func TestDataLoad(t *testing.T) {
	// go test  -v -timeout 30s gobitcast/v1 -run TestDataLoad
	dbPath := "./v1db"
	// os.RemoveAll(dbPath)
	b := OpenDB(dbPath)
	b.LoadData()
}
