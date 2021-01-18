package main

import (
	"fmt"
	b "gobitcast/v1"
)

func main() {
	dbPath := "./hello"
	b := b.OpenDB(dbPath)
	b.LoadData()

	err := b.Put("eric", "feng")
	if err != nil {
		fmt.Println("test put error")
	}

	_, v := b.Get("eric")
	fmt.Println("value:", v)
}
