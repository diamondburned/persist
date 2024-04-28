package persist_test

import (
	"fmt"
	"log"

	"libdb.so/persist"
	"libdb.so/persist/driver/badgerdb"
)

func Example_value() {
	type Config struct {
		EnableBananas bool
	}

	c, err := persist.NewMustValue[Config](badgerdb.Open, ":memory:")
	if err != nil {
		log.Fatalln("cannot create badgerdb-backed map:", err)
	}
	defer c.Close()

	c.Store(Config{EnableBananas: true})

	u, _ := c.Load()
	fmt.Printf("%+v\n", u)

	// Output:
	// {EnableBananas:true}
}
