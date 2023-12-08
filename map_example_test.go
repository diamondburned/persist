package persist_test

import (
	"fmt"
	"log"

	"libdb.so/persist"
	"libdb.so/persist/driver/badgerdb"
)

func Example_badgerDB() {
	type User struct {
		ID   int
		Name string
	}

	m, err := persist.NewMustMap[string, User](badgerdb.Open, ":memory:")
	if err != nil {
		log.Fatalln("cannot create badgerdb-backed map:", err)
	}
	defer m.Close()

	m.Set("foo", User{ID: 1, Name: "foo"})
	m.Set("bar", User{ID: 2, Name: "bar"})

	u, ok := m.Get("foo")
	fmt.Println(u, ok)

	iter := m.All()
	iter(func(k string, u User) bool {
		fmt.Printf("%s: %v\n", k, u)
		return true
	})

	// Output:
	// {1 foo} true
	// bar: {2 bar}
	// foo: {1 foo}
}
