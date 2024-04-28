package persist

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alecthomas/assert/v2"
	"github.com/fxamacker/cbor/v2"
)

type testStruct struct {
	Data string
	Int  int
}

func TestCBORDriver(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.cbor")

	d, err := NewMap[string, testStruct](CBORDriver, path)
	assert.NoError(t, err, "NewMap 1")

	defer d.Close()

	err = d.Store("key1", testStruct{Data: "data", Int: 42})
	assert.NoError(t, err, "Store 1")

	err = d.Store("key2", testStruct{})
	assert.NoError(t, err, "Store 2")

	v, ok, err := d.Load("key1")
	assert.NoError(t, err, "Load 1")
	assert.True(t, ok, "Load 1")
	assert.Equal(t, testStruct{Data: "data", Int: 42}, v, "Load 1")

	v, ok, err = d.Load("key2")
	assert.NoError(t, err, "Load 2")
	assert.True(t, ok, "Load 2")
	assert.Equal(t, testStruct{}, v, "Load 2")

	err = d.Close()
	assert.NoError(t, err, "Close")

	d, err = NewMap[string, testStruct](CBORDriver, path)
	assert.NoError(t, err, "NewMap 2")

	v, ok, err = d.Load("key1")
	assert.NoError(t, err, "Load 3")
	assert.True(t, ok, "Load 3")
	assert.Equal(t, testStruct{Data: "data", Int: 42}, v, "Load 3")

	v, ok, err = d.Load("key2")
	assert.NoError(t, err, "Load 4")
	assert.True(t, ok, "Load 4")
	assert.Equal(t, testStruct{}, v, "Load 4")

	assertCBORFile(t, path, map[cbor.ByteString]testStruct{
		cborKey("key1"): {Data: "data", Int: 42},
		cborKey("key2"): {},
	})

	err = d.Delete("key1")
	assert.NoError(t, err, "Delete 1")

	err = d.Delete("key2")
	assert.NoError(t, err, "Delete 2")

	assertCBORFile(t, path, map[cbor.ByteString]testStruct{})
}

func cborKey(k string) cbor.ByteString {
	b, _ := cbor.Marshal(k)
	return cbor.ByteString(b)
}

func assertCBORFile[T any](t *testing.T, path string, v T) {
	t.Helper()

	b, err := os.ReadFile(path)
	assert.NoError(t, err, "ReadFile")

	var m T
	err = cbor.Unmarshal(b, &m)
	assert.NoError(t, err, "Unmarshal")
	assert.Equal(t, v, m, "Unmarshal")
}
