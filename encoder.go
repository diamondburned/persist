// Package persist provides a basic type-safe map that persists to disk.
package persist

import (
	"bytes"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Encoder is a type that can encode a value to a byte slice.
// Such value must also be decodable from the same byte slice.
type Encoder[T any] interface {
	// Encode encodes a value to a byte slice.
	// A byte slice is passed to allow reusing the same slice
	// for multiple encodings.
	Encode(T, []byte) ([]byte, error)
	// Decode decodes a value from a byte slice. The byte slice
	// must not be modified or stored.
	Decode([]byte) (T, error)
}

// StringEncoder returns an Encoder that encodes values literally as strings.
// Use this for fast key formatting.
func StringEncoder[T ~string]() Encoder[T] {
	return stringEncoder[T]{}
}

type stringEncoder[T ~string] struct{}

func (stringEncoder[T]) Encode(v T, buf []byte) ([]byte, error) {
	return append(buf[:0], v...), nil
}

func (stringEncoder[T]) Decode(buf []byte) (T, error) {
	return T(buf), nil
}

// StringerEncoder returns an Encoder that encodes values using the Stringer
// interface. A parser function must be provided to decode the value
// from the string representation.
func StringerEncoder[T fmt.Stringer](parser func(s string) (T, error)) Encoder[T] {
	return stringerEncoder[T]{parser}
}

type stringerEncoder[T fmt.Stringer] struct {
	parser func(s string) (T, error)
}

func (stringerEncoder[T]) Encode(v T, buf []byte) ([]byte, error) {
	return append(buf[:0], v.String()...), nil
}

func (e stringerEncoder[T]) Decode(buf []byte) (T, error) {
	return e.parser(string(buf))
}

// BytesEncoder returns an Encoder that encodes values literally as byte slices.
// Use this for fast key formatting.
func BytesEncoder[T []byte]() Encoder[T] {
	return bytesEncoder[T]{}
}

type bytesEncoder[T []byte] struct{}

func (bytesEncoder[T]) Encode(v T, buf []byte) ([]byte, error) {
	return append(buf[:0], v...), nil
}

func (bytesEncoder[T]) Decode(buf []byte) (T, error) {
	return T(append([]byte(nil), buf...)), nil
}

// CBOREncoder returns an Encoder that encodes values using the CBOR format.
func CBOREncoder[T any]() Encoder[T] {
	return cborEncoder[T]{}
}

type cborEncoder[T any] struct{}

func (cborEncoder[T]) Encode(v T, buf []byte) ([]byte, error) {
	bbuf := bytes.NewBuffer(buf[:0])
	if err := cbor.NewEncoder(bbuf).Encode(v); err != nil {
		return nil, err
	}
	return bbuf.Bytes(), nil
}

func (cborEncoder[T]) Decode(buf []byte) (T, error) {
	var v T
	if err := cbor.Unmarshal(buf, &v); err != nil {
		return v, err
	}
	return v, nil
}
