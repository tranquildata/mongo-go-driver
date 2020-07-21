package bsoncore

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson/bsontype"
)

//LookupMultipleErr takes multiple keys, returns the first one found or an error if none are found.
// The return is the found key and the value if the key was found or an empty string, zero Value and ErrElementNotFound
// if none of the keys was found.  LookupMultipleErr will return when the first key is found.
func (d Document) LookupMultipleErr(keys ...string) (string, Value, error) {
	if len(keys) < 1 {
		return "", Value{}, ErrEmptyKey
	}
	length, rem, ok := ReadLength(d)
	if !ok {
		return "", Value{}, NewInsufficientBytesError(d, rem)
	}

	length -= 4
	stringSet := map[string]bool{}
	for _, key := range keys {
		stringSet[key] = true
	}

	var elem Element
	for length > 1 {
		elem, rem, ok = ReadElement(rem)
		length -= int32(len(elem))
		if !ok {
			return "", Value{}, NewInsufficientBytesError(d, rem)
		}
		// We use `KeyBytes` rather than `Key` to avoid a needless string alloc.
		key := string(elem.KeyBytes())
		if _, hit := stringSet[key]; hit {
			return key, elem.Value(), nil
		}
	}
	return "", Value{}, ErrElementNotFound
}

func (d Document) LookupCaseInsensitive(key string) (string, Value, error) {
	if key == "" {
		return "", Value{}, ErrEmptyKey
	}
	length, rem, ok := ReadLength(d)
	if !ok {
		return "", Value{}, NewInsufficientBytesError(d, rem)
	}

	length -= 4
	lowKey := strings.ToLower(key)

	var elem Element
	for length > 1 {
		elem, rem, ok = ReadElement(rem)
		length -= int32(len(elem))
		if !ok {
			return "", Value{}, NewInsufficientBytesError(d, rem)
		}
		// We use `KeyBytes` rather than `Key` to avoid a needless string alloc.
		if strings.ToLower(string(elem.KeyBytes())) == lowKey {
			return key, elem.Value(), nil
		}
	}
	return "", Value{}, ErrElementNotFound
}

func (d Document) LookupRecursiveReturnFirst(key string) (Value, error) {
	length, rem, ok := ReadLength(d)
	if !ok {
		return Value{}, NewInsufficientBytesError(d, rem)
	}

	length -= 4

	var elem Element
	for length > 1 {
		elem, rem, ok = ReadElement(rem)
		length -= int32(len(elem))
		if !ok {
			return Value{}, NewInsufficientBytesError(d, rem)
		}
		// We use `KeyBytes` rather than `Key` to avoid a needless string alloc.
		if string(elem.KeyBytes()) == key {
			if elemVal, err := elem.ValueErr(); err != nil {
				continue
			} else {
				return elemVal, nil
			}
		}
		tt := bsontype.Type(elem[0])
		switch tt {
		case bsontype.EmbeddedDocument:
			val, err := elem.Value().Document().LookupRecursiveReturnFirst(key)
			if err == nil {
				return val, err
			}
		case bsontype.Array:
			val, err := elem.Value().Array().LookupRecursiveReturnFirst(key)
			if err == nil {
				return val, err
			}
		}
	}
	return Value{}, ErrElementNotFound
}

//LookupAllRecursive returns all instances of values keyed by key. These are not checked for uniqueness,
// so callers must expect to occasionally receive duplicate values.
func (d Document) LookupAllRecursive(key string) ([]Value, error) {
	length, rem, ok := ReadLength(d)
	if !ok {
		return []Value{}, NewInsufficientBytesError(d, rem)
	}

	length -= 4

	returnValues := make([]Value, 2)[0:0]
	var elem Element
	for length > 1 {
		elem, rem, ok = ReadElement(rem)
		length -= int32(len(elem))
		if !ok {
			return []Value{}, NewInsufficientBytesError(d, rem)
		}
		// We use `KeyBytes` rather than `Key` to avoid a needless string alloc.
		if string(elem.KeyBytes()) == key {
			if elemVal, err := elem.ValueErr(); err != nil {
				continue
			} else {
				returnValues = append(returnValues, elemVal)
			}
		}
		tt := bsontype.Type(elem[0])
		switch tt {
		case bsontype.EmbeddedDocument:
			vals, err := elem.Value().Document().LookupAllRecursive(key)
			if err != nil {
				return []Value{}, err
			}
			returnValues = append(returnValues, vals...)
		case bsontype.Array:
			vals, err := elem.Value().Array().LookupAllRecursive(key)
			if err != nil {
				return []Value{}, err
			}
			returnValues = append(returnValues, vals...)
		}
	}
	if len(returnValues) == 0 {
		return []Value{}, ErrElementNotFound
	}
	return returnValues, nil
}
