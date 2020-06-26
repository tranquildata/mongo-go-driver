package bsoncore

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
		for _, key := range keys {
			if _, hit := stringSet[key]; hit {
				return key, elem.Value(), nil
			}
		}
	}
	return "", Value{}, ErrElementNotFound
}
