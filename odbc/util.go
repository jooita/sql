package odbc

import (
	"bytes"
	"encoding/gob"
	"unicode/utf16"
)

// StringToUTF16 returns the UTF-16 encoding of the UTF-8 string s,
// with a terminating NUL added.
func StringToUTF16(s string) []uint16 { return utf16.Encode([]rune(s + "\x00")) }

// UTF16ToString returns the UTF-8 encoding of the UTF-16 sequence s,
// with a terminating NUL removed.
func UTF16ToString(s []uint16) string {
	for i, v := range s {
		if v == 0 {
			s = s[0:i]
			break
		}
	}
	return string(utf16.Decode(s))
}

// StringToUTF16Ptr returns pointer to the UTF-16 encoding of
// the UTF-8 string s, with a terminating NUL added.
func StringToUTF16Ptr(s string) *uint16 { return &StringToUTF16(s)[0] }

//In Go, Strings are utf-8 encoded already.
func StringToUTF8(s string) []byte {
	return append([]byte(s), byte('\x00'))
	//return []byte(s)
}

func PutBytes(target []byte, data interface{}) bool {
	b := GetBytes(data)
	if len(target) >= len(b) {
		target = GetBytes(data)
		return true
	}
	return false
}

func GetBytes(data interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func GetInterface(bts []byte, data interface{}) error {
	buf := bytes.NewBuffer(bts)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(data)
	if err != nil {
		return err
	}
	return nil
}
