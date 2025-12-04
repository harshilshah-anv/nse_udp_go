package main

import (
	"encoding/binary"
	"math"
)

// Helper functions to unpack various data types from byte arrays
// These functions handle little-endian byte order (as used by Intel x86 systems)
// and perform "twiddling" (byte reversal) as required by NSE protocol

// UnpackByte extracts a single byte (CHAR/UCHAR) from data
func UnpackByte(data []byte, start int) (byte, bool) {
	if start >= len(data) {
		return 0, false
	}
	return data[start], true
}

// UnpackShort extracts a 16-bit SHORT (2 bytes) from data at given position
// Returns: value, success
func UnpackShort(data []byte, start int) (int16, bool) {
	if start+2 > len(data) {
		return 0, false
	}
	// Little-endian: least significant byte first
	value := binary.LittleEndian.Uint16(data[start : start+2])
	return int16(value), true
}

// UnpackUShort extracts a 16-bit unsigned SHORT (2 bytes) from data at given position
func UnpackUShort(data []byte, start int) (uint16, bool) {
	if start+2 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint16(data[start : start+2])
	return value, true
}

// UnpackInt extracts a 32-bit INT (4 bytes) from data at given position
func UnpackInt(data []byte, start int) (int32, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(data[start : start+4])
	return int32(value), true
}

// UnpackUInt extracts a 32-bit unsigned INT (4 bytes) from data at given position
func UnpackUInt(data []byte, start int) (uint32, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(data[start : start+4])
	return value, true
}

// UnpackLong extracts a 64-bit LONG (8 bytes) from data at given position
func UnpackLong(data []byte, start int) (int64, bool) {
	if start+8 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(data[start : start+8])
	return int64(value), true
}

// UnpackULong extracts a 64-bit unsigned LONG (8 bytes) from data at given position
func UnpackULong(data []byte, start int) (uint64, bool) {
	if start+8 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(data[start : start+8])
	return value, true
}

// UnpackDouble extracts a 64-bit DOUBLE (8 bytes) from data at given position
func UnpackDouble(data []byte, start int) (float64, bool) {
	if start+8 > len(data) {
		return 0, false
	}
	bits := binary.LittleEndian.Uint64(data[start : start+8])
	value := math.Float64frombits(bits)
	return value, true
}

// UnpackFloat extracts a 32-bit FLOAT (4 bytes) from data at given position
func UnpackFloat(data []byte, start int) (float32, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	bits := binary.LittleEndian.Uint32(data[start : start+4])
	value := math.Float32frombits(bits)
	return value, true
}

// UnpackString extracts a string of given length from data at given position
// NSE strings are padded with spaces and NOT null-terminated
func UnpackString(data []byte, start int, length int) (string, bool) {
	if start+length > len(data) {
		return "", false
	}
	return string(data[start : start+length]), true
}

// UnpackBytes extracts a byte slice of given length from data at given position
func UnpackBytes(data []byte, start int, length int) ([]byte, bool) {
	if start+length > len(data) {
		return nil, false
	}
	return data[start : start+length], true
}

// TrimSpaces removes trailing spaces from NSE string fields
func TrimSpaces(s string) string {
	i := len(s) - 1
	for i >= 0 && s[i] == ' ' {
		i--
	}
	return s[:i+1]
}

// PackShort packs a 16-bit SHORT into byte array (for sending data to NSE)
func PackShort(value int16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(value))
	return buf
}

// PackUShort packs a 16-bit unsigned SHORT into byte array
func PackUShort(value uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, value)
	return buf
}

// PackInt packs a 32-bit INT into byte array
func PackInt(value int32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(value))
	return buf
}

// PackUInt packs a 32-bit unsigned INT into byte array
func PackUInt(value uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	return buf
}

// PackLong packs a 64-bit LONG into byte array
func PackLong(value int64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))
	return buf
}

// PackULong packs a 64-bit unsigned LONG into byte array
func PackULong(value uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return buf
}

// PackDouble packs a 64-bit DOUBLE into byte array
func PackDouble(value float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(value)
	binary.LittleEndian.PutUint64(buf, bits)
	return buf
}

// PackFloat packs a 32-bit FLOAT into byte array
func PackFloat(value float32) []byte {
	buf := make([]byte, 4)
	bits := math.Float32bits(value)
	binary.LittleEndian.PutUint32(buf, bits)
	return buf
}

// PackString packs a string into byte array with space padding (NSE format)
// Strings are converted to uppercase and padded with spaces (not null-terminated)
func PackString(s string, length int) []byte {
	buf := make([]byte, length)
	// Fill with spaces
	for i := 0; i < length; i++ {
		buf[i] = ' '
	}
	// Copy string (convert to uppercase as per NSE requirement)
	for i := 0; i < len(s) && i < length; i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			c = c - 'a' + 'A' // Convert to uppercase
		}
		buf[i] = c
	}
	return buf
}

// Range extraction helpers - extract data between start and end positions

// ExtractShort extracts SHORT from start to end (must be 2 bytes)
func ExtractShort(data []byte, start, end int) (int16, bool) {
	if end-start != 2 {
		return 0, false
	}
	return UnpackShort(data, start)
}

// ExtractUShort extracts unsigned SHORT from start to end (must be 2 bytes)
func ExtractUShort(data []byte, start, end int) (uint16, bool) {
	if end-start != 2 {
		return 0, false
	}
	return UnpackUShort(data, start)
}

// ExtractInt extracts INT from start to end (must be 4 bytes)
func ExtractInt(data []byte, start, end int) (int32, bool) {
	if end-start != 4 {
		return 0, false
	}
	return UnpackInt(data, start)
}

// ExtractUInt extracts unsigned INT from start to end (must be 4 bytes)
func ExtractUInt(data []byte, start, end int) (uint32, bool) {
	if end-start != 4 {
		return 0, false
	}
	return UnpackUInt(data, start)
}

// ExtractLong extracts LONG from start to end (must be 8 bytes)
func ExtractLong(data []byte, start, end int) (int64, bool) {
	if end-start != 8 {
		return 0, false
	}
	return UnpackLong(data, start)
}

// ExtractULong extracts unsigned LONG from start to end (must be 8 bytes)
func ExtractULong(data []byte, start, end int) (uint64, bool) {
	if end-start != 8 {
		return 0, false
	}
	return UnpackULong(data, start)
}

// ExtractDouble extracts DOUBLE from start to end (must be 8 bytes)
func ExtractDouble(data []byte, start, end int) (float64, bool) {
	if end-start != 8 {
		return 0, false
	}
	return UnpackDouble(data, start)
}

// ExtractFloat extracts FLOAT from start to end (must be 4 bytes)
func ExtractFloat(data []byte, start, end int) (float32, bool) {
	if end-start != 4 {
		return 0, false
	}
	return UnpackFloat(data, start)
}

// ExtractString extracts string from start to end
func ExtractString(data []byte, start, end int) (string, bool) {
	length := end - start
	if length <= 0 {
		return "", false
	}
	return UnpackString(data, start, length)
}

// ExtractBytes extracts byte slice from start to end
func ExtractBytes(data []byte, start, end int) ([]byte, bool) {
	length := end - start
	if length <= 0 {
		return nil, false
	}
	return UnpackBytes(data, start, length)
}
