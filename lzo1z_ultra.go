// Package main provides a pure Go implementation of LZO1Z decompression.
//
// This is an ULTRA-OPTIMIZED version using unsafe pointers and manual inlining.
// WARNING: This trades safety for performance. Use only with validated inputs.
package main

import (
	"errors"
	"unsafe"
)

// LZO error constants
var (
	ErrInputOverrun  = errors.New("LZO input overrun")
	ErrOutputOverrun = errors.New("LZO output overrun")
	ErrCorrupted     = errors.New("LZO data corrupted")
)

// LZO1Z constants
const (
	M2_MAX_OFFSET = 0x0700
)

// DecompressUltra is an ultra-optimized version using unsafe pointers
// Optimizations over DecompressOptimized:
// - Uses unsafe.Pointer to eliminate all bounds checking
// - Manually inlined copyMatchFast for common cases
// - Unrolled small copy loops
// - Branchless optimizations where possible
// - 8-byte bulk copies using uint64
//
// Target: 270-300 MB/s (35-50% faster than DecompressOptimized)
func DecompressUltra(src []byte, dst []byte) (int, error) {
	if len(src) < 1 || len(dst) < 1 {
		return 0, ErrInputOverrun
	}
	var (
		ip       = 0
		op       = 0
		lastMOff = 0
		t        int
		mPos     int
		mLen     int
		off      int
		totalLen int
	)

	ipEnd := len(src)
	opEnd := len(dst)

	// Get unsafe pointers to arrays for fast access
	srcPtr := unsafe.Pointer(&src[0])
	dstPtr := unsafe.Pointer(&dst[0])

	// Helper function to read byte with unsafe (no bounds check)
	getU8 := func(ptr unsafe.Pointer, offset int) uint8 {
		return *(*uint8)(unsafe.Add(ptr, offset))
	}

	// Helper function to write byte with unsafe (no bounds check)
	setU8 := func(ptr unsafe.Pointer, offset int, val uint8) {
		*(*uint8)(unsafe.Add(ptr, offset)) = val
	}

	// Read first instruction
	t = int(getU8(srcPtr, ip))
	ip++

	// Handle initial literal run
	if t > 17 {
		t = t - 17
		if t < 4 {
			// Small literal: copy then continue as match_next
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}
			// Inline small copy (up to 3 bytes)
			for i := 0; i < t; i++ {
				setU8(dstPtr, op+i, getU8(srcPtr, ip+i))
			}
			op += t
			ip += t

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++
		} else {
			// Large literal: copy then goto first_literal_run
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}
			// Use bulk copy for larger literals
			copy(dst[op:op+t], src[ip:ip+t])
			op += t
			ip += t

			if ip >= ipEnd {
				return op, nil
			}

			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				// t >= 16: it's a match, will be handled in main loop
			} else {
				// M2 match after initial literal run
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}

				off = (1 + M2_MAX_OFFSET) + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
				ip++

				mPos = op - off
				lastMOff = off

				if op+3 > opEnd {
					return 0, ErrOutputOverrun
				}

				// Inline 3-byte match copy
				if mPos+3 > op {
					// Overlapping - byte by byte
					setU8(dstPtr, op, getU8(dstPtr, mPos))
					setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
					setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
				} else {
					// Non-overlapping - can use direct copy
					setU8(dstPtr, op, getU8(dstPtr, mPos))
					setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
					setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
				}
				op += 3

				if ip >= ipEnd {
					return op, nil
				}

				t = int(getU8(srcPtr, ip-1)) & 3

				if t != 0 {
					// Copy few literals
					if op+t > opEnd || ip+t > ipEnd {
						return 0, ErrOutputOverrun
					}
					for i := 0; i < t; i++ {
						setU8(dstPtr, op+i, getU8(srcPtr, ip+i))
					}
					op += t
					ip += t

					if ip >= ipEnd {
						return op, nil
					}

					t = int(getU8(srcPtr, ip))
					ip++
				} else {
					if ip+3 > ipEnd {
						return 0, ErrInputOverrun
					}
					t = int(getU8(srcPtr, ip))
					ip++
				}
			}
		}
	}

	// Main decompression loop
	for {
		if t >= 16 {
			goto matchHandling
		}

		// Literal run (t < 16)
		if t == 0 {
			// Extended literal length - unrolled for common case
			for ip < ipEnd && getU8(srcPtr, ip) == 0 {
				t += 255
				ip++
			}
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}
			t += 15 + int(getU8(srcPtr, ip))
			ip++
		}

		// Copy literals (minimum 3)
		t += 3
		if op+t > opEnd || ip+t > ipEnd {
			return 0, ErrOutputOverrun
		}
		copy(dst[op:op+t], src[ip:ip+t])
		op += t
		ip += t

		if ip >= ipEnd {
			return op, nil
		}

		t = int(getU8(srcPtr, ip))
		ip++

		if t >= 16 {
			goto matchHandling
		}

		// M1 match after literal run
		if ip >= ipEnd {
			return 0, ErrInputOverrun
		}

		off = 1 + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
		ip++
		mPos = op - off
		lastMOff = off

		if op+3 > opEnd {
			return 0, ErrOutputOverrun
		}

		// Inline 3-byte match copy
		if mPos+3 > op {
			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
		} else {
			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
		}
		op += 3

		goto matchDone

	matchHandling:
		// Match handling (M2/M3/M4/M1 in match loop)
		if t >= 64 {
			// M2 match (64-255)
			off = t & 0x1f

			if off >= 0x1c {
				// Use cached offset
				if lastMOff == 0 {
					return 0, ErrCorrupted
				}
				mPos = op - lastMOff
			} else {
				// New offset
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				off = 1 + (off << 6) + (int(getU8(srcPtr, ip)) >> 2)
				ip++
				mPos = op - off
				lastMOff = off
			}

			mLen = (t >> 5) - 1

		} else if t >= 32 {
			// M3 match (32-63)
			t &= 31

			if t == 0 {
				// Extended length
				for ip < ipEnd && getU8(srcPtr, ip) == 0 {
					t += 255
					ip++
				}
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				t += 31 + int(getU8(srcPtr, ip))
				ip++
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}

			off = 1 + (int(getU8(srcPtr, ip)) << 6) + (int(getU8(srcPtr, ip+1)) >> 2)
			ip += 2

			mPos = op - off
			lastMOff = off
			mLen = t

		} else if t >= 16 {
			// M4 match (16-31)
			mPos = op
			mPos -= (t & 8) << 11

			t &= 7

			if t == 0 {
				// Extended length
				for ip < ipEnd && getU8(srcPtr, ip) == 0 {
					t += 255
					ip++
				}
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				t += 7 + int(getU8(srcPtr, ip))
				ip++
			}
			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}

			mPos -= (int(getU8(srcPtr, ip)) << 6) + (int(getU8(srcPtr, ip+1)) >> 2)
			ip += 2
			if mPos == op {
				return op, nil
			}

			mPos -= 0x4000
			lastMOff = op - mPos
			mLen = t
		} else {
			// M1 match (0-15) in the inner match loop
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}

			off = 1 + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
			ip++

			mPos = op - off
			lastMOff = off

			if op+2 > opEnd {
				return 0, ErrOutputOverrun
			}

			// Inline 2-byte copy
			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			op += 2

			goto matchDone
		}
		// For M2/M3/M4: Copy match (2 + mLen bytes total)
		totalLen = 2 + mLen
		if op+totalLen > opEnd {
			return 0, ErrOutputOverrun
		}

		// Optimized match copy with unsafe
		op = copyMatchUltraFast(dstPtr, op, mPos, totalLen)

		goto matchDone

	matchDone:
		t = int(getU8(srcPtr, ip-1)) & 3

		if ip >= ipEnd {
			return op, nil
		}

		if t != 0 {
			// Copy few literals
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}

			// Unrolled for common cases (1-3 bytes)
			switch t {
			case 1:
				setU8(dstPtr, op, getU8(srcPtr, ip))
			case 2:
				setU8(dstPtr, op, getU8(srcPtr, ip))
				setU8(dstPtr, op+1, getU8(srcPtr, ip+1))
			case 3:
				setU8(dstPtr, op, getU8(srcPtr, ip))
				setU8(dstPtr, op+1, getU8(srcPtr, ip+1))
				setU8(dstPtr, op+2, getU8(srcPtr, ip+2))
			default:
				copy(dst[op:op+t], src[ip:ip+t])
			}
			op += t
			ip += t

			if ip >= ipEnd {
				return op, nil
			}

			t = int(getU8(srcPtr, ip))
			ip++
			goto matchHandling
		}

		if ip >= ipEnd {
			return op, nil
		}

		if ip+3 > ipEnd {
			return 0, ErrInputOverrun
		}

		t = int(getU8(srcPtr, ip))
		ip++
		continue
	}
}

// copyMatchUltraFast uses unsafe pointers and 8-byte chunks for maximum speed
func copyMatchUltraFast(dstPtr unsafe.Pointer, op, mPos, length int) int {
	// For very small lengths, use unrolled byte copies
	if length <= 8 {
		// Check for overlapping copy (pattern repeat)
		if mPos+length > op {
			// Overlapping - must copy byte by byte
			for i := 0; i < length; i++ {
				*(*uint8)(unsafe.Add(dstPtr, op+i)) = *(*uint8)(unsafe.Add(dstPtr, mPos+i))
			}
			return op + length
		}

		// Non-overlapping - unrolled byte copy
		switch length {
		case 1:
			*(*uint8)(unsafe.Add(dstPtr, op)) = *(*uint8)(unsafe.Add(dstPtr, mPos))
		case 2:
			*(*uint16)(unsafe.Add(dstPtr, op)) = *(*uint16)(unsafe.Add(dstPtr, mPos))
		case 3:
			*(*uint16)(unsafe.Add(dstPtr, op)) = *(*uint16)(unsafe.Add(dstPtr, mPos))
			*(*uint8)(unsafe.Add(dstPtr, op+2)) = *(*uint8)(unsafe.Add(dstPtr, mPos+2))
		case 4:
			*(*uint32)(unsafe.Add(dstPtr, op)) = *(*uint32)(unsafe.Add(dstPtr, mPos))
		case 5:
			*(*uint32)(unsafe.Add(dstPtr, op)) = *(*uint32)(unsafe.Add(dstPtr, mPos))
			*(*uint8)(unsafe.Add(dstPtr, op+4)) = *(*uint8)(unsafe.Add(dstPtr, mPos+4))
		case 6:
			*(*uint32)(unsafe.Add(dstPtr, op)) = *(*uint32)(unsafe.Add(dstPtr, mPos))
			*(*uint16)(unsafe.Add(dstPtr, op+4)) = *(*uint16)(unsafe.Add(dstPtr, mPos+4))
		case 7:
			*(*uint32)(unsafe.Add(dstPtr, op)) = *(*uint32)(unsafe.Add(dstPtr, mPos))
			*(*uint16)(unsafe.Add(dstPtr, op+4)) = *(*uint16)(unsafe.Add(dstPtr, mPos+4))
			*(*uint8)(unsafe.Add(dstPtr, op+6)) = *(*uint8)(unsafe.Add(dstPtr, mPos+6))
		case 8:
			*(*uint64)(unsafe.Add(dstPtr, op)) = *(*uint64)(unsafe.Add(dstPtr, mPos))
		}
		return op + length
	}

	// For larger lengths, check overlap
	if mPos+length > op {
		// Overlapping copy - byte by byte
		for i := 0; i < length; i++ {
			*(*uint8)(unsafe.Add(dstPtr, op+i)) = *(*uint8)(unsafe.Add(dstPtr, mPos+i))
		}
		return op + length
	}

	// Non-overlapping large copy - use 8-byte chunks
	// Copy 8 bytes at a time for maximum throughput
	for length >= 8 {
		*(*uint64)(unsafe.Add(dstPtr, op)) = *(*uint64)(unsafe.Add(dstPtr, mPos))
		op += 8
		mPos += 8
		length -= 8
	}

	// Handle remainder (0-7 bytes)
	if length >= 4 {
		*(*uint32)(unsafe.Add(dstPtr, op)) = *(*uint32)(unsafe.Add(dstPtr, mPos))
		op += 4
		mPos += 4
		length -= 4
	}
	if length >= 2 {
		*(*uint16)(unsafe.Add(dstPtr, op)) = *(*uint16)(unsafe.Add(dstPtr, mPos))
		op += 2
		mPos += 2
		length -= 2
	}
	if length == 1 {
		*(*uint8)(unsafe.Add(dstPtr, op)) = *(*uint8)(unsafe.Add(dstPtr, mPos))
		op++
	}

	return op
}
