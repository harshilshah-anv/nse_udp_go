// NSE Capital Market Multicast UDP Receiver - Message 18130 Only
// 
// FOCUS: Only process message code 18130 (BCAST_SECURITY_STATUS_CHG - Security Status Change)
// OUTPUT: csv_output/message_18130_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18130_live.go
// Output: csv_output/message_18130_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Pages 102-104
// Structure: SECURITY STATUS UPDATE INFORMATION (442 bytes)
// Contains: Up to 25 securities with status changes across 6 markets

package main

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// =============================================================================
// LZO DECOMPRESSION
// =============================================================================

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

// DecompressUltra - LZO1Z decompression for NSE broadcast packets
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

	srcPtr := unsafe.Pointer(&src[0])
	dstPtr := unsafe.Pointer(&dst[0])

	getU8 := func(ptr unsafe.Pointer, offset int) uint8 {
		return *(*uint8)(unsafe.Add(ptr, offset))
	}

	setU8 := func(ptr unsafe.Pointer, offset int, val uint8) {
		*(*uint8)(unsafe.Add(ptr, offset)) = val
	}

	t = int(getU8(srcPtr, ip))
	ip++

	if t > 17 {
		t = t - 17
		if t < 4 {
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
			for op < opEnd && t > 0 && ip < ipEnd {
				setU8(dstPtr, op, getU8(srcPtr, ip))
				op++
				ip++
				t--
			}
			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++
		}
	}

	for {
		if t < 16 {
			mPos = op - (1 + M2_MAX_OFFSET)
			mPos -= int(t) >> 2
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}
			mPos -= int(getU8(srcPtr, ip)) << 2
			ip++

			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			lastMOff = mPos - op + 3
			
			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone

		} else if t >= 64 {
			mPos = op - 1
			mPos -= (int(t) >> 2) & 7
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}
			mPos -= int(getU8(srcPtr, ip)) << 3
			ip++
			t = (t >> 5) - 1

			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen
			lastMOff = mPos - op

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone

		} else if t >= 32 {
			t &= 31
			if t == 0 {
				for {
					if ip >= ipEnd {
						return 0, ErrInputOverrun
					}
					tt := int(getU8(srcPtr, ip))
					ip++
					if tt != 0 {
						t = tt + 31
						break
					}
					t += 255
				}
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}
			off = int(getU8(srcPtr, ip))
			ip++
			off |= int(getU8(srcPtr, ip)) << 8
			ip++

			mPos = op - (off >> 2) - 1
			if mPos < 0 {
				return 0, ErrCorrupted
			}

			lastMOff = mPos - op

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 || mPos+i >= op {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone

		} else if t >= 16 {
			mPos = op
			mPos -= (int(t) & 8) << 11

			t &= 7
			if t == 0 {
				for {
					if ip >= ipEnd {
						return 0, ErrInputOverrun
					}
					tt := int(getU8(srcPtr, ip))
					ip++
					if tt != 0 {
						t = tt + 7
						break
					}
					t += 255
				}
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}
			off = int(getU8(srcPtr, ip))
			ip++
			off |= int(getU8(srcPtr, ip)) << 8
			ip++

			mPos -= (off >> 2)
			if mPos < 0 || mPos == op {
				return 0, ErrCorrupted
			}

			if off&3 != 0 {
				mPos -= 0x4000
			}

			lastMOff = mPos - op

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 || mPos+i >= op {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone
		}

	matchDone:
		if t >= 64 {
			mPos = op - 1
			mPos -= (int(t) >> 2) & 7
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}
			mPos -= int(getU8(srcPtr, ip)) << 3
			ip++
			t = (t >> 5) - 1

			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen
			lastMOff = mPos - op

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone

		} else if t >= 32 {
			t &= 31
			if t == 0 {
				for {
					if ip >= ipEnd {
						return 0, ErrInputOverrun
					}
					tt := int(getU8(srcPtr, ip))
					ip++
					if tt != 0 {
						t = tt + 31
						break
					}
					t += 255
				}
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}
			off = int(getU8(srcPtr, ip))
			ip++
			off |= int(getU8(srcPtr, ip)) << 8
			ip++

			mPos = op - (off >> 2) - 1
			if mPos < 0 {
				return 0, ErrCorrupted
			}

			lastMOff = mPos - op

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 || mPos+i >= op {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone

		} else if t >= 16 {
			mPos = op
			mPos -= (int(t) & 8) << 11

			t &= 7
			if t == 0 {
				for {
					if ip >= ipEnd {
						return 0, ErrInputOverrun
					}
					tt := int(getU8(srcPtr, ip))
					ip++
					if tt != 0 {
						t = tt + 7
						break
					}
					t += 255
				}
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}
			off = int(getU8(srcPtr, ip))
			ip++
			off |= int(getU8(srcPtr, ip)) << 8
			ip++

			mPos -= (off >> 2)
			if mPos < 0 || mPos == op {
				return 0, ErrCorrupted
			}

			if off&3 != 0 {
				mPos -= 0x4000
			}

			lastMOff = mPos - op

			mLen = t + 2
			for i := 0; i < mLen; i++ {
				if op+i >= opEnd {
					return 0, ErrOutputOverrun
				}
				if mPos+i < 0 || mPos+i >= op {
					return 0, ErrCorrupted
				}
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
				goto matchDone
			}

			mPos = op + lastMOff
			if mPos < 0 || mPos >= op {
				return 0, ErrCorrupted
			}

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++
			mPos++

			t = int(getU8(srcPtr, mPos))
			if op >= opEnd {
				return 0, ErrOutputOverrun
			}
			setU8(dstPtr, op, uint8(t))
			op++

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone
		}

		totalLen = t

		if totalLen < 4 {
			if op+totalLen > opEnd || ip+totalLen > ipEnd {
				return 0, ErrOutputOverrun
			}
			for i := 0; i < totalLen; i++ {
				setU8(dstPtr, op+i, getU8(srcPtr, ip+i))
			}
			op += totalLen
			ip += totalLen

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++
		} else {
			for op < opEnd && totalLen > 0 && ip < ipEnd {
				setU8(dstPtr, op, getU8(srcPtr, ip))
				op++
				ip++
				totalLen--
			}

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++
		}
	}
}

// =============================================================================
// MESSAGE 18130 STRUCTURE
// =============================================================================

// TokenAndEligibility - Single security status record (16 bytes)
// Protocol: NSE CM NNF Protocol v6.3, Table 31.1
type TokenAndEligibility struct {
	Token                   uint32
	SecurityStatusPerMarket [6]uint16 // 6 markets, 2 bytes each
}

// Message18130 - SECURITY STATUS UPDATE INFORMATION (442 bytes)
// Protocol: NSE CM NNF Protocol v6.3, Pages 102-104
type Message18130 struct {
	NumberOfRecords uint16
	Records         []TokenAndEligibility // Up to 25 records
}

// =============================================================================
// GLOBAL VARIABLES
// =============================================================================

var (
	// Basic counters
	packetCount         int64
	totalBytes          int64
	compressedCount     int64
	decompressedCount   int64
	decompressionErrors int64
	innerMsgCount       int64 // Count of inner broadcast messages processed

	// 18130 specific counters
	message18130Count int64
	message18130Saved int64

	// CSV file for 18130
	csvFile18130   *os.File
	csvWriter18130 *csv.Writer

	// Control channels
	startTime    time.Time
	shutdownChan chan bool
	packetChan   chan []byte

	// Track message codes for statistics
	messageCodeCounts map[uint16]int64
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	// Initialize tracking
	messageCodeCounts = make(map[uint16]int64)

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("NSE CM UDP Receiver - Message 18130 (BCAST_SECURITY_STATUS_CHG)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 18130 (0x46D2 in hex)\n")
	//fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18130
	if err := initialize18130CSV(); err != nil {
		log.Fatalf("Failed to initialize 18130 CSV: %v", err)
		return
	}

	// Setup signal handler (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start packet processor
	go processPackets()

	// Start UDP listener
	go startUDPListener()

	// Start statistics display (every 10 seconds)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				printStats()
			case <-shutdownChan:
				return
			}
		}
	}()

	// Wait for Ctrl+C
	<-sigChan

	close(shutdownChan)
	time.Sleep(1 * time.Second)

	// Close CSV files
	if csvWriter18130 != nil {
		csvWriter18130.Flush()
	}
	if csvFile18130 != nil {
		csvFile18130.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize18130CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_18130_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile18130 = file
	csvWriter18130 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"Token",
		"Market1_Status",
		"Market2_Status",
		"Market3_Status",
		"Market4_Status",
		"Market5_Status",
		"Market6_Status",
	}
	csvWriter18130.Write(headers)
	csvWriter18130.Flush()

	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	// After Market Hours Multicast
	//multicastIP := "231.31.31.4"
	//port := 18901

	// Live Market Hours Multicast
	multicastIP := "233.1.2.5"
	port := 8222

	// Create multicast address
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	// Join multicast group
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Fatalf("Failed to join multicast group: %v\n", err)
		return
	}
	defer conn.Close()

	// Set read buffer size (2MB)
	conn.SetReadBuffer(2 * 1024 * 1024)

	buffer := make([]byte, 2048)

	for {
		select {
		case <-shutdownChan:
			return
		default:
			// Read UDP packet
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				continue
			}

			// Track packet statistics
			atomic.AddInt64(&packetCount, 1)
			atomic.AddInt64(&totalBytes, int64(n))

			// Send packet to processing channel
			data := make([]byte, n)
			copy(data, buffer[:n])

			select {
			case packetChan <- data:
			default:
				// Channel full, skip packet
			}
		}
	}
}

// =============================================================================
// PACKET PROCESSOR
// =============================================================================

func processPackets() {
	for {
		select {
		case <-shutdownChan:
			return
		case data := <-packetChan:
			processUDPPacket(data)
		}
	}
}

func processUDPPacket(data []byte) {
	if len(data) < 6 {
		return
	}

	// Read iNoPackets (number of sequentially packed messages)
	iNoPackets := int(binary.BigEndian.Uint16(data[2:4]))
	cPackData := data[4:]

	offset := 0
	for p := 0; p < iNoPackets && offset+2 <= len(cPackData); p++ {
		iCompLen := binary.BigEndian.Uint16(cPackData[offset : offset+2])
		offset += 2

		var finalData []byte
		if iCompLen > 0 {
			// Compressed packet (only for specific message codes: 7201, 18703, 7208, 7210, 7214, 7215)
			// Note: 18130 is NOT compressed per NSE protocol spec Chapter 7
			if offset+int(iCompLen) > len(cPackData) {
				break
			}
			compressed := cPackData[offset : offset+int(iCompLen)]
			offset += int(iCompLen)

			decompressed := make([]byte, 10240)
			decompLen, err := DecompressUltra(compressed, decompressed)
			if err != nil {
				atomic.AddInt64(&decompressionErrors, 1)
				continue
			}
			atomic.AddInt64(&compressedCount, 1)
			atomic.AddInt64(&decompressedCount, 1)
			finalData = decompressed[:decompLen]
		} else {
			// Uncompressed packet - next SHORT gives uncompressed length
			// 18130 messages are typically uncompressed
			if offset+2 > len(cPackData) {
				break
			}
			unCompLen := int(binary.BigEndian.Uint16(cPackData[offset : offset+2]))
			offset += 2
			if offset+unCompLen > len(cPackData) {
				break
			}
			finalData = cPackData[offset : offset+unCompLen]
			offset += unCompLen
		}

		processInnerMessage(finalData)
	}
}

func processInnerMessage(packet []byte) {
	// BroadcastData layout per NSE CM NNF Protocol v6.3:
	// [0] = MarketType (1 byte) - 4=CM, 2=FO
	// [1..7] = Reserved (7 bytes) - IGNORE
	// [8..] = BCAST_HEADER (40 bytes) starts here
	//   Table 3 BCAST_HEADER structure:
	//   - [0..3] = Reserved (4 bytes)
	//   - [4..7] = LogTime (4 bytes)
	//   - [8..9] = AlphaChar (2 bytes)
	//   - [10..11] = TransCode (2 bytes) ← THIS IS WHERE WE READ!
	//   - [12..13] = ErrorCode (2 bytes)
	//   - ... rest of header
	
	if len(packet) < 8+40 {
		return
	}

	// Debug: Check market type
	marketType := packet[0]
	if marketType != 4 {
		// Not Capital Market, skip
		return
	}

	bcastHeader := packet[8:] // BCAST_HEADER starts at byte 8
	if len(bcastHeader) < 40 {
		return
	}

	// Count inner messages processed
	atomic.AddInt64(&innerMsgCount, 1)

	// TransactionCode is at offset 10 within BCAST_HEADER (per Table 3)
	transactionCode := binary.BigEndian.Uint16(bcastHeader[10:12])

	// Track message codes
	messageCodeCounts[transactionCode]++

	// Debug: Print first occurrence of each message code WITH hex dump
	if messageCodeCounts[transactionCode] == 1 {
		fmt.Printf("📊 Found message code: %d (hex: 0x%04X) - first occurrence\n", transactionCode, transactionCode)
		// Show the bytes around TransCode for debugging
		if len(bcastHeader) >= 20 {
			fmt.Printf("   Bytes [8-19]: %02X (AlphaChar + TransCode + ErrorCode + more)\n", 
				bcastHeader[8:20])
		}
	}

	// Only process message 18130
	if transactionCode != 18130 {
		return
	}

	fmt.Printf("✅ Message 18130 (0x%04X) received! Processing...\n", transactionCode)
	fmt.Printf("   Full BCAST_HEADER (first 40 bytes): %02X\n", bcastHeader[0:40])
	
	// Pass only the payload after BCAST_HEADER (40 bytes)
	if len(bcastHeader) < 40+2 {
		return
	}
	process18130Message(bcastHeader[40:])
}

// =============================================================================
// MESSAGE 18130 PROCESSOR
// =============================================================================

func process18130Message(data []byte) {
	// data starts at the payload (after 40-byte MESSAGEHEADER)
	// Structure: NumberOfRecords (2) + TOKEN_AND_ELIGIBILITY[25] (16 bytes each)
	if len(data) < 2 {
		return
	}

	atomic.AddInt64(&message18130Count, 1)

	var msg Message18130

	// NumberOfRecords is at offset 0 in payload
	msg.NumberOfRecords = binary.BigEndian.Uint16(data[0:2])

	// Validate number of records (max 25)
	if msg.NumberOfRecords == 0 || msg.NumberOfRecords > 25 {
		return
	}

	// Check if we have enough data for all records
	requiredLen := 2 + int(msg.NumberOfRecords)*16
	if len(data) < requiredLen {
		return
	}

	// Parse TOKEN_AND_ELIGIBILITY array (starts at offset 2)
	offset := 2
	for i := uint16(0); i < msg.NumberOfRecords; i++ {
		if offset+16 > len(data) {
			break
		}

		var record TokenAndEligibility
		
		// Token: 4 bytes
		record.Token = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4

		// SecurityStatusPerMarket[6]: 12 bytes (2 bytes each)
		for j := 0; j < 6; j++ {
			record.SecurityStatusPerMarket[j] = binary.BigEndian.Uint16(data[offset : offset+2])
			offset += 2
		}

		msg.Records = append(msg.Records, record)
	}

	// Export to CSV
	exportTo18130CSV(msg)
}

func exportTo18130CSV(msg Message18130) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Map status values to readable names
	getStatusName := func(status uint16) string {
		switch status {
		case 1:
			return "Preopen"
		case 2:
			return "Open"
		case 3:
			return "Suspended"
		case 4:
			return "Preopen-Extended"
		case 6:
			return "Price-Discovery"
		default:
			return fmt.Sprintf("%d", status)
		}
	}

	// Write one row per security
	for _, record := range msg.Records {
		csvRecord := []string{
			timestamp,
			"18130",
			fmt.Sprintf("%d", record.Token),
			getStatusName(record.SecurityStatusPerMarket[0]),
			getStatusName(record.SecurityStatusPerMarket[1]),
			getStatusName(record.SecurityStatusPerMarket[2]),
			getStatusName(record.SecurityStatusPerMarket[3]),
			getStatusName(record.SecurityStatusPerMarket[4]),
			getStatusName(record.SecurityStatusPerMarket[5]),
		}

		csvWriter18130.Write(csvRecord)
		atomic.AddInt64(&message18130Saved, 1)
	}

	csvWriter18130.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	innerMsgs := atomic.LoadInt64(&innerMsgCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg18130 := atomic.LoadInt64(&message18130Count)
	saved18130 := atomic.LoadInt64(&message18130Saved)

	if duration > 0 {
		fmt.Printf("%.0fs | %d UDP pkts (%.0f/s) | %d inner msgs | Compressed: %d\n",
			duration, packets, float64(packets)/duration, innerMsgs, compressed)
		fmt.Printf("       18130: %d msgs, %d securities saved\n", msg18130, saved18130)
		
		// Show all message codes found
		if len(messageCodeCounts) > 0 {
			fmt.Printf("       Message codes: ")
			for code, count := range messageCodeCounts {
				marker := ""
				if code == 18130 {
					marker = "🎯"
				}
				fmt.Printf("%d%s(%d) ", code, marker, count)
			}
			fmt.Println()
		}
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg18130 := atomic.LoadInt64(&message18130Count)
	saved18130 := atomic.LoadInt64(&message18130Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Runtime: %v | Packets: %d | Message 18130: %d | Securities Saved: %d\n",
		duration, packets, msg18130, saved18130)
	
	// Show all message codes found
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\nAll message codes received:\n")
		for code, count := range messageCodeCounts {
			fmt.Printf("  Code %d: %d times\n", code, count)
		}
	}
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}
