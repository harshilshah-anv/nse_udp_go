// NSE Capital Market Multicast UDP Receiver - Message 7201 Only
// 
// FOCUS: Only process message code 7201 (BCAST_MW_ROUND_ROBIN - Market Watch Round Robin)
// OUTPUT: csv_output/message_7201_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7201_live.go
// Output: csv_output/message_7201_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 88, Table 39
// Structure: BCAST_MW_ROUND_ROBIN (466 bytes total)
// Layout: 
//   - BCAST_HEADER (40 bytes)
//   - NumberOfRecords (2 bytes) 
//   - MARKETWATCHBROADCAST[4] (4 records × 106 bytes each = 424 bytes)
//     Each MARKETWATCHBROADCAST:
//       - Token (4 bytes)
//       - MARKETWISEINFORMATION[3] (3 market types × 34 bytes each = 102 bytes)
//         Each MARKETWISEINFORMATION:
//           - MBO_MBP_INDICATOR (2 bytes)
//           - BuyVolume (8 bytes), BuyPrice (4 bytes)  
//           - SellVolume (8 bytes), SellPrice (4 bytes)
//           - LastTradePrice (4 bytes), LastTradeTime (4 bytes)
//
// Contains: Market watch snapshot with best buy/sell and LTP for multiple market types per token

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
// MESSAGE 7201 STRUCTURE
// =============================================================================

// MarketWiseInformation - 34 bytes per market type
// Protocol: NSE CM NNF Protocol v6.3, Table 39
type MarketWiseInformation struct {
	MboMbpIndicator uint16 // 2 bytes
	BuyVolume       uint64 // 8 bytes
	BuyPrice        uint32 // 4 bytes
	SellVolume      uint64 // 8 bytes
	SellPrice       uint32 // 4 bytes
	LastTradePrice  uint32 // 4 bytes
	LastTradeTime   uint32 // 4 bytes
}

// MarketWatchBroadcast - 106 bytes per record (4 bytes Token + 3 × 34 bytes MarketWiseInfo)
type MarketWatchBroadcast struct {
	Token           uint32                   // 4 bytes
	MarketWiseInfo  [3]MarketWiseInformation // 3 × 34 = 102 bytes
}

// Message7201 - BCAST_MW_ROUND_ROBIN (466 bytes)
// Protocol: NSE CM NNF Protocol v6.3, Page 88, Table 39
// Structure: BCAST_HEADER (40) + NumberOfRecords (2) + MARKETWATCHBROADCAST[4] (4 × 106 = 424)
type Message7201 struct {
	NumberOfRecords uint16                     // 2 bytes at offset 40
	Records         [4]MarketWatchBroadcast    // 4 × 106 = 424 bytes starting at offset 42
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

	// 7201 specific counters
	message7201Count int64
	message7201Saved int64

	// CSV file for 7201
	csvFile7201   *os.File
	csvWriter7201 *csv.Writer

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

	// Show startup banner
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("NSE CM UDP Receiver - Message 7201 (BCAST_MW_ROUND_ROBIN)")
	fmt.Println("Listening for message code 7201 (0x1C21 in hex)")
	fmt.Println("Structure: 4 records × 3 market types × buy/sell/LTP data")
	fmt.Println("Multicast: 233.1.2.5:8222")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println(strings.Repeat("=", 60))

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 7201
	if err := initialize7201CSV(); err != nil {
		log.Fatalf("Failed to initialize 7201 CSV: %v", err)
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
	if csvWriter7201 != nil {
		csvWriter7201.Flush()
	}
	if csvFile7201 != nil {
		csvFile7201.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7201CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7201_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile7201 = file
	csvWriter7201 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"NumberOfRecords",
		"RecordIndex",
		"Token",
		"MarketTypeIndex",
		"MboMbpIndicator",
		"BuyVolume",
		"BuyPrice",
		"SellVolume",
		"SellPrice",
		"LastTradePrice",
		"LastTradeTime",
	}
	csvWriter7201.Write(headers)
	csvWriter7201.Flush()

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
		log.Fatalf("❌ Failed to join multicast group: %v\n", err)
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

	cPackData := data[4:]
	if len(cPackData) < 2 {
		return
	}

	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])

	var finalData []byte
	isCompressed := iCompLen > 0

	if isCompressed {
		offset := 2
		if offset+int(iCompLen) > len(cPackData) {
			return
		}

		atomic.AddInt64(&compressedCount, 1)
		compressedPacket := cPackData[offset : offset+int(iCompLen)]
		decompressedData := make([]byte, 10240)
		decompLen, err := DecompressUltra(compressedPacket, decompressedData)
		if err != nil {
			atomic.AddInt64(&decompressionErrors, 1)
			return
		}
		atomic.AddInt64(&decompressedCount, 1)
		finalData = decompressedData[:decompLen]
	} else {
		finalData = cPackData[2:]
	}

	if len(finalData) < 28 {
		return
	}

	// Skip first 8 bytes
	finalData = finalData[8:]

	if len(finalData) < 48 {
		return
	}

	// Read transaction code
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes
	messageCodeCounts[transactionCode]++

	// Debug: Print first occurrence of each message code
	if messageCodeCounts[transactionCode] == 1 {
		if transactionCode == 7201 {
			fmt.Printf("🎯 TARGET message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		} else {
			fmt.Printf("📊 New message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		}
	}

	// Only process message 7201
	if transactionCode != 7201 {
		return
	}

	fmt.Printf("✅ Message 7201 received! Processing...\n")
	process7201Message(finalData)
}

// =============================================================================
// MESSAGE 7201 PROCESSOR
// =============================================================================

func process7201Message(data []byte) {
	if len(data) < 466 { // Must have full 466-byte structure
		fmt.Printf("⚠️  Message 7201 too short: %d bytes (expected 466)\n", len(data))
		return
	}

	atomic.AddInt64(&message7201Count, 1)

	// Parse NumberOfRecords at offset 40 (after 40-byte BCAST_HEADER)
	numRecords := int(binary.BigEndian.Uint16(data[40:42]))
	
	if numRecords > 4 {
		fmt.Printf("⚠️  Invalid NumberOfRecords: %d (max 4)\n", numRecords)
		return
	}

	fmt.Printf("📊 7201 Message: NumberOfRecords=%d\n", numRecords)

	var msg Message7201
	msg.NumberOfRecords = uint16(numRecords)

	// MARKETWATCHBROADCAST array starts at offset 42
	offset := 42
	recordLen := 106 // Each MARKETWATCHBROADCAST is 106 bytes

	for i := 0; i < numRecords && i < 4; i++ {
		if offset+recordLen > len(data) {
			fmt.Printf("⚠️  Record %d would exceed data length\n", i)
			break
		}

		rec := data[offset : offset+recordLen]
		
		// Parse Token (4 bytes)
		msg.Records[i].Token = binary.BigEndian.Uint32(rec[0:4])
		
		// Parse 3 MARKETWISEINFORMATION blocks (34 bytes each)
		for j := 0; j < 3; j++ {
			mwOffset := 4 + j*34
			if mwOffset+34 > len(rec) {
				continue
			}
			
			msg.Records[i].MarketWiseInfo[j].MboMbpIndicator = binary.BigEndian.Uint16(rec[mwOffset : mwOffset+2])
			msg.Records[i].MarketWiseInfo[j].BuyVolume = binary.BigEndian.Uint64(rec[mwOffset+2 : mwOffset+10])
			msg.Records[i].MarketWiseInfo[j].BuyPrice = binary.BigEndian.Uint32(rec[mwOffset+10 : mwOffset+14])
			msg.Records[i].MarketWiseInfo[j].SellVolume = binary.BigEndian.Uint64(rec[mwOffset+14 : mwOffset+22])
			msg.Records[i].MarketWiseInfo[j].SellPrice = binary.BigEndian.Uint32(rec[mwOffset+22 : mwOffset+26])
			msg.Records[i].MarketWiseInfo[j].LastTradePrice = binary.BigEndian.Uint32(rec[mwOffset+26 : mwOffset+30])
			msg.Records[i].MarketWiseInfo[j].LastTradeTime = binary.BigEndian.Uint32(rec[mwOffset+30 : mwOffset+34])
		}

		offset += recordLen
	}

	// Export to CSV
	exportTo7201CSV(msg)
}

func exportTo7201CSV(msg Message7201) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Export each record and each market type within the record
	for i := 0; i < int(msg.NumberOfRecords) && i < 4; i++ {
		record := msg.Records[i]
		
		for j := 0; j < 3; j++ {
			mwInfo := record.MarketWiseInfo[j]
			
			// Convert prices from paise to rupees
			buyPriceRupees := float64(mwInfo.BuyPrice) / 100.0
			sellPriceRupees := float64(mwInfo.SellPrice) / 100.0
			ltpRupees := float64(mwInfo.LastTradePrice) / 100.0

			csvRecord := []string{
				timestamp,
				"7201",
				fmt.Sprintf("%d", msg.NumberOfRecords),
				fmt.Sprintf("%d", i),
				fmt.Sprintf("%d", record.Token),
				fmt.Sprintf("%d", j),
				fmt.Sprintf("%d", mwInfo.MboMbpIndicator),
				fmt.Sprintf("%d", mwInfo.BuyVolume),
				fmt.Sprintf("%.2f", buyPriceRupees),
				fmt.Sprintf("%d", mwInfo.SellVolume),
				fmt.Sprintf("%.2f", sellPriceRupees),
				fmt.Sprintf("%.2f", ltpRupees),
				fmt.Sprintf("%d", mwInfo.LastTradeTime),
			}

			csvWriter7201.Write(csvRecord)
		}
	}
	
	csvWriter7201.Flush()
	atomic.AddInt64(&message7201Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg7201 := atomic.LoadInt64(&message7201Count)
	saved7201 := atomic.LoadInt64(&message7201Saved)

	if duration > 0 {
		fmt.Printf("\n%.0fs | %d pkts (%.0f/s) | 7201: %d msgs, %d saved\n",
			duration, packets, float64(packets)/duration, msg7201, saved7201)
		
		// Show all message codes detected in organized format
		if len(messageCodeCounts) > 0 {
			fmt.Printf("Message codes found: ")
			for code, count := range messageCodeCounts {
				if code == 7201 {
					fmt.Printf("7201(%d) ", count)
				}
			}
			for code, count := range messageCodeCounts {
				if code != 7201 {
					fmt.Printf("%d(%d) ", code, count)
				}
			}
			fmt.Printf("\n")
		}
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg7201 := atomic.LoadInt64(&message7201Count)
	saved7201 := atomic.LoadInt64(&message7201Saved)

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("FINAL STATISTICS - Message 7201 Decoder")
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Runtime: %v\n", duration)
	fmt.Printf("Total Packets Received: %d\n", packets)
	fmt.Printf("Message 7201 Count: %d\n", msg7201)
	fmt.Printf("Records Saved: %d\n", saved7201)
	
	if len(messageCodeCounts) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("All Message Codes Detected:")
		for code, count := range messageCodeCounts {
			if code == 7201 {
				fmt.Printf("  %d (0x%04X) [TARGET]: %d occurrences\n", code, code, count)
			} else {
				fmt.Printf("  %d (0x%04X): %d occurrences\n", code, code, count)
			}
		}
	}
	fmt.Println(strings.Repeat("=", 60))
}
