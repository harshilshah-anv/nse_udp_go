// NSE Capital Market Multicast UDP Receiver - Message 7216 Only
// 
// FOCUS: Only process message code 7216 (BCAST_INDICES_VIX - India VIX Index)
// OUTPUT: csv_output/message_7216_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7216_live.go
// Output: csv_output/message_7216_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Pages 142-144
// Structure: INDICES (71 bytes per record)
// Maximum Records: 6 per broadcast packet
// Contains: India VIX volatility index

package main

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"math"
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
// MESSAGE 7216 STRUCTURE
// =============================================================================

// IndexInfo - INDICES structure (71 bytes per record)
// Protocol: NSE CM NNF Protocol v6.3, Pages 142-144
type IndexInfo struct {
	IndexName            [21]byte
	IndexValue           uint32
	HighIndexValue       uint32
	LowIndexValue        uint32
	OpeningIndex         uint32
	ClosingIndex         uint32
	PercentChange        uint32
	YearlyHigh           uint32
	YearlyLow            uint32
	NoOfUpmoves          uint32
	NoOfDownmoves        uint32
	MarketCapitalisation float64
	NetChangeIndicator   byte
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

	// 7216 specific counters
	message7216Count int64
	message7216Saved int64

	// CSV file for 7216
	csvFile7216   *os.File
	csvWriter7216 *csv.Writer

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

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 7216
	if err := initialize7216CSV(); err != nil {
		log.Fatalf("Failed to initialize 7216 CSV: %v", err)
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
	if csvWriter7216 != nil {
		csvWriter7216.Flush()
	}
	if csvFile7216 != nil {
		csvFile7216.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7216CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7216_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile7216 = file
	csvWriter7216 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"IndexName",
		"IndexValue",
		"HighIndexValue",
		"LowIndexValue",
		"OpeningIndex",
		"ClosingIndex",
		"PercentChange",
		"YearlyHigh",
		"YearlyLow",
		"NoOfUpmoves",
		"NoOfDownmoves",
		"MarketCapitalisation",
		"NetChangeIndicator",
	}
	csvWriter7216.Write(headers)
	csvWriter7216.Flush()

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

	// Only process message 7216
	if transactionCode != 7216 {
		return
	}

	process7216Message(finalData)
}

// =============================================================================
// MESSAGE 7216 PROCESSOR
// =============================================================================

func process7216Message(data []byte) {
	if len(data) < 468 {
		return
	}

	atomic.AddInt64(&message7216Count, 1)

	// Read number of records
	noOfRecords := binary.BigEndian.Uint16(data[40:42])

	if noOfRecords == 0 {
		return
	}

	// Process each index record (71 bytes per record)
	offset := 42

	for i := 0; i < int(noOfRecords) && i < 6; i++ {
		if offset+71 > len(data) {
			break
		}

		var indexInfo IndexInfo

		// Parse INDICES structure
		copy(indexInfo.IndexName[:], data[offset:offset+21])
		indexInfo.IndexValue = binary.BigEndian.Uint32(data[offset+21 : offset+25])
		indexInfo.HighIndexValue = binary.BigEndian.Uint32(data[offset+25 : offset+29])
		indexInfo.LowIndexValue = binary.BigEndian.Uint32(data[offset+29 : offset+33])
		indexInfo.OpeningIndex = binary.BigEndian.Uint32(data[offset+33 : offset+37])
		indexInfo.ClosingIndex = binary.BigEndian.Uint32(data[offset+37 : offset+41])
		indexInfo.PercentChange = binary.BigEndian.Uint32(data[offset+41 : offset+45])
		indexInfo.YearlyHigh = binary.BigEndian.Uint32(data[offset+45 : offset+49])
		indexInfo.YearlyLow = binary.BigEndian.Uint32(data[offset+49 : offset+53])
		indexInfo.NoOfUpmoves = binary.BigEndian.Uint32(data[offset+53 : offset+57])
		indexInfo.NoOfDownmoves = binary.BigEndian.Uint32(data[offset+57 : offset+61])

		// Read DOUBLE (8 bytes) - MarketCapitalisation
		marketCapBits := binary.BigEndian.Uint64(data[offset+61 : offset+69])
		indexInfo.MarketCapitalisation = math.Float64frombits(marketCapBits)

		indexInfo.NetChangeIndicator = data[offset+69]

		// Export to CSV
		exportTo7216CSV(indexInfo)

		offset += 71
	}
}

func exportTo7216CSV(indexInfo IndexInfo) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean index name (remove null bytes and trim spaces)
	indexName := strings.TrimSpace(string(indexInfo.IndexName[:]))
	indexName = strings.Trim(indexName, "\x00")

	// Convert index values from paise to actual value (÷100)
	indexValue := float64(indexInfo.IndexValue) / 100.0
	highValue := float64(indexInfo.HighIndexValue) / 100.0
	lowValue := float64(indexInfo.LowIndexValue) / 100.0
	openingValue := float64(indexInfo.OpeningIndex) / 100.0
	closingValue := float64(indexInfo.ClosingIndex) / 100.0
	yearlyHigh := float64(indexInfo.YearlyHigh) / 100.0
	yearlyLow := float64(indexInfo.YearlyLow) / 100.0

	// Convert percent change from basis points to percentage (÷10000)
	percentChange := float64(indexInfo.PercentChange) / 10000.0

	record := []string{
		timestamp,
		"7216",
		indexName,
		fmt.Sprintf("%.2f", indexValue),
		fmt.Sprintf("%.2f", highValue),
		fmt.Sprintf("%.2f", lowValue),
		fmt.Sprintf("%.2f", openingValue),
		fmt.Sprintf("%.2f", closingValue),
		fmt.Sprintf("%.4f", percentChange),
		fmt.Sprintf("%.2f", yearlyHigh),
		fmt.Sprintf("%.2f", yearlyLow),
		fmt.Sprintf("%d", indexInfo.NoOfUpmoves),
		fmt.Sprintf("%d", indexInfo.NoOfDownmoves),
		fmt.Sprintf("%.2f", indexInfo.MarketCapitalisation),
		fmt.Sprintf("%c", indexInfo.NetChangeIndicator),
	}

	csvWriter7216.Write(record)
	csvWriter7216.Flush()

	atomic.AddInt64(&message7216Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg7216 := atomic.LoadInt64(&message7216Count)
	saved7216 := atomic.LoadInt64(&message7216Saved)

	if duration > 0 {
		fmt.Printf("%.0fs | %d pkts (%.0f/s) | 7216: %d msgs, %d saved\n",
			duration, packets, float64(packets)/duration, msg7216, saved7216)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg7216 := atomic.LoadInt64(&message7216Count)
	saved7216 := atomic.LoadInt64(&message7216Saved)

	fmt.Printf("\nRuntime: %v | Packets: %d | Message 7216: %d | Saved: %d\n",
		duration, packets, msg7216, saved7216)
}
