// NSE Capital Market Multicast UDP Receiver - Message 7208 Only
// 
// FOCUS: Only process message code 7208 (BCAST_ONLY_MBP - Market By Price Only)
// OUTPUT: csv_output/message_7208_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7208_live.go
// Output: csv_output/message_7208_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Pages 118-123
// Structure: BROADCAST ONLY MBP (566 bytes)
// Contains: Market By Price data (5 best bid/ask levels without order count)

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
// MESSAGE 7208 STRUCTURE
// =============================================================================

// MBPInfo - Market By Price information (16 bytes)
type MBPInfo struct {
	Quantity       int64
	Price          uint32
	NumberOfOrders uint16
	BbBuySellFlag  uint16
}

// Message7208 - BROADCAST ONLY MBP (262 bytes per record)
// Protocol: NSE CM NNF Protocol v6.3, Pages 118-123
type Message7208 struct {
	Token                          uint32
	BookType                       uint16
	TradingStatus                  uint16
	VolumeTradedToday              int64
	LastTradedPrice                uint32
	NetChangeIndicator             byte
	NetPriceChangeFromClosingPrice uint32
	LastTradeQuantity              uint32
	LastTradeTime                  uint32
	AverageTradePrice              uint32
	AuctionNumber                  uint16
	AuctionStatus                  uint16
	InitiatorType                  uint16
	InitiatorPrice                 uint32
	InitiatorQuantity              uint32
	AuctionPrice                   uint32
	AuctionQuantity                uint32
	BbTotalBuyFlag                 uint16
	BbTotalSellFlag                uint16
	TotalBuyQuantity               int64
	TotalSellQuantity              int64
	ClosingPrice                   uint32
	OpenPrice                      uint32
	HighPrice                      uint32
	LowPrice                       uint32
	IndicativeClosePrice           uint32
	MBPData                        [10]MBPInfo
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

	// 7208 specific counters
	message7208Count int64
	message7208Saved int64

	// CSV file for 7208
	csvFile7208   *os.File
	csvWriter7208 *csv.Writer

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
	fmt.Println("NSE CM UDP Receiver - Message 7208 (BCAST_ONLY_MBP)")
	fmt.Println("Listening for message code 7208 (0x1C28 in hex)")
	fmt.Println("Multicast: 233.1.2.5:8222")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println(strings.Repeat("=", 60))

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 7208
	if err := initialize7208CSV(); err != nil {
		log.Fatalf("Failed to initialize 7208 CSV: %v", err)
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
	if csvWriter7208 != nil {
		csvWriter7208.Flush()
	}
	if csvFile7208 != nil {
		csvFile7208.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7208CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7208_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile7208 = file
	csvWriter7208 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"Token",
		"BookType",
		"TradingStatus",
		"VolumeTradedToday",
		"LastTradedPrice",
		"NetChangeIndicator",
		"NetPriceChange",
		"LastTradeQuantity",
		"LastTradeTime",
		"AverageTradePrice",
		"TotalBuyQuantity",
		"TotalSellQuantity",
		"ClosingPrice",
		"OpenPrice",
		"HighPrice",
		"LowPrice",
		"IndicativeClosePrice",
		"BestBuyPrice_1",
		"BestBuyQty_1",
		"BestSellPrice_1",
		"BestSellQty_1",
		"BestBuyPrice_2",
		"BestBuyQty_2",
		"BestSellPrice_2",
		"BestSellQty_2",
		"BestBuyPrice_3",
		"BestBuyQty_3",
		"BestSellPrice_3",
		"BestSellQty_3",
		"BestBuyPrice_4",
		"BestBuyQty_4",
		"BestSellPrice_4",
		"BestSellQty_4",
		"BestBuyPrice_5",
		"BestBuyQty_5",
		"BestSellPrice_5",
		"BestSellQty_5",
	}
	csvWriter7208.Write(headers)
	csvWriter7208.Flush()

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

	// Debug: Print first occurrence of each message code
	if messageCodeCounts[transactionCode] == 1 {
		fmt.Printf("📊 Found message code: %d (hex: 0x%04X) - first occurrence\n", transactionCode, transactionCode)
	}

	// Only process message 7208
	if transactionCode != 7208 {
		return
	}

	fmt.Printf("🎯 Processing message 7208, finalData length: %d\n", len(finalData))
	process7208Message(finalData)
}

// =============================================================================
// MESSAGE 7208 PROCESSOR
// =============================================================================

func process7208Message(data []byte) {
	if len(data) < 42 { // Minimum length check - 40-byte header + 2 bytes for records
		return
	}

	atomic.AddInt64(&message7208Count, 1)

	// Read number of records
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	
	// Debug: Check for first few messages
	currentCount := atomic.LoadInt64(&message7208Count)
	if currentCount <= 3 {
		fmt.Printf("🔍 Message 7208 #%d: %d records, data length = %d\n", currentCount, noOfRecords, len(data))
	}

	if noOfRecords == 0 {
		if currentCount <= 3 {
			fmt.Printf("⚠️ Message 7208 #%d: No records to process\n", currentCount)
		}
		return
	}

	// Process each record (262 bytes per record)
	offset := 42
	recordsProcessed := 0

	for i := 0; i < int(noOfRecords) && i < 2; i++ {
		if offset+262 > len(data) {
			if currentCount <= 3 {
				fmt.Printf("❌ Message 7208 #%d: Record %d would exceed data length (offset %d + 262 > %d)\n", currentCount, i+1, offset, len(data))
			}
			break
		}

		var msg Message7208

		// Parse INTERACTIVE ONLY MBP DATA structure
		msg.Token = binary.BigEndian.Uint32(data[offset : offset+4])
		msg.BookType = binary.BigEndian.Uint16(data[offset+4 : offset+6])
		msg.TradingStatus = binary.BigEndian.Uint16(data[offset+6 : offset+8])
		msg.VolumeTradedToday = int64(binary.BigEndian.Uint64(data[offset+8 : offset+16]))
		msg.LastTradedPrice = binary.BigEndian.Uint32(data[offset+16 : offset+20])
		msg.NetChangeIndicator = data[offset+20]
		msg.NetPriceChangeFromClosingPrice = binary.BigEndian.Uint32(data[offset+22 : offset+26])
		msg.LastTradeQuantity = binary.BigEndian.Uint32(data[offset+26 : offset+30])
		msg.LastTradeTime = binary.BigEndian.Uint32(data[offset+30 : offset+34])
		msg.AverageTradePrice = binary.BigEndian.Uint32(data[offset+34 : offset+38])
		msg.AuctionNumber = binary.BigEndian.Uint16(data[offset+38 : offset+40])
		msg.AuctionStatus = binary.BigEndian.Uint16(data[offset+40 : offset+42])
		msg.InitiatorType = binary.BigEndian.Uint16(data[offset+42 : offset+44])
		msg.InitiatorPrice = binary.BigEndian.Uint32(data[offset+44 : offset+48])
		msg.InitiatorQuantity = binary.BigEndian.Uint32(data[offset+48 : offset+52])
		msg.AuctionPrice = binary.BigEndian.Uint32(data[offset+52 : offset+56])
		msg.AuctionQuantity = binary.BigEndian.Uint32(data[offset+56 : offset+60])

		// Parse MBP INFORMATION (10 levels × 16 bytes = 160 bytes)
		mbpOffset := offset + 60
		for j := 0; j < 10; j++ {
			mbpStart := mbpOffset + (j * 16)
			if mbpStart+16 <= offset+262 {
				msg.MBPData[j].Quantity = int64(binary.BigEndian.Uint64(data[mbpStart : mbpStart+8]))
				msg.MBPData[j].Price = binary.BigEndian.Uint32(data[mbpStart+8 : mbpStart+12])
				msg.MBPData[j].NumberOfOrders = binary.BigEndian.Uint16(data[mbpStart+12 : mbpStart+14])
				msg.MBPData[j].BbBuySellFlag = binary.BigEndian.Uint16(data[mbpStart+14 : mbpStart+16])
			}
		}

		msg.BbTotalBuyFlag = binary.BigEndian.Uint16(data[offset+220 : offset+222])
		msg.BbTotalSellFlag = binary.BigEndian.Uint16(data[offset+222 : offset+224])
		msg.TotalBuyQuantity = int64(binary.BigEndian.Uint64(data[offset+224 : offset+232]))
		msg.TotalSellQuantity = int64(binary.BigEndian.Uint64(data[offset+232 : offset+240]))
		msg.ClosingPrice = binary.BigEndian.Uint32(data[offset+242 : offset+246])
		msg.OpenPrice = binary.BigEndian.Uint32(data[offset+246 : offset+250])
		msg.HighPrice = binary.BigEndian.Uint32(data[offset+250 : offset+254])
		msg.LowPrice = binary.BigEndian.Uint32(data[offset+254 : offset+258])
		msg.IndicativeClosePrice = binary.BigEndian.Uint32(data[offset+258 : offset+262])

		// Export to CSV
		exportTo7208CSV(msg)
		recordsProcessed++

		offset += 262
	}
	
	if currentCount <= 3 {
		fmt.Printf("✅ Message 7208 #%d: Successfully processed %d records\n", currentCount, recordsProcessed)
	}
}

func exportTo7208CSV(msg Message7208) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Convert prices from paise to rupees
	ltpRupees := float64(msg.LastTradedPrice) / 100.0
	avgPriceRupees := float64(msg.AverageTradePrice) / 100.0
	closePriceRupees := float64(msg.ClosingPrice) / 100.0
	openPriceRupees := float64(msg.OpenPrice) / 100.0
	highPriceRupees := float64(msg.HighPrice) / 100.0
	lowPriceRupees := float64(msg.LowPrice) / 100.0
	indicativeClosePriceRupees := float64(msg.IndicativeClosePrice) / 100.0
	netChangeRupees := float64(msg.NetPriceChangeFromClosingPrice) / 100.0

	// Extract buy and sell prices (first 5 levels of each)
	var buyPrices [5]float64
	var buyQtys [5]int64
	var sellPrices [5]float64
	var sellQtys [5]int64

	buyIdx := 0
	sellIdx := 0

	for i := 0; i < 10; i++ {
		if msg.MBPData[i].BbBuySellFlag == 0 && buyIdx < 5 { // Buy order
			buyPrices[buyIdx] = float64(msg.MBPData[i].Price) / 100.0
			buyQtys[buyIdx] = msg.MBPData[i].Quantity
			buyIdx++
		} else if msg.MBPData[i].BbBuySellFlag == 1 && sellIdx < 5 { // Sell order
			sellPrices[sellIdx] = float64(msg.MBPData[i].Price) / 100.0
			sellQtys[sellIdx] = msg.MBPData[i].Quantity
			sellIdx++
		}
	}

	record := []string{
		timestamp,
		"7208",
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.BookType),
		fmt.Sprintf("%d", msg.TradingStatus),
		fmt.Sprintf("%d", msg.VolumeTradedToday),
		fmt.Sprintf("%.2f", ltpRupees),
		fmt.Sprintf("%c", msg.NetChangeIndicator),
		fmt.Sprintf("%.2f", netChangeRupees),
		fmt.Sprintf("%d", msg.LastTradeQuantity),
		fmt.Sprintf("%d", msg.LastTradeTime),
		fmt.Sprintf("%.2f", avgPriceRupees),
		fmt.Sprintf("%d", msg.TotalBuyQuantity),
		fmt.Sprintf("%d", msg.TotalSellQuantity),
		fmt.Sprintf("%.2f", closePriceRupees),
		fmt.Sprintf("%.2f", openPriceRupees),
		fmt.Sprintf("%.2f", highPriceRupees),
		fmt.Sprintf("%.2f", lowPriceRupees),
		fmt.Sprintf("%.2f", indicativeClosePriceRupees),
		fmt.Sprintf("%.2f", buyPrices[0]),
		fmt.Sprintf("%d", buyQtys[0]),
		fmt.Sprintf("%.2f", sellPrices[0]),
		fmt.Sprintf("%d", sellQtys[0]),
		fmt.Sprintf("%.2f", buyPrices[1]),
		fmt.Sprintf("%d", buyQtys[1]),
		fmt.Sprintf("%.2f", sellPrices[1]),
		fmt.Sprintf("%d", sellQtys[1]),
		fmt.Sprintf("%.2f", buyPrices[2]),
		fmt.Sprintf("%d", buyQtys[2]),
		fmt.Sprintf("%.2f", sellPrices[2]),
		fmt.Sprintf("%d", sellQtys[2]),
		fmt.Sprintf("%.2f", buyPrices[3]),
		fmt.Sprintf("%d", buyQtys[3]),
		fmt.Sprintf("%.2f", sellPrices[3]),
		fmt.Sprintf("%d", sellQtys[3]),
		fmt.Sprintf("%.2f", buyPrices[4]),
		fmt.Sprintf("%d", buyQtys[4]),
		fmt.Sprintf("%.2f", sellPrices[4]),
		fmt.Sprintf("%d", sellQtys[4]),
	}

	csvWriter7208.Write(record)
	csvWriter7208.Flush()

	atomic.AddInt64(&message7208Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg7208 := atomic.LoadInt64(&message7208Count)
	saved7208 := atomic.LoadInt64(&message7208Saved)

	if duration > 0 {
		fmt.Printf("\n%.0fs | %d pkts (%.0f/s) | 7208: %d msgs, %d saved\n",
			duration, packets, float64(packets)/duration, msg7208, saved7208)
		
		// Show all message codes detected in organized format
		if len(messageCodeCounts) > 0 {
			fmt.Printf("Message codes found: ")
			for code, count := range messageCodeCounts {
				if code == 7208 {
					fmt.Printf("7208(%d) ", count)
				}
			}
			for code, count := range messageCodeCounts {
				if code != 7208 {
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
	msg7208 := atomic.LoadInt64(&message7208Count)
	saved7208 := atomic.LoadInt64(&message7208Saved)

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("FINAL STATISTICS - Message 7208 Decoder")
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("Runtime: %v\n", duration)
	fmt.Printf("Total Packets Received: %d\n", packets)
	fmt.Printf("Message 7208 Count: %d\n", msg7208)
	fmt.Printf("Records Saved: %d\n", saved7208)
	
	if len(messageCodeCounts) > 0 {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Println("All Message Codes Detected:")
		for code, count := range messageCodeCounts {
			if code == 7208 {
				fmt.Printf("  %d (0x%04X) [TARGET]: %d occurrences\n", code, code, count)
			} else {
				fmt.Printf("  %d (0x%04X): %d occurrences\n", code, code, count)
			}
		}
	}
	fmt.Println(strings.Repeat("=", 60))
}
