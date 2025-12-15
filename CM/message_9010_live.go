// NSE Capital Market Multicast UDP Receiver - Message 9010 Only
// 
// FOCUS: Only process message code 9010 (BCAST_TURNOVER_EXCEEDED - Broker Turnover Limit Alert)
// OUTPUT: csv_output/message_9010_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_9010_live.go
// Output: csv_output/message_9010_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 102-104 (Table: BROADCAST_LIMIT_EXCEEDED)
// Structure: BROADCAST_LIMIT_EXCEEDED (77 bytes)
// Contains: Broker turnover limit warnings and violations

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
						t += tt
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
						t += tt
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
						t += tt
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
						t += tt
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
// MESSAGE 9010 STRUCTURE
// =============================================================================

// Message9010 - BCAST_TURNOVER_EXCEEDED (77 bytes)
// Protocol: NSE CM NNF Protocol v6.3, Page 102-104
type Message9010 struct {
	TransactionCode   uint16   // Always 9010
	BrokerCode        [5]byte  // Broker who exceeded/approaching limit
	CounterBrokerCode [5]byte  // Not in use
	WarningType       int16    // 1=About to exceed, 2=Exceeded (deactivated)
	Symbol            [10]byte // Symbol of last trade
	Series            [2]byte  // Series of last trade
	TradeNumber       int32    // Trade number
	TradePrice        int32    // Price in paise
	TradeVolume       int32    // Quantity
	Final             byte     // Final auction trade indicator
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

	// 9010 specific counters
	message9010Count int64
	message9010Saved int64

	// CSV file for 9010
	csvFile9010   *os.File
	csvWriter9010 *csv.Writer

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

	// Initialize CSV file for 9010
	if err := initialize9010CSV(); err != nil {
		log.Fatalf("Failed to initialize 9010 CSV: %v", err)
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
	if csvWriter9010 != nil {
		csvWriter9010.Flush()
	}
	if csvFile9010 != nil {
		csvFile9010.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize9010CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_9010_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile9010 = file
	csvWriter9010 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"BrokerCode",
		"WarningType",
		"WarningDescription",
		"Symbol",
		"Series",
		"TradeNumber",
		"TradePrice",
		"TradeVolume",
		"Final",
	}
	csvWriter9010.Write(headers)
	csvWriter9010.Flush()

	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	// After Market Hours Multicast
	multicastIP := "231.31.31.4"
	port := 18901

	// Live Market Hours Multicast
	//multicastIP := "233.1.2.5"
	//port := 8222

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
		if transactionCode == 9010 {
			fmt.Printf("🎯 TARGET message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		} else {
			fmt.Printf("📊 New message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		}
	}

	// Only process message 9010
	if transactionCode != 9010 {
		return
	}

	process9010Message(finalData)
}

// =============================================================================
// MESSAGE 9010 PROCESSOR
// =============================================================================

func process9010Message(data []byte) {
	if len(data) < 77 {
		return
	}

	atomic.AddInt64(&message9010Count, 1)

	// Parse message structure (BigEndian encoding)
	var msg Message9010

	// Transaction code at offset 10 in BCAST_HEADER
	msg.TransactionCode = binary.BigEndian.Uint16(data[10:12])

	// Protocol structure: MESSAGE_HEADER(40) + LimitType(1) + BrokerId(5) + ...
	// LimitType at offset 40 (1 byte)
	// BrokerCode at offset 41-45 (5 bytes)
	copy(msg.BrokerCode[:], data[41:46])
	copy(msg.CounterBrokerCode[:], data[46:51])
	msg.WarningType = int16(binary.BigEndian.Uint16(data[51:53]))

	// SEC_INFO fields (symbol and series) - adjusted for correct offset
	copy(msg.Symbol[:], data[53:63])
	copy(msg.Series[:], data[63:65])

	// Trade details - adjusted for correct offset
	msg.TradeNumber = int32(binary.BigEndian.Uint32(data[65:69]))
	msg.TradePrice = int32(binary.BigEndian.Uint32(data[69:73]))
	msg.TradeVolume = int32(binary.BigEndian.Uint32(data[73:77]))
	if len(data) > 77 {
		msg.Final = data[77]
	}

	// Export to CSV
	exportTo9010CSV(msg)
}

func exportTo9010CSV(msg Message9010) {
	// Clean strings
	brokerCode := strings.TrimSpace(string(msg.BrokerCode[:]))
	brokerCode = strings.Trim(brokerCode, "\x00")

	symbol := strings.TrimSpace(string(msg.Symbol[:]))
	symbol = strings.Trim(symbol, "\x00")

	series := strings.TrimSpace(string(msg.Series[:]))
	series = strings.Trim(series, "\x00")

	// Warning type description
	var warningDesc string
	switch msg.WarningType {
	case 1:
		warningDesc = "About to exceed limit"
	case 2:
		warningDesc = "Limit exceeded - Broker deactivated"
	default:
		warningDesc = fmt.Sprintf("Unknown (%d)", msg.WarningType)
	}

	// Convert trade price from paise to rupees
	tradePrice := float64(msg.TradePrice) / 100.0

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	record := []string{
		timestamp,
		fmt.Sprintf("%d", msg.TransactionCode),
		brokerCode,
		fmt.Sprintf("%d", msg.WarningType),
		warningDesc,
		symbol,
		series,
		fmt.Sprintf("%d", msg.TradeNumber),
		fmt.Sprintf("%.2f", tradePrice),
		fmt.Sprintf("%d", msg.TradeVolume),
		fmt.Sprintf("%c", msg.Final),
	}

	csvWriter9010.Write(record)
	csvWriter9010.Flush()

	atomic.AddInt64(&message9010Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg9010 := atomic.LoadInt64(&message9010Count)
	saved9010 := atomic.LoadInt64(&message9010Saved)

	if duration > 0 {
		fmt.Printf("%.0fs | %d pkts (%.0f/s) | 9010: %d msgs, %d saved\n",
			duration, packets, float64(packets)/duration, msg9010, saved9010)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg9010 := atomic.LoadInt64(&message9010Count)
	saved9010 := atomic.LoadInt64(&message9010Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - Message 9010 Decoder\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Runtime: %v | Packets: %d | Message 9010: %d | Saved: %d\n",
		duration, packets, msg9010, saved9010)
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique):\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		for code, count := range messageCodeCounts {
			if code == 9010 {
				fmt.Printf("  %d (0x%04X) [TARGET]: %d occurrences\n", code, code, count)
			} else {
				fmt.Printf("  %d (0x%04X): %d occurrences\n", code, code, count)
			}
		}
	}
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}
