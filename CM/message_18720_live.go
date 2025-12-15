// NSE Capital Market Multicast UDP Receiver - Message 18720 Only
// 
// FOCUS: Only process message code 18720 (BCAST_SECURITY_MSTR_CHG - Security Master Change)
// OUTPUT: csv_output/message_18720_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18720_live.go
// Output: csv_output/message_18720_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Pages 94-98
// Structure: SECURITY UPDATE INFORMATION (260 bytes)
// Contains: Security master data changes (Symbol, ISIN, Token, Instrument Type, etc.)

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
// MESSAGE 18720 STRUCTURE
// =============================================================================

// Message18720 - SECURITY UPDATE INFORMATION (260 bytes)
// Protocol: NSE CM NNF Protocol v6.3, Pages 94-98
type Message18720 struct {
	Token                uint32
	Symbol               [10]byte
	Series               [2]byte
	InstrumentType       uint16
	PermittedToTrade     uint16
	IssuedCapital        float64
	SettlementType       uint16
	FreezePercent        uint16
	CreditRating         [19]byte
	IssueStartDate       uint32
	InterestPaymentDate  uint32
	IssueMaturityDate    uint32
	BoardLotQuantity     uint32
	TickSize             uint32
	Name                 [25]byte
	ListingDate          uint32
	ExpulsionDate        uint32
	ReAdmissionDate      uint32
	RecordDate           uint32
	ExpiryDate           uint32
	NoDeliveryStartDate  uint32
	NoDeliveryEndDate    uint32
	BookClosureStartDate uint32
	BookClosureEndDate   uint32
	LocalUpdateDateTime  uint32
	DeleteFlag           byte
	Remark               [25]byte
	FaceValue            uint32
	ISINNumber           [12]byte
	MktMakerSpread       uint32
	MktMakerMinQty       uint32
	CallAuction1Flag     uint16
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

	// 18720 specific counters
	message18720Count int64
	message18720Saved int64

	// CSV file for 18720
	csvFile18720   *os.File
	csvWriter18720 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 18720 (BCAST_SECURITY_MSTR_CHG)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 18720 (0x492C in hex)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18720
	if err := initialize18720CSV(); err != nil {
		log.Fatalf("Failed to initialize 18720 CSV: %v", err)
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
	if csvWriter18720 != nil {
		csvWriter18720.Flush()
	}
	if csvFile18720 != nil {
		csvFile18720.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize18720CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_18720_%s.csv", timestamp))

	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	csvFile18720 = file
	csvWriter18720 = csv.NewWriter(file)

	// Write headers
	headers := []string{
		"Timestamp",
		"TransactionCode",
		"Token",
		"Symbol",
		"Series",
		"InstrumentType",
		"PermittedToTrade",
		"IssuedCapital",
		"TickSize",
		"Name",
		"ISINNumber",
		"FaceValue",
		"BoardLotQuantity",
		"ListingDate",
		"ExpiryDate",
		"IssueStartDate",
		"IssueMaturityDate",
		"BookClosureStartDate",
		"BookClosureEndDate",
		"NoDeliveryStartDate",
		"NoDeliveryEndDate",
		"DeleteFlag",
	}
	csvWriter18720.Write(headers)
	csvWriter18720.Flush()

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

	// Read transaction code at offset 10-12 (BigEndian SHORT)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes
	messageCodeCounts[transactionCode]++

	// Debug: Print first occurrence of each message code
	if messageCodeCounts[transactionCode] == 1 {
		fmt.Printf("📊 Found message code: %d (hex: 0x%04X) - first occurrence\n", transactionCode, transactionCode)
	}

	// Only process message 18720
	if transactionCode != 18720 {
		return
	}

	fmt.Printf("✅ Message 18720 (0x%04X) received! Processing...\n", transactionCode)
	process18720Message(finalData)
}

// =============================================================================
// MESSAGE 18720 PROCESSOR
// =============================================================================

func process18720Message(data []byte) {
	if len(data) < 260 {
		return
	}

	atomic.AddInt64(&message18720Count, 1)

	var msg Message18720

	// Parse SECURITY UPDATE INFORMATION structure
	msg.Token = binary.BigEndian.Uint32(data[40:44])
	
	// SEC_INFO at offset 44 (12 bytes: Symbol 10 bytes + Series 2 bytes)
	copy(msg.Symbol[:], data[44:54])
	copy(msg.Series[:], data[54:56])
	
	msg.InstrumentType = binary.BigEndian.Uint16(data[56:58])
	msg.PermittedToTrade = binary.BigEndian.Uint16(data[58:60])
	
	// IssuedCapital is DOUBLE (8 bytes)
	issuedCapBits := binary.BigEndian.Uint64(data[60:68])
	msg.IssuedCapital = math.Float64frombits(issuedCapBits)
	
	msg.SettlementType = binary.BigEndian.Uint16(data[68:70])
	msg.FreezePercent = binary.BigEndian.Uint16(data[70:72])
	copy(msg.CreditRating[:], data[72:91])
	
	msg.IssueStartDate = binary.BigEndian.Uint32(data[118:122])
	msg.InterestPaymentDate = binary.BigEndian.Uint32(data[122:126])
	msg.IssueMaturityDate = binary.BigEndian.Uint32(data[126:130])
	msg.BoardLotQuantity = binary.BigEndian.Uint32(data[130:134])
	msg.TickSize = binary.BigEndian.Uint32(data[134:138])
	copy(msg.Name[:], data[138:163])
	
	msg.ListingDate = binary.BigEndian.Uint32(data[164:168])
	msg.ExpulsionDate = binary.BigEndian.Uint32(data[168:172])
	msg.ReAdmissionDate = binary.BigEndian.Uint32(data[172:176])
	msg.RecordDate = binary.BigEndian.Uint32(data[176:180])
	msg.ExpiryDate = binary.BigEndian.Uint32(data[180:184])
	msg.NoDeliveryStartDate = binary.BigEndian.Uint32(data[184:188])
	msg.NoDeliveryEndDate = binary.BigEndian.Uint32(data[188:192])
	
	msg.BookClosureStartDate = binary.BigEndian.Uint32(data[194:198])
	msg.BookClosureEndDate = binary.BigEndian.Uint32(data[198:202])
	msg.LocalUpdateDateTime = binary.BigEndian.Uint32(data[204:208])
	msg.DeleteFlag = data[208]
	copy(msg.Remark[:], data[209:234])
	msg.FaceValue = binary.BigEndian.Uint32(data[234:238])
	copy(msg.ISINNumber[:], data[238:250])
	msg.MktMakerSpread = binary.BigEndian.Uint32(data[250:254])
	msg.MktMakerMinQty = binary.BigEndian.Uint32(data[254:258])
	msg.CallAuction1Flag = binary.BigEndian.Uint16(data[258:260])

	// Export to CSV
	exportTo18720CSV(msg)
}

func exportTo18720CSV(msg Message18720) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean text fields
	symbol := strings.TrimSpace(string(msg.Symbol[:]))
	symbol = strings.Trim(symbol, "\x00")
	series := strings.TrimSpace(string(msg.Series[:]))
	series = strings.Trim(series, "\x00")
	name := strings.TrimSpace(string(msg.Name[:]))
	name = strings.Trim(name, "\x00")
	isinNumber := strings.TrimSpace(string(msg.ISINNumber[:]))
	isinNumber = strings.Trim(isinNumber, "\x00")

	// Convert tick size from paise to rupees
	tickSizeRupees := float64(msg.TickSize) / 100.0
	faceValueRupees := float64(msg.FaceValue) / 100.0

	// Convert dates (seconds since Jan 1, 1980)
	baseTime := time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
	
	var listingDate, expiryDate, issueStartDate, issueMaturityDate string
	var bookClosureStart, bookClosureEnd, noDeliveryStart, noDeliveryEnd string
	
	if msg.ListingDate > 0 {
		listingDate = baseTime.Add(time.Duration(msg.ListingDate) * time.Second).Format("2006-01-02")
	}
	if msg.ExpiryDate > 0 {
		expiryDate = baseTime.Add(time.Duration(msg.ExpiryDate) * time.Second).Format("2006-01-02")
	}
	if msg.IssueStartDate > 0 {
		issueStartDate = baseTime.Add(time.Duration(msg.IssueStartDate) * time.Second).Format("2006-01-02")
	}
	if msg.IssueMaturityDate > 0 {
		issueMaturityDate = baseTime.Add(time.Duration(msg.IssueMaturityDate) * time.Second).Format("2006-01-02")
	}
	if msg.BookClosureStartDate > 0 {
		bookClosureStart = baseTime.Add(time.Duration(msg.BookClosureStartDate) * time.Second).Format("2006-01-02")
	}
	if msg.BookClosureEndDate > 0 {
		bookClosureEnd = baseTime.Add(time.Duration(msg.BookClosureEndDate) * time.Second).Format("2006-01-02")
	}
	if msg.NoDeliveryStartDate > 0 {
		noDeliveryStart = baseTime.Add(time.Duration(msg.NoDeliveryStartDate) * time.Second).Format("2006-01-02")
	}
	if msg.NoDeliveryEndDate > 0 {
		noDeliveryEnd = baseTime.Add(time.Duration(msg.NoDeliveryEndDate) * time.Second).Format("2006-01-02")
	}

	record := []string{
		timestamp,
		"18720",
		fmt.Sprintf("%d", msg.Token),
		symbol,
		series,
		fmt.Sprintf("%d", msg.InstrumentType),
		fmt.Sprintf("%d", msg.PermittedToTrade),
		fmt.Sprintf("%.2f", msg.IssuedCapital),
		fmt.Sprintf("%.2f", tickSizeRupees),
		name,
		isinNumber,
		fmt.Sprintf("%.2f", faceValueRupees),
		fmt.Sprintf("%d", msg.BoardLotQuantity),
		listingDate,
		expiryDate,
		issueStartDate,
		issueMaturityDate,
		bookClosureStart,
		bookClosureEnd,
		noDeliveryStart,
		noDeliveryEnd,
		fmt.Sprintf("%c", msg.DeleteFlag),
	}

	csvWriter18720.Write(record)
	csvWriter18720.Flush()

	atomic.AddInt64(&message18720Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg18720 := atomic.LoadInt64(&message18720Count)
	saved18720 := atomic.LoadInt64(&message18720Saved)

	if duration > 0 {
		fmt.Printf("%.0fs | %d pkts (%.0f/s) | 18720: %d msgs, %d saved\n",
			duration, packets, float64(packets)/duration, msg18720, saved18720)
		
		// Show all message codes found
		if len(messageCodeCounts) > 0 {
			fmt.Printf("   Message codes found: ")
			for code, count := range messageCodeCounts {
				fmt.Printf("%d(%d) ", code, count)
			}
			fmt.Println()
		}
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg18720 := atomic.LoadInt64(&message18720Count)
	saved18720 := atomic.LoadInt64(&message18720Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Runtime: %v | Packets: %d | Message 18720: %d | Saved: %d\n",
		duration, packets, msg18720, saved18720)
	
	// Show all message codes found
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\nAll message codes received:\n")
		for code, count := range messageCodeCounts {
			fmt.Printf("  Code %d: %d times\n", code, count)
		}
	}
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}
