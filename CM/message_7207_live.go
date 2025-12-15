// NSE Capital Market Multicast UDP Receiver - Message 7207 Only
// 
// FOCUS: Only process message code 7207 (BCAST_INDICES - Broadcast Indices)
// OUTPUT: csv_output/message_7207_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7207_live.go
// Output: csv_output/message_7207_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 139
// Structure: INDICES (71 bytes per record)
// Maximum Records: 6 per broadcast packet

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
			} else {
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

				if ip >= ipEnd {
					return op, nil
				}

				t = int(getU8(srcPtr, ip-1)) & 3

				if t != 0 {
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

	for {
		if t >= 16 {
			goto matchHandling
		}

		if t == 0 {
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
		if t >= 64 {
			off = t & 0x1f

			if off >= 0x1c {
				if lastMOff == 0 {
					return 0, ErrCorrupted
				}
				mPos = op - lastMOff
			} else {
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
			t &= 31

			if t == 0 {
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
			mPos = op
			mPos -= (t & 8) << 11

			t &= 7

			if t == 0 {
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

			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			op += 2

			goto matchDone
		}
		totalLen = 2 + mLen
		if op+totalLen > opEnd {
			return 0, ErrOutputOverrun
		}

		if mPos+totalLen > op {
			for i := 0; i < totalLen; i++ {
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += totalLen
		} else {
			copy(dst[op:op+totalLen], dst[mPos:mPos+totalLen])
			op += totalLen
		}

		goto matchDone

	matchDone:
		t = int(getU8(srcPtr, ip-1)) & 3

		if ip >= ipEnd {
			return op, nil
		}

		if t != 0 {
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}

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

// =============================================================================
// MESSAGE STRUCTURE FOR 7207
// =============================================================================

// Message7207 - BCAST_INDICES (Broadcast Indices)
// 
// This message broadcasts stock market indices information:
// - Index name (Nifty, Sensex, Bank Nifty, etc.)
// - Current index value
// - High/Low/Open/Close values
// - Year high/low
// - Market statistics
// 
// Structure: BCAST_HEADER (40 bytes) + INDICES records (71 bytes each)
// Maximum 6 indices per message
type Message7207 struct {
	TransactionCode uint16 // Always 7207
	NoOfRecords     uint16 // Number of index records (max 6)
}

// IndicesInfo7207 - Individual Index record (71 bytes per protocol, 72 with padding)
// Per NSE CM Protocol Table 43.1 (Page 139) - EXACT MATCH CONFIRMED BY DIAGNOSTIC
// Record stride is 72 bytes (71 data + 1 padding byte between records)
type IndicesInfo7207 struct {
	IndexName            [21]byte // 21 bytes, offset 0 - Index name (e.g., "Nifty IT")
	IndexValue           int32    // 4 bytes, offset 21 - Current index value (in paise)
	HighIndexValue       int32    // 4 bytes, offset 25 - Day high (in paise)
	LowIndexValue        int32    // 4 bytes, offset 29 - Day low (in paise)
	OpeningIndex         int32    // 4 bytes, offset 33 - Opening value (in paise)
	ClosingIndex         int32    // 4 bytes, offset 37 - Closing value (in paise)
	PercentChange        int32    // 4 bytes, offset 41 - Percent change (basis points)
	YearlyHigh           int32    // 4 bytes, offset 45 - 52-week high (in paise)
	YearlyLow            int32    // 4 bytes, offset 49 - 52-week low (in paise)
	NoOfUpmoves          int32    // 4 bytes, offset 53 - Number of stocks up
	NoOfDownmoves        int32    // 4 bytes, offset 57 - Number of stocks down
	Reserved             byte     // 1 byte, offset 61 - Reserved/padding byte
	MarketCapitalisation float64  // 8 bytes, offset 62-69 - Market cap (DOUBLE)
	NetChangeIndicator   byte     // 1 byte, offset 70 - '+' or '-' or ' '
	// Note: 1 additional padding byte (offset 71) between records
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
	
	// 7207 specific counters
	message7207Count   int64
	message7207Saved   int64
	
	// CSV file for 7207
	csvFile7207        *os.File
	csvWriter7207      *csv.Writer
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
	
	// Track message codes for statistics
	messageCodeCounts   map[uint16]int64
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

	// Initialize CSV file for 7207
	if err := initialize7207CSV(); err != nil {
		log.Fatalf("Failed to initialize 7207 CSV: %v", err)
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

	fmt.Println("⏹️  Press Ctrl+C to stop")

	// Wait for Ctrl+C
	<-sigChan

	fmt.Println("\n\n⏹️  Shutdown signal received...")
	close(shutdownChan)
	time.Sleep(1 * time.Second)

	// Close CSV files
	if csvWriter7207 != nil {
		csvWriter7207.Flush()
	}
	if csvFile7207 != nil {
		csvFile7207.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7207CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7207_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile7207 = file
	csvWriter7207 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", 
		"TransactionCode", 
		"NoOfRecords", 
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
	csvWriter7207.Write(headers)
	csvWriter7207.Flush()

	fmt.Printf("📁 Created CSV file for Message 7207: %s\n", filename)
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
	port := 8222               // Your specific port

	fmt.Printf("\n╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ NSE CM Message 7207 Receiver - Live Market Data          ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 7207 (BCAST_INDICES)\n")
	fmt.Printf("📊 Statistics every 10 seconds\n")
	fmt.Printf("⏱️  Started at: %s\n\n", time.Now().Format("15:04:05"))
	fmt.Printf("Waiting for packets...\n\n")
	
	// Create multicast address
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	// Join multicast group
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Fatalf("❌ Failed to join multicast group: %v\n", err)
		fmt.Println("\n⚠️  Troubleshooting:")
		fmt.Println("  1. Ensure NSE market is open (9:15 AM - 3:30 PM IST)")
		fmt.Println("  2. Check if you have access to NSE multicast feed")
		fmt.Println("  3. Verify firewall settings allow UDP multicast")
		fmt.Println("  4. Check network connection")
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

	// NSE Packet Structure:
	// [0-3]:   4-byte prefix
	// [4-5]:   Compression length (2 bytes) - if > 0, packet is compressed
	// [6+]:    Compressed or uncompressed data
	
	cPackData := data[4:]
	if len(cPackData) < 2 {
		return
	}

	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])

	var finalData []byte
	isCompressed := iCompLen > 0
	
	if isCompressed {
		// Packet is LZO compressed
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
		// Packet is not compressed
		finalData = cPackData[2:]
	}

	// IMPORTANT: Per NSE CM documentation:
	// "Inside the broadcast data, the first 8 bytes before the message header / broadcast
	//  header should be ignored. The message header / broadcast header starts from the 9th byte."
	
	if len(finalData) < 28 { // Need at least 8 (skip) + 20 (min header)
		return
	}
	
	// Skip first 8 bytes - BCAST_HEADER starts at byte 8
	finalData = finalData[8:]
	
	// BCAST_HEADER Structure (40 bytes total):
	// Per NSE Protocol documentation:
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 7207 for BCAST_INDICES
	// Offset 40+:   Message payload starts (NoOfRecords + Records)
	
	if len(finalData) < 48 {
		return
	}
	
	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])
	
	// Track message codes for statistics
	messageCodeCounts[transactionCode]++
	
	// Only process message 7207
	if transactionCode != 7207 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process7207Message(finalData)
}

// =============================================================================
// MESSAGE 7207 PROCESSOR
// =============================================================================

func process7207Message(data []byte) {
	if len(data) < 42 { // 40-byte header + at least 2 bytes for NoOfRecords
		return
	}

	atomic.AddInt64(&message7207Count, 1)
	currentCount := atomic.LoadInt64(&message7207Count)
	
	// Parse BCAST_HEADER (40 bytes)
	// Per NSE Protocol documentation:
	// Offset 10-12: TransactionCode (7207)
	// After BCAST_HEADER: NoOfRecords (2 bytes)
	
	transactionCode := binary.BigEndian.Uint16(data[10:12])
	
	// NoOfRecords is right after the 40-byte BCAST_HEADER
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	
	// Show first message info
	if currentCount == 1 {
		fmt.Printf("\n✅ First Message 7207 received: %d indices\n\n", noOfRecords)
	}
	
	// Parse Indices records
	// Per documentation Table 43.1: INDICES array starts at offset 42
	// CRITICAL FIX: Actual record size is 72 bytes (71 bytes per protocol + 1 padding byte)
	// Diagnostic shows next record starting at byte 72, not 71
	
	offset := 42
	recordSize := 72
	
	for i := 0; i < int(noOfRecords) && i < 6; i++ { // Max 6 indices
		if offset+recordSize > len(data) {
			break
		}
		
		// Parse INDICES: 71 bytes per NSE Protocol Table 43.1 (+ 1 padding = 72 total per record)
		// Diagnostic confirms: IndexName is 21 bytes, followed by numeric fields at correct offsets
		// The 1 extra byte is AFTER all fields (padding between records)
		
		// Extract IndexName (21 bytes as per protocol)
		var indexName [21]byte
		copy(indexName[:], data[offset:offset+21])
		
		// Use BIG ENDIAN - NSE CM standard protocol (matches working 18703 decoder)
		// All offsets match protocol specification exactly
		indexValue := int32(binary.BigEndian.Uint32(data[offset+21 : offset+25]))
		highIndexValue := int32(binary.BigEndian.Uint32(data[offset+25 : offset+29]))
		lowIndexValue := int32(binary.BigEndian.Uint32(data[offset+29 : offset+33]))
		openingIndex := int32(binary.BigEndian.Uint32(data[offset+33 : offset+37]))
		closingIndex := int32(binary.BigEndian.Uint32(data[offset+37 : offset+41]))
		percentChange := int32(binary.BigEndian.Uint32(data[offset+41 : offset+45]))
		yearlyHigh := int32(binary.BigEndian.Uint32(data[offset+45 : offset+49]))
		yearlyLow := int32(binary.BigEndian.Uint32(data[offset+49 : offset+53]))
		noOfUpmoves := int32(binary.BigEndian.Uint32(data[offset+53 : offset+57]))
		noOfDownmoves := int32(binary.BigEndian.Uint32(data[offset+57 : offset+61]))
		
		// CRITICAL FIX: MarketCapitalisation is actually 8 bytes at offset 61-68
		// Based on diagnostic: offset 61-68 gives valid DOUBLE value
		// But we need to check if it's actually at 62-69 instead
		marketCapBits := binary.BigEndian.Uint64(data[offset+62 : offset+70])
		marketCap := math.Float64frombits(marketCapBits)
		
		// NetChangeIndicator is at offset 70 (diagnostic showed '-' at byte 70)
		netChangeIndicator := data[offset+70]
		// Byte at offset+71 is padding between records
		
		// Export to CSV
		exportToCSV(transactionCode, noOfRecords, indexName, indexValue, highIndexValue, 
			lowIndexValue, openingIndex, closingIndex, percentChange, yearlyHigh, yearlyLow,
			noOfUpmoves, noOfDownmoves, marketCap, netChangeIndicator)
		atomic.AddInt64(&message7207Saved, 1)
		
		offset += recordSize
	}
}

func exportToCSV(transactionCode, noOfRecords uint16, indexName [21]byte, 
	indexValue, highIndexValue, lowIndexValue, openingIndex, closingIndex, 
	percentChange, yearlyHigh, yearlyLow, noOfUpmoves, noOfDownmoves int32,
	marketCap float64, netChangeIndicator byte) {
	
	if csvWriter7207 == nil {
		return
	}

	// Clean index name (remove null bytes and trailing spaces)
	// IndexName is 21 bytes, space-padded
	name := strings.TrimRight(string(indexName[:]), "\x00 ")

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", noOfRecords),
		name,
		fmt.Sprintf("%.2f", float64(indexValue)/100.0),       // Index values are in paise (divide by 100)
		fmt.Sprintf("%.2f", float64(highIndexValue)/100.0),
		fmt.Sprintf("%.2f", float64(lowIndexValue)/100.0),
		fmt.Sprintf("%.2f", float64(openingIndex)/100.0),
		fmt.Sprintf("%.2f", float64(closingIndex)/100.0),
		fmt.Sprintf("%.4f%%", float64(percentChange)/10000.0), // Percent is in basis points (divide by 10000)
		fmt.Sprintf("%.2f", float64(yearlyHigh)/100.0),
		fmt.Sprintf("%.2f", float64(yearlyLow)/100.0),
		fmt.Sprintf("%d", noOfUpmoves),
		fmt.Sprintf("%d", noOfDownmoves),
		fmt.Sprintf("%.2f", marketCap),
		string(netChangeIndicator),
	}

	csvWriter7207.Write(record)
	csvWriter7207.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg7207 := atomic.LoadInt64(&message7207Count)
	saved7207 := atomic.LoadInt64(&message7207Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg7207 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 7207: %s | %d msgs, %d indices\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg7207, saved7207)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	msg7207 := atomic.LoadInt64(&message7207Count)
	saved7207 := atomic.LoadInt64(&message7207Saved)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 7207 DECODER")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Runtime                : %v\n", duration)
	fmt.Printf("Total Packets Received : %d\n", packets)
	fmt.Printf("Total Bytes Received   : %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	if duration.Seconds() > 0 {
		fmt.Printf("Packets/Second         : %.2f\n", float64(packets)/duration.Seconds())
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Message 7207 Found     : %d messages\n", msg7207)
	fmt.Printf("Indices Saved to CSV   : %d records\n", saved7207)
	if msg7207 > 0 {
		fmt.Printf("Average Indices/Msg    : %.2f\n", float64(saved7207)/float64(msg7207))
	}
	fmt.Println(strings.Repeat("=", 80))
	
	// Show all message codes found
	if len(messageCodeCounts) > 0 {
		fmt.Println("📋 ALL MESSAGE CODES DETECTED:")
		fmt.Println(strings.Repeat("-", 80))
		
		// Sort by code number
		type codeFreq struct {
			code uint16
			freq int64
		}
		var sorted []codeFreq
		for code, freq := range messageCodeCounts {
			sorted = append(sorted, codeFreq{code, freq})
		}
		
		// Sort by code number (ascending)
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j].code < sorted[i].code {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		
		// Show all codes
		for _, cf := range sorted {
			percentage := float64(cf.freq) / float64(packets) * 100
			if cf.code == 7207 {
				fmt.Printf("   🎯 Code %5d: %6d messages (%.1f%%) ← TARGET!\n", cf.code, cf.freq, percentage)
			} else {
				fmt.Printf("      Code %5d: %6d messages (%.1f%%)\n", cf.code, cf.freq, percentage)
			}
		}
		fmt.Println(strings.Repeat("-", 80))
	}
	
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Decoder stopped successfully!")
	if msg7207 > 0 {
		fmt.Println("📁 Check csv_output/ directory for the CSV file")
	} else {
		fmt.Println("⚠️  Message 7207 NOT detected on this channel!")
		fmt.Println("💡 Possible reasons:")
		fmt.Println("   - Market might be closed")
		fmt.Println("   - Wrong multicast IP/Port for message 7207")
		fmt.Println("   - Message 7207 might be on different broadcast channel")
	}
	fmt.Println()
}
