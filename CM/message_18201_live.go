// NSE Capital Market Multicast UDP Receiver - Message 18201 Only
// 
// FOCUS: Only process message code 18201 (MARKET_STATS_REPORT_DATA - Bhav Copy)
// OUTPUT: csv_output/message_18201_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18201_live.go
// Output: csv_output/message_18201_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Post-Market Section
// Structure: Header (106 bytes) + Security Records (478 bytes each) + Trailer (46 bytes)
// Contains: End-of-day market statistics (Bhav Copy) for all securities

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
			if t == 0 {
				t = int(getU8(srcPtr, ip))
				ip++
				for t == 0 {
					if ip >= ipEnd {
						return op, nil
					}
					t = int(getU8(srcPtr, ip))
					ip++
				}

				if t > 15 {
					goto matchDone
				}
			}

			mPos = op - lastMOff
			if t < 8 {
				mLen = t + 1
			} else {
				mLen = t - 7
			}

			if mPos < 0 || mPos+mLen > op {
				return 0, ErrCorrupted
			}

			if op+mLen > opEnd {
				return 0, ErrOutputOverrun
			}

			for i := 0; i < mLen; i++ {
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += mLen

			lastMOff = 0

			if ip >= ipEnd {
				return op, nil
			}
			t = int(getU8(srcPtr, ip))
			ip++

			goto matchDone
		}

	matchDone:
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
// MESSAGE STRUCTURE FOR 18201
// =============================================================================

// BhavCopyHeader - Header for Bhav Copy broadcast (106 bytes)
type BhavCopyHeader struct {
	TransactionCode     uint16   // Always 18201
	NoOfRecords         uint32   // Number of security records following
	Date                uint32   // Bhav copy date
	Time                uint32   // Generation time
	Reserved            [94]byte // Reserved fields
}

// SecurityRecord - Individual security statistics (478 bytes)
// Per NSE Protocol: MARKET_STATS_REPORT_DATA structure
type SecurityRecord struct {
	// Basic identifiers
	Symbol              [10]byte // Security symbol
	Series              [2]byte  // Series (EQ, BE, etc.)
	Token               uint32   // Security token
	
	// Price data (all in paise)
	OpenPrice           uint32   // Day opening price
	HighPrice           uint32   // Day high price
	LowPrice            uint32   // Day low price
	ClosePrice          uint32   // Closing price
	LastTradePrice      uint32   // Last traded price
	PreviousClosePrice  uint32   // Previous day close
	
	// Volume and trade data
	TotalTradedQty      uint32   // Total volume traded
	TotalTradedValue    uint64   // Total turnover value
	TotalTrades         uint32   // Number of trades
	
	// 52-week data
	Week52High          uint32   // 52-week high
	Week52Low           uint32   // 52-week low
	
	// Price changes
	NetChange           int32    // Price change from previous close
	PercentChange       int32    // Percentage change (basis points)
	
	// Additional market data
	VWAP                uint32   // Volume weighted average price
	DeliveryQty         uint32   // Delivery quantity
	DeliveryPercent     uint32   // Delivery percentage
	
	// Market lot and other details
	MarketLot           uint32   // Market lot size
	FaceValue           uint32   // Face value
	
	// Reserved space for future fields
	Reserved            [350]byte // Remaining bytes to make 478 total
}

// BhavCopyTrailer - Trailer for Bhav Copy broadcast (46 bytes)
type BhavCopyTrailer struct {
	TotalRecords        uint32   // Total records processed
	Checksum            uint32   // Data integrity checksum
	Reserved            [38]byte // Reserved fields
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

	// 18201 specific counters
	message18201Count int64
	message18201Saved int64
	bhavCopyHeaders   int64
	bhavCopyTrailers  int64

	// CSV file for 18201
	csvFile18201   *os.File
	csvWriter18201 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 18201 (MARKET_STATS_REPORT_DATA)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 18201 (0x4719 in hex)\n")
	fmt.Printf("Purpose: Bhav Copy - End-of-day market statistics\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18201
	if err := initialize18201CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter18201 != nil {
			csvWriter18201.Flush()
		}
		if csvFile18201 != nil {
			csvFile18201.Close()
		}
	}()

	// Set up signal handling for graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Printf("\n\n⏹️  Shutdown signal received. Stopping gracefully...\n")
		shutdownChan <- true
	}()

	// Start goroutines
	go startUDPListener()
	go processPackets()

	// Print statistics every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			printStats()
		case <-shutdownChan:
			ticker.Stop()
			time.Sleep(500 * time.Millisecond) // Let final packets process
			printFinalStats()
			return
		}
	}
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize18201CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_18201_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile18201 = file
	csvWriter18201 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"Symbol",
		"Series",
		"Token",
		"OpenPrice_Rupees",
		"HighPrice_Rupees",
		"LowPrice_Rupees",
		"ClosePrice_Rupees",
		"LastTradePrice_Rupees",
		"PreviousClosePrice_Rupees",
		"TotalTradedQty",
		"TotalTradedValue_Rupees",
		"TotalTrades",
		"Week52High_Rupees",
		"Week52Low_Rupees",
		"NetChange_Rupees",
		"PercentChange",
		"VWAP_Rupees",
		"DeliveryQty",
		"DeliveryPercent",
		"MarketLot",
		"FaceValue_Rupees",
	}

	csvWriter18201.Write(header)
	csvWriter18201.Flush()

	fmt.Printf("📁 CSV file created: %s\n", filepath)
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


	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", multicastIP, port))
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Fatalf("Failed to listen on multicast address: %v", err)
		return
	}
	defer conn.Close()

	// Set buffer size to 2MB for high throughput
	conn.SetReadBuffer(2 * 1024 * 1024)

	fmt.Printf("🌐 Listening on %s:%d\n\n", multicastIP, port)

	buffer := make([]byte, 65536) // Larger buffer for Bhav Copy data

	for {
		select {
		case <-shutdownChan:
			return
		default:
			n, err := conn.Read(buffer)
			if err != nil {
				continue
			}

			atomic.AddInt64(&packetCount, 1)
			atomic.AddInt64(&totalBytes, int64(n))

			// Send copy of data to processing channel
			dataCopy := make([]byte, n)
			copy(dataCopy, buffer[:n])
			
			select {
			case packetChan <- dataCopy:
			default:
				// Drop packet if channel is full
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
		case data := <-packetChan:
			processUDPPacket(data)
		case <-shutdownChan:
			return
		}
	}
}

func processUDPPacket(data []byte) {
	// NSE packet structure:
	// Byte 0-1: NetID (2 chars)
	// Byte 2-3: NoOfPackets (short)
	// Byte 4+: cPackData (compressed data)

	if len(data) < 6 {
		return
	}

	// Extract cPackData starting from byte 4
	cPackData := data[4:]

	if len(cPackData) < 2 {
		return
	}

	// LZO compression check
	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])

	var finalData []byte
	if iCompLen > 0 {
		// Compressed packet
		atomic.AddInt64(&compressedCount, 1)
		compressedData := cPackData[2 : 2+iCompLen]
		decompressedData := make([]byte, 65536) // Larger buffer for Bhav Copy
		decompLen, err := DecompressUltra(compressedData, decompressedData)
		if err != nil {
			atomic.AddInt64(&decompressionErrors, 1)
			return
		}
		atomic.AddInt64(&decompressedCount, 1)
		finalData = decompressedData[:decompLen]
	} else {
		// Uncompressed packet
		finalData = cPackData[2:]
	}

	// IMPORTANT: Per NSE documentation (Page 152):
	// "Inside the broadcast data, the first 8 bytes before the message header / broadcast
	// header should be ignored. The message header / broadcast header starts from the 9th byte."

	if len(finalData) < 28 {
		return
	}

	// Skip first 8 bytes
	finalData = finalData[8:]

	// BCAST_HEADER Structure (40 bytes total):
	// Per NSE Protocol documentation:
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 18201 for MARKET_STATS_REPORT_DATA
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 18201
	if transactionCode != 18201 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process18201Message(finalData)
}

// =============================================================================
// MESSAGE 18201 PROCESSOR
// =============================================================================

func process18201Message(data []byte) {
	if len(data) < 42 { // Minimum length check - 40-byte header + 2 bytes minimum
		return
	}

	atomic.AddInt64(&message18201Count, 1)

	// Parse header to determine message structure
	offset := 40 // Skip BCAST_HEADER

	if len(data) < offset+106 {
		return // Not enough data for header
	}

	// Check if this is a header packet (contains header structure)
	if isBhavCopyHeader(data[offset:]) {
		atomic.AddInt64(&bhavCopyHeaders, 1)
		processBhavCopyHeader(data[offset:])
		return
	}

	// Check if this is a trailer packet
	if len(data) >= offset+46 && isBhavCopyTrailer(data[offset:]) {
		atomic.AddInt64(&bhavCopyTrailers, 1)
		processBhavCopyTrailer(data[offset:])
		return
	}

	// Otherwise, process as security records
	processSecurityRecords(data[offset:])
}

func isBhavCopyHeader(data []byte) bool {
	// Simple heuristic: Header typically has a specific pattern
	// Check if it looks like a header by examining the first few bytes
	if len(data) < 106 {
		return false
	}
	
	// Transaction code should be 18201 at the beginning
	transCode := binary.BigEndian.Uint16(data[0:2])
	return transCode == 18201
}

func isBhavCopyTrailer(data []byte) bool {
	// Simple heuristic: Trailer is typically small (46 bytes)
	// and contains summary information
	if len(data) < 46 {
		return false
	}
	
	// Check if the data looks like a trailer by examining patterns
	// Trailers typically have total record counts
	totalRecords := binary.BigEndian.Uint32(data[0:4])
	return totalRecords > 0 && totalRecords < 10000 // Reasonable range
}

func processBhavCopyHeader(data []byte) {
	var header BhavCopyHeader
	
	header.TransactionCode = binary.BigEndian.Uint16(data[0:2])
	header.NoOfRecords = binary.BigEndian.Uint32(data[2:6])
	header.Date = binary.BigEndian.Uint32(data[6:10])
	header.Time = binary.BigEndian.Uint32(data[10:14])
	copy(header.Reserved[:], data[14:106])
	
	fmt.Printf("📊 Bhav Copy Header: %d records, Date: %d, Time: %d\n", 
		header.NoOfRecords, header.Date, header.Time)
}

func processBhavCopyTrailer(data []byte) {
	var trailer BhavCopyTrailer
	
	trailer.TotalRecords = binary.BigEndian.Uint32(data[0:4])
	trailer.Checksum = binary.BigEndian.Uint32(data[4:8])
	copy(trailer.Reserved[:], data[8:46])
	
	fmt.Printf("📊 Bhav Copy Trailer: %d total records, Checksum: %d\n", 
		trailer.TotalRecords, trailer.Checksum)
}

func processSecurityRecords(data []byte) {
	// Process multiple security records in the packet
	recordSize := 478
	offset := 0
	
	for offset+recordSize <= len(data) {
		var record SecurityRecord
		
		// Parse security record
		copy(record.Symbol[:], data[offset:offset+10])
		offset += 10
		copy(record.Series[:], data[offset:offset+2])
		offset += 2
		record.Token = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Price data (all in paise)
		record.OpenPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.HighPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.LowPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.ClosePrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.LastTradePrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.PreviousClosePrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Volume and trade data
		record.TotalTradedQty = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.TotalTradedValue = binary.BigEndian.Uint64(data[offset : offset+8])
		offset += 8
		record.TotalTrades = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// 52-week data
		record.Week52High = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.Week52Low = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Price changes
		record.NetChange = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		record.PercentChange = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		
		// Additional market data
		record.VWAP = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.DeliveryQty = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.DeliveryPercent = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Market lot and face value
		record.MarketLot = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		record.FaceValue = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Skip remaining reserved bytes
		offset += 350
		
		// Export to CSV
		exportTo18201CSV(record)
	}
}

func exportTo18201CSV(record SecurityRecord) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean up symbol and series
	symbol := strings.TrimRight(string(record.Symbol[:]), "\x00")
	series := strings.TrimRight(string(record.Series[:]), "\x00")

	csvRecord := []string{
		timestamp,
		"18201",
		symbol,
		series,
		fmt.Sprintf("%d", record.Token),
		fmt.Sprintf("%.2f", float64(record.OpenPrice)/100.0),
		fmt.Sprintf("%.2f", float64(record.HighPrice)/100.0),
		fmt.Sprintf("%.2f", float64(record.LowPrice)/100.0),
		fmt.Sprintf("%.2f", float64(record.ClosePrice)/100.0),
		fmt.Sprintf("%.2f", float64(record.LastTradePrice)/100.0),
		fmt.Sprintf("%.2f", float64(record.PreviousClosePrice)/100.0),
		fmt.Sprintf("%d", record.TotalTradedQty),
		fmt.Sprintf("%.2f", float64(record.TotalTradedValue)/100.0),
		fmt.Sprintf("%d", record.TotalTrades),
		fmt.Sprintf("%.2f", float64(record.Week52High)/100.0),
		fmt.Sprintf("%.2f", float64(record.Week52Low)/100.0),
		fmt.Sprintf("%.2f", float64(record.NetChange)/100.0),
		fmt.Sprintf("%.4f", float64(record.PercentChange)/10000.0), // Basis points to percentage
		fmt.Sprintf("%.2f", float64(record.VWAP)/100.0),
		fmt.Sprintf("%d", record.DeliveryQty),
		fmt.Sprintf("%.2f", float64(record.DeliveryPercent)/100.0),
		fmt.Sprintf("%d", record.MarketLot),
		fmt.Sprintf("%.2f", float64(record.FaceValue)/100.0),
	}

	csvWriter18201.Write(csvRecord)
	csvWriter18201.Flush()
	atomic.AddInt64(&message18201Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg18201 := atomic.LoadInt64(&message18201Count)
	saved18201 := atomic.LoadInt64(&message18201Saved)
	headers := atomic.LoadInt64(&bhavCopyHeaders)
	trailers := atomic.LoadInt64(&bhavCopyTrailers)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg18201 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 18201: %s | %d msgs (%d H, %d T), %d saved\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg18201, headers, trailers, saved18201)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg18201 := atomic.LoadInt64(&message18201Count)
	saved18201 := atomic.LoadInt64(&message18201Saved)
	headers := atomic.LoadInt64(&bhavCopyHeaders)
	trailers := atomic.LoadInt64(&bhavCopyTrailers)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 18201 DECODER (BHAV COPY)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	fmt.Printf("📊 LISTENER PERFORMANCE\n")
	fmt.Printf("  Runtime:              %v\n", duration.Truncate(time.Second))
	fmt.Printf("  Total Packets:        %s\n", formatNumber(packets))
	fmt.Printf("  Total Data:           %.1f MB\n", totalMB)
	if duration.Seconds() > 0 {
		fmt.Printf("  Avg Packet Rate:      %.2f packets/sec\n", float64(packets)/duration.Seconds())
		fmt.Printf("  Avg Data Rate:        %.2f KB/sec\n", totalMB*1024/duration.Seconds())
	}

	fmt.Printf("\n📦 DECOMPRESSION STATISTICS\n")
	fmt.Printf("  Compressed Packets:   %s (%.1f%%)\n", formatNumber(compressed), 
		float64(compressed)*100/float64(packets))
	fmt.Printf("  Decompressed OK:      %s\n", formatNumber(decompressed))
	fmt.Printf("  Decompression Errors: %s\n", formatNumber(errors))
	if compressed > 0 {
		fmt.Printf("  Success Rate:         %.1f%%\n", float64(decompressed)*100/float64(compressed))
	}

	fmt.Printf("\n🎯 MESSAGE 18201 STATISTICS (BHAV COPY)\n")
	fmt.Printf("  Total Messages:       %s\n", formatNumber(msg18201))
	fmt.Printf("  Headers Processed:    %s\n", formatNumber(headers))
	fmt.Printf("  Trailers Processed:   %s\n", formatNumber(trailers))
	fmt.Printf("  Securities Saved:     %s\n", formatNumber(saved18201))
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		fmt.Printf("%-8s %-40s %s\n", "Code", "Description", "Count")
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		
		for code, count := range messageCodeCounts {
			name := "Unknown"
			if code == 18201 {
				name = "MARKET_STATS_REPORT_DATA (Bhav Copy)"
			}
			fmt.Printf("%-8d %-40s %s\n", code, name, formatNumber(count))
		}
	}

	fmt.Printf("\n📁 CSV FILE CREATED\n")
	fmt.Printf(strings.Repeat("-", 80) + "\n")
	fmt.Printf("  Location: csv_output/\n")
	fmt.Printf("  Securities: %s\n", formatNumber(saved18201))
	fmt.Printf("  Format: End-of-day market statistics (Bhav Copy)\n")

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg18201 > 0 {
		fmt.Printf("✅ SUCCESS: Bhav Copy (18201) processing completed\n")
		fmt.Printf("📊 Captured %d securities with complete market statistics\n", saved18201)
	} else {
		fmt.Printf("⚠️  WARNING: No Bhav Copy (18201) found during session\n")
		fmt.Printf("💡 Note: Bhav Copy is typically broadcast after market hours\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_18201_*.csv file\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}

func formatNumber(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	} else if n < 1000000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000.0)
	} else {
		return fmt.Sprintf("%.1fM", float64(n)/1000000.0)
	}
}
