// NSE Capital Market Multicast UDP Receiver - Message 18708 Only
// 
// FOCUS: Only process message code 18708 (BCAST_BUY_BACK - Buyback Information Broadcast)
// OUTPUT: csv_output/message_18708_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18708_live.go
// Output: csv_output/message_18708_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Table 46
// Structure: BROADCAST BUYBACK (426 bytes total)
// Layout:
//   - BCAST_HEADER (40 bytes)
//   - NumberOfRecords (2 bytes)
//   - BuyBackData[6] (6 records × 64 bytes each = 384 bytes)
//     Each BuyBackData:
//       - Token (4), Symbol (10), Series (2)
//       - PdayCumVol (8), PdayHighPrice (4), PdayLowPrice (4), PdayWtAvg (4)
//       - CdayCumVol (8), CdayHighPrice (4), CdayLowPrice (4), CdayWtAvg (4)  
//       - StartDate (4), EndDate (4)
//
// Contains: Previous day and current day buyback statistics per security

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
// MESSAGE STRUCTURE FOR 18708
// =============================================================================

// BuyBackData - Individual buyback record (64 bytes each)
// Protocol: NSE CM NNF Protocol v6.3, Table 46 - BROADCAST BUYBACK
type BuyBackData struct {
	Token         uint32   // LONG 4 bytes - Security token
	Symbol        [10]byte // CHAR[10] - Symbol name
	Series        [2]byte  // CHAR[2] - Series code  
	PdayCumVol    float64  // DOUBLE 8 bytes - Previous day cumulative volume
	PdayHighPrice uint32   // LONG 4 bytes - Previous day high price (paise)
	PdayLowPrice  uint32   // LONG 4 bytes - Previous day low price (paise)
	PdayWtAvg     uint32   // LONG 4 bytes - Previous day weighted average (paise)
	CdayCumVol    float64  // DOUBLE 8 bytes - Current day cumulative volume
	CdayHighPrice uint32   // LONG 4 bytes - Current day high price (paise)
	CdayLowPrice  uint32   // LONG 4 bytes - Current day low price (paise)
	CdayWtAvg     uint32   // LONG 4 bytes - Current day weighted average (paise)
	StartDate     uint32   // LONG 4 bytes - Buyback start date
	EndDate       uint32   // LONG 4 bytes - Buyback end date
}

// Message18708 - BCAST_BUY_BACK (426 bytes total)
// Structure: BCAST_HEADER (40) + NumberOfRecords (2) + BuyBackData[6] (6 × 64 = 384)
type Message18708 struct {
	TransactionCode   uint16         // Always 18708
	NumberOfRecords   uint16         // Number of buyback records (max 6)
	BuyBackRecords    [6]BuyBackData // Array of buyback data records
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

	// 18708 specific counters
	message18708Count int64
	message18708Saved int64

	// CSV file for 18708
	csvFile18708   *os.File
	csvWriter18708 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 18708 (BCAST_BUY_BACK)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 18708 (0x491C in hex)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18708
	if err := initialize18708CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter18708 != nil {
			csvWriter18708.Flush()
		}
		if csvFile18708 != nil {
			csvFile18708.Close()
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

func initialize18708CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_18708_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile18708 = file
	csvWriter18708 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"RecordIndex",
		"Token",
		"Symbol",
		"Series",
		"PdayCumVol",
		"PdayHighPrice",
		"PdayLowPrice",
		"PdayWtAvg",
		"CdayCumVol",
		"CdayHighPrice",
		"CdayLowPrice",
		"CdayWtAvg",
		"StartDate",
		"EndDate",
	}

	csvWriter18708.Write(header)
	csvWriter18708.Flush()

	fmt.Printf("📁 CSV file created: %s\n", filepath)
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

	buffer := make([]byte, 2048)

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
		decompressedData := make([]byte, 10240) // 10 KB buffer
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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 18708 for BCAST_BUY_BACK
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Debug: Print first occurrence of each message code
	if messageCodeCounts[transactionCode] == 1 {
		if transactionCode == 18708 {
			fmt.Printf("🎯 TARGET message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		} else {
			fmt.Printf("📊 New message code detected: %d (0x%04X)\n", transactionCode, transactionCode)
		}
	}

	// Only process message 18708
	if transactionCode != 18708 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process18708Message(finalData)
}

// =============================================================================
// MESSAGE 18708 PROCESSOR
// =============================================================================

func process18708Message(data []byte) {
	if len(data) < 426 { // Full packet length check (40 + 2 + 6*64 = 426)
		fmt.Printf("⚠️  Message 18708 too short: %d bytes (expected 426)\n", len(data))
		return
	}

	atomic.AddInt64(&message18708Count, 1)

	// Parse NumberOfRecords at offset 40 (after 40-byte BCAST_HEADER)
	numRecords := int(binary.BigEndian.Uint16(data[40:42]))
	
	if numRecords > 6 {
		fmt.Printf("⚠️  Invalid NumberOfRecords: %d (max 6)\n", numRecords)
		return
	}

	fmt.Printf("📊 18708 Message: NumberOfRecords=%d\n", numRecords)

	var msg Message18708
	msg.TransactionCode = binary.BigEndian.Uint16(data[10:12])
	msg.NumberOfRecords = uint16(numRecords)

	// BuyBackData array starts at offset 42
	offset := 42

	for i := 0; i < numRecords && i < 6; i++ {
		if offset+64 > len(data) {
			fmt.Printf("⚠️  Record %d would exceed data length\n", i)
			break
		}

		rec := &msg.BuyBackRecords[i]
		
		// Parse each BuyBackData record (64 bytes)
		rec.Token = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		copy(rec.Symbol[:], data[offset:offset+10])
		offset += 10
		
		copy(rec.Series[:], data[offset:offset+2])
		offset += 2
		
		// Parse DOUBLE values using math.Float64frombits
		rec.PdayCumVol = math.Float64frombits(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
		
		rec.PdayHighPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.PdayLowPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.PdayWtAvg = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.CdayCumVol = math.Float64frombits(binary.BigEndian.Uint64(data[offset : offset+8]))
		offset += 8
		
		rec.CdayHighPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.CdayLowPrice = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.CdayWtAvg = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.StartDate = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		rec.EndDate = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	// Export to CSV
	exportTo18708CSV(msg)
}

func exportTo18708CSV(msg Message18708) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Export each buyback record
	for i := 0; i < int(msg.NumberOfRecords) && i < 6; i++ {
		rec := msg.BuyBackRecords[i]
		
		// Clean up symbol and series
		symbol := strings.TrimRight(string(rec.Symbol[:]), "\x00")
		series := strings.TrimRight(string(rec.Series[:]), "\x00")

		record := []string{
			timestamp,
			fmt.Sprintf("%d", msg.TransactionCode),
			fmt.Sprintf("%d", i),
			fmt.Sprintf("%d", rec.Token),
			symbol,
			series,
			fmt.Sprintf("%.0f", rec.PdayCumVol),
			fmt.Sprintf("%.2f", float64(rec.PdayHighPrice)/100.0), // Convert paise to rupees
			fmt.Sprintf("%.2f", float64(rec.PdayLowPrice)/100.0),
			fmt.Sprintf("%.2f", float64(rec.PdayWtAvg)/100.0),
			fmt.Sprintf("%.0f", rec.CdayCumVol),
			fmt.Sprintf("%.2f", float64(rec.CdayHighPrice)/100.0),
			fmt.Sprintf("%.2f", float64(rec.CdayLowPrice)/100.0),
			fmt.Sprintf("%.2f", float64(rec.CdayWtAvg)/100.0),
			fmt.Sprintf("%d", rec.StartDate),
			fmt.Sprintf("%d", rec.EndDate),
		}

		csvWriter18708.Write(record)
	}
	
	csvWriter18708.Flush()
	atomic.AddInt64(&message18708Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg18708 := atomic.LoadInt64(&message18708Count)
	saved18708 := atomic.LoadInt64(&message18708Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg18708 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 18708: %s | %d msgs, %d saved\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg18708, saved18708)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg18708 := atomic.LoadInt64(&message18708Count)
	saved18708 := atomic.LoadInt64(&message18708Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 18708 DECODER\n")
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

	fmt.Printf("\n🎯 MESSAGE 18708 STATISTICS\n")
	fmt.Printf("  Messages Found:       %s\n", formatNumber(msg18708))
	fmt.Printf("  Records Saved:        %s\n", formatNumber(saved18708))
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		fmt.Printf("%-8s %-40s %s\n", "Code", "Description", "Count")
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		
		for code, count := range messageCodeCounts {
			name := getMessageName(code)
			if code == 18708 {
				fmt.Printf("%-8d %-40s %s [TARGET]\n", code, name, formatNumber(count))
			} else {
				fmt.Printf("%-8d %-40s %s\n", code, name, formatNumber(count))
			}
		}
	}

	fmt.Printf("\n📁 CSV FILE CREATED\n")
	fmt.Printf(strings.Repeat("-", 80) + "\n")
	fmt.Printf("  Location: csv_output/\n")
	fmt.Printf("  Records:  %s\n", formatNumber(saved18708))

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg18708 > 0 {
		fmt.Printf("✅ SUCCESS: Message 18708 processing completed\n")
	} else {
		fmt.Printf("⚠️  WARNING: No Message 18708 found during session\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_18708_*.csv file\n")
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

func getMessageName(code uint16) string {
	switch code {
	case 18708:
		return "BCAST_BUY_BACK"
	case 18703:
		return "BCAST_TICKER_AND_MKT_INDEX"
	case 7200:
		return "BROADCAST_MBO_MBP"
	case 7201:
		return "BCAST_MW_ROUND_ROBIN"
	case 7207:
		return "BCAST_INDICES"
	case 7208:
		return "BCAST_ONLY_MBP"
	case 6541:
		return "BC_CIRCUIT_CHECK"
	case 6501:
		return "BCAST_JRNL_VCT_MSG"
	case 6531:
		return "BC_PREOPEN_SHUTDOWN_MSG"
	case 18720:
		return "BCAST_SECURITY_MSTR_CHG"
	case 18130:
		return "BCAST_SECURITY_STATUS_CHG"
	default:
		return "Unknown"
	}
}
