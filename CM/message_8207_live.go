// NSE Capital Market Multicast UDP Receiver - Message 8207 Only
// 
// FOCUS: Only process message code 8207 (BCAST_INDICATIVE_INDICES)
// OUTPUT: csv_output/message_8207_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_8207_live.go
// Output: csv_output/message_8207_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 140-142
// Structure: INDICATIVE INDICES (71 bytes per record, up to 6 records)
// Session: Preopen (9:00 AM - 9:15 AM) - Indicative index values

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
// MESSAGE STRUCTURE FOR 8207
// =============================================================================

// Message8207 - BCAST_INDICATIVE_INDICES structure (474 bytes)
// Per NSE Protocol: BROADCAST INDICATIVE INDICES structure
type Message8207 struct {
	// BCAST_HEADER already processed before this
	NumberOfRecords     uint16                // Number of indicative indices (up to 6)
	IndicativeIndices   [6]IndicativeIndex    // Array of up to 6 indicative indices
}

// IndicativeIndex - Individual indicative index structure (71 bytes)
// Per NSE Protocol: INDICATIVE INDICES structure  
type IndicativeIndex struct {
	IndexName             [21]byte  // Index name (e.g., "Nifty 50", "Nifty Bank")
	IndicativeCloseValue  uint32    // Indicative index close value (in paise)
	Reserved1             uint32    // Reserved field
	Reserved2             uint32    // Reserved field  
	Reserved3             uint32    // Reserved field
	ClosingIndex          uint32    // Previous day closing index (in paise)
	PercentChange         int32     // Percentage change (basis points)
	Reserved4             uint32    // Reserved field
	Reserved5             uint32    // Reserved field
	Change                int32     // Absolute change (in paise)
	Reserved6             uint32    // Reserved field
	MarketCapitalization  float64   // Market capitalization (DOUBLE - 8 bytes)
	NetChangeIndicator    byte      // '+', '-', or ' ' (space)
	Filler                byte      // Padding byte
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

	// 8207 specific counters
	message8207Count int64
	message8207Saved int64
	message8207Processed int64  // Track how many passed validation
	indicativeIndicesSaved int64

	// CSV file for 8207
	csvFile8207   *os.File
	csvWriter8207 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 8207 (BCAST_INDICATIVE_INDICES)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 8207 (0x200F in hex)\n")
	fmt.Printf("Purpose: Indicative indices during preopen session\n")
	fmt.Printf("Session: Preopen (9:00 AM - 9:15 AM)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 8207
	if err := initialize8207CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter8207 != nil {
			csvWriter8207.Flush()
		}
		if csvFile8207 != nil {
			csvFile8207.Close()
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

func initialize8207CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_8207_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile8207 = file
	csvWriter8207 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"IndexName",
		"IndicativeCloseValue_Rupees",
		"ClosingIndex_Rupees",
		"PercentChange",
		"Change_Rupees",
		"MarketCapitalization_Crores",
		"NetChangeIndicator",
	}

	csvWriter8207.Write(header)
	csvWriter8207.Flush()

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

	buffer := make([]byte, 65536)

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
		decompressedData := make([]byte, 65536)
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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 8207 for BCAST_INDICATIVE_INDICES
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 8207
	if transactionCode != 8207 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process8207Message(finalData)
}

// =============================================================================
// MESSAGE 8207 PROCESSOR
// =============================================================================

func process8207Message(data []byte) {
	if len(data) < 42 { // Minimum length check - 40-byte header + 2 bytes minimum
		return
	}

	atomic.AddInt64(&message8207Count, 1)

	var msg Message8207
	offset := 40 // Skip BCAST_HEADER

	// Parse NumberOfRecords
	msg.NumberOfRecords = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Ensure we don't read beyond available data and don't exceed max records (6)
	numRecords := int(msg.NumberOfRecords)
	if numRecords > 6 {
		numRecords = 6
	}
	
	// Validate number of records is reasonable (should be 1-6 for valid data)
	if numRecords <= 0 {
		return
	}

	// Check if we have enough data for all records
	requiredLength := offset + numRecords*71 // 71 bytes per INDICATIVE INDICES record
	if len(data) < requiredLength {
		return
	}

	// Parse each IndicativeIndex record
	for i := 0; i < numRecords; i++ {
		var index IndicativeIndex
		
		// IndexName (21 bytes)
		copy(index.IndexName[:], data[offset:offset+21])
		offset += 21
		
		// IndicativeCloseValue (4 bytes)
		index.IndicativeCloseValue = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Reserved1 (4 bytes)
		index.Reserved1 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Reserved2 (4 bytes)  
		index.Reserved2 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Reserved3 (4 bytes)
		index.Reserved3 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// ClosingIndex (4 bytes)
		index.ClosingIndex = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// PercentChange (4 bytes, signed)
		index.PercentChange = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		
		// Reserved4 (4 bytes)
		index.Reserved4 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Reserved5 (4 bytes)
		index.Reserved5 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// Change (4 bytes, signed)
		index.Change = int32(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		
		// Reserved6 (4 bytes)
		index.Reserved6 = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// MarketCapitalization (8 bytes, DOUBLE)
		capBits := binary.BigEndian.Uint64(data[offset : offset+8])
		index.MarketCapitalization = math.Float64frombits(capBits)
		offset += 8
		
		// NetChangeIndicator (1 byte)
		index.NetChangeIndicator = data[offset]
		offset++
		
		// Filler (1 byte)
		index.Filler = data[offset]
		offset++

		// Store the record
		msg.IndicativeIndices[i] = index
		
		// Export to CSV
		exportTo8207CSV(index)
	}
	
	// Mark this message as successfully processed
	atomic.AddInt64(&message8207Processed, 1)
}

func exportTo8207CSV(index IndicativeIndex) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean up index name
	indexName := strings.TrimRight(string(index.IndexName[:]), "\x00")

	csvRecord := []string{
		timestamp,
		"8207",
		indexName,
		fmt.Sprintf("%.2f", float64(index.IndicativeCloseValue)/100.0),      // Convert paise to rupees
		fmt.Sprintf("%.2f", float64(index.ClosingIndex)/100.0),              // Convert paise to rupees
		fmt.Sprintf("%.4f", float64(index.PercentChange)/10000.0),           // Convert basis points to percentage
		fmt.Sprintf("%.2f", float64(index.Change)/100.0),                    // Convert paise to rupees
		fmt.Sprintf("%.2f", index.MarketCapitalization/10000000.0),          // Convert to crores
		string(index.NetChangeIndicator),
	}

	csvWriter8207.Write(csvRecord)
	csvWriter8207.Flush()
	atomic.AddInt64(&message8207Saved, 1)
	atomic.AddInt64(&indicativeIndicesSaved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg8207 := atomic.LoadInt64(&message8207Count)
	processed8207 := atomic.LoadInt64(&message8207Processed)
	indices := atomic.LoadInt64(&indicativeIndicesSaved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg8207 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 8207: %s | %d msgs, %d processed, %d indices\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg8207, processed8207, indices)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg8207 := atomic.LoadInt64(&message8207Count)
	indices := atomic.LoadInt64(&indicativeIndicesSaved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 8207 DECODER (INDICATIVE INDICES)\n")
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

	fmt.Printf("\n🎯 MESSAGE 8207 STATISTICS (INDICATIVE INDICES)\n")
	fmt.Printf("  Total Messages:       %s\n", formatNumber(msg8207))
	fmt.Printf("  Processed Messages:   %s\n", formatNumber(atomic.LoadInt64(&message8207Processed)))
	fmt.Printf("  Index Records:        %s\n", formatNumber(indices))
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		fmt.Printf("%-8s %-40s %s\n", "Code", "Description", "Count")
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		
		for code, count := range messageCodeCounts {
			name := getMessageCodeDescription(code)
			fmt.Printf("%-8d %-40s %s\n", code, name, formatNumber(count))
		}
	}

	fmt.Printf("\n📁 CSV FILE CREATED\n")
	fmt.Printf(strings.Repeat("-", 80) + "\n")
	fmt.Printf("  Location: csv_output/\n")
	fmt.Printf("  Index Records: %s\n", formatNumber(indices))
	fmt.Printf("  Format: Indicative index values during preopen\n")

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg8207 > 0 {
		fmt.Printf("✅ SUCCESS: Indicative Indices (8207) processing completed\n")
		fmt.Printf("📊 Captured %d index records during preopen session\n", indices)
	} else {
		fmt.Printf("⚠️  WARNING: No Indicative Indices (8207) found during session\n")
		fmt.Printf("💡 Note: Indicative indices are broadcast during preopen (9:00-9:15 AM)\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_8207_*.csv file\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}

func getMessageCodeDescription(code uint16) string {
	switch code {
	case 8207:
		return "BCAST_INDICATIVE_INDICES (Preopen Indices)"
	case 7207:
		return "BCAST_ORDERBOOK_UPDATE"
	case 7216:
		return "BCAST_PARTICIPANT_WISE_POSITION"
	case 6541:
		return "BCAST_STOCK_STATUS_CHG"
	default:
		return "Unknown"
	}
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
