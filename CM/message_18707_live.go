// NSE Capital Market Multicast UDP Receiver - Message 18707 Only
// 
// FOCUS: Only process message code 18707 (BCAST_SECURITY_STATUS_CHG_PREOPEN)
// OUTPUT: csv_output/message_18707_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18707_live.go
// Output: csv_output/message_18707_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 102
// Structure: SECURITY STATUS UPDATE INFORMATION (442 bytes)
// Session: Preopen (9:00 AM - 9:15 AM) - Security status changes during preopen

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
// MESSAGE STRUCTURE FOR 18707
// =============================================================================

// Message18707 - BCAST_SECURITY_STATUS_CHG_PREOPEN structure (442 bytes)
// Per NSE Protocol: SECURITY STATUS UPDATE INFORMATION structure
type Message18707 struct {
	// BCAST_HEADER already processed before this
	NumberOfRecords     uint16                    // Number of TOKEN AND ELIGIBILITY records (up to 25)
	TokenEligibility    [25]TokenEligibilityInfo  // Array of up to 25 token eligibility records
}

// TokenEligibilityInfo - Individual TOKEN AND ELIGIBILITY structure (16 bytes)
// Per NSE Protocol: TOKEN AND ELIGIBILITY structure  
type TokenEligibilityInfo struct {
	Token              uint32                  // Security token number
	SecurityStatus     [6]SecurityMarketStatus // Status per market (6 markets)
}

// SecurityMarketStatus - Individual SECURITY STATUS PER MARKET structure (2 bytes)
// Per NSE Protocol: SECURITY STATUS PER MARKET structure
type SecurityMarketStatus struct {
	Status             uint16                  // Security status code
}

// =============================================================================
// STATUS MAPPING
// =============================================================================

// getStatusName converts numeric status to readable name
func getStatusName(status uint16) string {
	switch status {
	case 1:
		return "Preopen"
	case 2:
		return "Open"
	case 3:
		return "Suspended"
	case 4:
		return "Preopen Extended"
	case 6:
		return "Price Discovery"
	default:
		return fmt.Sprintf("Unknown(%d)", status)
	}
}

// getMarketName converts market index to market name
func getMarketName(index int) string {
	markets := []string{
		"Market1_Normal",
		"Market2_OddLot", 
		"Market3_Spot",
		"Market4_Auction",
		"Market5_CallAuction1",
		"Market6_CallAuction2",
	}
	if index >= 0 && index < len(markets) {
		return markets[index]
	}
	return fmt.Sprintf("Market%d", index+1)
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

	// 18707 specific counters
	message18707Count int64
	message18707Saved int64
	securityStatusSaved int64

	// CSV file for 18707
	csvFile18707   *os.File
	csvWriter18707 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 18707 (BCAST_SECURITY_STATUS_CHG_PREOPEN)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 18707 (0x491B in hex)\n")
	fmt.Printf("Purpose: Security status changes during preopen session\n")
	fmt.Printf("Session: Preopen (9:00 AM - 9:15 AM)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18707
	if err := initialize18707CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter18707 != nil {
			csvWriter18707.Flush()
		}
		if csvFile18707 != nil {
			csvFile18707.Close()
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

func initialize18707CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_18707_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile18707 = file
	csvWriter18707 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"Token",
		"Market1_Normal_Status",
		"Market2_OddLot_Status",
		"Market3_Spot_Status", 
		"Market4_Auction_Status",
		"Market5_CallAuction1_Status",
		"Market6_CallAuction2_Status",
		"Market1_Normal_StatusName",
		"Market2_OddLot_StatusName",
		"Market3_Spot_StatusName",
		"Market4_Auction_StatusName",
		"Market5_CallAuction1_StatusName",
		"Market6_CallAuction2_StatusName",
	}

	csvWriter18707.Write(header)
	csvWriter18707.Flush()

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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 18707 for BCAST_SECURITY_STATUS_CHG_PREOPEN
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 18707
	if transactionCode != 18707 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process18707Message(finalData)
}

// =============================================================================
// MESSAGE 18707 PROCESSOR
// =============================================================================

func process18707Message(data []byte) {
	if len(data) < 42 { // Minimum length check - 40-byte header + 2 bytes minimum
		return
	}

	atomic.AddInt64(&message18707Count, 1)

	var msg Message18707
	offset := 40 // Skip BCAST_HEADER

	// Parse NumberOfRecords
	msg.NumberOfRecords = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Ensure we don't read beyond available data and don't exceed max records (25)
	numRecords := int(msg.NumberOfRecords)
	if numRecords > 25 {
		numRecords = 25
	}

	// Check if we have enough data for all records
	requiredLength := offset + numRecords*16 // 16 bytes per TOKEN AND ELIGIBILITY record
	if len(data) < requiredLength {
		return
	}

	// Parse each TokenEligibilityInfo record
	for i := 0; i < numRecords; i++ {
		var tokenInfo TokenEligibilityInfo
		
		// Token (4 bytes)
		tokenInfo.Token = binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		
		// SecurityStatus per market (6 markets, 2 bytes each = 12 bytes total)
		for j := 0; j < 6; j++ {
			tokenInfo.SecurityStatus[j].Status = binary.BigEndian.Uint16(data[offset : offset+2])
			offset += 2
		}

		// Store the record
		msg.TokenEligibility[i] = tokenInfo
		
		// Export to CSV
		exportTo18707CSV(tokenInfo)
	}
}

func exportTo18707CSV(tokenInfo TokenEligibilityInfo) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	csvRecord := []string{
		timestamp,
		"18707",
		fmt.Sprintf("%d", tokenInfo.Token),
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[0].Status),  // Market1_Normal
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[1].Status),  // Market2_OddLot
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[2].Status),  // Market3_Spot
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[3].Status),  // Market4_Auction
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[4].Status),  // Market5_CallAuction1
		fmt.Sprintf("%d", tokenInfo.SecurityStatus[5].Status),  // Market6_CallAuction2
		getStatusName(tokenInfo.SecurityStatus[0].Status),      // Market1_Normal_Name
		getStatusName(tokenInfo.SecurityStatus[1].Status),      // Market2_OddLot_Name
		getStatusName(tokenInfo.SecurityStatus[2].Status),      // Market3_Spot_Name
		getStatusName(tokenInfo.SecurityStatus[3].Status),      // Market4_Auction_Name
		getStatusName(tokenInfo.SecurityStatus[4].Status),      // Market5_CallAuction1_Name
		getStatusName(tokenInfo.SecurityStatus[5].Status),      // Market6_CallAuction2_Name
	}

	csvWriter18707.Write(csvRecord)
	csvWriter18707.Flush()
	atomic.AddInt64(&message18707Saved, 1)
	atomic.AddInt64(&securityStatusSaved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg18707 := atomic.LoadInt64(&message18707Count)
	statusRecords := atomic.LoadInt64(&securityStatusSaved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg18707 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 18707: %s | %d msgs, %d status records\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg18707, statusRecords)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg18707 := atomic.LoadInt64(&message18707Count)
	statusRecords := atomic.LoadInt64(&securityStatusSaved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 18707 DECODER (PREOPEN SECURITY STATUS)\n")
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

	fmt.Printf("\n🎯 MESSAGE 18707 STATISTICS (PREOPEN SECURITY STATUS)\n")
	fmt.Printf("  Total Messages:       %s\n", formatNumber(msg18707))
	fmt.Printf("  Status Records:       %s\n", formatNumber(statusRecords))
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		fmt.Printf("%-8s %-40s %s\n", "Code", "Description", "Count")
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		
		for code, count := range messageCodeCounts {
			name := "Unknown"
			if code == 18707 {
				name = "BCAST_SECURITY_STATUS_CHG_PREOPEN"
			}
			fmt.Printf("%-8d %-40s %s\n", code, name, formatNumber(count))
		}
	}

	fmt.Printf("\n📁 CSV FILE CREATED\n")
	fmt.Printf(strings.Repeat("-", 80) + "\n")
	fmt.Printf("  Location: csv_output/\n")
	fmt.Printf("  Status Records: %s\n", formatNumber(statusRecords))
	fmt.Printf("  Format: Security status changes during preopen\n")

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg18707 > 0 {
		fmt.Printf("✅ SUCCESS: Preopen Security Status (18707) processing completed\n")
		fmt.Printf("📊 Captured %d security status records during preopen session\n", statusRecords)
	} else {
		fmt.Printf("⚠️  WARNING: No Preopen Security Status (18707) found during session\n")
		fmt.Printf("💡 Note: Preopen security status changes are broadcast during preopen (9:00-9:15 AM)\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_18707_*.csv file\n")
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
