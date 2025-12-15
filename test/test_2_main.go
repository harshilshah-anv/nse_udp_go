// NSE Multicast UDP Receiver - Simplified for Message 17130 Only
// 
// FOCUS: Only process message code 17130 (ENHNCD_MKT_MVMT_CM_OI_IN - Enhanced Market Movement CM Open Interest)
// OUTPUT: csv_output_2/message_17130_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run test_2_main.go lzo_decompressor_safe.go
// Output: csv_output_2/message_17130_TIMESTAMP.csv

package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

// =============================================================================
// MESSAGE STRUCTURE FOR 17130
// =============================================================================

// Message17130 - ENHNCD_MKT_MVMT_CM_OI_IN (Enhanced Market Movement CM Open Interest)
// 
// This message broadcasts AGGREGATED Open Interest by UNDERLYING ASSET
// - TokenNo: Underlying asset token (e.g., 1232=NIFTY, 2475=BANKNIFTY)
// - CurrentOI: Total OI across ALL derivatives of that underlying
// 
// Structure: BCAST_HEADER (40 bytes) + OPEN_INTEREST records (12 bytes each)
// Maximum 39 records per message
// 
// NOTE: These are NOT individual contract tokens (which are typically 40000+)
//       To get symbol names, use NSE Security Master file for token mapping
type Message17130 struct {
	TransactionCode uint16 // Always 17130
	NoOfRecords     uint16 // Number of Open Interest records
}

// OpenInterest17130 - Individual Open Interest record (12 bytes)
// Per NSE Table 99_A: Only 2 fields available, no additional data
type OpenInterest17130 struct {
	TokenNo   uint32 // Underlying asset token (NOT derivative contract token)
	CurrentOI int64  // Total Open Interest (LONG LONG - 8 bytes)
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
	
	// 17130 specific counters
	message17130Count   int64
	message17130Saved   int64
	
	// CSV file for 17130
	csvFile17130        *os.File
	csvWriter17130      *csv.Writer
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
	
	// Debug: Track all message codes
	messageCodeCounts   map[uint16]int64
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	// Initialize debug tracking
	messageCodeCounts = make(map[uint16]int64)

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	// Create output directory
	if err := os.MkdirAll("csv_output_2", 0755); err != nil {
		log.Fatalf("Failed to create csv_output_2 directory: %v", err)
		return
	}

	// Initialize CSV file for 17130
	if err := initialize17130CSV(); err != nil {
		log.Fatalf("Failed to initialize 17130 CSV: %v", err)
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
	if csvWriter17130 != nil {
		csvWriter17130.Flush()
	}
	if csvFile17130 != nil {
		csvFile17130.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize17130CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output_2", fmt.Sprintf("message_17130_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile17130 = file
	csvWriter17130 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", "TransactionCode", "NoOfRecords", "TokenNo", "CurrentOI",
	}
	csvWriter17130.Write(headers)
	csvWriter17130.Flush()

	fmt.Printf("📁 Created CSV file for Message 17130: %s\n", filename)
	return nil
}



// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "233.1.2.5"
	//multicastIP := "231.31.31.4"
	port := 34330
	//port := 55655
	
	fmt.Printf("\n╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ NSE Message 17130 Receiver                                ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 17130 (ENHNCD_MKT_MVMT_CM_OI_IN)\n")
	fmt.Printf("📊 Statistics every 10 seconds | Progress every 1000 packets\n")
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
		log.Fatalf("Failed to join multicast group: %v", err)
		return
	}
	defer conn.Close()

	// Set read buffer size
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

	// IMPORTANT: Per NSE documentation (Page 152):
	// "Inside the broadcast data, the first 8 bytes before the message header / broadcast header should be ignored.
	//  The message header / broadcast header starts from the 9th byte."
	// So we need to skip first 8 bytes after decompression
	
	if len(finalData) < 28 { // Need at least 8 (skip) + 20 (min header)
		return
	}
	
	// Skip first 8 bytes - BCAST_HEADER starts at byte 8 (0-indexed)
	finalData = finalData[8:]
	
	// Now BCAST_HEADER is at offset 0
	// For broadcast messages, TransactionCode is in BCAST_HEADER at offset 10-12
	messageCode := binary.BigEndian.Uint16(finalData[10:12])
	
	// Track all message codes for debugging
	messageCodeCounts[messageCode]++
	
	// Show message code distribution every 1000 packets
	totalPackets := atomic.LoadInt64(&packetCount)
	if totalPackets%1000 == 0 && totalPackets > 0 {
		has17130 := messageCodeCounts[17130] > 0
		status := "❌ NOT FOUND"
		if has17130 {
			status = fmt.Sprintf("✅ Found %d times", messageCodeCounts[17130])
		}
		fmt.Printf("📊 After %d packets | Message 17130: %s | %d unique codes\n", 
			totalPackets, status, len(messageCodeCounts))
	}

	// Process ONLY message 17130
	if messageCode == 17130 {
		process17130Message(finalData)
	}
}

// =============================================================================
// MESSAGE 17130 PROCESSOR
// =============================================================================

func process17130Message(data []byte) {
	if len(data) < 40 {
		return
	}

	atomic.AddInt64(&message17130Count, 1)
	currentCount := atomic.LoadInt64(&message17130Count)
	
	// Parse BCAST_HEADER (40 bytes) - used for broadcast messages
	// According to documentation (Table 3 BCAST_HEADER):
	// Offset 0-1: Reserved (CHAR 2)
	// Offset 2-3: Reserved (CHAR 2)
	// Offset 4-7: LogTime (LONG)
	// Offset 8-9: AlphaChar (CHAR 2)
	// Offset 10-11: TransactionCode (SHORT) *** THIS IS THE KEY ***
	// Offset 12-13: ErrorCode (SHORT)
	// Offset 14-17: BCSeqNo (LONG)
	// Offset 18: Reserved (CHAR 1)
	// Offset 19-21: Reserved (CHAR 3)
	// Offset 22-29: TimeStamp2 (CHAR 8)
	// Offset 30-37: Filler (8 BYTE)
	// Offset 38-39: MessageLength (SHORT)
	
	transactionCode := binary.BigEndian.Uint16(data[10:12])
	noOfRecords := binary.BigEndian.Uint16(data[12:14])
	
	if currentCount == 1 {
		fmt.Printf("\n✅ First Message 17130: %d records (underlying assets)\n", noOfRecords)
		fmt.Printf("   Note: TokenNo represents underlying asset tokens (indices/stocks)\n")
		fmt.Printf("         Not individual derivative contracts\n\n")
	}
	
	// Parse Open Interest records
	// Per documentation Table 98_A: OPEN_INTEREST array starts at offset 40
	// This is 40 bytes from the start of the structure (which includes BCAST_HEADER)
	// Since BCAST_HEADER is 40 bytes and is included in the offset count,
	// the OPEN_INTEREST array actually starts right after the 40-byte header
	
	offset := 40
	recordSize := 12
	
	for i := 0; i < int(noOfRecords); i++ {
		if offset+recordSize > len(data) {
			break
		}
		
		// Parse ENHNCD_OPEN_INTEREST: TokenNo (4 bytes) + CurrentOI (8 bytes)
		tokenNo := binary.BigEndian.Uint32(data[offset : offset+4])
		currentOI := int64(binary.BigEndian.Uint64(data[offset+4 : offset+12]))
		
		// Show first record of first message only
		if currentCount == 1 && i == 0 {
			fmt.Printf("Sample: Token %d → CurrentOI: %d\n\n", tokenNo, currentOI)
		}
		
		// Export to CSV
		exportToCSV(transactionCode, noOfRecords, tokenNo, currentOI)
		atomic.AddInt64(&message17130Saved, 1)
		
		offset += recordSize
	}
}

func exportToCSV(transactionCode, noOfRecords uint16, tokenNo uint32, currentOI int64) {
	if csvWriter17130 == nil {
		fmt.Printf("CSV writer is nil!\n")
		return
	}

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", noOfRecords),
		fmt.Sprintf("%d", tokenNo),
		fmt.Sprintf("%d", currentOI),
	}

	csvWriter17130.Write(record)
	csvWriter17130.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg17130 := atomic.LoadInt64(&message17130Count)
	saved17130 := atomic.LoadInt64(&message17130Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg17130 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🎯 17130: %s | %d msgs, %d records\n", 
			duration, packets, float64(packets)/duration, status, msg17130, saved17130)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg17130 := atomic.LoadInt64(&message17130Count)
	saved17130 := atomic.LoadInt64(&message17130Saved)

	fmt.Printf("\n📊 Final Statistics:\n")
	fmt.Printf("Runtime: %v | Packets: %d | 17130: %d messages, %d records saved to CSV\n", 
		duration, packets, msg17130, saved17130)
}
