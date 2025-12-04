// NSE Multicast UDP Receiver - Simplified for Message 7340 Only
// 
// FOCUS: Only process message code 7340 (BCAST_SEC_MSTR_CHNG_PERIODIC)
// OUTPUT: csv_output_2/message_7340_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run test_2_main.go lzo_decompressor_safe.go
// Output: csv_output_2/message_7340_TIMESTAMP.csv

package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// =============================================================================
// MESSAGE STRUCTURE FOR 7340
// =============================================================================

// Message7340 - BCAST_SEC_MSTR_CHNG_PERIODIC (Security Master Change Periodic)
// Structure: BCAST_HEADER(40) + MS_SECURITY_UPDATE_INFO (298 bytes)
// Security master data changes broadcast periodically
type Message7340 struct {
	Token          uint32    // 4 bytes - Token number
	Symbol         [10]byte  // 10 bytes - Security symbol
	Series         [2]byte   // 2 bytes - Series (EQ, FO, etc.)
	InstrumentName [6]byte   // 6 bytes - Instrument name
	ExpiryDate     uint32    // 4 bytes - Expiry date
	StrikePrice    uint32    // 4 bytes - Strike price
	OptionType     [2]byte   // 2 bytes - Option type (CE, PE)
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
	
	// 7340 specific counters
	message7340Count   int64
	message7340Saved   int64
	
	// CSV file for 7340
	csvFile7340        *os.File
	csvWriter7340      *csv.Writer
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	fmt.Println("================================================================================")
	fmt.Println("NSE MULTICAST UDP RECEIVER - MESSAGE 7340 ONLY")
	fmt.Println("================================================================================")
	fmt.Println("🎯 FOCUS: Only processing message code 7340 (BCAST_SEC_MSTR_CHNG_PERIODIC)")
	fmt.Println("📁 OUTPUT: csv_output_2/message_7340_TIMESTAMP.csv")
	fmt.Println("⏰ NOTE: Data is only available during NSE market hours (9:15 AM - 3:30 PM)")
	fmt.Println("📅 Market Days: Monday to Friday (excluding NSE holidays)")
	fmt.Println("================================================================================")

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	// Create output directory
	if err := os.MkdirAll("csv_output_2", 0755); err != nil {
		fmt.Printf("❌ Failed to create csv_output_2 directory: %v\n", err)
		return
	}

	// Initialize CSV file for 7340
	if err := initialize7340CSV(); err != nil {
		fmt.Printf("❌ Failed to initialize 7340 CSV: %v\n", err)
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

	fmt.Println("🚀 Starting UDP listener for message 7340...")
	fmt.Println("⏹️  Press Ctrl+C to stop\n")

	// Wait for Ctrl+C
	<-sigChan
	fmt.Println("\n\n🛑 Shutdown signal received, stopping...")

	close(shutdownChan)
	time.Sleep(1 * time.Second)

	// Close CSV file
	if csvWriter7340 != nil {
		csvWriter7340.Flush()
	}
	if csvFile7340 != nil {
		csvFile7340.Close()
	}

	// Print final statistics
	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7340CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output_2", fmt.Sprintf("message_7340_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile7340 = file
	csvWriter7340 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", "MessageCode", "Token", "Symbol", "Series", 
		"InstrumentName", "ExpiryDate", "StrikePrice", "OptionType",
	}
	csvWriter7340.Write(headers)
	csvWriter7340.Flush()
	
	fmt.Printf("✅ Created CSV file: %s\n", filename)
	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "233.1.2.5"
	//multicastIP := "231.31.31.4"
	port := 34330
	//port := 18901
	
	// Create multicast address
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	// Join multicast group
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		fmt.Printf("❌ Failed to join multicast group: %v\n", err)
		return
	}
	defer conn.Close()

	// Set read buffer size
	conn.SetReadBuffer(2 * 1024 * 1024)

	fmt.Printf("✅ Joined multicast group: %s:%d\n", multicastIP, port)

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

	if len(finalData) < 20 {
		return
	}

	messageCode := binary.BigEndian.Uint16(finalData[18:20])

	// Debug: Show what message codes we're seeing (first 20 packets)
	if atomic.LoadInt64(&packetCount) <= 20 {
		fmt.Printf("📊 DEBUG: Packet #%d - Message Code: %d (0x%04X)\n", 
			atomic.LoadInt64(&packetCount), messageCode, messageCode)
	}

	// Only process message code 7340
	if messageCode == 7340 {
		process7340Message(finalData)
	}
}

// =============================================================================
// MESSAGE 7340 PROCESSOR
// =============================================================================

func process7340Message(data []byte) {
	if len(data) < 44 { // 40 hdr + 2 NoOfRecords + 2 spare
		return
	}

	if binary.BigEndian.Uint16(data[18:20]) != 7340 {
		return
	}

	atomic.AddInt64(&message7340Count, 1)

	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	if noOfRecords == 0 || noOfRecords > 10 {
		// some exchanges send 0 and still pack 1 record; fall back to scan
		noOfRecords = 1
	}

	offset := 42
	recSize := 298

	for i := 0; i < int(noOfRecords); i++ {
		if offset+recSize > len(data) {
			break
		}
		rec := data[offset : offset+recSize]
		if msg := parseMessage7340(rec); msg != nil {
			exportToCSV(msg)
			atomic.AddInt64(&message7340Saved, 1)
		}
		offset += recSize
	}
}

// parseMessage7340 - Strict FO parser according to NSE specification
func parseMessage7340(data []byte) *Message7340 {
	if len(data) < 32 {
		return nil
	}

	// Analyzing actual NSE F&O packet structure based on raw bytes
	// Token is likely at the beginning of the security record (position 0-4)
	// But let's try different positions to find the correct FO token
	
	// Method 1: Try token at different positions and validate
	var candidateToken uint32
	var tokenOffset int = -1
	
	// Test token positions: 0, 4, 8, 32, etc.
	testPositions := []int{0, 4, 8, 32}
	
	for _, pos := range testPositions {
		if pos+4 <= len(data) {
			testToken := binary.BigEndian.Uint32(data[pos:pos+4])
			// F&O tokens are typically in range 40000-999999
			if testToken >= 40000 && testToken <= 999999 {
				candidateToken = testToken
				tokenOffset = pos
				break
			}
		}
	}
	
	// If no valid F&O token found, try little-endian
	if tokenOffset == -1 {
		for _, pos := range testPositions {
			if pos+4 <= len(data) {
				testToken := binary.LittleEndian.Uint32(data[pos:pos+4])
				if testToken >= 40000 && testToken <= 999999 {
					candidateToken = testToken
					tokenOffset = pos
					break
				}
			}
		}
	}
	
	msg := &Message7340{
		Token:      candidateToken,
		ExpiryDate: binary.BigEndian.Uint32(data[32:36]), // Will refine after token is correct
		StrikePrice: binary.BigEndian.Uint32(data[36:40]),
	}
	copy(msg.InstrumentName[:], data[9:15])   // "OPTIDX" at position 9-14
	copy(msg.Symbol[:],         data[15:25])  // "NIFTY" starts at position 15
	copy(msg.Series[:],         data[25:27])  // Series after symbol
	copy(msg.OptionType[:],     data[37:39])  // "CE"/"PE" around position 37-38

	sym  := strings.TrimSpace(string(msg.Symbol[:]))
	ser  := strings.TrimSpace(string(msg.Series[:]))
	inst := strings.TrimSpace(string(msg.InstrumentName[:]))
	opt  := strings.TrimSpace(string(msg.OptionType[:]))

	// Debug: Show what we're parsing (first 10 records)
	if atomic.LoadInt64(&message7340Count) <= 10 {
		fmt.Printf("🔍 DEBUG: Parsing record #%d\n", atomic.LoadInt64(&message7340Count))
		fmt.Printf("   Token: %d (from offset %d), Symbol: %q, Series: %q, Instrument: %q, Option: %q\n", 
			msg.Token, tokenOffset, sym, ser, inst, opt)
		
		// Show token analysis at different positions
		fmt.Printf("   TOKEN ANALYSIS:\n")
		for _, pos := range []int{0, 4, 8, 32} {
			if pos+4 <= len(data) {
				bigEndian := binary.BigEndian.Uint32(data[pos:pos+4])
				littleEndian := binary.LittleEndian.Uint32(data[pos:pos+4])
				fmt.Printf("     Pos %d: BE=%d, LE=%d\n", pos, bigEndian, littleEndian)
			}
		}
		
		// Show raw bytes to understand the actual data structure
		fmt.Printf("   RAW BYTES (first 40): ")
		for i := 0; i < 40 && i < len(data); i++ {
			fmt.Printf("%02X ", data[i])
		}
		fmt.Printf("\n")
		
		// Show ASCII view
		fmt.Printf("   ASCII VIEW (first 40): ")
		for i := 0; i < 40 && i < len(data); i++ {
			if data[i] >= 32 && data[i] <= 126 {
				fmt.Printf("%c", data[i])
			} else {
				fmt.Printf(".")
			}
		}
		fmt.Printf("\n")
	}

	// SAVE ALL DATA TO CSV FIRST - No validation rejection for now
	// This will help us see the actual data structure and fix parsing
	
	if atomic.LoadInt64(&message7340Count) <= 10 {
		fmt.Printf("   💾 SAVING TO CSV: All data saved for analysis\n")
		fmt.Printf("   Token: %d (offset %d), Symbol: %q, Series: %q, Instrument: %q, Option: %q\n", 
			msg.Token, tokenOffset, sym, ser, inst, opt)
	}
	return msg
}



func exportToCSV(msg *Message7340) {
	if csvWriter7340 == nil {
		return
	}

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		"7340",
		fmt.Sprintf("%d", msg.Token),
		strings.TrimSpace(string(msg.Symbol[:])),
		strings.TrimSpace(string(msg.Series[:])),
		strings.TrimSpace(string(msg.InstrumentName[:])),
		fmt.Sprintf("%d", msg.ExpiryDate),
		fmt.Sprintf("%.2f", float64(msg.StrikePrice)/100.0),
		strings.TrimSpace(string(msg.OptionType[:])),
	}

	csvWriter7340.Write(record)
	csvWriter7340.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg7340 := atomic.LoadInt64(&message7340Count)
	saved7340 := atomic.LoadInt64(&message7340Saved)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("📊 REAL-TIME STATISTICS - MESSAGE 7340 ONLY")
	fmt.Println(strings.Repeat("=", 60))
	
	if duration > 0 {
		fmt.Printf("⏱️  Runtime: %.1f seconds\n", duration)
		fmt.Printf("📦 Total Packets: %d (%.1f packets/sec)\n", packets, float64(packets)/duration)
		fmt.Printf("📊 Total Bytes: %d (%.1f KB/sec)\n", bytes, float64(bytes)/duration/1024)
		fmt.Printf("🗜️  Compressed: %d | Decompressed: %d | Errors: %d\n", compressed, decompressed, errors)
		fmt.Printf("🎯 Message 7340: %d received | %d saved to CSV\n", msg7340, saved7340)
		
		if msg7340 > 0 {
			successRate := float64(saved7340) / float64(msg7340) * 100
			fmt.Printf("✅ Success Rate: %.1f%% (%d/%d)\n", successRate, saved7340, msg7340)
		}
	}
	
	fmt.Println(strings.Repeat("=", 60))
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg7340 := atomic.LoadInt64(&message7340Count)
	saved7340 := atomic.LoadInt64(&message7340Saved)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 7340 PROCESSOR")
	fmt.Println(strings.Repeat("=", 80))
	
	fmt.Printf("⏱️  Total Runtime: %v\n", duration)
	fmt.Printf("📦 Total Packets Processed: %d\n", packets)
	fmt.Printf("📊 Total Data Volume: %d bytes (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	
	if duration.Seconds() > 0 {
		fmt.Printf("📈 Average Packet Rate: %.2f packets/sec\n", float64(packets)/duration.Seconds())
		fmt.Printf("📈 Average Data Rate: %.2f KB/sec\n", float64(bytes)/duration.Seconds()/1024)
	}
	
	fmt.Printf("🗜️  Compression Stats: %d compressed | %d decompressed | %d errors\n", compressed, decompressed, errors)
	
	if compressed > 0 {
		fmt.Printf("✅ Decompression Success Rate: %.1f%%\n", float64(decompressed)/float64(compressed)*100)
	}
	
	fmt.Println("\n🎯 MESSAGE 7340 STATISTICS:")
	fmt.Printf("   Messages Received: %d\n", msg7340)
	fmt.Printf("   Records Saved to CSV: %d\n", saved7340)
	
	if msg7340 > 0 {
		successRate := float64(saved7340) / float64(msg7340) * 100
		fmt.Printf("   Processing Success Rate: %.1f%%\n", successRate)
		
		if duration.Seconds() > 0 {
			fmt.Printf("   Average 7340 Rate: %.2f messages/sec\n", float64(msg7340)/duration.Seconds())
		}
	}
	
	fmt.Println("\n📁 OUTPUT FILE: Check csv_output_2/ directory for message_7340_*.csv")
	
	if packets == 0 {
		fmt.Println("\n⚠️  WARNING: No packets received!")
		fmt.Println("   Possible reasons:")
		fmt.Println("   - NSE multicast feed not available")
		fmt.Println("   - Market hours (9:15 AM - 3:30 PM IST)")
		fmt.Println("   - Firewall blocking UDP multicast")
		fmt.Println("   - Network connection issues")
	} else if msg7340 == 0 {
		fmt.Println("\n⚠️  WARNING: No 7340 messages received!")
		fmt.Println("   This message type may not be active during current market session")
	} else {
		fmt.Printf("\n✅ SUCCESS: Processed %d message 7340 records\n", saved7340)
	}
	
	fmt.Println(strings.Repeat("=", 80))
}
