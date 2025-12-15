// NSE Capital Market Multicast UDP Receiver - Message 7306 Only
// 
// FOCUS: Only process message code 7306 (BCAST_PART_MSTR_CHG - Participant Master Change)
// OUTPUT: csv_output/message_7306_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7306_live.go
// Output: csv_output/message_7306_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3
// Structure: PARTICIPANT MASTER CHANGE (84 bytes)
// Contains: Participant information and status changes

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
// MESSAGE STRUCTURE FOR 7306
// =============================================================================

// Message7306 - BCAST_PART_MSTR_CHG (Participant Master Change)
// 
// This message broadcasts participant master changes including:
// - Participant ID and name
// - Participant status and flags
// - Suspension dates and details
// - Market access permissions
// 
// Structure: BCAST_HEADER (40 bytes) + Participant Information (44 bytes)
// Total packet size: 84 bytes
type Message7306 struct {
	TransactionCode     uint16   // Always 7306
	ParticipantId       [5]byte  // Participant ID
	ParticipantName     [25]byte // Participant name
	ParticipantStatus   uint16   // Status (Active/Suspended/etc.)
	SuspendedDate       uint32   // Suspension date
	EffectiveDate       uint32   // Effective date
	MarketAccess        uint16   // Market access flags
	TradingRights       uint16   // Trading rights
	Reserved            [2]byte  // Reserved bytes
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

	// 7306 specific counters
	message7306Count int64
	message7306Saved int64

	// CSV file for 7306
	csvFile7306   *os.File
	csvWriter7306 *csv.Writer

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
	fmt.Printf("NSE CM UDP Receiver - Message 7306 (BCAST_PART_MSTR_CHG)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 7306 (0x1C8A in hex)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 7306
	if err := initialize7306CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter7306 != nil {
			csvWriter7306.Flush()
		}
		if csvFile7306 != nil {
			csvFile7306.Close()
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

func initialize7306CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_7306_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile7306 = file
	csvWriter7306 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"ParticipantId",
		"ParticipantName",
		"ParticipantStatus",
		"SuspendedDate",
		"EffectiveDate",
		"MarketAccess",
		"TradingRights",
	}

	csvWriter7306.Write(header)
	csvWriter7306.Flush()

	fmt.Printf("📁 CSV file created: %s\n", filepath)
	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	// NSE Capital Market multicast address and port
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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 7306 for BCAST_PART_MSTR_CHG
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 7306
	if transactionCode != 7306 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process7306Message(finalData)
}

// =============================================================================
// MESSAGE 7306 PROCESSOR
// =============================================================================

func process7306Message(data []byte) {
	if len(data) < 84 { // Minimum length check - 40-byte header + 44 bytes data
		return
	}

	atomic.AddInt64(&message7306Count, 1)

	// Parse message structure (BigEndian encoding)
	var msg Message7306

	// Transaction code from header
	msg.TransactionCode = binary.BigEndian.Uint16(data[10:12])

	// Parse participant information starting after BCAST_HEADER (40 bytes)
	offset := 40

	// Participant ID (5 bytes)
	copy(msg.ParticipantId[:], data[offset:offset+5])
	offset += 5

	// Participant Name (25 bytes)
	copy(msg.ParticipantName[:], data[offset:offset+25])
	offset += 25

	// Participant Status (2 bytes)
	msg.ParticipantStatus = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Dates (4 bytes each)
	msg.SuspendedDate = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4
	msg.EffectiveDate = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Access and rights (2 bytes each)
	msg.MarketAccess = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2
	msg.TradingRights = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Reserved bytes
	copy(msg.Reserved[:], data[offset:offset+2])

	// Export to CSV
	exportTo7306CSV(msg)
}

func exportTo7306CSV(msg Message7306) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean up participant ID and name
	participantId := strings.TrimRight(string(msg.ParticipantId[:]), "\x00")
	participantName := strings.TrimRight(string(msg.ParticipantName[:]), "\x00")

	record := []string{
		timestamp,
		fmt.Sprintf("%d", msg.TransactionCode),
		participantId,
		participantName,
		getParticipantStatusName(msg.ParticipantStatus),
		fmt.Sprintf("%d", msg.SuspendedDate),
		fmt.Sprintf("%d", msg.EffectiveDate),
		fmt.Sprintf("%d", msg.MarketAccess),
		fmt.Sprintf("%d", msg.TradingRights),
	}

	csvWriter7306.Write(record)
	csvWriter7306.Flush()
	atomic.AddInt64(&message7306Saved, 1)
}

func getParticipantStatusName(status uint16) string {
	switch status {
	case 0:
		return "Inactive"
	case 1:
		return "Active"
	case 2:
		return "Suspended"
	case 3:
		return "Debarred"
	case 4:
		return "Expelled"
	default:
		return fmt.Sprintf("Unknown(%d)", status)
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg7306 := atomic.LoadInt64(&message7306Count)
	saved7306 := atomic.LoadInt64(&message7306Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg7306 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 7306: %s | %d msgs, %d saved\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg7306, saved7306)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg7306 := atomic.LoadInt64(&message7306Count)
	saved7306 := atomic.LoadInt64(&message7306Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 7306 DECODER\n")
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

	fmt.Printf("\n🎯 MESSAGE 7306 STATISTICS\n")
	fmt.Printf("  Messages Found:       %s\n", formatNumber(msg7306))
	fmt.Printf("  Records Saved:        %s\n", formatNumber(saved7306))
	
	if len(messageCodeCounts) > 0 {
		fmt.Printf("\n📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodeCounts))
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		fmt.Printf("%-8s %-40s %s\n", "Code", "Description", "Count")
		fmt.Printf(strings.Repeat("-", 80) + "\n")
		
		for code, count := range messageCodeCounts {
			name := "Unknown"
			if code == 7306 {
				name = "BCAST_PART_MSTR_CHG"
			}
			fmt.Printf("%-8d %-40s %s\n", code, name, formatNumber(count))
		}
	}

	fmt.Printf("\n📁 CSV FILE CREATED\n")
	fmt.Printf(strings.Repeat("-", 80) + "\n")
	fmt.Printf("  Location: csv_output/\n")
	fmt.Printf("  Records:  %s\n", formatNumber(saved7306))

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg7306 > 0 {
		fmt.Printf("✅ SUCCESS: Message 7306 processing completed\n")
	} else {
		fmt.Printf("⚠️  WARNING: No Message 7306 found during session\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_7306_*.csv file\n")
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
