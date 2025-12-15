// NSE Capital Market Multicast UDP Receiver - Message 6511 Only
// 
// FOCUS: Only process message code 6511 (BC_OPEN_MESSAGE)
// OUTPUT: csv_output/message_6511_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_6511_live.go
// Output: csv_output/message_6511_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3
// Structure: BCAST_VCT_MESSAGES (298 bytes)
// Session: Regular Market - Market open notification

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
// MESSAGE STRUCTURE FOR 6511
// =============================================================================

// Message6511 - BC_OPEN_MESSAGE (Market Open Messages)
// Per NSE CM Protocol - BCAST_VCT_MESSAGES structure
// Total packet: 298 bytes
type Message6511 struct {
	TransactionCode uint16   // Always 6511
	BranchNumber    uint16   // 2 bytes
	BrokerNumber    [5]byte  // 5 bytes
	ActionCode      [3]byte  // 3 bytes - Action code
	Reserved        [4]byte  // 4 bytes
	TraderWsBit     byte     // 1 byte - bit flags
	Reserved2       byte     // 1 byte
	MsgLength       uint16   // 2 bytes
	Msg             [240]byte // 240 bytes - Market open message content
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
	
	// 6511 specific counters
	message6511Count   int64
	message6511Saved   int64
	
	// CSV file for 6511
	csvFile6511        *os.File
	csvWriter6511      *csv.Writer
	
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

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("NSE CM UDP Receiver - Message 6511 (BC_OPEN_MESSAGE)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 6511 (0x1977 in hex)\n")
	fmt.Printf("Purpose: Market open notification\n")
	fmt.Printf("Session: Regular Market (9:15 AM start)\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 6511
	if err := initialize6511CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}

	// Setup signal handler (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start packet processor
	go processPackets()

	// Start UDP listener
	go startUDPListener()
	
	// Start statistics display
	go func() {
		ticker := time.NewTicker(1 * time.Second)
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
	if csvWriter6511 != nil {
		csvWriter6511.Flush()
	}
	if csvFile6511 != nil {
		csvFile6511.Close()
	}

	printFinalStats()
	fmt.Printf("👋 Program terminated successfully\n")
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize6511CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_6511_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile6511 = file
	csvWriter6511 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"BranchNumber",
		"BrokerNumber",
		"ActionCode",
		"TraderWsBit",
		"MsgLength",
		"Message",
	}

	csvWriter6511.Write(header)
	csvWriter6511.Flush()

	fmt.Printf("📁 CSV file created: %s\n", filepath)
	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	// Live Market Hours Multicast
	multicastIP := "233.1.2.5"
	port := 8222

	// After Market Hours Multicast
	//multicastIP := "231.31.31.4"
	//port := 18901

	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", multicastIP, port))
	if err != nil {
		log.Printf("Failed to resolve UDP address: %v", err)
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("Failed to listen on multicast address: %v", err)
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
		case <-shutdownChan:
			return
		case data := <-packetChan:
			processUDPPacket(data)
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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 6511 for BC_OPEN_MESSAGE
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 6511
	if transactionCode != 6511 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process6511Message(finalData)
}

// =============================================================================
// MESSAGE 6511 PROCESSOR
// =============================================================================

func process6511Message(data []byte) {
	if len(data) < 298 { // 40-byte header + 258-byte message
		return
	}

	atomic.AddInt64(&message6511Count, 1)

	var msg Message6511
	offset := 40 // Skip BCAST_HEADER

	// Parse BCAST_VCT_MESSAGES structure
	msg.TransactionCode = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.BranchNumber = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	copy(msg.BrokerNumber[:], data[offset:offset+5])
	offset += 5

	copy(msg.ActionCode[:], data[offset:offset+3])
	offset += 3

	copy(msg.Reserved[:], data[offset:offset+4])
	offset += 4

	msg.TraderWsBit = data[offset]
	offset++

	msg.Reserved2 = data[offset]
	offset++

	msg.MsgLength = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	copy(msg.Msg[:], data[offset:offset+240])

	// Export to CSV
	exportTo6511CSV(msg)
}

func exportTo6511CSV(msg Message6511) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Clean up strings
	brokerNumber := strings.TrimRight(string(msg.BrokerNumber[:]), "\x00")
	actionCode := strings.TrimRight(string(msg.ActionCode[:]), "\x00")
	message := strings.TrimRight(string(msg.Msg[:]), "\x00")

	csvRecord := []string{
		timestamp,
		fmt.Sprintf("%d", msg.TransactionCode),
		fmt.Sprintf("%d", msg.BranchNumber),
		brokerNumber,
		actionCode,
		fmt.Sprintf("%d", msg.TraderWsBit),
		fmt.Sprintf("%d", msg.MsgLength),
		message,
	}

	csvWriter6511.Write(csvRecord)
	csvWriter6511.Flush()
	atomic.AddInt64(&message6511Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg6511 := atomic.LoadInt64(&message6511Count)
	saved := atomic.LoadInt64(&message6511Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg6511 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 6511: %s | %d msgs, %d saved\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg6511, saved)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg6511 := atomic.LoadInt64(&message6511Count)
	saved := atomic.LoadInt64(&message6511Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 6511 DECODER (BC_OPEN_MESSAGE)\n")
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

	fmt.Printf("\n🎯 MESSAGE 6511 STATISTICS (BC_OPEN_MESSAGE)\n")
	fmt.Printf("  Total Messages:       %s\n", formatNumber(msg6511))
	fmt.Printf("  Messages Saved:       %s\n", formatNumber(saved))
	
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
	fmt.Printf("  Messages: %s\n", formatNumber(saved))
	fmt.Printf("  Format: Market open notifications\n")

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg6511 > 0 {
		fmt.Printf("✅ SUCCESS: Market Open Messages (6511) processing completed\n")
		fmt.Printf("📊 Captured %d market open notifications\n", saved)
	} else {
		fmt.Printf("⚠️  WARNING: No Market Open Messages (6511) found during session\n")
		fmt.Printf("💡 Note: Market open messages are broadcast at market start (9:15 AM)\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_6511_*.csv file\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}

func getMessageCodeDescription(code uint16) string {
	switch code {
	case 6511:
		return "BC_OPEN_MESSAGE (Market Open)"
	case 6521:
		return "BC_CLOSE_MESSAGE (Market Close)"
	case 6531:
		return "BC_PREOPEN_SHUTDOWN_MSG (Preopen)"
	case 6541:
		return "BC_CIRCUIT_CHECK (Heartbeat)"
	case 6571:
		return "BC_NORMAL_MKT_PREOPEN_ENDED"
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
