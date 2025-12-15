// NSE Capital Market Multicast UDP Receiver - Message 7206 Only
// 
// FOCUS: Only process message code 7206 (BCAST_SYSTEM_INFORMATION_OUT)
// OUTPUT: csv_output/message_7206_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_7206_live.go
// Output: csv_output/message_7206_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3
// Structure: SYSTEM_INFORMATION_DATA (90 bytes)
// Session: System configuration broadcast information

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
// MESSAGE STRUCTURE FOR 7206
// =============================================================================

// Message7206 - BCAST_SYSTEM_INFORMATION_OUT (System Information Data)
// Per NSE CM Protocol - SYSTEM_INFORMATION_DATA structure
// Total packet: 40 (BCAST_HEADER) + 90 (SYSTEM_INFORMATION_DATA) = 130 bytes
type Message7206 struct {
	TransactionCode                   uint16 // Always 7206
	Normal                           uint16 // Normal market status
	OddLot                           uint16 // Odd lot market status
	Spot                             uint16 // Spot market status
	Auction                          uint16 // Auction market status
	CallAuction1                     uint16 // Call auction 1 status
	CallAuction2                     uint16 // Call auction 2 status
	MarketIndex                      uint32 // Market index value
	DefaultSettlementPeriodNormal    uint16 // Settlement period for normal
	DefaultSettlementPeriodSpot      uint16 // Settlement period for spot
	DefaultSettlementPeriodAuction   uint16 // Settlement period for auction
	CompetitorPeriod                 uint16 // Competitor period
	SolicitorPeriod                  uint16 // Solicitor period
	WarningPercent                   uint16 // Warning percentage
	VolumeFreezePercent              uint16 // Volume freeze percentage
	Reserved1                        [2]byte // Reserved field
	TerminalIdleTime                 uint16 // Terminal idle time
	BoardLotQuantity                 uint32 // Board lot quantity
	TickSize                         uint32 // Tick size
	MaximumGtcDays                   uint16 // Maximum GTC days
	SecurityEligibleIndicators       uint16 // Security eligible indicators
	DisclosedQuantityPercentAllowed  uint16 // Disclosed quantity percent allowed
	Reserved2                        [6]byte // Reserved field
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
	
	// 7206 specific counters
	message7206Count   int64
	message7206Saved   int64
	
	// CSV file for 7206
	csvFile7206        *os.File
	csvWriter7206      *csv.Writer
	
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
	fmt.Printf("NSE CM UDP Receiver - Message 7206 (BCAST_SYSTEM_INFORMATION_OUT)\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code 7206 (0x1C26 in hex)\n")
	fmt.Printf("Purpose: System configuration and market parameters\n")
	fmt.Printf("Session: System information broadcast\n")
	fmt.Printf("Note: Market status, settlement periods, tick sizes\n")
	fmt.Printf("Multicast: 233.1.2.5:8222\n")
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 7206
	if err := initialize7206CSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %v", err)
		return
	}
	defer func() {
		if csvWriter7206 != nil {
			csvWriter7206.Flush()
		}
		if csvFile7206 != nil {
			csvFile7206.Close()
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

func initialize7206CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("message_7206_%s.csv", timestamp)
	filepath := filepath.Join("csv_output", filename)

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}

	csvFile7206 = file
	csvWriter7206 = csv.NewWriter(file)

	// Write header
	header := []string{
		"Timestamp",
		"TransactionCode",
		"Normal",
		"OddLot", 
		"Spot",
		"Auction",
		"CallAuction1",
		"CallAuction2",
		"MarketIndex",
		"DefaultSettlementPeriodNormal",
		"DefaultSettlementPeriodSpot",
		"DefaultSettlementPeriodAuction",
		"CompetitorPeriod",
		"SolicitorPeriod",
		"WarningPercent",
		"VolumeFreezePercent",
		"TerminalIdleTime",
		"BoardLotQuantity",
		"TickSize",
		"MaximumGtcDays",
		"SecurityEligibleIndicators",
		"DisclosedQuantityPercentAllowed",
	}

	csvWriter7206.Write(header)
	csvWriter7206.Flush()

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
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 7206 for Auction Trading
	// Offset 40+:   Message payload starts

	if len(finalData) < 48 {
		return
	}

	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])

	// Track message codes for statistics
	messageCodeCounts[transactionCode]++

	// Only process message 7206
	if transactionCode != 7206 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process7206Message(finalData)
}

// =============================================================================
// MESSAGE 7206 PROCESSOR
// =============================================================================

func process7206Message(data []byte) {
	if len(data) < 130 { // 40-byte header + 90-byte message
		return
	}

	atomic.AddInt64(&message7206Count, 1)

	var msg Message7206
	offset := 40 // Skip BCAST_HEADER

	// Parse SYSTEM_INFORMATION_DATA structure (90 bytes)
	msg.TransactionCode = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.Normal = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.OddLot = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.Spot = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.Auction = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.CallAuction1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.CallAuction2 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.MarketIndex = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	msg.DefaultSettlementPeriodNormal = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.DefaultSettlementPeriodSpot = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.DefaultSettlementPeriodAuction = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.CompetitorPeriod = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.SolicitorPeriod = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.WarningPercent = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.VolumeFreezePercent = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Skip Reserved1 (2 bytes)
	copy(msg.Reserved1[:], data[offset:offset+2])
	offset += 2

	msg.TerminalIdleTime = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.BoardLotQuantity = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	msg.TickSize = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	msg.MaximumGtcDays = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.SecurityEligibleIndicators = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	msg.DisclosedQuantityPercentAllowed = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Skip Reserved2 (6 bytes)
	copy(msg.Reserved2[:], data[offset:offset+6])

	// Export to CSV
	exportTo7206CSV(msg)
}

func exportTo7206CSV(msg Message7206) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	csvRecord := []string{
		timestamp,
		fmt.Sprintf("%d", msg.TransactionCode),
		fmt.Sprintf("%d", msg.Normal),
		fmt.Sprintf("%d", msg.OddLot),
		fmt.Sprintf("%d", msg.Spot),
		fmt.Sprintf("%d", msg.Auction),
		fmt.Sprintf("%d", msg.CallAuction1),
		fmt.Sprintf("%d", msg.CallAuction2),
		fmt.Sprintf("%d", msg.MarketIndex),
		fmt.Sprintf("%d", msg.DefaultSettlementPeriodNormal),
		fmt.Sprintf("%d", msg.DefaultSettlementPeriodSpot),
		fmt.Sprintf("%d", msg.DefaultSettlementPeriodAuction),
		fmt.Sprintf("%d", msg.CompetitorPeriod),
		fmt.Sprintf("%d", msg.SolicitorPeriod),
		fmt.Sprintf("%d", msg.WarningPercent),
		fmt.Sprintf("%d", msg.VolumeFreezePercent),
		fmt.Sprintf("%d", msg.TerminalIdleTime),
		fmt.Sprintf("%d", msg.BoardLotQuantity),
		fmt.Sprintf("%d", msg.TickSize),
		fmt.Sprintf("%d", msg.MaximumGtcDays),
		fmt.Sprintf("%d", msg.SecurityEligibleIndicators),
		fmt.Sprintf("%d", msg.DisclosedQuantityPercentAllowed),
	}

	csvWriter7206.Write(csvRecord)
	csvWriter7206.Flush()
	atomic.AddInt64(&message7206Saved, 1)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg7206 := atomic.LoadInt64(&message7206Count)
	saved := atomic.LoadInt64(&message7206Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg7206 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 7206: %s | %d msgs, %d saved\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg7206, saved)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	totalMB := float64(atomic.LoadInt64(&totalBytes)) / 1024.0 / 1024.0
	compressed := atomic.LoadInt64(&compressedCount)
	decompressed := atomic.LoadInt64(&decompressedCount)
	errors := atomic.LoadInt64(&decompressionErrors)
	msg7206 := atomic.LoadInt64(&message7206Count)
	saved := atomic.LoadInt64(&message7206Saved)

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("FINAL STATISTICS - MESSAGE 7206 DECODER (BCAST_SYSTEM_INFORMATION_OUT)\n")
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

	fmt.Printf("\n🎯 MESSAGE 7206 STATISTICS (BCAST_SYSTEM_INFORMATION_OUT)\n")
	fmt.Printf("  Total Messages:       %s\n", formatNumber(msg7206))
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
	fmt.Printf("  Format: System configuration data\n")

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	if msg7206 > 0 {
		fmt.Printf("✅ SUCCESS: System Information Messages (7206) processing completed\n")
		fmt.Printf("📊 Captured %d system configuration updates\n", saved)
	} else {
		fmt.Printf("⚠️  WARNING: No System Information Messages (7206) found during session\n")
		fmt.Printf("💡 Note: System information is broadcast at market events\n")
	}
	fmt.Printf("✅ Check csv_output/ for message_7206_*.csv file\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n")
}

func getMessageCodeDescription(code uint16) string {
	switch code {
	case 7200:
		return "BCAST_ONLY_MBO (Market by Order)"
	case 7201:
		return "BCAST_SECURITY_STATUS"
	case 7202:
		return "BCAST_TICKER_AND_MKT_INDEX"
	case 7206:
		return "BCAST_SYSTEM_INFORMATION_OUT"
	case 7207:
		return "BCAST_AUCTION_MKTWATCH"
	case 7208:
		return "BCAST_LTP_REPLY"
	case 7211:
		return "BCAST_MW_ROUND_ROBIN"
	case 7220:
		return "BCAST_ONLY_MBP (Market by Price)"
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
