// NSE Message Code Counter - 15 Minute Intervals
// 
// PURPOSE: Count all message codes received every 15 minutes
// START TIME: 7:58 AM
// OUTPUT: message_codes_YYYYMMDD.txt (appends every 15 minutes)
//
// USAGE:
// ======
// Run: go run message_code_counter.go lzo_decompressor_safe.go
// Output: message_codes_YYYYMMDD.txt

package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// GLOBAL VARIABLES
// =============================================================================

var (
	// Message code counters (map protected by mutex)
	messageCodeCounts sync.Map // map[uint16]int64
	
	// Total packet counters
	totalPackets        int64
	compressedCount     int64
	decompressedCount   int64
	decompressionErrors int64
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Println("║   NSE Message Code Counter - 15 Minute Intervals          ║")
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Wait until 7:58 AM
	waitUntilStartTime()

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 1000)

	// Start packet processor
	go processPackets()

	// Start UDP listener
	go startUDPListener()
	
	// Start 15-minute interval reporter
	go reportEvery15Minutes()
	
	// Keep running until Ctrl+C
	fmt.Println("📊 Monitoring message codes... Press Ctrl+C to stop")
	fmt.Println()
	
	select {}
}

// =============================================================================
// WAIT FOR START TIME (7:58 AM)
// =============================================================================

func waitUntilStartTime() {
	now := time.Now()
	target := time.Date(now.Year(), now.Month(), now.Day(), 7, 58, 0, 0, now.Location())
	
	// If 7:58 AM already passed today, start immediately
	if now.After(target) {
		fmt.Printf("⏰ Current time: %s (already past 7:58 AM)\n", now.Format("15:04:05"))
		fmt.Println("🚀 Starting immediately...\n")
		return
	}
	
	// Wait until 7:58 AM
	duration := target.Sub(now)
	fmt.Printf("⏰ Current time: %s\n", now.Format("15:04:05"))
	fmt.Printf("⏳ Waiting until 7:58 AM... (%v to go)\n\n", duration.Round(time.Second))
	
	time.Sleep(duration)
	fmt.Println("🚀 Starting at 7:58 AM...\n")
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "231.31.31.4"
	port := 34330
	
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Fatalf("Failed to join multicast group: %v", err)
		return
	}
	defer conn.Close()

	conn.SetReadBuffer(2 * 1024 * 1024)

	buffer := make([]byte, 2048)

	for {
		select {
		case <-shutdownChan:
			return
		default:
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				continue
			}

			atomic.AddInt64(&totalPackets, 1)

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

	// Extract message code
	messageCode := binary.BigEndian.Uint16(finalData[18:20])
	
	// Increment counter for this message code
	incrementMessageCode(messageCode)
}

func incrementMessageCode(code uint16) {
	// Load current count or default to 0
	val, _ := messageCodeCounts.LoadOrStore(code, new(int64))
	counter := val.(*int64)
	atomic.AddInt64(counter, 1)
}

// =============================================================================
// 15-MINUTE INTERVAL REPORTER
// =============================================================================

func reportEvery15Minutes() {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			saveMessageCodeReport()
		case <-shutdownChan:
			// Save final report before exit
			saveMessageCodeReport()
			return
		}
	}
}

func saveMessageCodeReport() {
	now := time.Now()
	
	// Create filename with date
	filename := filepath.Join("csv_output_2", fmt.Sprintf("message_codes_%s.txt", 
		now.Format("20060102")))
	
	// Open file in append mode
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return
	}
	defer file.Close()

	// Write header
	fmt.Fprintf(file, "\n")
	fmt.Fprintf(file, "═══════════════════════════════════════════════════════════════\n")
	fmt.Fprintf(file, "Report Time: %s\n", now.Format("2006-01-02 15:04:05"))
	fmt.Fprintf(file, "Duration: %v\n", time.Since(startTime).Round(time.Second))
	fmt.Fprintf(file, "═══════════════════════════════════════════════════════════════\n")
	fmt.Fprintf(file, "\n")
	
	// Collect and sort message codes
	type codeCount struct {
		code  uint16
		count int64
	}
	
	var codes []codeCount
	totalMessages := int64(0)
	
	messageCodeCounts.Range(func(key, value interface{}) bool {
		code := key.(uint16)
		counter := value.(*int64)
		count := atomic.LoadInt64(counter)
		
		codes = append(codes, codeCount{code: code, count: count})
		totalMessages += count
		return true
	})
	
	// Sort by message code
	for i := 0; i < len(codes); i++ {
		for j := i + 1; j < len(codes); j++ {
			if codes[i].code > codes[j].code {
				codes[i], codes[j] = codes[j], codes[i]
			}
		}
	}
	
	// Write message code summary
	fmt.Fprintf(file, "Message Code Statistics:\n")
	fmt.Fprintf(file, "───────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(file, "%-15s | %-15s | %-10s\n", "Message Code", "Count", "Percentage")
	fmt.Fprintf(file, "───────────────────────────────────────────────────────────────\n")
	
	for _, cc := range codes {
		percentage := float64(cc.count) / float64(totalMessages) * 100
		fmt.Fprintf(file, "%-15d | %-15d | %6.2f%%\n", cc.code, cc.count, percentage)
	}
	
	fmt.Fprintf(file, "───────────────────────────────────────────────────────────────\n")
	fmt.Fprintf(file, "Total Messages: %d\n", totalMessages)
	fmt.Fprintf(file, "Total Packets:  %d\n", atomic.LoadInt64(&totalPackets))
	fmt.Fprintf(file, "Compressed:     %d\n", atomic.LoadInt64(&compressedCount))
	fmt.Fprintf(file, "Decompressed:   %d\n", atomic.LoadInt64(&decompressedCount))
	fmt.Fprintf(file, "Errors:         %d\n", atomic.LoadInt64(&decompressionErrors))
	fmt.Fprintf(file, "\n")
	
	// Also print to console
	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════╗")
	fmt.Printf("║ 15-Minute Report: %s                      ║\n", now.Format("15:04:05"))
	fmt.Println("╚════════════════════════════════════════════════════════════╝")
	fmt.Printf("📊 Total Messages: %d\n", totalMessages)
	fmt.Printf("📦 Total Packets:  %d\n", atomic.LoadInt64(&totalPackets))
	fmt.Println()
	fmt.Println("Message Codes Received:")
	for _, cc := range codes {
		percentage := float64(cc.count) / float64(totalMessages) * 100
		fmt.Printf("  Code %4d: %10d messages (%6.2f%%)\n", cc.code, cc.count, percentage)
	}
	fmt.Printf("\n💾 Saved to: %s\n\n", filename)
}
