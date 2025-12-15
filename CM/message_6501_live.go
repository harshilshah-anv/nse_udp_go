// NSE Capital Market Multicast UDP Receiver - Message 6501 Only
// 
// FOCUS: Only process message code 6501 (BCAST_JRNL_VCT_MSG - Journal/VCT Messages)
// OUTPUT: csv_output/message_6501_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_6501_live.go
// Output: csv_output/message_6501_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 79-80 (Table 23)
// Structure: MS_TRADER_INT_MSG (298 bytes)
// Contains: System messages, auction notifications, margin violations, listings

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
		off      int
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
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}
			copy(dst[op:op+t], src[ip:ip+t])
			op += t
			ip += t

			if ip >= ipEnd {
				return op, nil
			}

			t = int(getU8(srcPtr, ip))
			ip++

			if t >= 16 {
			} else {
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}

				off = (1 + M2_MAX_OFFSET) + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
				ip++

				mPos = op - off
				lastMOff = off

				if op+3 > opEnd {
					return 0, ErrOutputOverrun
				}

				if mPos+3 > op {
					setU8(dstPtr, op, getU8(dstPtr, mPos))
					setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
					setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
				} else {
					setU8(dstPtr, op, getU8(dstPtr, mPos))
					setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
					setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
				}
				op += 3

				if ip >= ipEnd {
					return op, nil
				}

				t = int(getU8(srcPtr, ip-1)) & 3

				if t != 0 {
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
					if ip+3 > ipEnd {
						return 0, ErrInputOverrun
					}
					t = int(getU8(srcPtr, ip))
					ip++
				}
			}
		}
	}

	for {
		if t >= 16 {
			goto matchHandling
		}

		if t == 0 {
			for ip < ipEnd && getU8(srcPtr, ip) == 0 {
				t += 255
				ip++
			}
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}
			t += 15 + int(getU8(srcPtr, ip))
			ip++
		}

		t += 3
		if op+t > opEnd || ip+t > ipEnd {
			return 0, ErrOutputOverrun
		}
		copy(dst[op:op+t], src[ip:ip+t])
		op += t
		ip += t

		if ip >= ipEnd {
			return op, nil
		}

		t = int(getU8(srcPtr, ip))
		ip++

		if t >= 16 {
			goto matchHandling
		}

		if ip >= ipEnd {
			return 0, ErrInputOverrun
		}

		off = 1 + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
		ip++
		mPos = op - off
		lastMOff = off

		if op+3 > opEnd {
			return 0, ErrOutputOverrun
		}

		if mPos+3 > op {
			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
		} else {
			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			setU8(dstPtr, op+2, getU8(dstPtr, mPos+2))
		}
		op += 3

		goto matchDone

	matchHandling:
		if t >= 64 {
			off = t & 0x1f

			if off >= 0x1c {
				if lastMOff == 0 {
					return 0, ErrCorrupted
				}
				mPos = op - lastMOff
			} else {
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				off = 1 + (off << 6) + (int(getU8(srcPtr, ip)) >> 2)
				ip++
				mPos = op - off
				lastMOff = off
			}

			mLen = (t >> 5) - 1

		} else if t >= 32 {
			t &= 31

			if t == 0 {
				for ip < ipEnd && getU8(srcPtr, ip) == 0 {
					t += 255
					ip++
				}
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				t += 31 + int(getU8(srcPtr, ip))
				ip++
			}

			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}

			off = 1 + (int(getU8(srcPtr, ip)) << 6) + (int(getU8(srcPtr, ip+1)) >> 2)
			ip += 2

			mPos = op - off
			lastMOff = off
			mLen = t

		} else if t >= 16 {
			mPos = op
			mPos -= (t & 8) << 11

			t &= 7

			if t == 0 {
				for ip < ipEnd && getU8(srcPtr, ip) == 0 {
					t += 255
					ip++
				}
				if ip >= ipEnd {
					return 0, ErrInputOverrun
				}
				t += 7 + int(getU8(srcPtr, ip))
				ip++
			}
			if ip+2 > ipEnd {
				return 0, ErrInputOverrun
			}

			mPos -= (int(getU8(srcPtr, ip)) << 6) + (int(getU8(srcPtr, ip+1)) >> 2)
			ip += 2
			if mPos == op {
				return op, nil
			}

			mPos -= 0x4000
			lastMOff = op - mPos
			mLen = t
		} else {
			if ip >= ipEnd {
				return 0, ErrInputOverrun
			}

			off = 1 + (t << 6) + (int(getU8(srcPtr, ip)) >> 2)
			ip++

			mPos = op - off
			lastMOff = off

			if op+2 > opEnd {
				return 0, ErrOutputOverrun
			}

			setU8(dstPtr, op, getU8(dstPtr, mPos))
			setU8(dstPtr, op+1, getU8(dstPtr, mPos+1))
			op += 2

			goto matchDone
		}
		totalLen = 2 + mLen
		if op+totalLen > opEnd {
			return 0, ErrOutputOverrun
		}

		if mPos+totalLen > op {
			for i := 0; i < totalLen; i++ {
				setU8(dstPtr, op+i, getU8(dstPtr, mPos+i))
			}
			op += totalLen
		} else {
			copy(dst[op:op+totalLen], dst[mPos:mPos+totalLen])
			op += totalLen
		}

		goto matchDone

	matchDone:
		t = int(getU8(srcPtr, ip-1)) & 3

		if ip >= ipEnd {
			return op, nil
		}

		if t != 0 {
			if op+t > opEnd || ip+t > ipEnd {
				return 0, ErrOutputOverrun
			}

			switch t {
			case 1:
				setU8(dstPtr, op, getU8(srcPtr, ip))
			case 2:
				setU8(dstPtr, op, getU8(srcPtr, ip))
				setU8(dstPtr, op+1, getU8(srcPtr, ip+1))
			case 3:
				setU8(dstPtr, op, getU8(srcPtr, ip))
				setU8(dstPtr, op+1, getU8(srcPtr, ip+1))
				setU8(dstPtr, op+2, getU8(srcPtr, ip+2))
			default:
				copy(dst[op:op+t], src[ip:ip+t])
			}
			op += t
			ip += t

			if ip >= ipEnd {
				return op, nil
			}

			t = int(getU8(srcPtr, ip))
			ip++
			goto matchHandling
		}

		if ip >= ipEnd {
			return op, nil
		}

		if ip+3 > ipEnd {
			return 0, ErrInputOverrun
		}

		t = int(getU8(srcPtr, ip))
		ip++
		continue
	}
}

// =============================================================================
// MESSAGE STRUCTURE FOR 6501
// =============================================================================

// Message6501 - BCAST_JRNL_VCT_MSG (Journal/VCT Messages)
// Per NSE CM Protocol Table 23 (Page 79-80)
// Total packet: 298 bytes
type Message6501 struct {
	TransactionCode uint16   // Always 6501
	BranchNumber    uint16   // 2 bytes
	BrokerNumber    [5]byte  // 5 bytes
	ActionCode      [3]byte  // 3 bytes - 'SYS', 'AUI', 'AUC', 'LIS', 'MAR'
	Reserved        [4]byte  // 4 bytes
	TraderWsBit     byte     // 1 byte - bit flags
	Reserved2       byte     // 1 byte
	MsgLength       uint16   // 2 bytes
	Msg             [240]byte // 240 bytes - Actual message content
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
	
	// 6501 specific counters
	message6501Count   int64
	message6501Saved   int64
	
	// CSV file for 6501
	csvFile6501        *os.File
	csvWriter6501      *csv.Writer
	
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

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 6501
	if err := initialize6501CSV(); err != nil {
		log.Fatalf("Failed to initialize 6501 CSV: %v", err)
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

	fmt.Println("⏹️  Press Ctrl+C to stop")

	// Wait for Ctrl+C
	<-sigChan

	fmt.Println("\n\n⏹️  Shutdown signal received...")
	close(shutdownChan)
	time.Sleep(1 * time.Second)

	// Close CSV files
	if csvWriter6501 != nil {
		csvWriter6501.Flush()
	}
	if csvFile6501 != nil {
		csvFile6501.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize6501CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_6501_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile6501 = file
	csvWriter6501 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", 
		"TransactionCode", 
		"BranchNumber",
		"BrokerNumber",
		"ActionCode",
		"MsgLength",
		"Message",
	}
	csvWriter6501.Write(headers)
	csvWriter6501.Flush()

	fmt.Printf("📁 Created CSV file for Message 6501: %s\n", filename)
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
            
	fmt.Printf("\n╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ NSE CM Message 6501 Receiver - Live Market Data          ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 6501 (BCAST_JRNL_VCT_MSG)\n")
	fmt.Printf("📊 Statistics every 10 seconds\n")
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
		log.Fatalf("❌ Failed to join multicast group: %v\n", err)
		return
	}
	defer conn.Close()

	// Set read buffer size (2MB)
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

	if len(finalData) < 28 {
		return
	}
	
	// Skip first 8 bytes
	finalData = finalData[8:]
	
	if len(finalData) < 48 {
		return
	}
	
	// Read transaction code
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])
	
	// Track message codes
	messageCodeCounts[transactionCode]++
	
	// Only process message 6501
	if transactionCode != 6501 {
		return
	}

	process6501Message(finalData)
}

// =============================================================================
// MESSAGE 6501 PROCESSOR
// =============================================================================

func process6501Message(data []byte) {
	if len(data) < 298 {
		return
	}

	atomic.AddInt64(&message6501Count, 1)
	currentCount := atomic.LoadInt64(&message6501Count)
	
	transactionCode := binary.BigEndian.Uint16(data[10:12])
	
	// Parse message fields - Per Table 23
	branchNumber := binary.BigEndian.Uint16(data[40:42])
	
	var brokerNumber [5]byte
	copy(brokerNumber[:], data[42:47])
	
	var actionCode [3]byte
	copy(actionCode[:], data[47:50])
	
	msgLength := binary.BigEndian.Uint16(data[56:58])
	
	var msg [240]byte
	copy(msg[:], data[58:298])
	
	if currentCount == 1 {
		fmt.Printf("\n✅ First Message 6501 received\n\n")
	}
	
	// Export to CSV
	exportToCSV(transactionCode, branchNumber, brokerNumber, actionCode, msgLength, msg)
	atomic.AddInt64(&message6501Saved, 1)
}

func exportToCSV(transactionCode uint16, branchNumber uint16, brokerNumber [5]byte,
	actionCode [3]byte, msgLength uint16, msg [240]byte) {
	
	if csvWriter6501 == nil {
		return
	}

	// Clean strings
	broker := strings.TrimRight(string(brokerNumber[:]), "\x00 ")
	action := strings.TrimRight(string(actionCode[:]), "\x00 ")
	message := strings.TrimRight(string(msg[:msgLength]), "\x00")

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", branchNumber),
		broker,
		action,
		fmt.Sprintf("%d", msgLength),
		message,
	}

	csvWriter6501.Write(record)
	csvWriter6501.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg6501 := atomic.LoadInt64(&message6501Count)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg6501 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 6501: %s | %d msgs\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg6501)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	msg6501 := atomic.LoadInt64(&message6501Count)
	saved6501 := atomic.LoadInt64(&message6501Saved)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 6501 DECODER")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Runtime                : %v\n", duration)
	fmt.Printf("Total Packets Received : %d\n", packets)
	fmt.Printf("Total Bytes Received   : %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	if duration.Seconds() > 0 {
		fmt.Printf("Packets/Second         : %.2f\n", float64(packets)/duration.Seconds())
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Message 6501 Found     : %d messages\n", msg6501)
	fmt.Printf("Messages Saved to CSV  : %d records\n", saved6501)
	fmt.Println(strings.Repeat("=", 80))
	
	// Show all message codes found
	if len(messageCodeCounts) > 0 {
		fmt.Println("📋 ALL MESSAGE CODES DETECTED:")
		fmt.Println(strings.Repeat("-", 80))
		
		type codeFreq struct {
			code uint16
			freq int64
		}
		var sorted []codeFreq
		for code, freq := range messageCodeCounts {
			sorted = append(sorted, codeFreq{code, freq})
		}
		
		// Sort by code number (ascending)
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j].code < sorted[i].code {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		
		// Show all codes
		for _, cf := range sorted {
			percentage := float64(cf.freq) / float64(packets) * 100
			if cf.code == 6501 {
				fmt.Printf("   🎯 Code %5d: %6d messages (%.1f%%) ← TARGET!\n", cf.code, cf.freq, percentage)
			} else {
				fmt.Printf("      Code %5d: %6d messages (%.1f%%)\n", cf.code, cf.freq, percentage)
			}
		}
		fmt.Println(strings.Repeat("-", 80))
	}
	
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Decoder stopped successfully!")
	if msg6501 > 0 {
		fmt.Println("📁 Check csv_output/ directory for the CSV file")
	}
	fmt.Println()
}
