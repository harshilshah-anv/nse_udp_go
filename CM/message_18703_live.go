// NSE Capital Market Multicast UDP Receiver - Message 18703 Only
// 
// FOCUS: Only process message code 18703 (BCAST_TICKER_AND_MKT_INDEX - Ticker and Market Index)
// OUTPUT: csv_output/message_18703_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_18703_live.go
// Output: csv_output/message_18703_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 111-112, Table 36.1
// Structure: BCAST_HEADER(40) + NoOfRecords(2) + TICKER_INDEX_INFORMATION[28](18 bytes each)
// Fields per record: Token, MarketType, FillPrice, FillVolume, MarketIndexValue
// Maximum Records: 28 per broadcast packet (546 total bytes)

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
// MESSAGE STRUCTURE FOR 18703
// =============================================================================

// Message18703 - BCAST_TICKER_AND_MKT_INDEX (Ticker and Market Index)
// 
// This message broadcasts ticker and market index information with:
// - Token number (unique security identifier)
// - Market type (Regular/Auction/OddLot/Spot)
// - Fill price and volume (last trade info)
// - Market index value
// 
// Structure: BCAST_HEADER (40 bytes) + TICKER INDEX INFORMATION records (18 bytes each)
// Maximum 28 records per message
type Message18703 struct {
	TransactionCode uint16 // Always 18703
	NoOfRecords     uint16 // Number of Ticker Index records (max 28)
}

// TickerIndexInfo18703 - Individual Ticker Index record (18 bytes)
// Per NSE CM Protocol Table 36.1 (Page 111-112)
type TickerIndexInfo18703 struct {
	Token            uint32 // 4 bytes, offset 0 - Token number (LONG - CM tokens 1-39999)
	MarketType       int16  // 2 bytes, offset 4 - Market type (1=Regular, 2=Auction, 3=OddLot, 4=Spot)
	FillPrice        int32  // 4 bytes, offset 6 - Last traded price (LTP) in paise
	FillVolume       int32  // 4 bytes, offset 10 - Last trade quantity
	MarketIndexValue int32  // 4 bytes, offset 14 - Market index value
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
	
	// 18703 specific counters
	message18703Count   int64
	message18703Saved   int64
	
	// CSV file for 18703
	csvFile18703        *os.File
	csvWriter18703      *csv.Writer
	
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
	// Initialize debug tracking
	messageCodeCounts = make(map[uint16]int64)

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	// Initialize CSV file for 18703
	if err := initialize18703CSV(); err != nil {
		log.Fatalf("Failed to initialize 18703 CSV: %v", err)
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
	if csvWriter18703 != nil {
		csvWriter18703.Flush()
	}
	if csvFile18703 != nil {
		csvFile18703.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize18703CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_18703_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile18703 = file
	csvWriter18703 = csv.NewWriter(file)
	
	// Write headers - Only fields available in NSE Protocol Table 36.1 (TICKER INDEX INFORMATION)
	headers := []string{
		"Timestamp",
		"TransactionCode", 
		"NoOfRecords",
		"Token",
		"MarketType",
		"MarketTypeName", 
		"FillPrice",
		"FillVolume",
		"MarketIndexValue",
	}
	csvWriter18703.Write(headers)
	csvWriter18703.Flush()

	fmt.Printf("📁 Created CSV file for Message 18703: %s\n", filename)
	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "233.1.2.5"  // NSE CM broadcast multicast IP
	port := 8222                // NSE CM port
	
	fmt.Printf("\n╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ NSE CM Message 18703 Receiver - Live Market Data         ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 18703 (BCAST_TICKER_AND_MKT_INDEX)\n")
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
		log.Fatalf("❌ Failed to join multicast group: %v\n", err)
		fmt.Println("\n⚠️  Troubleshooting:")
		fmt.Println("  1. Ensure NSE market is open (9:15 AM - 3:30 PM IST)")
		fmt.Println("  2. Check if you have access to NSE multicast feed")
		fmt.Println("  3. Verify firewall settings allow UDP multicast")
		fmt.Println("  4. Check network connection")
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

	// NSE Packet Structure:
	// [0-3]:   4-byte prefix
	// [4-5]:   Compression length (2 bytes) - if > 0, packet is compressed
	// [6+]:    Compressed or uncompressed data
	
	cPackData := data[4:]
	if len(cPackData) < 2 {
		return
	}

	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])

	var finalData []byte
	isCompressed := iCompLen > 0
	
	if isCompressed {
		// Packet is LZO compressed
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
		// Packet is not compressed
		finalData = cPackData[2:]
	}

	// IMPORTANT: Per NSE CM documentation:
	// "Inside the broadcast data, the first 8 bytes before the message header / broadcast
	//  header should be ignored. The message header / broadcast header starts from the 9th byte."
	
	if len(finalData) < 28 { // Need at least 8 (skip) + 20 (min header)
		return
	}
	
	// Skip first 8 bytes - BCAST_HEADER starts at byte 8
	finalData = finalData[8:]
	
	// BCAST_HEADER Structure (40 bytes total):
	// Per NSE Protocol documentation (F&O Protocol applies to CM as well):
	// Offset 0-1:   Reserved (2 bytes)
	// Offset 2-3:   Reserved (2 bytes)
	// Offset 4-7:   LogTime (LONG 4 bytes)
	// Offset 8-9:   AlphaChar (2 bytes)
	// Offset 10-11: TransactionCode (SHORT 2 bytes) ← 18703 for BCAST_TICKER_AND_MKT_INDEX
	// Offset 12-13: ErrorCode (SHORT 2 bytes)
	// Offset 14-17: BCSeqNo (LONG 4 bytes)
	// Offset 18-39: Reserved/Timestamp/Filler fields
	// Offset 40+:   Message payload starts (NoOfRecords + Records)
	
	if len(finalData) < 48 {
		return
	}
	
	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	// Per NSE Protocol: BCAST_HEADER has TransactionCode at offset 10 (2 bytes, SHORT)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])
	
	// Track message codes for statistics
	messageCodeCounts[transactionCode]++
	
	// Only process message 18703
	if transactionCode != 18703 {
		return
	}

	// Process the message (finalData already has 8 bytes skipped)
	process18703Message(finalData)
}

// =============================================================================
// MESSAGE 18703 PROCESSOR
// =============================================================================

func process18703Message(data []byte) {
	if len(data) < 42 { // 40-byte header + at least 2 bytes for NoOfRecords
		return
	}

	atomic.AddInt64(&message18703Count, 1)
	currentCount := atomic.LoadInt64(&message18703Count)
	
	// Parse BCAST_HEADER (40 bytes)
	// Per NSE Protocol documentation:
	// Offset 10-12: TransactionCode (18703)
	// After BCAST_HEADER: NoOfRecords (2 bytes)
	
	transactionCode := binary.BigEndian.Uint16(data[10:12])
	
	// NoOfRecords is right after the 40-byte BCAST_HEADER
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	
	// Show first message info
	if currentCount == 1 {
		fmt.Printf("\n✅ First Message 18703 received:\n")
		fmt.Printf("   📦 Packet size: %d bytes\n", len(data))
		fmt.Printf("   📊 Records: %d\n", noOfRecords)
		fmt.Printf("   📐 Expected size: %d bytes (header) + %d bytes (records) = %d bytes\n", 
			42, int(noOfRecords)*18, 42+int(noOfRecords)*18)
		fmt.Printf("   ✅ Structure: NSE Protocol Table 36.1 (TICKER INDEX INFORMATION)\n")
		fmt.Println()
	}
	
	// Parse records according to NSE Protocol Table 36.1 (18 bytes per record)
	offset := 42
	
	for i := 0; i < int(noOfRecords) && i < 28; i++ { // Max 28 records per NSE protocol
		if offset+18 > len(data) {
			break
		}
		
		// Parse TICKER INDEX INFORMATION: 18 bytes exactly per NSE Protocol Table 36.1
		token := binary.BigEndian.Uint32(data[offset : offset+4])
		marketType := int16(binary.BigEndian.Uint16(data[offset+4 : offset+6]))
		fillPrice := int32(binary.BigEndian.Uint32(data[offset+6 : offset+10]))
		fillVolume := int32(binary.BigEndian.Uint32(data[offset+10 : offset+14]))
		marketIndexValue := int32(binary.BigEndian.Uint32(data[offset+14 : offset+18]))

		// Export to CSV with actual protocol data only
		exportTo18703CSV(transactionCode, noOfRecords, token, marketType, 
			fillPrice, fillVolume, marketIndexValue)
		
		atomic.AddInt64(&message18703Saved, 1)
		
		// Move to next record (exactly 18 bytes per NSE protocol)
		offset += 18
	}
}

func exportTo18703CSV(transactionCode, noOfRecords uint16, token uint32, marketType int16, 
	fillPrice, fillVolume, marketIndexValue int32) {
	
	if csvWriter18703 == nil {
		return
	}

	// Get market type name
	marketTypeName := getMarketTypeName(marketType)
	
	// Format the CSV record with actual protocol fields only
	record := []string{
		time.Now().Format("15:04:05.0"),                 // Timestamp
		fmt.Sprintf("%d", transactionCode),              // TransactionCode (18703)
		fmt.Sprintf("%d", noOfRecords),                  // NoOfRecords 
		fmt.Sprintf("%d", token),                        // Token
		fmt.Sprintf("%d", marketType),                   // MarketType (numeric)
		marketTypeName,                                  // MarketTypeName (text)
		fmt.Sprintf("%.2f", float64(fillPrice)/100.0),  // FillPrice (converted to rupees)
		fmt.Sprintf("%d", fillVolume),                   // FillVolume
		fmt.Sprintf("%d", marketIndexValue),             // MarketIndexValue
	}

	csvWriter18703.Write(record)
	csvWriter18703.Flush()
}



// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

func getMarketTypeName(marketType int16) string {
	switch marketType {
	case 1:
		return "REGULAR"
	case 2:
		return "AUCTION"
	case 3:
		return "ODDLOT"
	case 4:
		return "SPOT"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", marketType)
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg18703 := atomic.LoadInt64(&message18703Count)
	saved18703 := atomic.LoadInt64(&message18703Saved)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg18703 > 0 {
			status = "✅ RECEIVING"
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 18703: %s | %d msgs, %d records\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg18703, saved18703)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	msg18703 := atomic.LoadInt64(&message18703Count)
	saved18703 := atomic.LoadInt64(&message18703Saved)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 18703 DECODER")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Runtime                : %v\n", duration)
	fmt.Printf("Total Packets Received : %d\n", packets)
	fmt.Printf("Total Bytes Received   : %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	if duration.Seconds() > 0 {
		fmt.Printf("Packets/Second         : %.2f\n", float64(packets)/duration.Seconds())
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Message 18703 Found    : %d messages\n", msg18703)
	fmt.Printf("Records Saved to CSV   : %d records\n", saved18703)
	if msg18703 > 0 {
		fmt.Printf("Average Records/Msg    : %.2f\n", float64(saved18703)/float64(msg18703))
	}
	fmt.Println(strings.Repeat("=", 80))
	
	// Show all message codes found
	if len(messageCodeCounts) > 0 {
		fmt.Println("📋 ALL MESSAGE CODES DETECTED:")
		fmt.Println(strings.Repeat("-", 80))
		
		// Sort by code number
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
			if cf.code == 18703 {
				fmt.Printf("   🎯 Code %5d: %6d messages (%.1f%%) ← TARGET!\n", cf.code, cf.freq, percentage)
			} else {
				fmt.Printf("      Code %5d: %6d messages (%.1f%%)\n", cf.code, cf.freq, percentage)
			}
		}
		fmt.Println(strings.Repeat("-", 80))
	}
	
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Decoder stopped successfully!")
	if msg18703 > 0 {
		fmt.Println("📁 Check csv_output/ directory for the CSV file")
	} else {
		fmt.Println("⚠️  Message 18703 NOT detected on this channel!")
		fmt.Println("💡 Possible reasons:")
		fmt.Println("   - Market might be closed")
		fmt.Println("   - Wrong multicast IP/Port for message 18703")
		fmt.Println("   - Message 18703 might be on different broadcast channel")
		fmt.Println("   - Check if you need different port or IP for CM Ticker data")
	}
	fmt.Println()
}
