// NSE Multicast UDP Receiver - Message 17202 Only
// 
// FOCUS: Only process message code 17202 (BCAST_ENHNCD_TICKER_AND_MKT_INDEX - Enhanced Ticker and Market Index)
// OUTPUT: csv_output/message_17202_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run message_17202_live.go
// Output: csv_output/message_17202_TIMESTAMP.csv
//
// Protocol Reference: NSE NNF Protocol v9.46, Page 169
// Structure: ST_ENHNCD_TICKER_INDEX_INFO (38 bytes per record)
// Maximum Records: 12 per broadcast packet

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
// MESSAGE STRUCTURE FOR 17202
// =============================================================================

// Message17202 - BCAST_ENHNCD_TICKER_AND_MKT_INDEX (Enhanced Ticker and Market Index)
// 
// This message broadcasts enhanced ticker and market index information with:
// - Token number (unique security identifier)
// - Market type (Regular/Auction/OddLot/Spot)
// - Fill price and volume (last trade info)
// - Open Interest (OI) data with 8-byte precision (LONG LONG)
// - Day high and low OI values
// 
// Structure: BCAST_HEADER (40 bytes) + ST_ENHNCD_TICKER_INDEX_INFO records (38 bytes each)
// Maximum 12 records per message
type Message17202 struct {
	TransactionCode uint16 // Always 17202
	NoOfRecords     uint16 // Number of Ticker Index records (max 12)
}

// TickerIndexInfo17202 - Individual Ticker Index record (38 bytes)
// Per NSE Table 72_A (Page 169): Enhanced version with 8-byte OI fields
type TickerIndexInfo17202 struct {
	Token        uint32 // 4 bytes, offset 0 - Token number (UNSIGNED LONG - unique security ID)
	MarketType   int16  // 2 bytes, offset 4 - Market type (1=Regular, 2=Auction, 3=OddLot, 4=Spot)
	FillPrice    int32  // 4 bytes, offset 6 - Last traded price (LTP) in paise
	FillVolume   int32  // 4 bytes, offset 10 - Last trade quantity
	OpenInterest int64  // 8 bytes, offset 14 - Current Open Interest (LONG LONG)
	DayHiOI      int64  // 8 bytes, offset 22 - Day High Open Interest (LONG LONG)
	DayLoOI      int64  // 8 bytes, offset 30 - Day Low Open Interest (LONG LONG)
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
	
	// 17202 specific counters
	message17202Count   int64
	message17202Saved   int64
	
	// CSV file for 17202
	csvFile17202        *os.File
	csvWriter17202      *csv.Writer
	
	// CSV file specifically for token 26049
	csvFile26049        *os.File
	csvWriter26049      *csv.Writer
	token26049Found     bool
	token26049Count     int64
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
	
	// Debug: Track all message codes
	messageCodeCounts   map[uint16]int64
	debugPrinted        bool
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

	// Initialize CSV file for 17202
	if err := initialize17202CSV(); err != nil {
		log.Fatalf("Failed to initialize 17202 CSV: %v", err)
		return
	}
	
	// Initialize CSV file specifically for token 26049
	if err := initialize26049CSV(); err != nil {
		log.Fatalf("Failed to initialize 26049 CSV: %v", err)
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
	if csvWriter17202 != nil {
		csvWriter17202.Flush()
	}
	if csvFile17202 != nil {
		csvFile17202.Close()
	}
	
	if csvWriter26049 != nil {
		csvWriter26049.Flush()
	}
	if csvFile26049 != nil {
		csvFile26049.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize17202CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_17202_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile17202 = file
	csvWriter17202 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", 
		"TransactionCode", 
		"NoOfRecords", 
		"Token", 
		"MarketType",
		"MarketTypeName",
		"FillPrice", 
		"FillVolume", 
		"OpenInterest",
		"DayHiOI",
		"DayLoOI",
		"OI_Change",
	}
	csvWriter17202.Write(headers)
	csvWriter17202.Flush()

	fmt.Printf("📁 Created CSV file for Message 17202: %s\n", filename)
	return nil
}

func initialize26049CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("token_26049_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile26049 = file
	csvWriter26049 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", 
		"TransactionCode", 
		"Token", 
		"MarketType",
		"MarketTypeName",
		"FillPrice", 
		"FillVolume", 
		"OpenInterest",
		"DayHiOI",
		"DayLoOI",
		"OI_Change",
	}
	csvWriter26049.Write(headers)
	csvWriter26049.Flush()

	fmt.Printf("📁 Created CSV file for Token 26049: %s\n", filename)
	return nil
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "233.1.2.5"  // NSE F&O broadcast multicast IP
	//multicastIP := "231.31.31.4" // Alternative multicast IP
	//port := 34330  
	port := 8222             // NSE F&O port
	//port := 55655               // Alternative port
	
	fmt.Printf("\n╔════════════════════════════════════════════════════════════╗\n")
	fmt.Printf("║ NSE Message 17202 Receiver - Live Market Data            ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 17202 (BCAST_ENHNCD_TICKER_AND_MKT_INDEX)\n")
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

	// Debug: Print first decompressed packet structure once
	if !debugPrinted && atomic.LoadInt64(&packetCount) > 100 {
		debugPrinted = true
		fmt.Printf("\n🔍 DEBUG: First decompressed packet:\n")
		fmt.Printf("   Compressed: %v, Decompressed length: %d bytes\n", isCompressed, len(finalData))
		fmt.Printf("   First 24 bytes (hex): ")
		for i := 0; i < 24 && i < len(finalData); i++ {
			fmt.Printf("%02X ", finalData[i])
		}
		fmt.Printf("\n")
		if len(finalData) >= 20 {
			code_offset_10 := binary.BigEndian.Uint16(finalData[10:12])
			code_offset_14 := binary.BigEndian.Uint16(finalData[14:16])
			code_offset_18 := binary.BigEndian.Uint16(finalData[18:20])
			fmt.Printf("   Transaction code at offset 10-12: %d\n", code_offset_10)
			fmt.Printf("   Transaction code at offset 14-16: %d\n", code_offset_14)
			fmt.Printf("   Transaction code at offset 18-20: %d\n", code_offset_18)
		}
		fmt.Printf("\n")
	}

	// IMPORTANT: Per NSE documentation (Page 152):
	// "Inside the broadcast data, the first 8 bytes before the message header / broadcast
	//  header should be ignored. The message header / broadcast header starts from the 9th byte."
	
	if len(finalData) < 28 { // Need at least 8 (skip) + 20 (min header)
		return
	}
	
	// Skip first 8 bytes - BCAST_HEADER starts at byte 8
	finalData = finalData[8:]
	
	// Now BCAST_HEADER is at offset 0
	// BCAST_HEADER structure (40 bytes):
	// Offset 0-2:   MessageLength (2 bytes)
	// Offset 2-6:   MessageSequence (4 bytes)
	// Offset 6-8:   TransactionCode (2 bytes) <- INCORRECT! This is wrong offset
	// 
	// CORRECT structure per NSE protocol:
	// Offset 0-4:   Padding/Reserved (4 bytes)
	// Offset 4-8:   Something (4 bytes)
	// Offset 8-10:  Something (2 bytes)
	// Offset 10-12: TransactionCode (2 bytes) <- CORRECT OFFSET!
	
	if len(finalData) < 48 {
		return
	}
	
	// Read transaction code at offset 10-12 in BCAST_HEADER (after 8-byte skip)
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])
	
	// Track ALL message codes for debugging
	messageCodeCounts[transactionCode]++
	
	// Only process message 17202
	if transactionCode != 17202 {
		return
	}
	
	// Show message code distribution every 5000 packets
	totalPackets := atomic.LoadInt64(&packetCount)
	if totalPackets%5000 == 0 && totalPackets > 0 {
		has17202 := messageCodeCounts[17202] > 0
		status := "❌ NOT FOUND"
		if has17202 {
			status = fmt.Sprintf("✅ Found %d times", messageCodeCounts[17202])
		}
		
		// Show top 10 message codes found
		fmt.Printf("\n📊 After %d packets | Message 17202: %s\n", totalPackets, status)
		fmt.Printf("   Top message codes found: ")
		count := 0
		for code, freq := range messageCodeCounts {
			if count < 10 {
				fmt.Printf("%d(%d) ", code, freq)
				count++
			}
		}
		fmt.Printf("\n")
	}

	// Process the message (finalData already has 8 bytes skipped)
	process17202Message(finalData)
}

// =============================================================================
// MESSAGE 17202 PROCESSOR
// =============================================================================

func process17202Message(data []byte) {
	if len(data) < 42 { // 40-byte header + at least 2 bytes for NoOfRecords
		return
	}

	atomic.AddInt64(&message17202Count, 1)
	currentCount := atomic.LoadInt64(&message17202Count)
	
	// Parse BCAST_HEADER (40 bytes)
	// Per NSE Protocol (Page 169):
	// Offset 6-8: TransactionCode (17202)
	// After BCAST_HEADER: NoOfRecords (2 bytes)
	
	transactionCode := binary.BigEndian.Uint16(data[6:8])
	
	// NoOfRecords is right after the 40-byte BCAST_HEADER
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	
	if currentCount == 1 {
		fmt.Printf("\n✅ First Message 17202: %d records\n", noOfRecords)
		fmt.Printf("   Structure: 38 bytes per record (enhanced version)\n")
		fmt.Printf("   Fields: Token, MarketType, FillPrice, FillVolume, OI, DayHiOI, DayLoOI\n\n")
	}
	
	// Parse Ticker Index records
	// Per documentation Table 72_A: ST_ENHNCD_TICKER_INDEX_INFO array starts at offset 42
	// Each record is 38 bytes
	
	offset := 42
	recordSize := 38
	
	for i := 0; i < int(noOfRecords) && i < 12; i++ { // Max 12 records
		if offset+recordSize > len(data) {
			break
		}
		
		// Parse ST_ENHNCD_TICKER_INDEX_INFO: 38 bytes total
		token := binary.BigEndian.Uint32(data[offset : offset+4])              // Token is UNSIGNED LONG
		marketType := int16(binary.BigEndian.Uint16(data[offset+4 : offset+6]))
		fillPrice := int32(binary.BigEndian.Uint32(data[offset+6 : offset+10]))
		fillVolume := int32(binary.BigEndian.Uint32(data[offset+10 : offset+14]))
		openInterest := int64(binary.BigEndian.Uint64(data[offset+14 : offset+22]))
		dayHiOI := int64(binary.BigEndian.Uint64(data[offset+22 : offset+30]))
		dayLoOI := int64(binary.BigEndian.Uint64(data[offset+30 : offset+38]))
		
		// Show first record of first message only
		if currentCount == 1 && i == 0 {
			fmt.Printf("Sample: Token %d → Price %.2f, Volume %d, OI %d\n\n", 
				token, float64(fillPrice)/100.0, fillVolume, openInterest)
		}
		
		// Check if this is token 26049
		if token == 26049 {
			if !token26049Found {
				token26049Found = true
				fmt.Printf("\n🎯 TOKEN 26049 FOUND! Starting to save data...\n")
				fmt.Printf("   Price: %.2f, Volume: %d, OI: %d\n\n", 
					float64(fillPrice)/100.0, fillVolume, openInterest)
			}
			atomic.AddInt64(&token26049Count, 1)
			exportToken26049ToCSV(transactionCode, token, marketType, fillPrice, fillVolume, openInterest, dayHiOI, dayLoOI)
		}
		
		// Export to CSV (all tokens)
		exportToCSV(transactionCode, noOfRecords, token, marketType, fillPrice, fillVolume, openInterest, dayHiOI, dayLoOI)
		atomic.AddInt64(&message17202Saved, 1)
		
		offset += recordSize
	}
}

func exportToCSV(transactionCode, noOfRecords uint16, token uint32, marketType int16, 
	fillPrice, fillVolume int32, openInterest, dayHiOI, dayLoOI int64) {
	
	if csvWriter17202 == nil {
		return
	}

	// Calculate OI change
	oiChange := dayHiOI - dayLoOI

	// Get market type name
	marketTypeName := getMarketTypeName(marketType)

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", noOfRecords),
		fmt.Sprintf("%d", token),
		fmt.Sprintf("%d", marketType),
		marketTypeName,
		fmt.Sprintf("%.2f", float64(fillPrice)/100.0), // Convert paise to rupees
		fmt.Sprintf("%d", fillVolume),
		fmt.Sprintf("%d", openInterest),
		fmt.Sprintf("%d", dayHiOI),
		fmt.Sprintf("%d", dayLoOI),
		fmt.Sprintf("%d", oiChange),
	}

	csvWriter17202.Write(record)
	csvWriter17202.Flush()
}

func exportToken26049ToCSV(transactionCode uint16, token uint32, marketType int16, 
	fillPrice, fillVolume int32, openInterest, dayHiOI, dayLoOI int64) {
	
	if csvWriter26049 == nil {
		return
	}

	// Calculate OI change
	oiChange := dayHiOI - dayLoOI

	// Get market type name
	marketTypeName := getMarketTypeName(marketType)

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", token),
		fmt.Sprintf("%d", marketType),
		marketTypeName,
		fmt.Sprintf("%.2f", float64(fillPrice)/100.0), // Convert paise to rupees
		fmt.Sprintf("%d", fillVolume),
		fmt.Sprintf("%d", openInterest),
		fmt.Sprintf("%d", dayHiOI),
		fmt.Sprintf("%d", dayLoOI),
		fmt.Sprintf("%d", oiChange),
	}

	csvWriter26049.Write(record)
	csvWriter26049.Flush()
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
	msg17202 := atomic.LoadInt64(&message17202Count)
	saved17202 := atomic.LoadInt64(&message17202Saved)
	token26049 := atomic.LoadInt64(&token26049Count)

	if duration > 0 {
		status := "❌ NOT FOUND"
		if msg17202 > 0 {
			status = "✅ RECEIVING"
		}
		
		tokenStatus := ""
		if token26049 > 0 {
			tokenStatus = fmt.Sprintf(" | 🎯 Token 26049: ✅ %d records", token26049)
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | 🎯 17202: %s | %d msgs, %d records%s\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg17202, saved17202, tokenStatus)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	msg17202 := atomic.LoadInt64(&message17202Count)
	saved17202 := atomic.LoadInt64(&message17202Saved)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 17202 DECODER")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Runtime                : %v\n", duration)
	fmt.Printf("Total Packets Received : %d\n", packets)
	fmt.Printf("Total Bytes Received   : %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	if duration.Seconds() > 0 {
		fmt.Printf("Packets/Second         : %.2f\n", float64(packets)/duration.Seconds())
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Message 17202 Found    : %d messages\n", msg17202)
	fmt.Printf("Records Saved to CSV   : %d records\n", saved17202)
	if msg17202 > 0 {
		fmt.Printf("Average Records/Msg    : %.2f\n", float64(saved17202)/float64(msg17202))
	}
	fmt.Println(strings.Repeat("=", 80))
	token26049 := atomic.LoadInt64(&token26049Count)
	if token26049 > 0 {
		fmt.Printf("🎯 Token 26049 Found   : ✅ YES (%d records saved)\n", token26049)
	} else {
		fmt.Printf("🎯 Token 26049 Found   : ❌ NO (not detected in feed)\n")
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Decoder stopped successfully!")
	fmt.Println("📁 Check csv_output/ directory for the CSV files")
	if token26049 > 0 {
		fmt.Println("   📄 message_17202_*.csv - All tokens")
		fmt.Println("   📄 token_26049_*.csv - Only token 26049 data")
	}
	fmt.Println()
}
