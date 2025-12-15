// NSE Capital Market Multicast UDP Receiver - Message 6541 Only
// 
// FOCUS: Only process message code 6541 (BC_CIRCUIT_CHECK - Heartbeat Pulse)
// OUTPUT: csv_output/message_6541_TIMESTAMP.csv (timestamp + count)
//
// USAGE:
// ======
// Run: go run message_6541_live.go
// Output: csv_output/message_6541_TIMESTAMP.csv
//
// Protocol Reference: NSE CM NNF Protocol v6.3, Page 138
// Structure: Only BCAST_HEADER (40 bytes) - No additional fields
// Purpose: Heartbeat pulse sent every ~9 seconds when no other data

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
// LZO DECOMPRESSION (same as other decoders)
// =============================================================================

// LZO error constants
var (
	ErrInputOverrun  = errors.New("LZO input overrun")
	ErrOutputOverrun = errors.New("LZO output overrun")
	ErrCorrupted     = errors.New("LZO data corrupted")
)

const M2_MAX_OFFSET = 0x0700

// DecompressUltra - LZO1Z decompression
func DecompressUltra(src []byte, dst []byte) (int, error) {
	// [Full LZO decompression code - same as others]
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
// GLOBAL VARIABLES
// =============================================================================

var (
	packetCount         int64
	totalBytes          int64
	compressedCount     int64
	decompressedCount   int64
	decompressionErrors int64
	
	message6541Count   int64
	lastHeartbeatTime  time.Time
	
	csvFile6541        *os.File
	csvWriter6541      *csv.Writer
	
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
	
	messageCodeCounts   map[uint16]int64
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	messageCodeCounts = make(map[uint16]int64)
	startTime = time.Now()
	lastHeartbeatTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %v", err)
		return
	}

	if err := initialize6541CSV(); err != nil {
		log.Fatalf("Failed to initialize 6541 CSV: %v", err)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go processPackets()
	go startUDPListener()
	
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
	<-sigChan

	fmt.Println("\n\n⏹️  Shutdown signal received...")
	close(shutdownChan)
	time.Sleep(1 * time.Second)

	if csvWriter6541 != nil {
		csvWriter6541.Flush()
	}
	if csvFile6541 != nil {
		csvFile6541.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize6541CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output", fmt.Sprintf("message_6541_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile6541 = file
	csvWriter6541 = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", 
		"TransactionCode",
		"LogTime",
		"AlphaChar",
		"ErrorCode",
		"BCSeqNo",
		"TimeStamp2",
		"MessageLength",
		"HeartbeatNumber",
		"SecondsSinceLastHeartbeat",
	}
	csvWriter6541.Write(headers)
	csvWriter6541.Flush()

	fmt.Printf("📁 Created CSV file for Message 6541: %s\n", filename)
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
	fmt.Printf("║ NSE CM Message 6541 Receiver - Heartbeat Monitor         ║\n")
	fmt.Printf("╚════════════════════════════════════════════════════════════╝\n")
	fmt.Printf("📡 Multicast: %s:%d\n", multicastIP, port)
	fmt.Printf("🎯 Target: Message 6541 (BC_CIRCUIT_CHECK - Heartbeat)\n")
	fmt.Printf("💓 Expected: ~9 seconds between heartbeats\n")
	fmt.Printf("📊 Statistics every 10 seconds\n")
	fmt.Printf("⏱️  Started at: %s\n\n", time.Now().Format("15:04:05"))
	fmt.Printf("Waiting for packets...\n\n")
	
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Fatalf("❌ Failed to join multicast group: %v\n", err)
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

			atomic.AddInt64(&packetCount, 1)
			atomic.AddInt64(&totalBytes, int64(n))

			data := make([]byte, n)
			copy(data, buffer[:n])
			
			select {
			case packetChan <- data:
			default:
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
	
	finalData = finalData[8:]
	
	if len(finalData) < 48 {
		return
	}
	
	transactionCode := binary.BigEndian.Uint16(finalData[10:12])
	messageCodeCounts[transactionCode]++
	
	if transactionCode != 6541 {
		return
	}

	process6541Message(finalData)
}

// =============================================================================
// MESSAGE 6541 PROCESSOR (HEARTBEAT)
// =============================================================================

func process6541Message(data []byte) {
	// Message 6541 is just BCAST_HEADER (40 bytes) - no additional fields
	if len(data) < 40 {
		return
	}

	atomic.AddInt64(&message6541Count, 1)
	currentCount := atomic.LoadInt64(&message6541Count)
	
	// Parse BCAST_HEADER fields (offset 0-39)
	// Reserved: data[0:4]
	logTime := binary.BigEndian.Uint32(data[4:8])
	var alphaChar [2]byte
	copy(alphaChar[:], data[8:10])
	transactionCode := binary.BigEndian.Uint16(data[10:12])
	errorCode := binary.BigEndian.Uint16(data[12:14])
	bcSeqNo := binary.BigEndian.Uint32(data[14:18])
	// Reserved: data[18:22]
	var timeStamp2 [8]byte
	copy(timeStamp2[:], data[22:30])
	// Filler2: data[30:38]
	messageLength := binary.BigEndian.Uint16(data[38:40])
	
	now := time.Now()
	secondsSinceLast := now.Sub(lastHeartbeatTime).Seconds()
	lastHeartbeatTime = now
	
	if currentCount == 1 {
		fmt.Printf("\n💓 First Heartbeat (6541) received\n\n")
	} else {
		if secondsSinceLast >= 8.0 && secondsSinceLast <= 10.0 {
			fmt.Printf("💓 Heartbeat #%d - %.1fs since last (NORMAL)\n", currentCount, secondsSinceLast)
		} else {
			fmt.Printf("⚠️  Heartbeat #%d - %.1fs since last (ABNORMAL - expected ~9s)\n", currentCount, secondsSinceLast)
		}
	}
	
	exportToCSV(transactionCode, logTime, alphaChar, errorCode, bcSeqNo, 
		timeStamp2, messageLength, currentCount, secondsSinceLast)
}

func exportToCSV(transactionCode uint16, logTime uint32, alphaChar [2]byte,
	errorCode uint16, bcSeqNo uint32, timeStamp2 [8]byte, messageLength uint16,
	heartbeatNumber int64, secondsSinceLast float64) {
	
	if csvWriter6541 == nil {
		return
	}

	// Clean strings
	alpha := strings.TrimRight(string(alphaChar[:]), "\x00 ")
	timestamp2 := strings.TrimRight(string(timeStamp2[:]), "\x00 ")

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", transactionCode),
		fmt.Sprintf("%d", logTime),
		alpha,
		fmt.Sprintf("%d", errorCode),
		fmt.Sprintf("%d", bcSeqNo),
		timestamp2,
		fmt.Sprintf("%d", messageLength),
		fmt.Sprintf("%d", heartbeatNumber),
		fmt.Sprintf("%.3f", secondsSinceLast),
	}

	csvWriter6541.Write(record)
	csvWriter6541.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	compressed := atomic.LoadInt64(&compressedCount)
	msg6541 := atomic.LoadInt64(&message6541Count)

	if duration > 0 {
		status := "❌ NO HEARTBEAT"
		if msg6541 > 0 {
			avgInterval := duration / float64(msg6541)
			status = fmt.Sprintf("💓 BEATING (avg %.1fs)", avgInterval)
		}
		
		fmt.Printf("⏱️  %.0fs | 📦 %d pkts (%.0f/s) | 🗜️  %d compressed | %s | %d beats\n", 
			duration, packets, float64(packets)/duration, compressed, status, msg6541)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	bytes := atomic.LoadInt64(&totalBytes)
	msg6541 := atomic.LoadInt64(&message6541Count)

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 FINAL STATISTICS - MESSAGE 6541 HEARTBEAT MONITOR")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Runtime                : %v\n", duration)
	fmt.Printf("Total Packets Received : %d\n", packets)
	fmt.Printf("Total Bytes Received   : %d (%.2f MB)\n", bytes, float64(bytes)/(1024*1024))
	if duration.Seconds() > 0 {
		fmt.Printf("Packets/Second         : %.2f\n", float64(packets)/duration.Seconds())
	}
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("💓 Heartbeats (6541)   : %d\n", msg6541)
	if msg6541 > 1 {
		avgInterval := duration.Seconds() / float64(msg6541)
		fmt.Printf("   Average Interval    : %.2f seconds\n", avgInterval)
		if avgInterval >= 8.0 && avgInterval <= 10.0 {
			fmt.Println("   Status              : ✅ NORMAL (expected ~9s)")
		} else {
			fmt.Println("   Status              : ⚠️  ABNORMAL (expected ~9s)")
		}
	}
	fmt.Println(strings.Repeat("=", 80))
	
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
		
		for i := 0; i < len(sorted); i++ {
			for j := i + 1; j < len(sorted); j++ {
				if sorted[j].code < sorted[i].code {
					sorted[i], sorted[j] = sorted[j], sorted[i]
				}
			}
		}
		
		for _, cf := range sorted {
			percentage := float64(cf.freq) / float64(packets) * 100
			if cf.code == 6541 {
				fmt.Printf("   💓 Code %5d: %6d messages (%.1f%%) ← HEARTBEAT!\n", cf.code, cf.freq, percentage)
			} else {
				fmt.Printf("      Code %5d: %6d messages (%.1f%%)\n", cf.code, cf.freq, percentage)
			}
		}
		fmt.Println(strings.Repeat("-", 80))
	}
	
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("✅ Heartbeat monitor stopped successfully!")
	if msg6541 > 0 {
		fmt.Println("📁 Check csv_output/ directory for heartbeat CSV")
	}
	fmt.Println()
}
