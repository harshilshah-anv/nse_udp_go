// NSE Multicast UDP Receiver - Simplified for Message 7211 Only
// 
// FOCUS: Only process message code 7211 (BCAST_SPD_MBP_DELTA)
// OUTPUT: csv_output_2/message_7211_TIMESTAMP.csv
//
// USAGE:
// ======
// Run: go run test_2_main.go lzo_decompressor_safe.go
// Output: csv_output_2/message_7211_TIMESTAMP.csv

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
// MESSAGE STRUCTURE FOR 7211
// =============================================================================

// Message7211 - BCAST_SPD_MBP_DELTA (Spread MBP Delta)
// Structure: BCAST_HEADER(40) + ST_SPD_MBP_DELTA records
// Each record: 172 bytes, maximum 5 records per packet
type Message7211 struct {
	Token               int32   // 4 bytes - Token number
	LastTradedPrice     int32   // 4 bytes - LTP
	LastTradedQuantity  int32   // 4 bytes - Last trade quantity
	Volume              int64   // 8 bytes - Total volume
	BidPrice            int32   // 4 bytes - Best bid price
	BidQuantity         int32   // 4 bytes - Best bid quantity
	AskPrice            int32   // 4 bytes - Best ask price
	AskQuantity         int32   // 4 bytes - Best ask quantity
	TotalTradedValue    int64   // 8 bytes - Total traded value
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
	
	// 7211 specific counters
	message7211Count   int64
	message7211Saved   int64
	
	// CSV file for 7211
	csvFile7211        *os.File
	csvWriter7211      *csv.Writer
	
	// Control channels
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {


	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)

	// Create output directory
	if err := os.MkdirAll("csv_output_2", 0755); err != nil {
		log.Fatalf("Failed to create csv_output_2 directory: %v", err)
		return
	}

	// Initialize CSV file for 7211
	if err := initialize7211CSV(); err != nil {
		log.Fatalf("Failed to initialize 7211 CSV: %v", err)
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
	if csvWriter7211 != nil {
		csvWriter7211.Flush()
	}
	if csvFile7211 != nil {
		csvFile7211.Close()
	}

	printFinalStats()
}

// =============================================================================
// CSV INITIALIZATION
// =============================================================================

func initialize7211CSV() error {
	timestamp := time.Now().Format("20060102_150405")
	filename := filepath.Join("csv_output_2", fmt.Sprintf("message_7211_%s.csv", timestamp))
	
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	csvFile7211 = file
	csvWriter7211 = csv.NewWriter(file)
	
	// Write headers
	headers := []string{
		"Timestamp", "MessageCode", "Token", "LastTradedPrice", "LastTradedQuantity", 
		"Volume", "BidPrice", "BidQuantity", "AskPrice", "AskQuantity", "TotalTradedValue",
	}
	csvWriter7211.Write(headers)
	csvWriter7211.Flush()

	return nil
}



// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	//multicastIP := "233.1.2.5"
	multicastIP := "231.31.31.4"
	//port := 34330
	port := 55655
	
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

	if len(finalData) < 20 {
		return
	}

	messageCode := binary.BigEndian.Uint16(finalData[18:20])



	// Only process message code 7211
	if messageCode == 7211 {
		process7211Message(finalData)
	}
}

// =============================================================================
// MESSAGE 7211 PROCESSOR
// =============================================================================

func process7211Message(data []byte) {
    if len(data) < 42 {
        fmt.Printf("7211 message too short: %d bytes\n", len(data))
        return
    }

    if binary.BigEndian.Uint16(data[18:20]) != 7211 {
        fmt.Printf("Message code mismatch in process7211Message\n")
        return
    }

    atomic.AddInt64(&message7211Count, 1)
    currentCount := atomic.LoadInt64(&message7211Count)
    
    // Print hex dump only for first 3 messages to avoid spam
    if currentCount <= 3 {
        fmt.Printf("\n=== Processing 7211 message #%d, length: %d ===\n", currentCount, len(data))
        fmt.Printf("Hex dump (first 100 bytes):\n")
        for i := 0; i < 100 && i < len(data); i += 20 {
            end := i + 20
            if end > len(data) {
                end = len(data)
            }
            fmt.Printf("Offset %3d: % X\n", i, data[i:end])
        }
    }
    
    // Check NoOfRecords field
    noOfRecords := uint16(0)
    if len(data) >= 42 {
        noOfRecords = binary.BigEndian.Uint16(data[40:42])
    }
    
    // Only print debug for first few messages
    if currentCount <= 3 {
        fmt.Printf("7211: NoOfRecords at offset 40: %d (0x%04X)\n", noOfRecords, noOfRecords)
    }
    
    if noOfRecords > 0 && noOfRecords <= 10 { // Reasonable multi-record scenario
        // Process multiple records starting at offset 48 (after 40-byte header + 6-byte filler + 2-byte count)
        offset := 48
        recSize := 40 // Basic record size for ST_SPD_MBP_DELTA fields
        
        for i := 0; i < int(noOfRecords); i++ {
            if offset+recSize > len(data) {
                break
            }
            
            if msg := parseMessage7211Direct(data, offset); msg != nil {
                exportToCSV(msg)
                atomic.AddInt64(&message7211Saved, 1)
            }
            offset += recSize
        }
    } else {
        // Single record parse (most common case)
        // DISCOVERY: Offset 40-45 contains spaces (0x20 20 20 20 20 20) - symbol filler
        // Offset 46-47: Contains 0x00CC (204) - possibly field count
        // Real token data starts at offset 48
        
        // Try offset 48 first (most likely)
        if len(data) >= 48+40 {
            if msg := parseMessage7211Direct(data, 48); msg != nil {
                exportToCSV(msg)
                atomic.AddInt64(&message7211Saved, 1)
                return
            }
        }
        
        // Try offset 46 as fallback
        if len(data) >= 46+40 {
            if msg := parseMessage7211Direct(data, 46); msg != nil {
                exportToCSV(msg)
                atomic.AddInt64(&message7211Saved, 1)
                return
            }
        }
    }
}

// parseMessage7211 - Parse NSE 7211 message structure
func parseMessage7211(data []byte) *Message7211 {
	if len(data) < 44 { // Minimum size for basic fields
		return nil
	}

	// Parse the NSE 7211 ST_SPD_MBP_DELTA structure
	msg := parseNSE7211Structure(data)
	
	return msg
}

// parseMessage7211Direct - Parse 7211 fields directly from message at given offset
func parseMessage7211Direct(data []byte, startOffset int) *Message7211 {
    if len(data) < startOffset+40 {
        fmt.Printf("7211 Direct: Not enough data from offset %d\n", startOffset)
        return nil
    }
    
    offset := startOffset
    
    // Read fields in sequence
    token := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    lastTradedPrice := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    lastTradedQuantity := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    // Volume - try as int32 (4 bytes)
    volume := int64(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    bidPrice := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    bidQuantity := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    askPrice := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    askQuantity := int32(binary.BigEndian.Uint32(data[offset:offset+4]))
    offset += 4
    
    // Total traded value - try as int64 (8 bytes)
    var totalTradedValue int64
    if len(data) >= offset+8 {
        totalTradedValue = int64(binary.BigEndian.Uint64(data[offset:offset+8]))
    }
    
    fmt.Printf("7211 Direct Parse: Token=%d, LTP=%d (%.2f), Qty=%d, Vol=%d\n", 
        token, lastTradedPrice, float64(lastTradedPrice)/100.0, lastTradedQuantity, volume)
    
    // Relaxed validation
    if token <= 0 || token > 99999999 {
        fmt.Printf("7211 Direct: Token validation failed: %d\n", token)
        return nil
    }
    
    if lastTradedPrice <= 0 || lastTradedPrice > 10000000 { // 1 crore max
        fmt.Printf("7211 Direct: Price validation failed: %d\n", lastTradedPrice)
        return nil
    }
    
    return &Message7211{
        Token:              token,
        LastTradedPrice:    lastTradedPrice,
        LastTradedQuantity: lastTradedQuantity,
        Volume:             volume,
        BidPrice:           bidPrice,
        BidQuantity:        bidQuantity,
        AskPrice:           askPrice,
        AskQuantity:        askQuantity,
        TotalTradedValue:   totalTradedValue,
    }
}

// parseNSE7211Structure - Parse NSE 7211 ST_SPD_MBP_DELTA structure
// Based on NSE documentation:
// Token (4 bytes) – int32
// LastTradedPrice (4 bytes) – int32
// LastTradedQuantity (4 bytes) – int32
// Volume (8 bytes) – int64
// BidPrice (4 bytes) – int32
// BidQuantity (4 bytes) – int32
// AskPrice (4 bytes) – int32
// AskQuantity (4 bytes) – int32
// TotalTradedValue (8 bytes) – int64
func parseNSE7211Structure(data []byte) *Message7211 {
    if len(data) < 44 {
        fmt.Printf("7211: Data too short (%d bytes)\n", len(data))
        return nil
    }
    
    // Use the working offsets - parsing from offset 40 works
    token := int32(binary.BigEndian.Uint32(data[0:4]))
    lastTradedPrice := int32(binary.BigEndian.Uint32(data[4:8]))
    lastTradedQuantity := int32(binary.BigEndian.Uint32(data[8:12]))
    
    fmt.Printf("7211 Debug: Token=%d, LTP=%d, Qty=%d\n", token, lastTradedPrice, lastTradedQuantity)
    
    // Volume field seems to be at wrong offset, try different positions
    var volume int64
    if len(data) >= 20 {
        // Try as int32 first (4 bytes) instead of int64 (8 bytes)
        volume = int64(binary.BigEndian.Uint32(data[12:16]))
    }
    
    // Extract other market data fields
    var bidPrice, bidQuantity, askPrice, askQuantity int32
    var totalTradedValue int64
    
    if len(data) >= 20 {
        bidPrice = int32(binary.BigEndian.Uint32(data[16:20]))
    }
    if len(data) >= 24 {
        bidQuantity = int32(binary.BigEndian.Uint32(data[20:24]))
    }
    if len(data) >= 28 {
        askPrice = int32(binary.BigEndian.Uint32(data[24:28]))
    }
    if len(data) >= 32 {
        askQuantity = int32(binary.BigEndian.Uint32(data[28:32]))
    }
    if len(data) >= 40 {
        // Try total traded value as int64
        totalTradedValue = int64(binary.BigEndian.Uint64(data[32:40]))
    }
    
    // Validate token and price ranges - MAKE VALIDATION MORE LENIENT
    if token <= 0 || token > 99999999 { // Increased token range
        fmt.Printf("7211: Token validation failed: %d\n", token)
        return nil
    }
    
    if lastTradedPrice <= 0 || lastTradedPrice > 1000000 { // Increased price range (10 crore)
        fmt.Printf("7211: Price validation failed: %d\n", lastTradedPrice)
        return nil
    }
    
    fmt.Printf("7211: Validation passed! Creating message...\n")
    
    // Create the message
    msg := &Message7211{
        Token:              token,
        LastTradedPrice:    lastTradedPrice,
        LastTradedQuantity: lastTradedQuantity,
        Volume:             volume,
        BidPrice:           bidPrice,
        BidQuantity:        bidQuantity,
        AskPrice:           askPrice,
        AskQuantity:        askQuantity,
        TotalTradedValue:   totalTradedValue,
    }
    
    return msg
}







func exportToCSV(msg *Message7211) {
	if csvWriter7211 == nil {
		fmt.Printf("CSV writer is nil!\n")
		return
	}



	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		"7211",
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%.2f", float64(msg.LastTradedPrice)/100.0),
		fmt.Sprintf("%d", msg.LastTradedQuantity),
		fmt.Sprintf("%d", msg.Volume),
		fmt.Sprintf("%.2f", float64(msg.BidPrice)/100.0),
		fmt.Sprintf("%d", msg.BidQuantity),
		fmt.Sprintf("%.2f", float64(msg.AskPrice)/100.0),
		fmt.Sprintf("%d", msg.AskQuantity),
		fmt.Sprintf("%.2f", float64(msg.TotalTradedValue)/100.0),
	}

	csvWriter7211.Write(record)
	csvWriter7211.Flush()
}

// =============================================================================
// STATISTICS
// =============================================================================

func printStats() {
	duration := time.Since(startTime).Seconds()
	packets := atomic.LoadInt64(&packetCount)
	msg7211 := atomic.LoadInt64(&message7211Count)
	saved7211 := atomic.LoadInt64(&message7211Saved)

	if duration > 0 {
		fmt.Printf("Runtime: %.1fs | Packets: %d (%.1f/s) | 7211: %d received, %d saved\n", 
			duration, packets, float64(packets)/duration, msg7211, saved7211)
	}
}

func printFinalStats() {
	duration := time.Since(startTime)
	packets := atomic.LoadInt64(&packetCount)
	msg7211 := atomic.LoadInt64(&message7211Count)
	saved7211 := atomic.LoadInt64(&message7211Saved)

	fmt.Printf("Runtime: %v | Packets: %d | 7211: %d received, %d saved to CSV\n", 
		duration, packets, msg7211, saved7211)
}
