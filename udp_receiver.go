package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// MessageStats holds statistics for a specific message code
type MessageStats struct {
	TransactionCode     uint16
	Count               int64
	TotalCompressedSize int64
	TotalRawSize        int64
	FirstSeen           time.Time
	LastSeen            time.Time
	ErrorCount          int64
	DecompressionErrors int64
}

// UDPStats holds overall UDP statistics
type UDPStats struct {
	mu                    sync.RWMutex
	TotalPackets          int64
	TotalBytes            int64
	CompressedPackets     int64
	DecompressedPackets   int64
	DecompressionFailures int64
	MessageCodeStats      map[uint16]*MessageStats
	StartTime             time.Time
	LastPacketTime        time.Time
}

// BroadcastHeader represents the NSE broadcast header structure (40 bytes)
type BroadcastHeader struct {
	Reserved1       [2]byte // 2 bytes, offset 0
	Reserved2       [2]byte // 2 bytes, offset 2
	LogTime         uint32  // 4 bytes, offset 4
	AlphaChar       [2]byte // 2 bytes, offset 8
	TransactionCode uint16  // 2 bytes, offset 10
	ErrorCode       uint16  // 2 bytes, offset 12
	BCSeqNo         uint32  // 4 bytes, offset 14
	Reserved3       byte    // 1 byte, offset 18
	Reserved4       [3]byte // 3 bytes, offset 19
	TimeStamp2      [8]byte // 8 bytes, offset 22
	Filler          [8]byte // 8 bytes, offset 30
	MessageLength   uint16  // 2 bytes, offset 38
}

// ParseBroadcastHeader parses the broadcast header from data
func ParseBroadcastHeader(data []byte) (*BroadcastHeader, error) {
	if len(data) < 40 {
		return nil, fmt.Errorf("insufficient data for broadcast header: need 40 bytes, got %d", len(data))
	}

	header := &BroadcastHeader{}
	
	// Parse fields (assuming big endian based on reference code)
	copy(header.Reserved1[:], data[0:2])
	copy(header.Reserved2[:], data[2:4])
	header.LogTime = binary.BigEndian.Uint32(data[4:8])
	copy(header.AlphaChar[:], data[8:10])
	header.TransactionCode = binary.BigEndian.Uint16(data[10:12])
	header.ErrorCode = binary.BigEndian.Uint16(data[12:14])
	header.BCSeqNo = binary.BigEndian.Uint32(data[14:18])
	header.Reserved3 = data[18]
	copy(header.Reserved4[:], data[19:22])
	copy(header.TimeStamp2[:], data[22:30])
	copy(header.Filler[:], data[30:38])
	header.MessageLength = binary.BigEndian.Uint16(data[38:40])

	return header, nil
}

// IsCompressedMessage checks if the transaction code uses LZO compression
func IsCompressedMessage(transactionCode uint16) bool {
	// Based on NSE NNF protocol, these codes use LZO compression
	compressedCodes := map[uint16]bool{
		7200:  true, // BCAST_MBO_MBP_UPDATE
		7201:  true, // BCAST_MW_ROUND_ROBIN
		7202:  true, // BCAST_TICKER_AND_MKT_INDEX
		7208:  true, // BCAST_ONLY_MBP
		7220:  true, // BCAST_LIMIT_PRICE_PROTECTION_RANGE
		17201: true, // BCAST_ENHNCD_MW_ROUND_ROBIN
		17202: true, // BCAST_ENHNCD_TICKER_AND_MKT_INDEX
	}
	return compressedCodes[transactionCode]
}

// GetTransactionCodeName returns human-readable name for transaction code
func GetTransactionCodeName(code uint16) string {
	names := map[uint16]string{
		7200:  "BCAST_MBO_MBP_UPDATE",
		7201:  "BCAST_MW_ROUND_ROBIN",
		7202:  "BCAST_TICKER_AND_MKT_INDEX",
		7203:  "BCAST_INDUSTRY_INDEX_UPDATE",
		7206:  "BCAST_SYSTEM_INFORMATION_OUT",
		7207:  "BCAST_INDICES",
		7208:  "BCAST_ONLY_MBP",
		7210:  "BCAST_SECURITY_STATUS_CHG_PREOPEN",
		7211:  "BCAST_SPD_MBP_DELTA",
		7220:  "BCAST_LIMIT_PRICE_PROTECTION_RANGE",
		7304:  "UPDATE_LOCALDB_DATA",
		7305:  "BCAST_SECURITY_MSTR_CHG",
		7306:  "BCAST_PART_MSTR_CHG",
		7307:  "UPDATE_LOCALDB_HEADER",
		7308:  "UPDATE_LOCALDB_TRAILER",
		7309:  "BCAST_SPD_MSTR_CHG",
		7320:  "BCAST_SECURITY_STATUS_CHG",
		7321:  "PARTIAL_SYSTEM_INFORMATION",
		7324:  "BCAST_INSTR_MSTR_CHG",
		7325:  "BCAST_INDEX_MSTR_CHG",
		7326:  "BCAST_INDEX_MAP_TABLE",
		7340:  "BCAST_SEC_MSTR_CHNG_PERIODIC",
		7341:  "BCAST_SPD_MSTR_CHG_PERIODIC",
		6511:  "BC_OPEN_MSG",
		6521:  "BC_CLOSE_MSG",
		6522:  "BC_POSTCLOSE_MSG",
		6531:  "BC_PRE_OR_POST_DAY_MSG",
		6541:  "BC_CIRCUIT_CHECK",
		6571:  "BC_NORMAL_MKT_PREOPEN_ENDED",
		6501:  "BCAST_JRNL_VCT_MSG",
		5295:  "CTRL_MSG_TO_TRADER",
		6013:  "SECURITY_OPEN_PRICE",
		9010:  "BCAST_TURNOVER_EXCEEDED",
		9011:  "BROADCAST_BROKER_REACTIVATED",
		7130:  "MKT_MVMT_CM_OI_IN",
		17130: "ENHNCD_MKT_MVMT_CM_OI_IN",
		17201: "BCAST_ENHNCD_MW_ROUND_ROBIN",
		17202: "BCAST_ENHNCD_TICKER_AND_MKT_INDEX",
		1833:  "RPRT_MARKET_STATS_OUT_RPT",
		11833: "ENHNCD_RPRT_MARKET_STATS_OUT_RPT",
		1862:  "SPD_BC_JRNL_VCT_MSG",
	}
	
	if name, ok := names[code]; ok {
		return name
	}
	return fmt.Sprintf("UNKNOWN_%d", code)
}

// NewUDPStats creates a new statistics tracker
func NewUDPStats() *UDPStats {
	return &UDPStats{
		MessageCodeStats: make(map[uint16]*MessageStats),
		StartTime:        time.Now(),
	}
}

// UpdateStats updates statistics for a received packet
func (s *UDPStats) UpdateStats(header *BroadcastHeader, compressedSize, rawSize int, decompressionError bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.TotalPackets++
	s.TotalBytes += int64(compressedSize)
	s.LastPacketTime = time.Now()

	if IsCompressedMessage(header.TransactionCode) {
		s.CompressedPackets++
		if decompressionError {
			s.DecompressionFailures++
		} else {
			s.DecompressedPackets++
		}
	}

	// Update message-specific stats
	stats, exists := s.MessageCodeStats[header.TransactionCode]
	if !exists {
		stats = &MessageStats{
			TransactionCode: header.TransactionCode,
			FirstSeen:       time.Now(),
		}
		s.MessageCodeStats[header.TransactionCode] = stats
	}

	stats.Count++
	stats.TotalCompressedSize += int64(compressedSize)
	stats.TotalRawSize += int64(rawSize)
	stats.LastSeen = time.Now()

	if header.ErrorCode != 0 {
		stats.ErrorCount++
	}

	if decompressionError {
		stats.DecompressionErrors++
	}
}

// PrintStats prints comprehensive statistics
func (s *UDPStats) PrintStats() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	duration := time.Since(s.StartTime)
	
	separator := "===================================================================================================="
	dashedLine := "----------------------------------------------------------------------------------------------------"
	
	fmt.Println("\n" + separator)
	fmt.Println("NSE MULTICAST UDP RECEIVER - MESSAGE CODE STATISTICS")
	fmt.Println(separator)
	
	fmt.Printf("\n📊 OVERALL STATISTICS\n")
	fmt.Printf("  Runtime:                %s\n", duration.Round(time.Second))
	fmt.Printf("  Total Packets:          %d\n", s.TotalPackets)
	fmt.Printf("  Total Bytes:            %d (%.2f MB)\n", s.TotalBytes, float64(s.TotalBytes)/(1024))
	fmt.Printf("  Compressed Packets:     %d\n", s.CompressedPackets)
	fmt.Printf("  Decompressed Success:   %d\n", s.DecompressedPackets)
	fmt.Printf("  Decompression Failures: %d\n", s.DecompressionFailures)
	
	if s.TotalPackets > 0 {
		fmt.Printf("  Avg Packet Rate:        %.2f packets/sec\n", float64(s.TotalPackets)/duration.Seconds())
		fmt.Printf("  Avg Data Rate:          %.2f KB/sec\n", float64(s.TotalBytes)/(1024*duration.Seconds()))
	}
	
	fmt.Printf("\n📈 MESSAGE CODE BREAKDOWN (%d unique codes)\n", len(s.MessageCodeStats))
	fmt.Println(dashedLine)
	fmt.Printf("%-6s %-40s %10s %15s %15s %10s %8s\n", 
		"Code", "Name", "Count", "Compressed(KB)", "Raw(KB)", "Ratio", "Errors")
	fmt.Println(dashedLine)
	
	// Sort by count (descending)
	type statEntry struct {
		code  uint16
		stats *MessageStats
	}
	entries := make([]statEntry, 0, len(s.MessageCodeStats))
	for code, stats := range s.MessageCodeStats {
		entries = append(entries, statEntry{code, stats})
	}
	
	// Simple bubble sort by count
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].stats.Count > entries[i].stats.Count {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	
	for _, entry := range entries {
		code := entry.code
		stats := entry.stats
		name := GetTransactionCodeName(code)
		
		compressedKB := float64(stats.TotalCompressedSize) / 1024
		rawKB := float64(stats.TotalRawSize) / 1024
		
		ratio := ""
		if stats.TotalRawSize > 0 && stats.TotalCompressedSize > 0 {
			ratio = fmt.Sprintf("%.2fx", float64(stats.TotalRawSize)/float64(stats.TotalCompressedSize))
		} else if IsCompressedMessage(code) {
			ratio = "N/A"
		} else {
			ratio = "-"
		}
		
		errorInfo := ""
		if stats.ErrorCount > 0 || stats.DecompressionErrors > 0 {
			errorInfo = fmt.Sprintf("%d/%d", stats.ErrorCount, stats.DecompressionErrors)
		} else {
			errorInfo = "-"
		}
		
		compressed := "  "
		if IsCompressedMessage(code) {
			compressed = "🗜️"
		}
		
		fmt.Printf("%-6d %-38s %s %10d %15.2f %15.2f %10s %8s\n",
			code, name, compressed, stats.Count, compressedKB, rawKB, ratio, errorInfo)
	}
	
	fmt.Println("====================================================================================================")
	
	// Print compression efficiency summary
	if s.CompressedPackets > 0 {
		fmt.Printf("\n🗜️  COMPRESSION SUMMARY\n")
		var totalCompressed, totalRaw int64
		for code, stats := range s.MessageCodeStats {
			if IsCompressedMessage(code) {
				totalCompressed += stats.TotalCompressedSize
				totalRaw += stats.TotalRawSize
			}
		}
		
		if totalRaw > 0 && totalCompressed > 0 {
			overallRatio := float64(totalRaw) / float64(totalCompressed)
			savedBytes := totalRaw - totalCompressed
			savedPercent := (float64(savedBytes) / float64(totalRaw)) * 100
			
			fmt.Printf("  Total Compressed Data:   %.2f MB\n", float64(totalCompressed)/(1024*1024))
			fmt.Printf("  Total Raw Data:          %.2f MB\n", float64(totalRaw)/(1024*1024))
			fmt.Printf("  Overall Compression:     %.2fx\n", overallRatio)
			fmt.Printf("  Bytes Saved:             %.2f MB (%.1f%%)\n", 
				float64(savedBytes)/(1024*1024), savedPercent)
		}
	}
	
	fmt.Println()
}

// ProcessUDPPacket processes a single UDP packet (NSE format)
func ProcessUDPPacket(data []byte, stats *UDPStats) {
	if len(data) < 6 {
		return // Too small - need at least NetID(2) + NoOfPackets(2) + CompLen(2)
	}

	// NSE UDP Packet Structure:
	// Bytes 0-1:   NetID (2 chars)
	// Bytes 2-3:   iNoOfPackets (short)
	// Bytes 4+:    cPackData[]
	//   - First 2 bytes: iCompLen (compressed length)
	//   - If iCompLen > 0: compressed data follows
	
	// Extract packet data starting at offset 4
	cPackData := data[4:]
	if len(cPackData) < 2 {
		return
	}

	// Read compressed length
	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])
	
	var transactionCode uint16
	compressedSize := len(data)
	rawSize := len(data)
	decompressionError := false
	
	if iCompLen > 0 {
		// Compressed packet
		offset := 2 // Skip iCompLen field
		
		if offset+int(iCompLen) > len(cPackData) {
			return // Invalid packet
		}
		
		compressedPacket := cPackData[offset : offset+int(iCompLen)]
		decompressBuffer := make([]byte, 10240) // 10KB buffer
		
		// Decompress
		decompLen, err := DecompressUltra(compressedPacket, decompressBuffer)
		if err != nil {
			decompressionError = true
			// Try to get message code from offset 18 of compressed data if possible
			if len(compressedPacket) >= 20 {
				transactionCode = binary.BigEndian.Uint16(compressedPacket[18:20])
			}
		} else {
			// Successfully decompressed - message code is at offset 18
			decompressedPacket := decompressBuffer[:decompLen]
			if len(decompressedPacket) >= 20 {
				transactionCode = binary.BigEndian.Uint16(decompressedPacket[18:20])
			}
			rawSize = len(decompressedPacket)
		}
	} else {
		// Uncompressed packet - message code at offset 18 of packet data
		if len(cPackData) >= 20 {
			transactionCode = binary.BigEndian.Uint16(cPackData[18:20])
		}
	}
	
	// Create a minimal header structure for stats
	header := &BroadcastHeader{
		TransactionCode: transactionCode,
		ErrorCode:       0, // Not available in this format
	}
	
	stats.UpdateStats(header, compressedSize, rawSize, decompressionError)
}

// StartUDPListener starts listening on UDP port and analyzes packets
func StartUDPListener(port int, stats *UDPStats) error {
	// Multicast group address
	multicastAddr := "233.1.2.5"
	
	// Create multicast address
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastAddr),
		Port: port,
	}

	// Listen on multicast address
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to listen on multicast %s:%d: %v", multicastAddr, port, err)
	}
	defer conn.Close()

	// Set read buffer size for better performance
	conn.SetReadBuffer(2 * 1024 * 1024) // 2MB buffer

	fmt.Printf("✅ Joined multicast group %s\n", multicastAddr)

	fmt.Println("\n====================================================================================================")
	fmt.Println("NSE MULTICAST UDP RECEIVER")
	fmt.Println("====================================================================================================")
	fmt.Printf("🎧 Listening on:  0.0.0.0:%d (multicast: %s)\n", port, multicastAddr)
	fmt.Printf("📊 Press Ctrl+C to stop and view statistics\n")
	fmt.Println("----------------------------------------------------------------------------------------------------\n")

	buffer := make([]byte, 65535) // Max UDP packet size

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Packet counter for periodic updates
	packetCount := int64(0)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			stats.mu.RLock()
			count := stats.TotalPackets
			stats.mu.RUnlock()
			
			if count > packetCount {
				fmt.Printf("📦 Received %d packets...\n", count)
				packetCount = count
			}
		}
	}()

	// Read packets in a goroutine
	packetChan := make(chan []byte, 100)
	
	go func() {
		for {
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			
			// Copy data to avoid buffer reuse issues
			data := make([]byte, n)
			copy(data, buffer[:n])
			packetChan <- data
		}
	}()

	// Process packets
	for {
		select {
		case <-sigChan:
			fmt.Println("\n🛑 Stopping UDP listener...")
			return nil
		case data := <-packetChan:
			ProcessUDPPacket(data, stats)
		}
	}
}

func main() {
	//port := 55655 // Default UDP port
	port := 34330 // Default UDP port
	// Parse command line arguments
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &port)
	}

	stats := NewUDPStats()

	err := StartUDPListener(port, stats)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Print final statistics
	stats.PrintStats()
}
