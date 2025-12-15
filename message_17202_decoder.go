// NSE Message Code 17202 Decoder
// BCAST_ENHNCD_TICKER_AND_MKT_INDEX (Enhanced Ticker and Market Index)
//
// Protocol Reference: NSE NNF Protocol v9.46, Page 169
// Structure: ST_ENHNCD_TICKER_INDEX_INFO
// Packet Length: 38 bytes per record
// Maximum Records: 12 per broadcast packet
//
// This decoder handles enhanced ticker and market index information including:
// - Token number (unique security identifier)
// - Market type
// - Fill price and volume (last trade info)
// - Open Interest (OI) data
// - Day high and low OI values

package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// DATA STRUCTURES - Based on NSE NNF Protocol v9.46
// =============================================================================

// BroadcastHeader - Standard NSE Broadcast Header (40 bytes)
// Present in all broadcast messages from NSE
type BroadcastHeader struct {
	MessageLength   uint16   // 2 bytes, offset 0 - Total packet length
	MessageSequence uint32   // 4 bytes, offset 2 - Sequence number
	TransactionCode uint16   // 2 bytes, offset 6 - Message code (17202)
	LogTime         uint64   // 8 bytes, offset 8 - Timestamp
	AlphaChar       [2]byte  // 2 bytes, offset 16 - Alpha characters
	TraderId        [5]byte  // 5 bytes, offset 18 - Trader ID
	ErrorCode       uint16   // 2 bytes, offset 23 - Error code (0 for data)
	Reserved1       [8]byte  // 8 bytes, offset 25 - Reserved
	MessageCategory byte     // 1 byte, offset 33 - Message category
	Reserved2       [10]byte // 10 bytes, offset 34 - Reserved
}

// TickerIndexInfo17202 - Enhanced Ticker Index Information (38 bytes)
// Structure Name: ST_ENHNCD_TICKER_INDEX_INFO
// As per NSE Protocol Page 169, Table 72_A
type TickerIndexInfo17202 struct {
	Token        int32  // 4 bytes, offset 0 - Token number (unique security ID)
	MarketType   int16  // 2 bytes, offset 4 - Market type (Regular/Auction/etc)
	FillPrice    int32  // 4 bytes, offset 6 - Last traded price (LTP) in paise
	FillVolume   int32  // 4 bytes, offset 10 - Last trade quantity
	OpenInterest int64  // 8 bytes, offset 14 - Current Open Interest (LONG LONG)
	DayHiOI      int64  // 8 bytes, offset 22 - Day High Open Interest (LONG LONG)
	DayLoOI      int64  // 8 bytes, offset 30 - Day Low Open Interest (LONG LONG)
}

// =============================================================================
// DECODER CLASS
// =============================================================================

// Message17202Decoder - Main decoder class for message code 17202
type Message17202Decoder struct {
	csvFile        *os.File
	csvWriter      *csv.Writer
	mutex          sync.Mutex
	messageCount   int64
	recordCount    int64
	outputDir      string
	filename       string
	initialized    bool
}

// =============================================================================
// CONSTRUCTOR & INITIALIZATION
// =============================================================================

// NewMessage17202Decoder - Create new decoder instance
func NewMessage17202Decoder(outputDir string) (*Message17202Decoder, error) {
	decoder := &Message17202Decoder{
		outputDir:    outputDir,
		messageCount: 0,
		recordCount:  0,
		initialized:  false,
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	return decoder, nil
}

// Initialize - Initialize CSV file and writer
func (d *Message17202Decoder) Initialize() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.initialized {
		return nil
	}

	// Create filename with timestamp
	timestamp := time.Now().Format("20060102_150405")
	d.filename = fmt.Sprintf("message_17202_%s.csv", timestamp)
	filepath := filepath.Join(d.outputDir, d.filename)

	// Create CSV file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %v", err)
	}

	d.csvFile = file
	d.csvWriter = csv.NewWriter(file)

	// Write CSV header
	header := []string{
		"Timestamp",
		"MessageCode",
		"Token",
		"MarketType",
		"FillPrice",
		"FillVolume",
		"OpenInterest",
		"DayHiOI",
		"DayLoOI",
	}

	if err := d.csvWriter.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %v", err)
	}

	d.csvWriter.Flush()
	d.initialized = true

	fmt.Printf("✅ CSV file created: %s\n", filepath)
	return nil
}

// =============================================================================
// DECODING METHODS
// =============================================================================

// DecodePacket - Main entry point to decode a UDP packet
// Expects: Complete packet with 40-byte header + N*38 bytes of records
func (d *Message17202Decoder) DecodePacket(packetData []byte) error {
	// Validate packet size
	if len(packetData) < 40 {
		return fmt.Errorf("packet too small: %d bytes (minimum 40)", len(packetData))
	}

	// Parse broadcast header
	header := d.parseBroadcastHeader(packetData[:40])

	// Verify transaction code
	if header.TransactionCode != 17202 {
		return fmt.Errorf("invalid transaction code: %d (expected 17202)", header.TransactionCode)
	}

	// Calculate number of records
	dataLength := len(packetData) - 40
	if dataLength%38 != 0 {
		return fmt.Errorf("invalid packet size: data length %d is not multiple of 38", dataLength)
	}

	numRecords := dataLength / 38
	if numRecords == 0 || numRecords > 12 {
		return fmt.Errorf("invalid record count: %d (expected 1-12)", numRecords)
	}

	// Initialize if needed
	if !d.initialized {
		if err := d.Initialize(); err != nil {
			return err
		}
	}

	// Parse each record
	offset := 40
	for i := 0; i < numRecords; i++ {
		if offset+38 > len(packetData) {
			break
		}

		recordData := packetData[offset : offset+38]
		record := d.parseTickerIndexRecord(recordData)

		// Export to CSV
		if err := d.exportToCSV(record, header); err != nil {
			return fmt.Errorf("failed to export record %d: %v", i, err)
		}

		d.recordCount++
		offset += 38
	}

	d.messageCount++
	return nil
}

// parseBroadcastHeader - Parse 40-byte broadcast header
func (d *Message17202Decoder) parseBroadcastHeader(data []byte) *BroadcastHeader {
	return &BroadcastHeader{
		MessageLength:   binary.BigEndian.Uint16(data[0:2]),
		MessageSequence: binary.BigEndian.Uint32(data[2:6]),
		TransactionCode: binary.BigEndian.Uint16(data[6:8]),
		LogTime:         binary.BigEndian.Uint64(data[8:16]),
		ErrorCode:       binary.BigEndian.Uint16(data[23:25]),
		MessageCategory: data[33],
	}
}

// parseTickerIndexRecord - Parse 38-byte ticker index record
// Structure: ST_ENHNCD_TICKER_INDEX_INFO (Page 169, Table 72_A)
func (d *Message17202Decoder) parseTickerIndexRecord(data []byte) *TickerIndexInfo17202 {
	if len(data) < 38 {
		return nil
	}

	return &TickerIndexInfo17202{
		Token:        int32(binary.BigEndian.Uint32(data[0:4])),   // offset 0
		MarketType:   int16(binary.BigEndian.Uint16(data[4:6])),   // offset 4
		FillPrice:    int32(binary.BigEndian.Uint32(data[6:10])),  // offset 6
		FillVolume:   int32(binary.BigEndian.Uint32(data[10:14])), // offset 10
		OpenInterest: int64(binary.BigEndian.Uint64(data[14:22])), // offset 14 (8 bytes)
		DayHiOI:      int64(binary.BigEndian.Uint64(data[22:30])), // offset 22 (8 bytes)
		DayLoOI:      int64(binary.BigEndian.Uint64(data[30:38])), // offset 30 (8 bytes)
	}
}

// =============================================================================
// CSV EXPORT
// =============================================================================

// exportToCSV - Export parsed record to CSV file
func (d *Message17202Decoder) exportToCSV(record *TickerIndexInfo17202, header *BroadcastHeader) error {
	if record == nil {
		return fmt.Errorf("nil record")
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Convert timestamp from NSE format (nanoseconds since epoch)
	timestamp := time.Unix(0, int64(header.LogTime))

	// Prepare CSV row
	row := []string{
		timestamp.Format("2006-01-02 15:04:05.000"),
		"17202",
		fmt.Sprintf("%d", record.Token),
		fmt.Sprintf("%d", record.MarketType),
		fmt.Sprintf("%.2f", float64(record.FillPrice)/100.0), // Convert paise to rupees
		fmt.Sprintf("%d", record.FillVolume),
		fmt.Sprintf("%d", record.OpenInterest),
		fmt.Sprintf("%d", record.DayHiOI),
		fmt.Sprintf("%d", record.DayLoOI),
	}

	// Write to CSV
	if err := d.csvWriter.Write(row); err != nil {
		return fmt.Errorf("failed to write CSV row: %v", err)
	}

	d.csvWriter.Flush()
	return nil
}

// =============================================================================
// STATISTICS & CLEANUP
// =============================================================================

// GetStatistics - Return decoder statistics
func (d *Message17202Decoder) GetStatistics() (messages int64, records int64) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.messageCount, d.recordCount
}

// PrintStatistics - Print decoder statistics
func (d *Message17202Decoder) PrintStatistics() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 MESSAGE CODE 17202 DECODER STATISTICS")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("Total Messages Decoded : %d\n", d.messageCount)
	fmt.Printf("Total Records Decoded  : %d\n", d.recordCount)
	if d.messageCount > 0 {
		fmt.Printf("Average Records/Message: %.2f\n", float64(d.recordCount)/float64(d.messageCount))
	}
	fmt.Printf("Output File           : %s\n", d.filename)
	fmt.Println(strings.Repeat("=", 80))
}

// Close - Close CSV file and cleanup resources
func (d *Message17202Decoder) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.csvWriter != nil {
		d.csvWriter.Flush()
	}

	if d.csvFile != nil {
		return d.csvFile.Close()
	}

	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// GetMarketTypeName - Convert market type code to readable name
func GetMarketTypeName(marketType int16) string {
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

// FormatPrice - Format price from paise to rupees with 2 decimals
func FormatPrice(paise int32) string {
	return fmt.Sprintf("%.2f", float64(paise)/100.0)
}

// =============================================================================
// MAIN FUNCTION - Example Usage
// =============================================================================

func main() {
	fmt.Println("🚀 NSE Message Code 17202 Decoder")
	fmt.Println("   BCAST_ENHNCD_TICKER_AND_MKT_INDEX")
	fmt.Println("   Enhanced Ticker and Market Index Information")
	fmt.Println()

	// Create decoder instance
	decoder, err := NewMessage17202Decoder("csv_output")
	if err != nil {
		fmt.Printf("❌ Error creating decoder: %v\n", err)
		os.Exit(1)
	}
	defer decoder.Close()

	// Example: Create sample packet for testing
	// In real usage, you would receive this from UDP multicast
	samplePacket := createSamplePacket()

	// Decode the packet
	if err := decoder.DecodePacket(samplePacket); err != nil {
		fmt.Printf("❌ Error decoding packet: %v\n", err)
		os.Exit(1)
	}

	// Print statistics
	decoder.PrintStatistics()

	fmt.Println("\n✅ Decoding completed successfully!")
	fmt.Println("📁 Check csv_output/ directory for the CSV file")
}

// =============================================================================
// SAMPLE DATA GENERATOR (For Testing)
// =============================================================================

// createSamplePacket - Create a sample packet for testing
func createSamplePacket() []byte {
	packet := make([]byte, 40+38*2) // Header + 2 records

	// Fill broadcast header (40 bytes)
	binary.BigEndian.PutUint16(packet[0:2], uint16(len(packet)))     // Message length
	binary.BigEndian.PutUint32(packet[2:6], 12345)                   // Sequence number
	binary.BigEndian.PutUint16(packet[6:8], 17202)                   // Transaction code
	binary.BigEndian.PutUint64(packet[8:16], uint64(time.Now().UnixNano())) // Log time

	// Fill first record (38 bytes starting at offset 40)
	offset := 40
	binary.BigEndian.PutUint32(packet[offset:offset+4], 26000)       // Token
	binary.BigEndian.PutUint16(packet[offset+4:offset+6], 1)         // Market type (Regular)
	binary.BigEndian.PutUint32(packet[offset+6:offset+10], 145025)   // Fill price (1450.25 rupees)
	binary.BigEndian.PutUint32(packet[offset+10:offset+14], 500)     // Fill volume
	binary.BigEndian.PutUint64(packet[offset+14:offset+22], 1500000) // Open Interest
	binary.BigEndian.PutUint64(packet[offset+22:offset+30], 1650000) // Day Hi OI
	binary.BigEndian.PutUint64(packet[offset+30:offset+38], 1450000) // Day Lo OI

	// Fill second record (38 bytes starting at offset 78)
	offset = 78
	binary.BigEndian.PutUint32(packet[offset:offset+4], 26009)       // Token
	binary.BigEndian.PutUint16(packet[offset+4:offset+6], 1)         // Market type
	binary.BigEndian.PutUint32(packet[offset+6:offset+10], 148550)   // Fill price (1485.50 rupees)
	binary.BigEndian.PutUint32(packet[offset+10:offset+14], 750)     // Fill volume
	binary.BigEndian.PutUint64(packet[offset+14:offset+22], 2100000) // Open Interest
	binary.BigEndian.PutUint64(packet[offset+22:offset+30], 2250000) // Day Hi OI
	binary.BigEndian.PutUint64(packet[offset+30:offset+38], 2050000) // Day Lo OI

	return packet
}
