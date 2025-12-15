// NSE Multicast UDP Receiver - Object-Oriented Class Architecture
// 
// ARCHITECTURE OVERVIEW:
// =====================
// 🏭 Factory Pattern: MessageFactory creates and manages processor classes
// 📦 Class-based Design: Each message code has a dedicated processor class  
// 🎯 Single Responsibility: Each class handles complete processing for one message type
// 🔄 Encapsulation: All parsing, validation, and export logic within each class
//
// USAGE:
// ======
// Run: go run test_main.go lzo_decompressor_safe.go
// Output: csv_output/message_XXXX_TIMESTAMP.csv (separate file per message type)
//
// CLASS STRUCTURE:
// ===============
// MessageProcessor (interface) → All processor classes implement this
// ├─ ProcessMessage() → Main entry point (like a class constructor call)
// ├─ ParsePacket() → Internal parsing logic  
// ├─ ExportData() → Internal CSV export logic
// └─ Initialize() → Class initialization
//
// SUPPORTED MESSAGE CLASSES:
// =========================
// • Processor6541  → BC_CIRCUIT_CHECK
// • Processor7200  → BCAST_MBO_MBP_UPDATE  
// • Processor7201  → BCAST_MW_ROUND_ROBIN (+ 17201)
// • Processor7202  → BCAST_TICKER_AND_MKT_INDEX
// • Processor7208  → BCAST_ONLY_MBP (Most Important)
// • Processor7211  → BCAST_SPD_MBP_DELTA
// • Processor7220  → BCAST_LIMIT_PRICE_PROTECTION_RANGE
// • Processor7340  → BCAST_SEC_MSTR_CHNG_PERIODIC  
// • Processor17202 → BCAST_ENHNCD_TICKER_AND_MKT_INDEX

package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// =============================================================================
// MESSAGE STRUCTURES (for parsing) - Complete NSE NNF Protocol v9.46
// =============================================================================

// Message6541 - BC_CIRCUIT_CHECK (Circuit Breaker Check)
// Structure: MESSAGE_HEADER (40 bytes only)
// Used for connection testing and circuit breaker status
type Message6541 struct {
	// Only message header, no additional data
	TransactionCode uint16 // Always 6541
	MessageLength   uint16 // Always 40
}

// Message7200 - BCAST_MBO_MBP_UPDATE (Market by Order/Price Update) 
// Structure: BCAST_HEADER(40) + ST_OMD_MKT_DATA_INFO + Variable Records
// Variable number of records (MBO/MBP data)
type Message7200 struct {
	Token               uint32  // 4 bytes (UNSIGNED LONG)
	BookType            uint16  // 2 bytes 
	TradingStatus       uint16  // 2 bytes
	VolumeTradedToday   uint32  // 4 bytes (UNSIGNED LONG)
	LastTradedPrice     uint32  // 4 bytes (UNSIGNED LONG)
	NetChangeIndicator  byte    // 1 byte
	LastUpdateTime      uint32  // 4 bytes (UNSIGNED LONG) 
	MessageSequenceNumber uint32 // 4 bytes (UNSIGNED LONG)
	// Additional MBO/MBP records follow...
}

// Message7201 - BCAST_MW_ROUND_ROBIN (Market Watch Round Robin)
// Structure: BCAST_HEADER(40) + NoOfRecords(2) + ST_MW_ROUND_ROBIN_INFO[5]
// Each record: 86 bytes, maximum 5 records per packet
type Message7201 struct {
	Token               int32   // 4 bytes - Token number
	LastTradedPrice     int32   // 4 bytes - LTP 
	LastTradedQuantity  int32   // 4 bytes - Last trade quantity
	LastTradedTime      int32   // 4 bytes - Last trade time
	AverageTradePrice   int32   // 4 bytes - Average price
	TotalBuyQuantity    int64   // 8 bytes - Total buy quantity (LONG LONG)
	TotalSellQuantity   int64   // 8 bytes - Total sell quantity (LONG LONG) 
	TotalTradedQuantity int64   // 8 bytes - Total traded quantity (LONG LONG)
	OpenPrice           int32   // 4 bytes - Open price
	HighPrice           int32   // 4 bytes - High price
	LowPrice            int32   // 4 bytes - Low price
	ClosePrice          int32   // 4 bytes - Close price
	// Additional 34 bytes of data...
}

// Message7202 - BCAST_TICKER_AND_MKT_INDEX (Ticker and Market Index)
// Structure: BCAST_HEADER(40) + NoOfRecords(2) + ST_TICKER_INDEX_INFO[25]
// Each record: 32 bytes, maximum 25 records per packet
type Message7202 struct {
	Token               int32   // 4 bytes - Token number (LONG)
	MarketType          int16   // 2 bytes - Market type
	LastTradedPrice     int32   // 4 bytes - Last traded price (LONG)
	HighPrice           int32   // 4 bytes - High price (LONG)
	LowPrice            int32   // 4 bytes - Low price (LONG) 
	OpenPrice           int32   // 4 bytes - Open price (LONG)
	ClosePrice          int32   // 4 bytes - Close price (LONG)
	PercentChange       float32 // 4 bytes - Percentage change (FLOAT)
	TotalTradedQuantity int64   // 8 bytes - Total traded quantity (LONG LONG)
	TotalTradedValue    int64   // 8 bytes - Total traded value (LONG LONG)
}

// Message7208 - BCAST_ONLY_MBP (Most important - Market by Price Only)
// Structure: BCAST_HEADER(40) + NoOfRecords(2) + INTERACTIVE_ONLY_MBP_DATA[2]
// Each record: 214 bytes, exactly 2 records per packet
// IMPORTANT: Token and all price fields are UNSIGNED (uint32) as per reference code
type Message7208 struct {
	Token          uint32  // offset 0, 4 bytes (UNSIGNED LONG - matches reference 7208_st.go)
	BookType       uint16  // offset 4, 2 bytes (UNSIGNED)
	TradingStatus  uint16  // offset 6, 2 bytes (UNSIGNED)
	VolumeTradedToday uint32 // offset 8, 4 bytes (UNSIGNED LONG)
	LastTradedPrice uint32  // offset 12, 4 bytes (UNSIGNED LONG - LTP in NSE format, divide by 100)
	NetChangeIndicator byte // offset 16, 1 byte
	VolTrdTodayExcdIndc byte // offset 17, 1 byte
	NetPriceChangeFromClosingPrice uint32 // offset 18, 4 bytes (UNSIGNED LONG)
	LastTradeQuantity uint32 // offset 22, 4 bytes (UNSIGNED LONG)
	LastTradeTime uint32    // offset 26, 4 bytes (UNSIGNED LONG)
	AverageTradePrice uint32 // offset 30, 4 bytes (UNSIGNED LONG)
	AuctionNumber uint16    // offset 34, 2 bytes (UNSIGNED)
	AuctionStatus uint16    // offset 36, 2 bytes (UNSIGNED)
	InitiatorType uint16    // offset 38, 2 bytes (UNSIGNED)
	InitiatorPrice uint32   // offset 40, 4 bytes (UNSIGNED LONG)
	InitiatorQuantity uint32 // offset 44, 4 bytes (UNSIGNED LONG)
	AuctionPrice uint32     // offset 48, 4 bytes (UNSIGNED LONG)
	AuctionQuantity uint32  // offset 52, 4 bytes (UNSIGNED LONG)
	// RecordBuffer (MBP_INFORMATION[10]) at offset 56-175 - parsed separately
	BestBidPrice    float64 // Derived from MBP_INFORMATION
	BestAskPrice    float64 // Derived from MBP_INFORMATION 
	BidAskSpread    float64 // Calculated as BestAskPrice - BestBidPrice
	BidLevels       int     // Number of bid price levels
	AskLevels       int     // Number of ask price levels
	BbTotalBuyFlag uint16   // offset 176, 2 bytes (UNSIGNED)
	BbTotalSellFlag uint16  // offset 178, 2 bytes (UNSIGNED)
	TotalBuyQuantity float64 // offset 180, 8 bytes (DOUBLE)
	TotalSellQuantity float64 // offset 188, 8 bytes (DOUBLE)
	// ST_INDICATOR at offset 196, 2 bytes - skipped for now
	ClosingPrice uint32     // offset 198, 4 bytes (UNSIGNED LONG)
	OpenPrice uint32        // offset 202, 4 bytes (UNSIGNED LONG)
	HighPrice uint32        // offset 206, 4 bytes (UNSIGNED LONG)
	LowPrice uint32         // offset 210, 4 bytes (UNSIGNED LONG)
}

// Message7211 - BCAST_SPD_MBP_DELTA (Spread Market by Price Delta)
// Structure: BCAST_HEADER(40) + NoOfRecords(2) + MS_SPD_MKT_INFO[5]
// Each record: 204 bytes, maximum 5 records per packet
// Used for spread trading (2 legs with different expiry dates)
type Message7211 struct {
	Token1              int32   // offset 0, 4 bytes - Token of security with early expiry (LONG)
	Token2              int32   // offset 4, 4 bytes - Token of security with later expiry (LONG)
	MbpBuy              int16   // offset 8, 2 bytes - Total number of buys
	MbpSell             int16   // offset 10, 2 bytes - Total number of sells
	LastActiveTime      int32   // offset 12, 4 bytes - Last active time (LONG)
	TradedVolume        uint32  // offset 16, 4 bytes - Traded volume (UNSIGNED LONG)
	TotalTradedValue    float64 // offset 20, 8 bytes - Total traded value (DOUBLE)
	// MbpBuys[5] at offset 28-77 (10 bytes each) - skipped for now
	// MbpSells[5] at offset 78-127 (10 bytes each) - skipped for now  
	TotalBuyVolume      float64 // offset 128, 8 bytes - Total buy volume (DOUBLE)
	TotalSellVolume     float64 // offset 136, 8 bytes - Total sell volume (DOUBLE)
	OpenPriceDifference int32   // offset 144, 4 bytes - Open price difference (LONG)
	DayHighPriceDifference int32 // offset 148, 4 bytes - Day high price difference (LONG)
	DayLowPriceDifference int32  // offset 152, 4 bytes - Day low price difference (LONG)
	LastTradedPriceDifference int32 // offset 156, 4 bytes - LTP difference (LONG)
	LastUpdateTime      int32   // offset 160, 4 bytes - Last update time (LONG)
}

// Message7220 - BCAST_LIMIT_PRICE_PROTECTION_RANGE (Limit Price Protection)
// Structure: BCAST_HEADER(40) + Variable Records 
// Variable number of records (up to 25), each 12 bytes
type Message7220 struct {
	TokenNumber  int32  // offset 0, 4 bytes - Token number (LONG)
	HighExecBand int32  // offset 4, 4 bytes - High LPP execution band (LONG)
	LowExecBand  int32  // offset 8, 4 bytes - Low LPP execution band (LONG)
	// Total: 12 bytes per record
}

// Message7340 - BCAST_SEC_MSTR_CHNG_PERIODIC (Security Master Change Periodic)
// Structure: BCAST_HEADER(40) + MS_SECURITY_UPDATE_INFO (298 bytes)
// Security master data changes broadcast periodically
type Message7340 struct {
	Token           uint32   // 4 bytes - Token number (LONG)
	Symbol          [10]byte // 10 bytes - Symbol name (CHAR array)
	Series          [2]byte  // 2 bytes - Series (CHAR array)
	InstrumentName  [6]byte  // 6 bytes - Instrument name (CHAR array) 
	ExpiryDate      uint32   // 4 bytes - Expiry date (LONG)
	StrikePrice     uint32   // 4 bytes - Strike price (LONG)
	OptionType      [2]byte  // 2 bytes - Option type (CE/PE) (CHAR array)
	// Additional security master fields... (total 298 bytes)
}

// Message17201 - BCAST_ENHNCD_MW_ROUND_ROBIN (Enhanced Market Watch Round Robin)
// Structure: BCAST_HEADER(40) + Variable ST_MW_ROUND_ROBIN_INFO records
// Each record: 86 bytes, maximum 10 records per packet (enhanced version)
type Message17201 struct {
	// Same structure as Message7201 but allows more records
	Token               int32   // 4 bytes - Token number
	LastTradedPrice     int32   // 4 bytes - LTP
	LastTradedQuantity  int32   // 4 bytes - Last trade quantity
	LastTradedTime      int32   // 4 bytes - Last trade time
	AverageTradePrice   int32   // 4 bytes - Average price
	TotalBuyQuantity    int64   // 8 bytes - Total buy quantity (LONG LONG)
	TotalSellQuantity   int64   // 8 bytes - Total sell quantity (LONG LONG)
	TotalTradedQuantity int64   // 8 bytes - Total traded quantity (LONG LONG)
	OpenPrice           int32   // 4 bytes - Open price
	HighPrice           int32   // 4 bytes - High price
	LowPrice            int32   // 4 bytes - Low price
	ClosePrice          int32   // 4 bytes - Close price
	// Additional 34 bytes of data...
}

// Message17202 - BCAST_ENHNCD_TICKER_AND_MKT_INDEX (Enhanced Ticker and Market Index)
// Structure: BCAST_HEADER(40) + Variable ST_ENHNCD_TICKER_INDEX_INFO records
// Each record: 38 bytes, maximum 12 records per packet (enhanced version with 64-bit OI)
type Message17202 struct {
	Token        int32 // 4 bytes - Token number (LONG)
	MarketType   int16 // 2 bytes - Market type 
	FillPrice    int32 // 4 bytes - Fill price (LONG)
	FillVolume   int32 // 4 bytes - Fill volume (LONG)
	OpenInterest int64 // 8 bytes - Open interest (LONG LONG)
	DayHiOI      int64 // 8 bytes - Day high open interest (LONG LONG)
	DayLoOI      int64 // 8 bytes - Day low open interest (LONG LONG)
	// Total: 38 bytes per record
}

// =============================================================================
// SECURITY MASTER - For 7340 integration with 7208
// =============================================================================

// SecurityInfo holds master data for securities from 7340 messages
type SecurityInfo struct {
	Token       uint32
	Symbol      string
	Instrument  string
	OptionType  string
	StrikePrice uint32
	ExpiryDate  uint32
	Series      string
}

// Global security master map (updated by 7340, used by 7208)
var securityMaster = make(map[uint32]*SecurityInfo)
var securityMasterMutex sync.RWMutex

// AddToSecurityMaster adds or updates security info
func AddToSecurityMaster(info *SecurityInfo) {
	securityMasterMutex.Lock()
	defer securityMasterMutex.Unlock()
	securityMaster[info.Token] = info
}

// GetFromSecurityMaster retrieves security info by token
func GetFromSecurityMaster(token uint32) (*SecurityInfo, bool) {
	securityMasterMutex.RLock()
	defer securityMasterMutex.RUnlock()
	info, exists := securityMaster[token]
	return info, exists
}

// =============================================================================
// MESSAGE PROCESSOR INTERFACE
// =============================================================================

// MessageProcessor interface that all message processors must implement
// Each processor is a complete class that handles all aspects of its message type
type MessageProcessor interface {
	Initialize(timestamp string) error
	ProcessMessage(data []byte) error  // Main processing entry point
	GetParsedCount() int64
	Close() error
	GetMessageCode() uint16
	GetMessageName() string
	
	// Internal class methods (can be called from ProcessMessage)
	ParsePacket(data []byte) error
	ExportData(data interface{}) error
}

// =============================================================================
// PROCESSOR 7208 - BCAST_ONLY_MBP (Market by Price)
// =============================================================================

type Processor7208 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7208() *Processor7208 {
	return &Processor7208{
		messageCode: 7208,
		messageName: "BCAST_ONLY_MBP",
	}
}

func (p *Processor7208) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7208_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "BookType", "TradingStatus",
		"VolumeTradedToday", "LastTradedPrice", "NetChangeIndicator",
		"VolTrdTodayExcdIndc", "NetPriceChangeFromClosingPrice",
		"LastTradeQuantity", "LastTradeTime", "AverageTradePrice",
		"AuctionNumber", "AuctionStatus", "InitiatorType",
		"InitiatorPrice", "InitiatorQuantity", "AuctionPrice", "AuctionQuantity",
		"BestBidPrice", "BestAskPrice", "BidAskSpread", "BidLevels", "AskLevels",
		"BbTotalBuyFlag", "BbTotalSellFlag",
		"TotalBuyQuantity", "TotalSellQuantity",
		"ClosingPrice", "OpenPrice", "HighPrice", "LowPrice",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7208: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7208 processing (class method)
func (p *Processor7208) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7208 packets
func (p *Processor7208) ParsePacket(data []byte) error {
	// Message 7208: BCAST_ONLY_MBP (Market by Price)
	// IMPORTANT: Use EXACT same parsing as reference udp_receiver.go
	// Reference code: token := binary.BigEndian.Uint32(decompressedPacket[50:54])
	if len(data) < 54 {
		return fmt.Errorf("packet too short for token extraction")
	}
	
	// Check message code first (reference: message_code := binary.BigEndian.Uint16(decompressedPacket[18:20]))
	messageCode := binary.BigEndian.Uint16(data[18:20])
	if messageCode != 7208 {
		return fmt.Errorf("not a 7208 message: %d", messageCode)
	}
	
	// Extract token at ABSOLUTE offset 50 (EXACT same as reference code)
	// Reference: token := binary.BigEndian.Uint32(decompressedPacket[50:54])
	if len(data) >= 54 {
		token := binary.BigEndian.Uint32(data[50:54])  // Reference code offset
		
		// Track token statistics
		tokenStats[token]++
		rangeType := getTokenRange(token)
		tokenRanges[rangeType]++
		
		// Update min/max token range
		if token < minToken {
			minToken = token
		}
		if token > maxToken {
			maxToken = token
		}
		
		// Debug: Show token detection with COMPLETE market data (same as reference code)
		if token == 46520 || token == 37054 {
			fmt.Printf("🔥 TOKEN %d FOUND in Message 7208! 🔥\n", token)
			
			// Extract LTP at offset 62-66 (same as reference approach)  
			if len(data) >= 66 {
				ltpRaw := binary.BigEndian.Uint32(data[62:66])
				ltp := float64(ltpRaw) / 100.0
				fmt.Printf("   💰 Last Traded Price (LTP): %.2f\n", ltp)
				
				// Extract additional CRITICAL data for accuracy verification
				if len(data) >= 76 {
					volumeRaw := binary.BigEndian.Uint32(data[58:62])
					lastQtyRaw := binary.BigEndian.Uint32(data[72:76])
					fmt.Printf("   📊 Volume Traded Today: %d\n", volumeRaw)
					fmt.Printf("   📈 Last Trade Quantity: %d\n", lastQtyRaw)
				}
				
				// Parse and display BID/ASK data for accuracy verification
				if len(data) >= 226 {  // Ensure enough data for MBP parsing
					bestBid, bestAsk, bidLevels, askLevels := ParseMBPInformation(data, 50)
					if bestBid > 0 && bestAsk > 0 {
						fmt.Printf("   🎯 Best Bid: %.2f | Best Ask: %.2f | Spread: %.2f\n", 
							bestBid, bestAsk, bestAsk-bestBid)
						fmt.Printf("   📋 Bid Levels: %d | Ask Levels: %d\n", 
							len(bidLevels), len(askLevels))
					}
				}
			}
		}
		
		// Token range analysis - uncomment to see all token ranges
		// fmt.Printf("📊 Token: %d (Range: %s)\n", token, getTokenRange(token))
		
		// 🔥 BID/ASK TERMINAL DISPLAY - Show for all tokens with market data
		if len(data) >= 226 {  // Ensure enough data for MBP parsing
			bestBid, bestAsk, bidLevels, askLevels := ParseMBPInformation(data, 50)
			
			// Track bid/ask statistics
			if bestBid > 0 {
				atomic.AddInt64(&tokensWithBids, 1)
			}
			if bestAsk > 0 {
				atomic.AddInt64(&tokensWithAsks, 1)
			}
			if bestBid > 0 && bestAsk > 0 {
				atomic.AddInt64(&tokensWithBoth, 1)
			}
			
			// Show bid/ask for tokens with actual market data (non-zero values)
			if bestBid > 0 || bestAsk > 0 {
				ltpRaw := binary.BigEndian.Uint32(data[62:66])
				ltp := float64(ltpRaw) / 100.0
				
				fmt.Printf("📈 Token %d | LTP: %.2f | Bid: %.2f | Ask: %.2f | Spread: %.2f | BidLvls: %d | AskLvls: %d\n", 
					token, ltp, bestBid, bestAsk, bestAsk-bestBid, len(bidLevels), len(askLevels))
			} else {
				// Show tokens with no bid/ask data (reduced spam)
				ltpRaw := binary.BigEndian.Uint32(data[62:66])
				ltp := float64(ltpRaw) / 100.0
				
				// Only show every 100th token to avoid spam
				if token%100 == 0 {
					fmt.Printf("⚠️  Token %d | LTP: %.2f | NO BID/ASK DATA\n", token, ltp)
				}
			}
		}
	}
	
	// Parse using REFERENCE CODE approach - single message at absolute offsets
	// Reference code extracts token at offset 50, so treat as single record starting at offset 42
	// This matches reference behavior: decompressedPacket[50:54] = data[50:54]
	if len(data) >= 256 { // Ensure enough data for full 7208 message
		if msg := p.parseMessage7208Reference(data); msg != nil {
			// Validate parsed data for accuracy
			if ValidateMessage7208Data(msg) {
				p.ExportData(msg)
				atomic.AddInt64(&p.parsedCount, 1)
			} else {
				// fmt.Printf("⚠️  Invalid data detected for token %d, skipping record\n", msg.Token)
			}
		}
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7208) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7208); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7208")
}

// parseMessage7208Reference - Parse using EXACT reference code offsets  
// Reference code: token := binary.BigEndian.Uint32(decompressedPacket[50:54])
// This means token is at absolute offset 50 in the decompressed packet
func (p *Processor7208) parseMessage7208Reference(data []byte) *Message7208 {
	if len(data) < 256 {
		return nil
	}

	// Parse using EXACT reference code offsets with ACCURATE field mapping
	// Reference code analysis:
	// - Token at decompressedPacket[50:54] → Record starts at offset 50  
	// - LTP at decompressedPacket[62:66] → LTP is at record offset 12 (50+12=62) ✓
	// This confirms: First record starts at absolute offset 50
	
	recordStart := 50  // First record starts here (confirmed by reference code)
	
	msg := &Message7208{
		// CORE FIELDS - Using exact offsets from INTERACTIVE_ONLY_MBP_DATA structure
		Token:                          binary.BigEndian.Uint32(data[recordStart+0:recordStart+4]),   // offset 50:54 ✅
		BookType:                       binary.BigEndian.Uint16(data[recordStart+4:recordStart+6]),   // offset 54:56
		TradingStatus:                  binary.BigEndian.Uint16(data[recordStart+6:recordStart+8]),   // offset 56:58
		VolumeTradedToday:              binary.BigEndian.Uint32(data[recordStart+8:recordStart+12]),  // offset 58:62
		LastTradedPrice:                binary.BigEndian.Uint32(data[recordStart+12:recordStart+16]), // offset 62:66 ✅
		NetChangeIndicator:             data[recordStart+16],                                          // offset 66
		VolTrdTodayExcdIndc:            data[recordStart+17],                                          // offset 67
		NetPriceChangeFromClosingPrice: binary.BigEndian.Uint32(data[recordStart+18:recordStart+22]), // offset 68:72
		LastTradeQuantity:              binary.BigEndian.Uint32(data[recordStart+22:recordStart+26]), // offset 72:76
		LastTradeTime:                  binary.BigEndian.Uint32(data[recordStart+26:recordStart+30]), // offset 76:80
		AverageTradePrice:              binary.BigEndian.Uint32(data[recordStart+30:recordStart+34]), // offset 80:84
		AuctionNumber:                  binary.BigEndian.Uint16(data[recordStart+34:recordStart+36]), // offset 84:86
		AuctionStatus:                  binary.BigEndian.Uint16(data[recordStart+36:recordStart+38]), // offset 86:88
		InitiatorType:                  binary.BigEndian.Uint16(data[recordStart+38:recordStart+40]), // offset 88:90
		InitiatorPrice:                 binary.BigEndian.Uint32(data[recordStart+40:recordStart+44]), // offset 90:94
		InitiatorQuantity:              binary.BigEndian.Uint32(data[recordStart+44:recordStart+48]), // offset 94:98
		AuctionPrice:                   binary.BigEndian.Uint32(data[recordStart+48:recordStart+52]), // offset 98:102
		AuctionQuantity:                binary.BigEndian.Uint32(data[recordStart+52:recordStart+56]), // offset 102:106
		
		// Initialize to safe defaults
		BbTotalBuyFlag:    0,
		BbTotalSellFlag:   0,
		TotalBuyQuantity:  0,
		TotalSellQuantity: 0,
		ClosingPrice:      0,
		OpenPrice:         0,
		HighPrice:         0,
		LowPrice:          0,
	}
	
	// CRITICAL: Parse MBP_INFORMATION (BID/ASK DATA) at offset 56-175
	// This contains the actual bid/ask prices that we need!
	// RecordBuffer [10]MBP_INFORMATION - 12 bytes each, total 120 bytes
	// NOTE: We'll parse this in the CSV export debug, but for now validate structure
	
	// Parse fields after MBP_INFORMATION buffer (offset 176+)
	if len(data) >= recordStart+214 {
		msg.BbTotalBuyFlag = binary.BigEndian.Uint16(data[recordStart+176:recordStart+178])   // offset 226:228
		msg.BbTotalSellFlag = binary.BigEndian.Uint16(data[recordStart+178:recordStart+180])  // offset 228:230
		
		// Parse DOUBLE fields (8 bytes each) - CRITICAL for volume data
		if len(data) >= recordStart+196 {
			msg.TotalBuyQuantity = float64FromBytes(data[recordStart+180:recordStart+188])    // offset 230:238
			msg.TotalSellQuantity = float64FromBytes(data[recordStart+188:recordStart+196])   // offset 238:246
		}
		
		// Parse OHLC prices - CRITICAL for price analysis
		if len(data) >= recordStart+214 {
			msg.ClosingPrice = binary.BigEndian.Uint32(data[recordStart+198:recordStart+202]) // offset 248:252
			msg.OpenPrice = binary.BigEndian.Uint32(data[recordStart+202:recordStart+206])    // offset 252:256
			msg.HighPrice = binary.BigEndian.Uint32(data[recordStart+206:recordStart+210])    // offset 256:260
			msg.LowPrice = binary.BigEndian.Uint32(data[recordStart+210:recordStart+214])     // offset 260:264
		}
	}
	
	// ✅ ZERO CHECKS + MARKET STATUS (as suggested)
	if msg.AuctionNumber == 0 {
		msg.AuctionStatus = 0  // No auction
	}
	
	// MBP Parsing with zero handling and proper recordStart
	if len(data) >= recordStart+175 {  // Ensure enough data for MBP parsing
		bestBid, bestAsk, bidLevels, askLevels := ParseMBPInformation(data, recordStart)
		
		// Store bid/ask values in message struct
		msg.BestBidPrice = bestBid
		msg.BestAskPrice = bestAsk
		msg.BidAskSpread = bestAsk - bestBid
		msg.BidLevels = len(bidLevels)
		msg.AskLevels = len(askLevels)
		
		if bestBid == 0 {
			// Only show warning for target tokens to avoid spam
			if msg.Token == 46520 || msg.Token == 37054 {
				fmt.Printf("⚠️ Token %d: NO BIDS\n", msg.Token)
			}
		}
		if bestAsk == 0 {
			// Only show warning for target tokens to avoid spam  
			if msg.Token == 46520 || msg.Token == 37054 {
				fmt.Printf("⚠️ Token %d: NO ASKS\n", msg.Token)
			}
		}
	}
	
	return msg
}

func (p *Processor7208) parseMessage7208(data []byte) *Message7208 {
	if len(data) < 214 {
		return nil
	}

	// Parse using exact same method as reference code (7208_st.go)
	// IMPORTANT: All fields are UNSIGNED as per INTERACTIVE_ONLY_MBP_DATA structure
	offset := 0
	
	msg := &Message7208{
		// Core fields - exactly as reference code
		Token:          binary.BigEndian.Uint32(data[offset : offset+4]),        // offset 0, UNSIGNED LONG
		BookType:       binary.BigEndian.Uint16(data[offset+4 : offset+6]),     // offset 4, UNSIGNED
		TradingStatus:  binary.BigEndian.Uint16(data[offset+6 : offset+8]),     // offset 6, UNSIGNED
		VolumeTradedToday: binary.BigEndian.Uint32(data[offset+8 : offset+12]), // offset 8, UNSIGNED LONG
		LastTradedPrice: binary.BigEndian.Uint32(data[offset+12 : offset+16]),  // offset 12, UNSIGNED LONG
		NetChangeIndicator: data[offset+16],                                     // offset 16, CHAR
		VolTrdTodayExcdIndc: data[offset+17],                                   // offset 17, CHAR
		NetPriceChangeFromClosingPrice: binary.BigEndian.Uint32(data[offset+18 : offset+22]), // offset 18, UNSIGNED LONG
		LastTradeQuantity: binary.BigEndian.Uint32(data[offset+22 : offset+26]),  // offset 22, UNSIGNED LONG
		LastTradeTime: binary.BigEndian.Uint32(data[offset+26 : offset+30]),      // offset 26, UNSIGNED LONG
		AverageTradePrice: binary.BigEndian.Uint32(data[offset+30 : offset+34]),  // offset 30, UNSIGNED LONG
		AuctionNumber: binary.BigEndian.Uint16(data[offset+34 : offset+36]),      // offset 34, UNSIGNED
		AuctionStatus: binary.BigEndian.Uint16(data[offset+36 : offset+38]),      // offset 36, UNSIGNED
		InitiatorType: binary.BigEndian.Uint16(data[offset+38 : offset+40]),      // offset 38, UNSIGNED
		InitiatorPrice: binary.BigEndian.Uint32(data[offset+40 : offset+44]),     // offset 40, UNSIGNED LONG
		InitiatorQuantity: binary.BigEndian.Uint32(data[offset+44 : offset+48]),  // offset 44, UNSIGNED LONG
		AuctionPrice: binary.BigEndian.Uint32(data[offset+48 : offset+52]),       // offset 48, UNSIGNED LONG
		AuctionQuantity: binary.BigEndian.Uint32(data[offset+52 : offset+56]),    // offset 52, UNSIGNED LONG
		// Skip MBP_INFORMATION buffer (offset 56-175)
		BbTotalBuyFlag: binary.BigEndian.Uint16(data[176:178]),                   // offset 176, UNSIGNED
		BbTotalSellFlag: binary.BigEndian.Uint16(data[178:180]),                  // offset 178, UNSIGNED
		TotalBuyQuantity: float64FromBytes(data[180:188]),                        // offset 180, DOUBLE
		TotalSellQuantity: float64FromBytes(data[188:196]),                       // offset 188, DOUBLE
		// Skip ST_INDICATOR (offset 196-197)
		ClosingPrice: binary.BigEndian.Uint32(data[198:202]),                     // offset 198, UNSIGNED LONG
		OpenPrice: binary.BigEndian.Uint32(data[202:206]),                        // offset 202, UNSIGNED LONG
		HighPrice: binary.BigEndian.Uint32(data[206:210]),                        // offset 206, UNSIGNED LONG
		LowPrice: binary.BigEndian.Uint32(data[210:214]),                         // offset 210, UNSIGNED LONG
	}
	
	return msg
}

func (p *Processor7208) exportToCSV(msg *Message7208) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// Use actual bid/ask data from the parsed message struct
	bestBid := msg.BestBidPrice
	bestAsk := msg.BestAskPrice
	bidAskSpread := msg.BidAskSpread
	bidLevels := msg.BidLevels
	askLevels := msg.AskLevels

	// Track bid/ask statistics
	hasBid := bestBid > 0
	hasAsk := bestAsk > 0
	

	
	if hasBid && hasAsk {
		atomic.AddInt64(&tokensWithBoth, 1)
	} else if hasBid {
		atomic.AddInt64(&tokensWithBids, 1)
	} else if hasAsk {
		atomic.AddInt64(&tokensWithAsks, 1)
	}

	// Display bid/ask data in terminal (every 100th token to reduce spam)  
	currentCount := atomic.LoadInt64(&p.parsedCount) + 1
	if (hasBid || hasAsk) && currentCount % 100 == 0 {
		//ltp := float64(msg.LastTradedPrice) / 100.0
		// fmt.Printf("📈 Token %d | LTP: %.2f | Bid: %.2f | Ask: %.2f | Spread: %.2f | BidLvls: %d | AskLvls: %d\n",
		//	msg.Token, ltp, bestBid, bestAsk, bidAskSpread, bidLevels, askLevels)
	}

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.BookType),
		fmt.Sprintf("%d", msg.TradingStatus),
		fmt.Sprintf("%d", msg.VolumeTradedToday),
		fmt.Sprintf("%.2f", float64(msg.LastTradedPrice)/100.0),
		fmt.Sprintf("%c", msg.NetChangeIndicator),
		fmt.Sprintf("%d", msg.VolTrdTodayExcdIndc),
		fmt.Sprintf("%.2f", float64(msg.NetPriceChangeFromClosingPrice)/100.0),
		fmt.Sprintf("%d", msg.LastTradeQuantity),
		fmt.Sprintf("%d", msg.LastTradeTime),
		fmt.Sprintf("%.2f", float64(msg.AverageTradePrice)/100.0),
		fmt.Sprintf("%d", msg.AuctionNumber),
		fmt.Sprintf("%d", msg.AuctionStatus),
		fmt.Sprintf("%d", msg.InitiatorType),
		fmt.Sprintf("%.2f", float64(msg.InitiatorPrice)/100.0),
		fmt.Sprintf("%d", msg.InitiatorQuantity),
		fmt.Sprintf("%.2f", float64(msg.AuctionPrice)/100.0),
		fmt.Sprintf("%d", msg.AuctionQuantity),
		fmt.Sprintf("%.2f", bestBid),        // Best bid price
		fmt.Sprintf("%.2f", bestAsk),        // Best ask price  
		fmt.Sprintf("%.2f", bidAskSpread),   // Bid-ask spread
		fmt.Sprintf("%d", bidLevels),        // Number of bid levels
		fmt.Sprintf("%d", askLevels),        // Number of ask levels
		fmt.Sprintf("%d", msg.BbTotalBuyFlag),
		fmt.Sprintf("%d", msg.BbTotalSellFlag),
		fmt.Sprintf("%.0f", msg.TotalBuyQuantity),
		fmt.Sprintf("%.0f", msg.TotalSellQuantity),
		fmt.Sprintf("%.2f", float64(msg.ClosingPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.OpenPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.HighPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.LowPrice)/100.0),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
	
	// Stage 4: Record saved to CSV
	TrackMessageSaved(p.messageCode)
}

func (p *Processor7208) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7208) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7208) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7208) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7201 - BCAST_MW_ROUND_ROBIN (Market Watch)
// =============================================================================

type Processor7201 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7201() *Processor7201 {
	return &Processor7201{
		messageCode: 7201,
		messageName: "BCAST_MW_ROUND_ROBIN",
	}
}

func (p *Processor7201) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7201_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "LastTradedPrice", "LastTradedQuantity",
		"LastTradeTime", "AverageTradePrice", "TotalBuyQuantity", "TotalSellQuantity",
		"TotalTradedQuantity", "OpenPrice", "HighPrice", "LowPrice", "ClosePrice",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7201: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7201 processing (class method)
func (p *Processor7201) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7201/17201 packets
func (p *Processor7201) ParsePacket(data []byte) error {
	if len(data) < 20 {
		return fmt.Errorf("packet too short")
	}
	
	messageCode := binary.BigEndian.Uint16(data[18:20])
	
	if messageCode == 7201 {
		return p.process7201(data)
	} else if messageCode == 17201 {
		return p.process17201(data)
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7201) ExportData(data interface{}) error {
	if exportData, ok := data.(struct{
		messageCode uint16
		message     *Message7201
	}); ok {
		p.exportToCSV(exportData.messageCode, exportData.message)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7201")
}

func (p *Processor7201) process7201(data []byte) error {
	if len(data) < 42 {
		return fmt.Errorf("packet too short")
	}
	
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	if noOfRecords == 0 || noOfRecords > 5 {
		return fmt.Errorf("invalid record count")
	}
	
	offset := 42
	for i := 0; i < int(noOfRecords); i++ {
		if offset+86 > len(data) {
			break
		}
		recordData := data[offset : offset+86]
		if msg := p.parseMessage7201(recordData); msg != nil {
			p.ExportData(struct{
				messageCode uint16
				message     *Message7201
			}{7201, msg})
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 86
	}
	return nil
}

func (p *Processor7201) process17201(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("packet too short")
	}
	
	dataLength := len(data) - 40
	if dataLength%86 != 0 {
		return fmt.Errorf("invalid packet size")
	}
	
	noOfRecords := dataLength / 86
	if noOfRecords == 0 || noOfRecords > 10 {
		return fmt.Errorf("invalid record count")
	}
	
	offset := 40
	for i := 0; i < noOfRecords; i++ {
		if offset+86 > len(data) {
			break
		}
		recordData := data[offset : offset+86]
		if msg := p.parseMessage7201(recordData); msg != nil {
			p.ExportData(struct{
				messageCode uint16
				message     *Message7201
			}{17201, msg})
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 86
	}
	return nil
}

func (p *Processor7201) parseMessage7201(data []byte) *Message7201 {
	if len(data) < 86 {
		return nil
	}

	return &Message7201{
		Token:          int32(binary.BigEndian.Uint32(data[0:4])),
		LastTradedPrice: int32(binary.BigEndian.Uint32(data[4:8])),
		LastTradedQuantity: int32(binary.BigEndian.Uint32(data[8:12])),
		LastTradedTime: int32(binary.BigEndian.Uint32(data[12:16])),
		AverageTradePrice: int32(binary.BigEndian.Uint32(data[16:20])),
		TotalBuyQuantity: int64(binary.BigEndian.Uint64(data[20:28])),
		TotalSellQuantity: int64(binary.BigEndian.Uint64(data[28:36])),
		TotalTradedQuantity: int64(binary.BigEndian.Uint64(data[36:44])),
		OpenPrice: int32(binary.BigEndian.Uint32(data[44:48])),
		HighPrice: int32(binary.BigEndian.Uint32(data[48:52])),
		LowPrice: int32(binary.BigEndian.Uint32(data[52:56])),
		ClosePrice: int32(binary.BigEndian.Uint32(data[56:60])),
	}
}

func (p *Processor7201) exportToCSV(messageCode uint16, msg *Message7201) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", messageCode),
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.LastTradedPrice),
		fmt.Sprintf("%d", msg.LastTradedQuantity),
		fmt.Sprintf("%d", msg.LastTradedTime),
		fmt.Sprintf("%d", msg.AverageTradePrice),
		fmt.Sprintf("%d", msg.TotalBuyQuantity),
		fmt.Sprintf("%d", msg.TotalSellQuantity),
		fmt.Sprintf("%d", msg.TotalTradedQuantity),
		fmt.Sprintf("%d", msg.OpenPrice),
		fmt.Sprintf("%d", msg.HighPrice),
		fmt.Sprintf("%d", msg.LowPrice),
		fmt.Sprintf("%d", msg.ClosePrice),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7201) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7201) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7201) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7201) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 6541 - BC_CIRCUIT_CHECK (Circuit Breaker Check)
// =============================================================================

type Processor6541 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor6541() *Processor6541 {
	return &Processor6541{
		messageCode: 6541,
		messageName: "BC_CIRCUIT_CHECK",
	}
}

func (p *Processor6541) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_6541_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{"Timestamp", "MessageCode", "TransactionCode", "MessageLength"}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 6541: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 6541 processing (class method)
func (p *Processor6541) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 6541 packets
func (p *Processor6541) ParsePacket(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("packet too short")
	}
	
	msg := &Message6541{
		TransactionCode: binary.BigEndian.Uint16(data[18:20]),
		MessageLength:   binary.BigEndian.Uint16(data[4:6]),
	}
	
	p.ExportData(msg)
	atomic.AddInt64(&p.parsedCount, 1)
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor6541) ExportData(data interface{}) error {
	if msg, ok := data.(*Message6541); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 6541")
}

func (p *Processor6541) exportToCSV(msg *Message6541) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.TransactionCode),
		fmt.Sprintf("%d", msg.MessageLength),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
	
	// Stage 4: Record saved to CSV
	TrackMessageSaved(p.messageCode)
}

func (p *Processor6541) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor6541) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor6541) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor6541) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7200 - BCAST_MBO_MBP_UPDATE (Market by Order/Price Update)
// =============================================================================

type Processor7200 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7200() *Processor7200 {
	return &Processor7200{
		messageCode: 7200,
		messageName: "BCAST_MBO_MBP_UPDATE",
	}
}

func (p *Processor7200) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7200_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "BookType", "TradingStatus",
		"VolumeTradedToday", "LastTradedPrice", "NetChangeIndicator",
		"LastUpdateTime", "MessageSequenceNumber",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7200: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7200 processing (class method)
func (p *Processor7200) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7200 packets
func (p *Processor7200) ParsePacket(data []byte) error {
	if len(data) < 60 {
		return fmt.Errorf("packet too short")
	}
	
	// Variable number of records - parse basic structure
	offset := 40
	if msg := p.parseMessage7200(data[offset:]); msg != nil {
		p.ExportData(msg)
		atomic.AddInt64(&p.parsedCount, 1)
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7200) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7200); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7200")
}

func (p *Processor7200) parseMessage7200(data []byte) *Message7200 {
	if len(data) < 20 {
		return nil
	}

	return &Message7200{
		Token:                 binary.BigEndian.Uint32(data[0:4]),
		BookType:             binary.BigEndian.Uint16(data[4:6]),
		TradingStatus:        binary.BigEndian.Uint16(data[6:8]),
		VolumeTradedToday:    binary.BigEndian.Uint32(data[8:12]),
		LastTradedPrice:      binary.BigEndian.Uint32(data[12:16]),
		NetChangeIndicator:   data[16],
		LastUpdateTime:       binary.BigEndian.Uint32(data[17:21]),
		MessageSequenceNumber: binary.BigEndian.Uint32(data[21:25]),
	}
}

func (p *Processor7200) exportToCSV(msg *Message7200) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.BookType),
		fmt.Sprintf("%d", msg.TradingStatus),
		fmt.Sprintf("%d", msg.VolumeTradedToday),
		fmt.Sprintf("%.2f", float64(msg.LastTradedPrice)/100.0),
		fmt.Sprintf("%c", msg.NetChangeIndicator),
		fmt.Sprintf("%d", msg.LastUpdateTime),
		fmt.Sprintf("%d", msg.MessageSequenceNumber),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7200) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7200) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7200) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7200) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7202 - BCAST_TICKER_AND_MKT_INDEX (Ticker and Market Index)
// =============================================================================

type Processor7202 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7202() *Processor7202 {
	return &Processor7202{
		messageCode: 7202,
		messageName: "BCAST_TICKER_AND_MKT_INDEX",
	}
}

func (p *Processor7202) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7202_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "MarketType", "LastTradedPrice",
		"HighPrice", "LowPrice", "OpenPrice", "ClosePrice", "PercentChange",
		"TotalTradedQuantity", "TotalTradedValue",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7202: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7202 processing (class method)
func (p *Processor7202) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7202 packets
func (p *Processor7202) ParsePacket(data []byte) error {
	if len(data) < 42 {
		return fmt.Errorf("packet too short")
	}
	
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	if noOfRecords == 0 || noOfRecords > 25 {
		return fmt.Errorf("invalid record count: %d", noOfRecords)
	}
	
	offset := 42
	for i := 0; i < int(noOfRecords); i++ {
		if offset+32 > len(data) {
			break
		}
		recordData := data[offset : offset+32]
		if msg := p.parseMessage7202(recordData); msg != nil {
			p.ExportData(msg)
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 32
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7202) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7202); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7202")
}

func (p *Processor7202) parseMessage7202(data []byte) *Message7202 {
	if len(data) < 32 {
		return nil
	}

	return &Message7202{
		Token:               int32(binary.BigEndian.Uint32(data[0:4])),
		MarketType:          int16(binary.BigEndian.Uint16(data[4:6])),
		LastTradedPrice:     int32(binary.BigEndian.Uint32(data[6:10])),
		HighPrice:           int32(binary.BigEndian.Uint32(data[10:14])),
		LowPrice:            int32(binary.BigEndian.Uint32(data[14:18])),
		OpenPrice:           int32(binary.BigEndian.Uint32(data[18:22])),
		ClosePrice:          int32(binary.BigEndian.Uint32(data[22:26])),
		PercentChange:       math.Float32frombits(binary.BigEndian.Uint32(data[26:30])),
		TotalTradedQuantity: int64(binary.BigEndian.Uint32(data[30:34])), // Fixed: only 4 bytes available
		TotalTradedValue:    int64(0), // Not enough data in 32-byte record
	}
}

func (p *Processor7202) exportToCSV(msg *Message7202) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.MarketType),
		fmt.Sprintf("%.2f", float64(msg.LastTradedPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.HighPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.LowPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.OpenPrice)/100.0),
		fmt.Sprintf("%.2f", float64(msg.ClosePrice)/100.0),
		fmt.Sprintf("%.2f", msg.PercentChange),
		fmt.Sprintf("%d", msg.TotalTradedQuantity),
		fmt.Sprintf("%d", msg.TotalTradedValue),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7202) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7202) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7202) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7202) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7211 - BCAST_SPD_MBP_DELTA (Spread Market by Price Delta)
// =============================================================================

type Processor7211 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7211() *Processor7211 {
	return &Processor7211{
		messageCode: 7211,
		messageName: "BCAST_SPD_MBP_DELTA",
	}
}

func (p *Processor7211) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7211_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token1", "Token2", "MbpBuy", "MbpSell",
		"LastActiveTime", "TradedVolume", "TotalTradedValue", "TotalBuyVolume",
		"TotalSellVolume", "OpenPriceDifference", "DayHighPriceDifference",
		"DayLowPriceDifference", "LastTradedPriceDifference", "LastUpdateTime",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7211: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7211 processing (class method)
func (p *Processor7211) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7211 packets
func (p *Processor7211) ParsePacket(data []byte) error {
	if len(data) < 42 {
		return fmt.Errorf("packet too short")
	}
	
	messageCode := binary.BigEndian.Uint16(data[18:20])
	
	if messageCode == 7211 {
		return p.process7211(data)
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7211) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7211); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7211")
}

func (p *Processor7211) process7211(data []byte) error {
	if len(data) < 42 {
		return fmt.Errorf("packet too short")
	}
	
	noOfRecords := binary.BigEndian.Uint16(data[40:42])
	if noOfRecords == 0 || noOfRecords > 5 {
		return fmt.Errorf("invalid record count")
	}
	
	offset := 42
	for i := 0; i < int(noOfRecords); i++ {
		if offset+204 > len(data) {
			break
		}
		recordData := data[offset : offset+204]
		if msg := p.parseMessage7211(recordData); msg != nil {
			p.ExportData(msg)
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 204
	}
	return nil
}

func (p *Processor7211) parseMessage7211(data []byte) *Message7211 {
	if len(data) < 204 {
		return nil
	}

	return &Message7211{
		Token1:              int32(binary.BigEndian.Uint32(data[0:4])),
		Token2:              int32(binary.BigEndian.Uint32(data[4:8])),
		MbpBuy:              int16(binary.BigEndian.Uint16(data[8:10])),
		MbpSell:             int16(binary.BigEndian.Uint16(data[10:12])),
		LastActiveTime:      int32(binary.BigEndian.Uint32(data[12:16])),
		TradedVolume:        binary.BigEndian.Uint32(data[16:20]),
		TotalTradedValue:    float64FromBytes(data[20:28]),
		// Skip MbpBuys[5] at offset 28-77 and MbpSells[5] at offset 78-127
		TotalBuyVolume:      float64FromBytes(data[128:136]),
		TotalSellVolume:     float64FromBytes(data[136:144]),
		OpenPriceDifference: int32(binary.BigEndian.Uint32(data[144:148])),
		DayHighPriceDifference: int32(binary.BigEndian.Uint32(data[148:152])),
		DayLowPriceDifference:  int32(binary.BigEndian.Uint32(data[152:156])),
		LastTradedPriceDifference: int32(binary.BigEndian.Uint32(data[156:160])),
		LastUpdateTime:      int32(binary.BigEndian.Uint32(data[160:164])),
	}
}

func (p *Processor7211) exportToCSV(msg *Message7211) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token1),
		fmt.Sprintf("%d", msg.Token2),
		fmt.Sprintf("%d", msg.MbpBuy),
		fmt.Sprintf("%d", msg.MbpSell),
		fmt.Sprintf("%d", msg.LastActiveTime),
		fmt.Sprintf("%d", msg.TradedVolume),
		fmt.Sprintf("%.2f", msg.TotalTradedValue),
		fmt.Sprintf("%.2f", msg.TotalBuyVolume),
		fmt.Sprintf("%.2f", msg.TotalSellVolume),
		fmt.Sprintf("%.2f", float64(msg.OpenPriceDifference)/100.0),
		fmt.Sprintf("%.2f", float64(msg.DayHighPriceDifference)/100.0),
		fmt.Sprintf("%.2f", float64(msg.DayLowPriceDifference)/100.0),
		fmt.Sprintf("%.2f", float64(msg.LastTradedPriceDifference)/100.0),
		fmt.Sprintf("%d", msg.LastUpdateTime),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7211) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7211) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7211) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7211) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7220 - BCAST_LIMIT_PRICE_PROTECTION_RANGE (Limit Price Protection)
// =============================================================================

type Processor7220 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7220() *Processor7220 {
	return &Processor7220{
		messageCode: 7220,
		messageName: "BCAST_LIMIT_PRICE_PROTECTION_RANGE",
	}
}

func (p *Processor7220) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7220_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "TokenNumber", "HighExecBand", "LowExecBand",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7220: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 7220 processing (class method)
func (p *Processor7220) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7220 packets
func (p *Processor7220) ParsePacket(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("packet too short")
	}
	
	// Check message code first
	messageCode := binary.BigEndian.Uint16(data[18:20])
	if messageCode != 7220 {
		return fmt.Errorf("not a 7220 message: %d", messageCode)
	}
	
	// Variable number of records, each 12 bytes
	dataLength := len(data) - 40
	if dataLength%12 != 0 {
		return fmt.Errorf("invalid packet size")
	}
	
	numRecords := dataLength / 12
	if numRecords == 0 || numRecords > 25 {
		return fmt.Errorf("invalid record count: %d", numRecords)
	}
	
	offset := 40
	for i := 0; i < numRecords; i++ {
		if offset+12 > len(data) {
			break
		}
		recordData := data[offset : offset+12]
		if msg := p.parseMessage7220(recordData); msg != nil {
			p.ExportData(msg)
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 12
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7220) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7220); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7220")
}

func (p *Processor7220) parseMessage7220(data []byte) *Message7220 {
	if len(data) < 12 {
		return nil
	}

	return &Message7220{
		TokenNumber:  int32(binary.BigEndian.Uint32(data[0:4])),
		HighExecBand: int32(binary.BigEndian.Uint32(data[4:8])),
		LowExecBand:  int32(binary.BigEndian.Uint32(data[8:12])),
	}
}

func (p *Processor7220) exportToCSV(msg *Message7220) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.TokenNumber),
		fmt.Sprintf("%.2f", float64(msg.HighExecBand)/100.0),
		fmt.Sprintf("%.2f", float64(msg.LowExecBand)/100.0),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7220) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7220) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7220) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7220) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 7340 - BCAST_SEC_MSTR_CHNG_PERIODIC (Security Master Change)
// =============================================================================

type Processor7340 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	debugFile    *os.File
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor7340() *Processor7340 {
	return &Processor7340{
		messageCode: 7340,
		messageName: "BCAST_SEC_MSTR_CHNG_PERIODIC",
	}
}

func (p *Processor7340) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_7340_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	// Create debug file
	debugFilename := filepath.Join("csv_output", fmt.Sprintf("debug_7340_%s.txt", timestamp))
	debugFile, err := os.Create(debugFilename)
	if err != nil {
		return err
	}
	p.debugFile = debugFile
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "Symbol", "Series", 
		"InstrumentName", "ExpiryDate", "StrikePrice", "OptionType",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 7340: %s\n", filename)
	fmt.Printf("📁 Created Debug file for Message 7340: %s\n", debugFilename)
	return nil
}

// writeDebugToFile - Write debug messages to text file with timestamp
func (p *Processor7340) writeDebugToFile(message string) {
	if p.debugFile != nil {
		p.mutex.Lock()
		defer p.mutex.Unlock()
		timestamp := time.Now().Format("2006-01-02 15:04:05.000")
		debugLine := fmt.Sprintf("[%s] %s\n", timestamp, message)
		p.debugFile.WriteString(debugLine)
		p.debugFile.Sync() // Flush to disk immediately
	}
}

// ProcessMessage - Main entry point for Message 7340 processing (class method)
func (p *Processor7340) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 7340 packets
func (p *Processor7340) ParsePacket(data []byte) error {
	if len(data) < 42 {
		return fmt.Errorf("packet too short for 7340")
	}
	
	// Check message code first
	messageCode := binary.BigEndian.Uint16(data[18:20])
	if messageCode != 7340 {
		return fmt.Errorf("not a 7340 message: %d", messageCode)
	}
	

	
	// Parse 7340 records - assume single 298-byte record after 40-byte header
	p.writeDebugToFile(fmt.Sprintf("Debug: Processing 7340 message %d", len(data)))
	if len(data) >= 40+298 {
		recordData := data[40 : 40+298]
		p.writeDebugToFile(fmt.Sprintf("Debug: 7340 record data length %d", len(recordData)))
		msg := p.parseMessage7340(recordData)
		p.writeDebugToFile(fmt.Sprintf("Debug: Parsed 7340 message %+v", msg))
		if msg != nil {
			p.ExportData(msg)
			atomic.AddInt64(&p.parsedCount, 1)
		}
	}
	
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor7340) ExportData(data interface{}) error {
	if msg, ok := data.(*Message7340); ok {
		// Update in-memory security master for 7208 integration
		info := &SecurityInfo{
			Token:       msg.Token,
			Symbol:      strings.TrimSpace(string(msg.Symbol[:])),
			Instrument:  strings.TrimSpace(string(msg.InstrumentName[:])),
			OptionType:  strings.TrimSpace(string(msg.OptionType[:])),
			StrikePrice: msg.StrikePrice,
			ExpiryDate:  msg.ExpiryDate,
			Series:      strings.TrimSpace(string(msg.Series[:])),
		}
		AddToSecurityMaster(info)
		
		// Export to CSV
		p.exportToCSV(msg)
		
		// Track CSV save for pipeline statistics
		TrackMessageSaved(p.messageCode)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 7340")
}

func (p *Processor7340) parseMessage7340(data []byte) *Message7340 {
	if len(data) < 32 {
		return nil
	}



	msg := &Message7340{
		Token:       binary.BigEndian.Uint32(data[0:4]),
		ExpiryDate:  binary.BigEndian.Uint32(data[16:20]),
		StrikePrice: binary.BigEndian.Uint32(data[20:24]),
	}
	
	copy(msg.Symbol[:], data[4:14])
	copy(msg.Series[:], data[14:16])
	copy(msg.InstrumentName[:], data[24:30])
	copy(msg.OptionType[:], data[30:32])
	

	
	return msg
}

func (p *Processor7340) exportToCSV(msg *Message7340) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token),
		strings.TrimSpace(string(msg.Symbol[:])),
		strings.TrimSpace(string(msg.Series[:])),
		strings.TrimSpace(string(msg.InstrumentName[:])),
		fmt.Sprintf("%d", msg.ExpiryDate),
		fmt.Sprintf("%.2f", float64(msg.StrikePrice)/100.0),
		strings.TrimSpace(string(msg.OptionType[:])),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor7340) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor7340) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.debugFile != nil {
		p.debugFile.Close()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor7340) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor7340) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// PROCESSOR 17202 - BCAST_ENHNCD_TICKER_AND_MKT_INDEX (Enhanced Ticker)
// =============================================================================

type Processor17202 struct {
	messageCode  uint16
	messageName  string
	csvFile      *os.File
	csvWriter    *csv.Writer
	mutex        sync.Mutex
	parsedCount  int64
}

func NewProcessor17202() *Processor17202 {
	return &Processor17202{
		messageCode: 17202,
		messageName: "BCAST_ENHNCD_TICKER_AND_MKT_INDEX",
	}
}

func (p *Processor17202) Initialize(timestamp string) error {
	filename := filepath.Join("csv_output", fmt.Sprintf("message_17202_%s.csv", timestamp))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	
	p.csvFile = file
	p.csvWriter = csv.NewWriter(file)
	
	headers := []string{
		"Timestamp", "MessageCode", "Token", "MarketType", "FillPrice",
		"FillVolume", "OpenInterest", "DayHiOI", "DayLoOI",
	}
	p.csvWriter.Write(headers)
	p.csvWriter.Flush()
	
	fmt.Printf("📁 Created CSV file for Message 17202: %s\n", filename)
	return nil
}

// ProcessMessage - Main entry point for Message 17202 processing (class method)
func (p *Processor17202) ProcessMessage(data []byte) error {
	return p.ParsePacket(data)
}

// ParsePacket - Internal class method for parsing Message 17202 packets
func (p *Processor17202) ParsePacket(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("packet too short")
	}
	
	// Check message code first
	messageCode := binary.BigEndian.Uint16(data[18:20])
	if messageCode != 17202 {
		return fmt.Errorf("not a 17202 message: %d", messageCode)
	}
	
	// Variable number of records, each 38 bytes
	dataLength := len(data) - 40
	if dataLength%38 != 0 {
		return fmt.Errorf("invalid packet size")
	}
	
	numRecords := dataLength / 38
	if numRecords == 0 || numRecords > 12 {
		return fmt.Errorf("invalid record count: %d", numRecords)
	}
	
	offset := 40
	for i := 0; i < numRecords; i++ {
		if offset+38 > len(data) {
			break
		}
		recordData := data[offset : offset+38]
		if msg := p.parseMessage17202(recordData); msg != nil {
			p.ExportData(msg)
			atomic.AddInt64(&p.parsedCount, 1)
		}
		offset += 38
	}
	return nil
}

// ExportData - Internal class method for exporting parsed data
func (p *Processor17202) ExportData(data interface{}) error {
	if msg, ok := data.(*Message17202); ok {
		p.exportToCSV(msg)
		return nil
	}
	return fmt.Errorf("invalid data type for processor 17202")
}

func (p *Processor17202) parseMessage17202(data []byte) *Message17202 {
	if len(data) < 38 {
		return nil
	}

	return &Message17202{
		Token:        int32(binary.BigEndian.Uint32(data[0:4])),
		MarketType:   int16(binary.BigEndian.Uint16(data[4:6])),
		FillPrice:    int32(binary.BigEndian.Uint32(data[6:10])),
		FillVolume:   int32(binary.BigEndian.Uint32(data[10:14])),
		OpenInterest: int64(binary.BigEndian.Uint64(data[14:22])),
		DayHiOI:      int64(binary.BigEndian.Uint64(data[22:30])), // 8 bytes - Day high open interest (LONG LONG)
		DayLoOI:      int64(binary.BigEndian.Uint64(data[30:38])), // 8 bytes - Day low open interest (LONG LONG)
		// Total: 38 bytes per record
	}
}

func (p *Processor17202) exportToCSV(msg *Message17202) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	record := []string{
		time.Now().Format("2006-01-02 15:04:05.000"),
		fmt.Sprintf("%d", p.messageCode),
		fmt.Sprintf("%d", msg.Token),
		fmt.Sprintf("%d", msg.MarketType),
		fmt.Sprintf("%.2f", float64(msg.FillPrice)/100.0),
		fmt.Sprintf("%d", msg.FillVolume),
		fmt.Sprintf("%d", msg.OpenInterest),
		fmt.Sprintf("%d", msg.DayHiOI),
		fmt.Sprintf("%d", msg.DayLoOI),
	}

	p.csvWriter.Write(record)
	p.csvWriter.Flush()
}

func (p *Processor17202) GetParsedCount() int64 {
	return atomic.LoadInt64(&p.parsedCount)
}

func (p *Processor17202) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	
	if p.csvWriter != nil {
		p.csvWriter.Flush()
	}
	if p.csvFile != nil {
		return p.csvFile.Close()
	}
	return nil
}

func (p *Processor17202) GetMessageCode() uint16 {
	return p.messageCode
}

func (p *Processor17202) GetMessageName() string {
	return p.messageName
}

// =============================================================================
// MESSAGE FACTORY - Factory pattern for creating and managing processor classes
// =============================================================================

type MessageFactory struct {
	processors map[uint16]MessageProcessor
	startTime  time.Time
}

func NewMessageFactory() *MessageFactory {
	return &MessageFactory{
		processors: make(map[uint16]MessageProcessor),
		startTime:  time.Now(),
	}
}

// RegisterProcessorClass - Register a processor class instance
func (f *MessageFactory) RegisterProcessorClass(processor MessageProcessor) error {
	timestamp := f.startTime.Format("20060102_150405")
	if err := processor.Initialize(timestamp); err != nil {
		return err
	}
	
	f.processors[processor.GetMessageCode()] = processor
	fmt.Printf("✅ Registered processor class %d (%s)\n", 
		processor.GetMessageCode(), processor.GetMessageName())
	return nil
}

// ProcessMessageByCode - Factory method to process message using appropriate class
func (f *MessageFactory) ProcessMessageByCode(data []byte) error {
	if len(data) < 20 {
		return fmt.Errorf("packet too short")
	}
	
	messageCode := binary.BigEndian.Uint16(data[18:20])
	
	// Handle special case for 17201 (use 7201 processor class)
	if messageCode == 17201 {
		if processor, exists := f.processors[7201]; exists {
			return processor.ProcessMessage(data)
		}
	}
	
	// Find and execute the appropriate processor class
	if processor, exists := f.processors[messageCode]; exists {
		err := processor.ProcessMessage(data)
		if err == nil {
			// Stage 3: Successfully processed by processor class
			TrackMessageProcessed(messageCode)
		}
		return err
	}
	
	return nil // Message code not registered - ignore silently
}

// DestroyAllClasses - Clean shutdown of all processor classes
func (f *MessageFactory) DestroyAllClasses() {
	for _, processor := range f.processors {
		processor.Close()
		fmt.Printf("✅ Processor class %d (%s) destroyed - %d records processed\n",
			processor.GetMessageCode(),
			processor.GetMessageName(),
			processor.GetParsedCount())
	}
}

// GetTotalProcessedCount - Get total count from all processor classes
func (f *MessageFactory) GetTotalProcessedCount() int64 {
	total := int64(0)
	for _, processor := range f.processors {
		total += processor.GetParsedCount()
	}
	return total
}

// GetAllProcessors - Get all registered processor classes (for statistics)
func (f *MessageFactory) GetAllProcessors() map[uint16]MessageProcessor {
	return f.processors
}

// =============================================================================
// PIPELINE TRACKING FUNCTIONS
// =============================================================================

// TrackMessageProcessed - Track when a message code is successfully processed
func TrackMessageProcessed(messageCode uint16) {
	if processedMessageCodes != nil {
		processedMessageCodes[messageCode]++
	}
}

// TrackMessageSaved - Track when a message code record is saved to CSV
func TrackMessageSaved(messageCode uint16) {
	if savedMessageCodes != nil {
		savedMessageCodes[messageCode]++
	}
}

// PrintPipelineStats - Print real-time pipeline statistics
func PrintPipelineStats() {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("📊 REAL-TIME MESSAGE PROCESSING PIPELINE ANALYSIS")
	fmt.Println(strings.Repeat("=", 80))
	
	// Get all unique message codes
	allCodes := make(map[uint16]bool)
	for code := range receivedMessageCodes {
		allCodes[code] = true
	}
	for code := range compressedMessageCodes {
		allCodes[code] = true
	}
	for code := range processedMessageCodes {
		allCodes[code] = true
	}
	for code := range savedMessageCodes {
		allCodes[code] = true
	}
	
	fmt.Printf("%-8s %-12s %-12s %-12s %-12s %-30s\n", 
		"Code", "Received", "Compressed", "Processed", "Saved CSV", "Message Name")
	fmt.Println(strings.Repeat("-", 80))
	
	codeNames := map[uint16]string{
		6541:  "BC_CIRCUIT_CHECK",
		7200:  "BCAST_MBO_MBP_UPDATE", 
		7201:  "BCAST_MW_ROUND_ROBIN",
		7202:  "BCAST_TICKER_AND_MKT_INDEX",
		7208:  "BCAST_ONLY_MBP",
		7211:  "BCAST_SPD_MBP_DELTA",
		7220:  "BCAST_LIMIT_PRICE_PROTECTION_RANGE",
		7340:  "BCAST_SEC_MSTR_CHNG_PERIODIC",
		17130: "UNKNOWN",
		17201: "BCAST_ENHNCD_MW_ROUND_ROBIN",
		17202: "BCAST_ENHNCD_TICKER_AND_MKT_INDEX",
	}
	
	for code := range allCodes {
		name := codeNames[code]
		if name == "" {
			name = "UNKNOWN"
		}
		
		received := receivedMessageCodes[code]
		compressed := compressedMessageCodes[code]
		processed := processedMessageCodes[code]
		saved := savedMessageCodes[code]
		
		fmt.Printf("%-8d %-12d %-12d %-12d %-12d %-30s\n",
			code, received, compressed, processed, saved, name)
	}
	
	// Summary totals
	fmt.Println(strings.Repeat("-", 80))
	totalReceived := int64(0)
	totalCompressed := int64(0)
	totalProcessed := int64(0)
	totalSaved := int64(0)
	
	for _, count := range receivedMessageCodes {
		totalReceived += count
	}
	for _, count := range compressedMessageCodes {
		totalCompressed += count
	}
	for _, count := range processedMessageCodes {
		totalProcessed += count
	}
	for _, count := range savedMessageCodes {
		totalSaved += count
	}
	
	fmt.Printf("%-8s %-12d %-12d %-12d %-12d %-30s\n",
		"TOTAL", totalReceived, totalCompressed, totalProcessed, totalSaved, "All Messages")
	
	// Calculate pipeline efficiency
	fmt.Println("\n📈 PIPELINE EFFICIENCY:")
	if totalReceived > 0 {
		processRate := float64(totalProcessed) / float64(totalReceived) * 100
		saveRate := float64(totalSaved) / float64(totalReceived) * 100
		fmt.Printf("  Processing Success Rate: %.1f%% (%d/%d)\n", processRate, totalProcessed, totalReceived)
		fmt.Printf("  CSV Save Success Rate:   %.1f%% (%d/%d)\n", saveRate, totalSaved, totalReceived)
	}
	
	fmt.Println(strings.Repeat("=", 80))
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
	
	// Pipeline tracking maps
	receivedMessageCodes   map[uint16]int64  // Stage 1: Raw message codes received
	compressedMessageCodes map[uint16]int64  // Stage 2: Message codes in compressed packets
	processedMessageCodes  map[uint16]int64  // Stage 3: Message codes successfully processed
	savedMessageCodes      map[uint16]int64  // Stage 4: Message codes saved to CSV
	
	messageCodes        map[uint16]int64     // Legacy counter (same as receivedMessageCodes)
	tokenStats          map[uint32]int64     // Track token frequency
	tokenRanges         map[string]int64     // Track token range statistics
	tokensWithBids      int64                // Count tokens with bid data
	tokensWithAsks      int64                // Count tokens with ask data
	tokensWithBoth      int64                // Count tokens with both bid and ask
	minToken            uint32 = 4294967295 // Start with max value
	maxToken            uint32 = 0          // Start with min value
	startTime           time.Time
	shutdownChan        chan bool
	packetChan          chan []byte
	messageFactory      *MessageFactory     // Factory for managing processor classes
)

// =============================================================================
// MAIN PROGRAM
// =============================================================================

func main() {
	fmt.Println("================================================================================")
	fmt.Println("NSE MULTICAST UDP RECEIVER - OBJECT-ORIENTED CLASS ARCHITECTURE")
	fmt.Println("================================================================================")
	fmt.Println("🏭 Factory Pattern: Each message code has a dedicated processor class")
	fmt.Println("📦 Complete OOP: All processing logic encapsulated within classes")  
	fmt.Println("🎯 Single Responsibility: Each class handles one message type only")
	fmt.Println("================================================================================\n")

	startTime = time.Now()
	shutdownChan = make(chan bool)
	packetChan = make(chan []byte, 100)
	
	// Initialize pipeline tracking maps
	receivedMessageCodes = make(map[uint16]int64)
	compressedMessageCodes = make(map[uint16]int64)
	processedMessageCodes = make(map[uint16]int64)
	savedMessageCodes = make(map[uint16]int64)
	
	messageCodes = make(map[uint16]int64)
	tokenStats = make(map[uint32]int64)
	tokenRanges = make(map[string]int64)

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		fmt.Printf("⚠️  Warning: Could not create csv_output directory: %v\n", err)
	}

	// Initialize message factory and register processor classes
	messageFactory = NewMessageFactory()
	
	fmt.Println("🏭 INITIALIZING PROCESSOR CLASSES")
	fmt.Println(strings.Repeat("-", 50))
	
	// Create and register ALL processor classes - each class handles complete processing
	processorClasses := []MessageProcessor{
		NewProcessor6541(),   // BC_CIRCUIT_CHECK class
		NewProcessor7200(),   // BCAST_MBO_MBP_UPDATE class
		NewProcessor7201(),   // BCAST_MW_ROUND_ROBIN class  
		NewProcessor7202(),   // BCAST_TICKER_AND_MKT_INDEX class
		NewProcessor7208(),   // BCAST_ONLY_MBP class (Most Important)
		NewProcessor7211(),   // BCAST_SPD_MBP_DELTA class
		NewProcessor7220(),   // BCAST_LIMIT_PRICE_PROTECTION_RANGE class
		NewProcessor7340(),   // BCAST_SEC_MSTR_CHNG_PERIODIC class
		NewProcessor17202(),  // BCAST_ENHNCD_TICKER_AND_MKT_INDEX class
	}
	
	for _, processorClass := range processorClasses {
		if err := messageFactory.RegisterProcessorClass(processorClass); err != nil {
			fmt.Printf("❌ Failed to register processor class %d: %v\n", processorClass.GetMessageCode(), err)
			return
		}
	}
	
	fmt.Println()

	// Setup signal handler (Ctrl+C)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start packet processor
	go processPackets()

	// Start UDP listener
	go startUDPListener()
	
	// Start real-time pipeline statistics display (every 10 seconds)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				PrintPipelineStats()
			case <-shutdownChan:
				return
			}
		}
	}()

	// Wait for Ctrl+C
	<-sigChan
	fmt.Println("\n\n🛑 Shutdown signal received, stopping...")

	close(shutdownChan)
	time.Sleep(1 * time.Second)

	// Destroy all processor classes
	messageFactory.DestroyAllClasses()

	// Print final statistics
	printFinalStats()
}

// =============================================================================
// UDP LISTENER
// =============================================================================

func startUDPListener() {
	multicastIP := "233.1.2.5"
	//multicastIP := "231.31.31.4"
	//port := 55655
	port := 34330
	//port := 18901
	
	// Create multicast address (using same method as parent udp_receiver.go)
	addr := &net.UDPAddr{
		IP:   net.ParseIP(multicastIP),
		Port: port,
	}

	// Join multicast group
	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		fmt.Printf("❌ Failed to join multicast group: %v\n", err)
		fmt.Println("\nTroubleshooting:")
		fmt.Println("  1. Check if parent directory's nse_receiver.exe works")
		fmt.Println("  2. Ensure NSE market is open (9:15 AM - 3:30 PM IST)")
		fmt.Println("  3. Check firewall settings")
		fmt.Println("  4. Verify network connection")
		return
	}
	defer conn.Close()

	// Set read buffer size
	conn.SetReadBuffer(2 * 1024 * 1024)

	fmt.Printf("✅ Joined multicast group: %s:%d\n", multicastIP, port)
	fmt.Println("📊 Receiving packets, decoding & exporting to CSV...")
	fmt.Println("⏹️  Press Ctrl+C to stop\n")

	buffer := make([]byte, 2048)

	for {
		select {
		case <-shutdownChan:
			return
		default:
			// Read UDP packet
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				continue
			}

			// Update counters
			atomic.AddInt64(&packetCount, 1)
			atomic.AddInt64(&totalBytes, int64(n))

			_ = remoteAddr // Avoid unused variable warning

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
// LZO1Z DECOMPRESSOR
// =============================================================================
// NOTE: DecompressUltra() is imported from lzo_decompressor_safe.go
// (The parent's proven, bounds-checked implementation)

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// Helper function to convert 8 bytes to float64 (DOUBLE)
func float64FromBytes(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	return math.Float64frombits(bits)
}

// Helper function to categorize NSE token ranges
func getTokenRange(token uint32) string {
	switch {
	case token >= 1 && token <= 9999:
		return "Equity (Early Range)"
	case token >= 10000 && token <= 39999:
		return "Options/Equity (Mid Range)"
	case token >= 40000 && token <= 49999:
		return "Futures (Primary Range)"
	case token >= 50000 && token <= 89999:
		return "Equity/Options (High Range)"
	case token >= 90000 && token <= 99925:
		return "Special Securities"
	case token >= 99926 && token <= 99999:
		return "Indices"
	case token >= 100000:
		return "Extended Range"
	default:
		return "Unknown Range"
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

	// 🔍 PIPELINE TRACKING - Stage by Stage
	// Stage 1: Message Code Received
	if receivedMessageCodes != nil {
		receivedMessageCodes[messageCode]++
	}
	
	// Stage 2: Message Code in Compressed Packet (if applicable)
	if isCompressed && compressedMessageCodes != nil {
		compressedMessageCodes[messageCode]++
	}

	// Legacy counter (for backward compatibility)
	if messageCodes != nil {
		messageCodes[messageCode]++
	}

	// Stage 3: Message Code Processing (will be tracked in factory)
	// Use message factory to process packet with appropriate processor class
	messageFactory.ProcessMessageByCode(finalData)
}

// =============================================================================
// STATISTICS
// =============================================================================

func printFinalStats() {
	duration := time.Since(startTime)
	finalPacketCount := atomic.LoadInt64(&packetCount)
	finalTotalBytes := atomic.LoadInt64(&totalBytes)
	finalCompressed := atomic.LoadInt64(&compressedCount)
	finalDecompressed := atomic.LoadInt64(&decompressedCount)
	finalErrors := atomic.LoadInt64(&decompressionErrors)

	fmt.Println("\n")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println("FINAL STATISTICS - DECOMPRESSION TEST")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()

	fmt.Println("📊 LISTENER PERFORMANCE")
	fmt.Printf("  Runtime:              %v\n", duration)
	fmt.Printf("  Total Packets:        %d\n", finalPacketCount)
	fmt.Printf("  Total Bytes:          %d (%.2f MB)\n", finalTotalBytes, float64(finalTotalBytes)/(1024*1024))
	
	if duration.Seconds() > 0 {
		fmt.Printf("  Avg Packet Rate:      %.2f packets/sec\n", float64(finalPacketCount)/duration.Seconds())
		fmt.Printf("  Avg Data Rate:        %.2f KB/sec\n", float64(finalTotalBytes)/duration.Seconds()/1024)
	}
	
	if finalPacketCount > 0 {
		fmt.Printf("  Avg Packet Size:      %d bytes\n", finalTotalBytes/finalPacketCount)
	}

	fmt.Println()
	fmt.Println("📦 DECOMPRESSION STATISTICS")
	fmt.Printf("  Compressed Packets:   %d (%.1f%%)\n", finalCompressed, float64(finalCompressed)/float64(finalPacketCount)*100)
	fmt.Printf("  Decompressed OK:      %d\n", finalDecompressed)
	fmt.Printf("  Decompression Errors: %d\n", finalErrors)
	
	if finalCompressed > 0 {
		fmt.Printf("  Success Rate:         %.1f%%\n", float64(finalDecompressed)/float64(finalCompressed)*100)
	}

	fmt.Println()
	fmt.Printf("📋 MESSAGE CODES DETECTED (%d unique)\n", len(messageCodes))
	fmt.Println(strings.Repeat("-", 80))
	
	if len(messageCodes) > 0 {
		fmt.Printf("%-8s %-40s %10s\n", "Code", "Name", "Received")
		fmt.Println(strings.Repeat("-", 80))
		
		// Message code names - Complete NSE NNF Protocol v9.46
		codeNames := map[uint16]string{
			6541:  "BC_CIRCUIT_CHECK",
			7200:  "BCAST_MBO_MBP_UPDATE",
			7201:  "BCAST_MW_ROUND_ROBIN",
			7202:  "BCAST_TICKER_AND_MKT_INDEX",
			7208:  "BCAST_ONLY_MBP",
			7211:  "BCAST_SPD_MBP_DELTA",

			7220:  "BCAST_LIMIT_PRICE_PROTECTION_RANGE",
			7340:  "BCAST_SEC_MSTR_CHNG_PERIODIC",
			17201: "BCAST_ENHNCD_MW_ROUND_ROBIN",
			17202: "BCAST_ENHNCD_TICKER_AND_MKT_INDEX",
		}
		
		for code := uint16(6500); code <= 17202; code++ {
			if count, exists := messageCodes[code]; exists {
				name := codeNames[code]
				if name == "" {
					name = "UNKNOWN"
				}
				fmt.Printf("%-8d %-40s %10d\n", code, name, count)
			}
		}
	}

	fmt.Println()
	fmt.Println("� PARSED MESSAGES BY PROCESSOR")
	fmt.Println(strings.Repeat("-", 80))
	
	totalParsed := messageFactory.GetTotalProcessedCount()
	for _, processorClass := range messageFactory.GetAllProcessors() {
		count := processorClass.GetParsedCount()
		percentage := float64(0)
		if totalParsed > 0 {
			percentage = float64(count) / float64(totalParsed) * 100
		}
		fmt.Printf("  %5d %-35s: %10d records (%.1f%%)\n",
			processorClass.GetMessageCode(),
			processorClass.GetMessageName(),
			count,
			percentage)
	}
	
	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("  TOTAL PARSED:                           %10d records\n", totalParsed)

	// Bid/Ask Statistics
	fmt.Println()
	fmt.Println("📈 BID/ASK STATISTICS")
	fmt.Println(strings.Repeat("-", 80))
	totalBidAsk := atomic.LoadInt64(&tokensWithBids) + atomic.LoadInt64(&tokensWithAsks) + atomic.LoadInt64(&tokensWithBoth)
	fmt.Printf("  Tokens with Bids only:   %8d\n", atomic.LoadInt64(&tokensWithBids))
	fmt.Printf("  Tokens with Asks only:   %8d\n", atomic.LoadInt64(&tokensWithAsks))
	fmt.Printf("  Tokens with Both:        %8d\n", atomic.LoadInt64(&tokensWithBoth))
	fmt.Printf("  Total with Bid/Ask:      %8d\n", totalBidAsk)

	// Token Statistics for Message 7208 (BCAST_ONLY_MBP)
	if len(tokenStats) > 0 {
		fmt.Println()
		fmt.Println("🎯 TOKEN STATISTICS - MESSAGE 7208 (BCAST_ONLY_MBP)")
		fmt.Println(strings.Repeat("-", 80))
		fmt.Printf("  Total Unique Tokens:     %d\n", len(tokenStats))
		fmt.Printf("  Token Range:             %d - %d\n", minToken, maxToken)
		fmt.Printf("  Range Span:              %d tokens\n", maxToken-minToken+1)
		
		fmt.Println()
		fmt.Println("📊 TOKEN DISTRIBUTION BY RANGE:")
		for rangeType, count := range tokenRanges {
			percentage := float64(count) / float64(len(tokenStats)) * 100
			fmt.Printf("  %-25s: %8d tokens (%.1f%%)\n", rangeType, count, percentage)
		}
		
		// Show top 10 most frequent tokens
		fmt.Println()
		fmt.Println("🔥 TOP 10 MOST FREQUENT TOKENS:")
		type tokenFreq struct {
			token uint32
			count int64
		}
		
		var tokenList []tokenFreq
		for token, count := range tokenStats {
			tokenList = append(tokenList, tokenFreq{token, count})
		}
		
		// Simple sort (top 10)
		for i := 0; i < len(tokenList); i++ {
			for j := i + 1; j < len(tokenList); j++ {
				if tokenList[j].count > tokenList[i].count {
					tokenList[i], tokenList[j] = tokenList[j], tokenList[i]
				}
			}
		}
		
		limit := 10
		if len(tokenList) < 10 {
			limit = len(tokenList)
		}
		
		for i := 0; i < limit; i++ {
			token := tokenList[i].token
			count := tokenList[i].count
			rangeType := getTokenRange(token)
			fmt.Printf("  #%2d Token %6d: %8d occurrences (%s)\n", i+1, token, count, rangeType)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()
	
	if finalPacketCount == 0 {
		fmt.Println("⚠️  WARNING: No packets received!")
		fmt.Println("   Possible reasons:")
		fmt.Println("   - NSE multicast feed not available")
		fmt.Println("   - Firewall blocking UDP multicast")
		fmt.Println("   - Not connected to correct network")
		fmt.Println("   - NSE market hours (9:15 AM - 3:30 PM IST)")
	} else if finalDecompressed == 0 && finalCompressed > 0 {
		fmt.Println("⚠️  WARNING: Decompression failing!")
		fmt.Println("   All compressed packets failed to decompress")
		fmt.Println("   This may indicate incorrect LZO implementation")
	} else {
		fmt.Println("✅ CLASS-BASED ARCHITECTURE WORKING!")
		fmt.Println("   ✅ Separate processor for each message code")
		fmt.Println("   ✅ UDP Multicast listener")
		fmt.Println("   ✅ LZO decompression")
		fmt.Println("   ✅ Message parsing with dedicated processors")
		fmt.Printf("   ✅ CSV export (%d total records)\n", totalParsed)
	}

	fmt.Println(strings.Repeat("=", 80))
}

// MBP_INFORMATION - Market by Price information for bid/ask data
// Each record is 12 bytes, total 10 records = 120 bytes (offset 56-175 in INTERACTIVE_ONLY_MBP_DATA)
type MBP_INFORMATION struct {
	Quantity        uint32  // 4 bytes - Order quantity
	Price          uint32  // 4 bytes - Bid/Ask price (divide by 100 for actual price)
	NumberOfOrders uint16  // 2 bytes - Number of orders at this price level
	BbBuySellFlag  uint16  // 2 bytes - Buy/Sell flag (1=Buy, 2=Sell)
}

// ParseMBPInformation - Extract bid/ask data from MBP_INFORMATION buffer
// Returns best bid, best ask, and all price levels
func ParseMBPInformation(data []byte, recordStart int) (bestBid, bestAsk float64, bidLevels, askLevels []MBP_INFORMATION) {
	if len(data) < recordStart+176 {
		return 0, 0, nil, nil
	}
	
	// Parse 10 MBP_INFORMATION records at offset 56-175 (relative to record start)
	mbpStart := recordStart + 56  // MBP_INFORMATION starts at record offset 56
	
	for i := 0; i < 10; i++ {
		offset := mbpStart + (i * 12)  // Each record is 12 bytes
		if offset+12 > len(data) {
			break
		}
		
		mbp := MBP_INFORMATION{
			Quantity:        binary.BigEndian.Uint32(data[offset:offset+4]),
			Price:          binary.BigEndian.Uint32(data[offset+4:offset+8]),
			NumberOfOrders: binary.BigEndian.Uint16(data[offset+8:offset+10]),
			BbBuySellFlag:  binary.BigEndian.Uint16(data[offset+10:offset+12]),
		}
		

		
		// Skip empty records
		if mbp.Price == 0 || mbp.Quantity == 0 {
			continue
		}
		
		price := float64(mbp.Price) / 100.0  // Convert from paise to rupees
		

		
		// NSE NNF spec might use different values - let's try multiple interpretations
		if mbp.BbBuySellFlag == 1 || mbp.BbBuySellFlag == 66 {  // Buy order (Bid) - try 'B' = 66
			bidLevels = append(bidLevels, mbp)
			if bestBid == 0 || price > bestBid {
				bestBid = price  // Highest bid is best bid
			}
		} else if mbp.BbBuySellFlag == 2 || mbp.BbBuySellFlag == 83 {  // Sell order (Ask) - try 'S' = 83
			askLevels = append(askLevels, mbp)
			if bestAsk == 0 || price < bestAsk {
				bestAsk = price  // Lowest ask is best ask
			}
		} else if mbp.BbBuySellFlag == 0 {
			// For Flag=0, let's assume it's bid/ask based on position (first 5 = bids, last 5 = asks)
			if i < 5 {  // First 5 records are typically bids
				bidLevels = append(bidLevels, mbp)
				if bestBid == 0 || price > bestBid {
					bestBid = price  
				}
			} else {  // Last 5 records are typically asks
				askLevels = append(askLevels, mbp)
				if bestAsk == 0 || price < bestAsk {
					bestAsk = price  
				}
			}
		}
	}
	

	
	return bestBid, bestAsk, bidLevels, askLevels
}

// ValidateMessage7208Data - Verify that parsed data makes sense
func ValidateMessage7208Data(msg *Message7208) bool {
	// Basic sanity checks for market data
	if msg.Token == 0 {
		return false  // Token should never be 0
	}
	
	// LTP should be reasonable (not 0, not extremely high)
	ltp := float64(msg.LastTradedPrice) / 100.0
	if ltp <= 0 || ltp > 1000000 {  // Max 10 lakh rupees
		return false
	}
	
	// Volume should be non-negative
	if msg.VolumeTradedToday < 0 {
		return false
	}
	
	// OHLC prices should make sense relative to LTP
	if msg.OpenPrice > 0 && msg.HighPrice > 0 && msg.LowPrice > 0 {
		open := float64(msg.OpenPrice) / 100.0
		high := float64(msg.HighPrice) / 100.0
		low := float64(msg.LowPrice) / 100.0
		
		// High should be >= Low, and LTP should be between High and Low
		// Also validate that Open price is reasonable relative to High/Low
		if high < low || ltp < low || ltp > high || open < low || open > high {
			fmt.Printf("⚠️  Data validation warning for token %d: LTP=%.2f, O=%.2f, H=%.2f, L=%.2f\n", 
				msg.Token, ltp, open, high, low)
		}
	}
	
	return true
}
