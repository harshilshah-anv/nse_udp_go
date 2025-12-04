package main

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

// MS_BCAST_ONLY_MBP - Structure for message code 7208
// Packet Length: 470 bytes
// Transaction Code: BCAST_ONLY_MBP (7208)
type MS_BCAST_ONLY_MBP struct {
	BcastHeader              BcastHeader                     // 40 bytes, offset 0
	NoOfRecords             uint16                          // 2 bytes, offset 40
	InteractiveOnlyMBPData  [2]INTERACTIVE_ONLY_MBP_DATA   // 214 bytes each, offset 42
}

// INTERACTIVE_ONLY_MBP_DATA - Structure for market by price data
// Packet Length: 214 bytes
type INTERACTIVE_ONLY_MBP_DATA struct {
	Token                           uint32          // 4 bytes, offset 0
	BookType                        uint16          // 2 bytes, offset 4
	TradingStatus                   uint16          // 2 bytes, offset 6
	VolumeTradedToday              uint32          // 4 bytes, offset 8
	LastTradedPrice                uint32          // 4 bytes, offset 12
	NetChangeIndicator             byte            // 1 byte, offset 16
	VolTrdTodayExcdIndc            byte            // 1 byte, offset 17
	NetPriceChangeFromClosingPrice uint32          // 4 bytes, offset 18
	LastTradeQuantity              uint32          // 4 bytes, offset 22
	LastTradeTime                  uint32          // 4 bytes, offset 26
	AverageTradePrice              uint32          // 4 bytes, offset 30
	AuctionNumber                  uint16          // 2 bytes, offset 34
	AuctionStatus                  uint16          // 2 bytes, offset 36
	InitiatorType                  uint16          // 2 bytes, offset 38
	InitiatorPrice                 uint32          // 4 bytes, offset 40
	InitiatorQuantity              uint32          // 4 bytes, offset 44
	AuctionPrice                   uint32          // 4 bytes, offset 48
	AuctionQuantity                uint32          // 4 bytes, offset 52
	RecordBuffer                   [10]MBP_INFORMATION // 120 bytes (12*10), offset 56
	BbTotalBuyFlag                 uint16          // 2 bytes, offset 176
	BbTotalSellFlag                uint16          // 2 bytes, offset 178
	TotalBuyQuantity               float64         // 8 bytes, offset 180
	TotalSellQuantity              float64         // 8 bytes, offset 188
	STIndicator                    ST_INDICATOR    // 2 bytes, offset 196
	ClosingPrice                   uint32          // 4 bytes, offset 198
	OpenPrice                      uint32          // 4 bytes, offset 202
	HighPrice                      uint32          // 4 bytes, offset 206
	LowPrice                       uint32          // 4 bytes, offset 210
}

// MBP_INFORMATION - Market By Price Information structure
// Packet Length: 12 bytes
type MBP_INFORMATION struct {
	Quantity        uint32  // 4 bytes, offset 0
	Price          uint32  // 4 bytes, offset 4
	NumberOfOrders uint16  // 2 bytes, offset 8
	BbBuySellFlag  uint16  // 2 bytes, offset 10
}

// BcastHeader - Broadcast header structure (40 bytes)
type BcastHeader struct {
	MessageLength    uint16    // 2 bytes
	MessageSequence  uint32    // 4 bytes
	MessageType      uint16    // 2 bytes
	LogTime          uint64    // 8 bytes
	AlphaChar        [2]byte   // 2 bytes
	TraderId         [5]byte   // 5 bytes
	ErrorCode        uint16    // 2 bytes
	Reserved1        [8]byte   // 8 bytes
	MessageCategory  byte      // 1 byte
	Reserved2        [10]byte  // 10 bytes
}

// ST_INDICATOR - Special Trade Indicator structure
type ST_INDICATOR struct {
	LastTradeMore byte // 1 bit flag
	LastTradeLess byte // 1 bit flag
	// Other flags can be added as needed
	Reserved      uint16 // 2 bytes total for the structure
}

// Trading Status constants
const (
	TRADING_STATUS_PREOPEN          = 1
	TRADING_STATUS_OPEN             = 2
	TRADING_STATUS_SUSPENDED        = 3
	TRADING_STATUS_PREOPEN_EXTENDED = 4
	TRADING_STATUS_PRICE_DISCOVERY  = 6
)

// Net Change Indicator constants
const (
	NET_CHANGE_INCREASE = '+'
	NET_CHANGE_DECREASE = '-'
)

// Book Type constants
const (
	BOOK_TYPE_RL = "RL" // Regular Lot
	BOOK_TYPE_ST = "ST" // Special Terms
	BOOK_TYPE_SL = "SL" // Stop Loss
	BOOK_TYPE_NT = "NT" // Normal Terms
	BOOK_TYPE_OL = "OL" // Odd Lot
	BOOK_TYPE_SP = "SP" // Special Price
)

// ParseMS_BCAST_ONLY_MBP parses message code 7208 from binary data
func ParseMS_BCAST_ONLY_MBP(data []byte) (*MS_BCAST_ONLY_MBP, error) {
	if len(data) < 470 {
		return nil, fmt.Errorf("insufficient data for MS_BCAST_ONLY_MBP: need 470 bytes, got %d", len(data))
	}

	msg := &MS_BCAST_ONLY_MBP{}
	
	// Parse using unsafe pointer for direct memory mapping (big endian)
	offset := 0
	
	// Parse BcastHeader (40 bytes)
	parseBcastHeader(data[offset:offset+40], &msg.BcastHeader)
	offset += 40
	
	// Parse NoOfRecords (2 bytes)
	msg.NoOfRecords = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// Parse INTERACTIVE_ONLY_MBP_DATA array
	for i := 0; i < int(msg.NoOfRecords) && i < 2; i++ {
		parseInteractiveOnlyMBPData(data[offset:offset+214], &msg.InteractiveOnlyMBPData[i])
		offset += 214
	}
	
	return msg, nil
}

// parseBcastHeader parses the broadcast header
func parseBcastHeader(data []byte, header *BcastHeader) {
	offset := 0
	
	header.MessageLength = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	header.MessageSequence = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	header.MessageType = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	header.LogTime = binary.BigEndian.Uint64(data[offset:offset+8])
	offset += 8
	
	copy(header.AlphaChar[:], data[offset:offset+2])
	offset += 2
	
	copy(header.TraderId[:], data[offset:offset+5])
	offset += 5
	
	header.ErrorCode = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	copy(header.Reserved1[:], data[offset:offset+8])
	offset += 8
	
	header.MessageCategory = data[offset]
	offset += 1
	
	copy(header.Reserved2[:], data[offset:offset+10])
}

// parseInteractiveOnlyMBPData parses the main market data structure
func parseInteractiveOnlyMBPData(data []byte, mbpData *INTERACTIVE_ONLY_MBP_DATA) {
	offset := 0
	
	// Token (4 bytes)
	mbpData.Token = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// BookType (2 bytes)
	mbpData.BookType = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// TradingStatus (2 bytes)
	mbpData.TradingStatus = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// VolumeTradedToday (4 bytes)
	mbpData.VolumeTradedToday = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// LastTradedPrice (4 bytes)
	mbpData.LastTradedPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// NetChangeIndicator (1 byte)
	mbpData.NetChangeIndicator = data[offset]
	offset += 1
	
	// VolTrdTodayExcdIndc (1 byte)
	mbpData.VolTrdTodayExcdIndc = data[offset]
	offset += 1
	
	// NetPriceChangeFromClosingPrice (4 bytes)
	mbpData.NetPriceChangeFromClosingPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// LastTradeQuantity (4 bytes)
	mbpData.LastTradeQuantity = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// LastTradeTime (4 bytes)
	mbpData.LastTradeTime = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// AverageTradePrice (4 bytes)
	mbpData.AverageTradePrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// AuctionNumber (2 bytes)
	mbpData.AuctionNumber = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// AuctionStatus (2 bytes)
	mbpData.AuctionStatus = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// InitiatorType (2 bytes)
	mbpData.InitiatorType = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// InitiatorPrice (4 bytes)
	mbpData.InitiatorPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// InitiatorQuantity (4 bytes)
	mbpData.InitiatorQuantity = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// AuctionPrice (4 bytes)
	mbpData.AuctionPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// AuctionQuantity (4 bytes)
	mbpData.AuctionQuantity = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// RecordBuffer - 10 MBP_INFORMATION structures (120 bytes total)
	for i := 0; i < 10; i++ {
		mbpData.RecordBuffer[i].Quantity = binary.BigEndian.Uint32(data[offset:offset+4])
		offset += 4
		
		mbpData.RecordBuffer[i].Price = binary.BigEndian.Uint32(data[offset:offset+4])
		offset += 4
		
		mbpData.RecordBuffer[i].NumberOfOrders = binary.BigEndian.Uint16(data[offset:offset+2])
		offset += 2
		
		mbpData.RecordBuffer[i].BbBuySellFlag = binary.BigEndian.Uint16(data[offset:offset+2])
		offset += 2
	}
	
	// BbTotalBuyFlag (2 bytes)
	mbpData.BbTotalBuyFlag = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// BbTotalSellFlag (2 bytes)
	mbpData.BbTotalSellFlag = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// TotalBuyQuantity (8 bytes - double)
	mbpData.TotalBuyQuantity = float64(binary.BigEndian.Uint64(data[offset:offset+8]))
	offset += 8
	
	// TotalSellQuantity (8 bytes - double)
	mbpData.TotalSellQuantity = float64(binary.BigEndian.Uint64(data[offset:offset+8]))
	offset += 8
	
	// ST_INDICATOR (2 bytes)
	mbpData.STIndicator.Reserved = binary.BigEndian.Uint16(data[offset:offset+2])
	offset += 2
	
	// ClosingPrice (4 bytes)
	mbpData.ClosingPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// OpenPrice (4 bytes)
	mbpData.OpenPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// HighPrice (4 bytes)
	mbpData.HighPrice = binary.BigEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// LowPrice (4 bytes)
	mbpData.LowPrice = binary.BigEndian.Uint32(data[offset:offset+4])
}

// FormatPrice converts NSE price format to human readable price (divide by 100)
func FormatPrice(price uint32) float64 {
	return float64(price) / 100.0
}

// FormatTradingStatus converts trading status code to string
func FormatTradingStatus(status uint16) string {
	switch status {
	case TRADING_STATUS_PREOPEN:
		return "Preopen"
	case TRADING_STATUS_OPEN:
		return "Open"
	case TRADING_STATUS_SUSPENDED:
		return "Suspended"
	case TRADING_STATUS_PREOPEN_EXTENDED:
		return "Preopen Extended"
	case TRADING_STATUS_PRICE_DISCOVERY:
		return "Price Discovery"
	default:
		return fmt.Sprintf("Unknown(%d)", status)
	}
}

// PrintMBPData prints formatted market by price data
func PrintMBPData(mbp *INTERACTIVE_ONLY_MBP_DATA) {
	// Check if this is NIFTY token (37054)
	if mbp.Token == 37054 {
		fmt.Printf("🔥 NIFTY TOKEN 37054 DETECTED! 🔥\n")
		fmt.Printf("📈 NIFTY LTP: %.2f | Volume: %d | Status: %s\n",
			FormatPrice(mbp.LastTradedPrice),
			mbp.VolumeTradedToday,
			FormatTradingStatus(mbp.TradingStatus))
		
		fmt.Printf("📊 NIFTY OHLC - Open: %.2f | High: %.2f | Low: %.2f | Close: %.2f\n",
			FormatPrice(mbp.OpenPrice),
			FormatPrice(mbp.HighPrice),
			FormatPrice(mbp.LowPrice),
			FormatPrice(mbp.ClosingPrice))
		
		// Print market depth for NIFTY
		fmt.Printf("💰 NIFTY Market Depth:\n")
		for i := 0; i < 10 && i < len(mbp.RecordBuffer); i++ {
			if mbp.RecordBuffer[i].Price > 0 {
				side := "BUY "
				if mbp.RecordBuffer[i].BbBuySellFlag == 1 {
					side = "SELL"
				}
				fmt.Printf("   %s: Price %.2f | Qty %d | Orders %d\n", 
					side,
					FormatPrice(mbp.RecordBuffer[i].Price), 
					mbp.RecordBuffer[i].Quantity,
					mbp.RecordBuffer[i].NumberOfOrders)
			}
		}
		
		// Additional NIFTY trade info
		fmt.Printf("📊 Last Trade: Qty %d | Time %d | Avg Price %.2f\n",
			mbp.LastTradeQuantity,
			mbp.LastTradeTime,
			FormatPrice(mbp.AverageTradePrice))
		
		fmt.Printf("💹 Net Change: %c | Total Buy: %.0f | Total Sell: %.0f\n",
			mbp.NetChangeIndicator,
			mbp.TotalBuyQuantity,
			mbp.TotalSellQuantity)
		
		fmt.Printf("═══════════════════════════════════════════════════\n")
	} else {
		// Standard format for other tokens
		fmt.Printf("🎯 Token: %d | Status: %s | LTP: %.2f | Volume: %d\n",
			mbp.Token,
			FormatTradingStatus(mbp.TradingStatus),
			FormatPrice(mbp.LastTradedPrice),
			mbp.VolumeTradedToday)
	}
}

// GetStructSize returns the size of the structures for validation
func GetStructSize() {
	fmt.Printf("Structure Sizes:\n")
	fmt.Printf("MS_BCAST_ONLY_MBP: %d bytes\n", unsafe.Sizeof(MS_BCAST_ONLY_MBP{}))
	fmt.Printf("INTERACTIVE_ONLY_MBP_DATA: %d bytes\n", unsafe.Sizeof(INTERACTIVE_ONLY_MBP_DATA{}))
	fmt.Printf("MBP_INFORMATION: %d bytes\n", unsafe.Sizeof(MBP_INFORMATION{}))
	fmt.Printf("BcastHeader: %d bytes\n", unsafe.Sizeof(BcastHeader{}))
}
