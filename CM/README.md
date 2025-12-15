# NSE Capital Market (CM) UDP Broadcast Message Codes

This document provides a comprehensive reference for all NSE Capital Market broadcast message codes, their purposes, and market session classifications.

---

## 📋 Table of Contents
- [Market Session Overview](#market-session-overview)
- [All Message Codes (Broadcast)](#all-message-codes-broadcast)
- [Preopen Session Messages](#preopen-session-messages)
- [Regular Market Session Messages](#regular-market-session-messages)
- [Post-Market/Closing Session Messages](#post-marketclosing-session-messages)
- [Available Decoders](#available-decoders)
- [Market Timing Reference](#market-timing-reference)

---

## 🕐 Market Session Overview

NSE Capital Market operates in different sessions throughout the trading day:

| Session | Time (Approximate) | Description |
|---------|-------------------|-------------|
| **Pre-Open** | 9:00 AM - 9:15 AM | Order collection, equilibrium price calculation |
| **Regular Market** | 9:15 AM - 3:30 PM | Continuous trading session |
| **Closing Session** | 3:30 PM - 4:00 PM | Closing price determination, 20-minute trading at closing price |
| **Post-Market** | After 4:00 PM | Bhav copy generation, end-of-day activities |

---

## 📡 All Message Codes (Broadcast)

### System & Status Messages (6xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **6501** | BCAST_JRNL_VCT_MSG | Journal/VCT messages - system alerts, auction notifications, margin violations | All | ✅ Yes |
| **6511** | BC_OPEN_MESSAGE | Market open notification | Regular | ❌ No |
| **6521** | BC_CLOSE_MESSAGE | Market close notification | Post-Market | ❌ No |
| **6531** | BC_PREOPEN_SHUTDOWN_MSG | Market preopen status notification | Preopen | ✅ Yes |
| **6541** | BC_CIRCUIT_CHECK | Heartbeat pulse (every ~9 seconds) | All | ✅ Yes |
| **6571** | BC_NORMAL_MKT_PREOPEN_ENDED | Preopen period ended notification | Preopen→Regular | ❌ No |
| **6581** | BC_AUCTION_STATUS_CHANGE | Auction status change notification | Auction | ❌ No |
| **6583** | BC_CLOSING_START | Closing session started | Post-Market | ❌ No |
| **6584** | BC_CLOSING_END | Closing session ended | Post-Market | ❌ No |

### Market Data Messages (7xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **7200** | BROADCAST_MBO_MBP | Market By Order + Market By Price data | Regular | ✅ Yes |
| **7201** | BCAST_MW_ROUND_ROBIN | Market watch round-robin updates | Regular | ✅ Yes |
| **7206** | BCAST_SYSTEM_INFORMATION_OUT | System information broadcast | All | ❌ No |
| **7207** | BCAST_INDICES | Stock market indices (Nifty 50, Bank Nifty, etc.) | Regular | ✅ Yes |
| **7208** | BCAST_ONLY_MBP | Market By Price only (no MBO) | Regular | ❌ No |
| **7210** | BCAST_CALL_AUCTION_ORD_CXL_UPDATE | Call auction order cancel updates | Auction | ❌ No |
| **7214** | BCAST_CALL_AUCTION_MBP | Call auction Market By Price | Auction | ❌ No |
| **7215** | BCAST_CA_MW | Call auction market watch | Auction | ❌ No |
| **7216** | BCAST_INDICES_VIX | India VIX index broadcast | Regular | ❌ No |

### Master Data & Security Updates (7xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **7304** | UPDATE_LOCALDB_DATA | Local database update packets | All | ❌ No |
| **7306** | BCAST_PART_MSTR_CHG | Participant master change | All | ❌ No |
| **7307** | UPDATE_LOCALDB_HEADER | Local database update header | All | ❌ No |
| **7308** | UPDATE_LOCALDB_TRAILER | Local database update trailer | All | ❌ No |
| **7321** | PARTIAL_SYSTEM_INFORMATION | Partial system info download | All | ❌ No |
| **7764** | BC_SYMBOL_STATUS_CHANGE_ACTION | Security-level trading/market status change | All | ❌ No |

### Special Index Messages (8xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **8207** | BCAST_INDICATIVE_INDICES | Indicative indices broadcast | Preopen | ✅ Yes |

### Turnover & Broker Status (9xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **9010** | BCAST_TURNOVER_EXCEEDED | Broker turnover limit exceeded | All | ❌ No |
| **9011** | BROADCAST_BROKER_REACTIVATED | Broker reactivated after limit reset | All | ❌ No |

### Live Trading & Market Statistics (18xxx Series)

| Code | Name | Description | Session | Decoder |
|------|------|-------------|---------|---------|
| **18130** | BCAST_SECURITY_STATUS_CHG | Security status change | All | ❌ No |
| **18201** | MARKET_STATS_REPORT_DATA | End-of-day market statistics (Bhav Copy) | Post-Market | ✅ Yes |
| **18700** | BCAST_AUCTION_INQUIRY_OUT | Auction inquiry response | Auction | ❌ No |
| **18703** | BCAST_TICKER_AND_MKT_INDEX | **Live ticker data** - real-time trade ticks | Regular | ✅ Yes |
| **18707** | BCAST_SECURITY_STATUS_CHG_PREOPEN | Security status change during preopen | Preopen | ✅ Yes |
| **18708** | BCAST_BUY_BACK | Buyback information broadcast | All | ❌ No |
| **18720** | BCAST_SECURITY_MSTR_CHG | Security master change | All | ❌ No |

---

## 🌅 Preopen Session Messages

**Timing**: 9:00 AM - 9:15 AM

During preopen, orders are collected but not matched. The equilibrium price is calculated.

### Message Code: 6531 - BC_PREOPEN_SHUTDOWN_MSG
- **Market Session**: Preopen
- **Purpose**: Notifies that market/security has entered preopen state
- **Packet Size**: 298 bytes
- **Fields**:
  - Symbol (6 bytes) - Security symbol
  - Series (2 bytes) - Series code (EQ, BE, etc.)
  - InstrumentType (4 bytes) - Instrument type code
  - MarketType (2 bytes) - Market type (Normal/Odd/Auction)
  - BroadcastMessageLength (2 bytes) - Message length
  - BroadcastMessage (240 bytes) - Status message text
- **Decoder**: ✅ Available (`message_6531_live.go`)

### Message Code: 6571 - BC_NORMAL_MKT_PREOPEN_ENDED
- **Market Session**: Preopen → Regular transition
- **Purpose**: Indicates preopen period has ended, regular market about to start
- **Packet Size**: 298 bytes
- **Fields**: Same as 6531
- **Decoder**: ❌ Not yet implemented

### Message Code: 8207 - BCAST_INDICATIVE_INDICES
- **Market Session**: Preopen
- **Purpose**: Broadcasts indicative index values during preopen (not final)
- **Packet Size**: 474 bytes
- **Fields**:
  - IndexName (21 bytes) - Index name (e.g., "Nifty 50")
  - IndicativeIndexValue (4 bytes) - Tentative index value
  - High/Low values (4 bytes each)
  - OpeningIndex, ClosingIndex
  - PercentChange, MarketCapitalisation
  - Up to 6 indices per packet
- **Decoder**: ❌ Not yet implemented

### Message Code: 18707 - BCAST_SECURITY_STATUS_CHG_PREOPEN
- **Market Session**: Preopen
- **Purpose**: Security-level status changes during preopen session
- **Packet Size**: 442 bytes
- **Fields**:
  - Symbol, Series, SecurityStatus
  - Eligibility indicators per market
  - Book closure, Ex-bonus, Ex-dividend flags
- **Decoder**: ❌ Not yet implemented

**Preopen Session Key Features**:
- Orders can be entered, modified, or cancelled
- No trades executed during this period
- Equilibrium price calculated for market opening
- All orders are limit orders (no market orders allowed)
- Preopen indicator bit set in order flags

---

## 🟢 Regular Market Session Messages

**Timing**: 9:15 AM - 3:30 PM

Continuous trading session with live order matching.

### Message Code: 6511 - BC_OPEN_MESSAGE
- **Market Session**: Regular Market
- **Purpose**: Notifies that market/security has opened for trading
- **Packet Size**: 298 bytes
- **Fields**: Same as 6531
- **Decoder**: ❌ Not yet implemented

### Message Code: 6541 - BC_CIRCUIT_CHECK
- **Market Session**: All Sessions
- **Purpose**: Heartbeat pulse to check broadcast circuit connectivity
- **Packet Size**: 40 bytes (BCAST_HEADER only)
- **Fields**:
  - TransactionCode (2 bytes) - Always 6541
  - LogTime (4 bytes) - Host generation time
  - AlphaChar (2 bytes) - Symbol prefix (usually blank)
  - ErrorCode (2 bytes) - Should be 0
  - BCSeqNo (4 bytes) - Broadcast sequence number
  - TimeStamp2 (8 bytes) - Host timestamp
  - MessageLength (2 bytes) - Total message length (40)
- **Frequency**: Every ~9 seconds when no other data
- **Decoder**: ✅ Available (`message_6541_live.go`)

### Message Code: 7200 - BROADCAST_MBO_MBP
- **Market Session**: Regular Market
- **Purpose**: Market By Order + Market By Price (order book depth with order details)
- **Packet Size**: 482 bytes
- **Fields**:
  - Symbol (10 bytes), Series (2 bytes)
  - BestBuyPrice, BestSellPrice
  - TotalBuyQty, TotalSellQty
  - VolTraded, LastTradePrice, LastTradeQty
  - BidInfo (5 levels) - Price, Qty, Number of Orders
  - AskInfo (5 levels) - Price, Qty, Number of Orders
  - OpenPrice, HighPrice, LowPrice, ClosePrice
- **Decoder**: ❌ Not yet implemented

### Message Code: 7201 - BCAST_MW_ROUND_ROBIN
- **Market Session**: Regular Market
- **Purpose**: Market watch updates (snapshot of securities)
- **Packet Size**: 466 bytes
- **Fields**: Similar to 7200 (MBP data without individual orders)
- **Decoder**: ❌ Not yet implemented

### Message Code: 7207 - BCAST_INDICES
- **Market Session**: Regular Market
- **Purpose**: Real-time stock market indices (Nifty 50, Bank Nifty, Sensex, etc.)
- **Packet Size**: 474 bytes
- **Fields** (per index, up to 6 indices):
  - **IndexName** (21 bytes) - Index name (e.g., "Nifty 50", "Nifty Bank")
  - **IndexValue** (4 bytes) - Current index value (in paise, ÷100 for actual value)
  - **HighIndexValue** (4 bytes) - Day high
  - **LowIndexValue** (4 bytes) - Day low
  - **OpeningIndex** (4 bytes) - Opening value
  - **ClosingIndex** (4 bytes) - Previous close
  - **PercentChange** (4 bytes) - Percentage change (in basis points, ÷10000)
  - **YearlyHigh** (4 bytes) - 52-week high
  - **YearlyLow** (4 bytes) - 52-week low
  - **NoOfUpmoves** (4 bytes) - Number of stocks rising
  - **NoOfDownmoves** (4 bytes) - Number of stocks falling
  - **MarketCapitalisation** (8 bytes) - Total market cap (DOUBLE)
  - **NetChangeIndicator** (1 byte) - '+', '-', or ' ' (space)
- **Frequency**: Real-time (typically every second)
- **Decoder**: ✅ Available (`message_7207_live.go`)

### Message Code: 7208 - BCAST_ONLY_MBP
- **Market Session**: Regular Market
- **Purpose**: Market By Price only (5 best bid/ask levels without order count)
- **Packet Size**: 566 bytes
- **Fields**: Similar to 7200 but without order-level details
- **Decoder**: ❌ Not yet implemented

### Message Code: 7216 - BCAST_INDICES_VIX
- **Market Session**: Regular Market
- **Purpose**: India VIX (Volatility Index) broadcast
- **Packet Size**: 474 bytes
- **Fields**: Same structure as 7207 but for VIX index
- **Decoder**: ❌ Not yet implemented

### Message Code: 18703 - BCAST_TICKER_AND_MKT_INDEX
- **Market Session**: Regular Market
- **Purpose**: Live ticker - real-time trade ticks (broadcast every trade execution)
- **Packet Size**: 546 bytes
- **Fields**:
  - **Symbol** (10 bytes) - Security symbol
  - **Series** (2 bytes) - Series code
  - **LastTradePrice** (4 bytes) - LTP
  - **LastTradeQty** (4 bytes) - Trade quantity
  - **LastTradeTime** (8 bytes) - Trade execution time
  - **VolTradeToday** (4 bytes) - Total volume traded
  - **OpenPrice** (4 bytes) - Opening price
  - **HighPrice** (4 bytes) - Day high
  - **LowPrice** (4 bytes) - Day low
  - **ClosePrice** (4 bytes) - Previous close
  - **NetChangeIndicator** (1 byte) - Price movement direction
  - **PercentChange** - Percentage change
  - **TotalBuyQty**, **TotalSellQty** - Order book totals
  - **BestBuyPrice**, **BestSellPrice** - Top of book
  - And more market depth fields
- **Frequency**: Every trade (high frequency - 50,000+ per hour)
- **Decoder**: ✅ Available (`message_18703_live.go`)

**Regular Market Session Key Features**:
- Continuous order matching and trade execution
- Real-time price discovery mechanism
- Circuit breaker filters operational
- Most active trading period
- All order types allowed (market, limit, stop-loss, etc.)

---

## 🌆 Post-Market/Closing Session Messages

**Timing**: 3:30 PM - 4:00 PM (Closing Session) + After 4:00 PM (Bhav Copy)

### Closing Session (3:30 PM - 4:00 PM)

### Message Code: 6521 - BC_CLOSE_MESSAGE
- **Market Session**: Post-Market (Closing)
- **Purpose**: Notifies that regular market has closed
- **Packet Size**: 298 bytes
- **Fields**: Same as 6531
- **Decoder**: ❌ Not yet implemented

### Message Code: 6583 - BC_CLOSING_START
- **Market Session**: Post-Market (Closing)
- **Purpose**: Closing session started - trading at closing price only
- **Packet Size**: 298 bytes
- **Fields**: Same as 6531
- **Note**: 20-minute session where orders can only be placed at closing price
- **Decoder**: ❌ Not yet implemented

### Message Code: 6584 - BC_CLOSING_END
- **Market Session**: Post-Market (Closing)
- **Purpose**: Closing session ended notification
- **Packet Size**: 298 bytes
- **Fields**: Same as 6531
- **Decoder**: ❌ Not yet implemented

**Closing Session Features**:
- Trading allowed only at closing price (calculated)
- 20-minute duration (approximately 3:40 PM - 4:00 PM)
- Orders must be at market price (which equals closing price)
- Used for closing price determination and final trades

### Post-Market (After 4:00 PM)

### Message Code: 18201 - MARKET_STATS_REPORT_DATA
- **Market Session**: Post-Market
- **Purpose**: Bhav Copy - End-of-day market statistics for all securities
- **Packet Size**: Variable (Header: 106 bytes, Data: 478 bytes per security, Trailer: 46 bytes)
- **Fields** (per security):
  - **Symbol** (10 bytes), **Series** (2 bytes)
  - **OpenPrice** (4 bytes) - Day opening price
  - **HighPrice** (4 bytes) - Day high
  - **LowPrice** (4 bytes) - Day low
  - **ClosePrice** (4 bytes) - Closing price
  - **LastTradePrice** (4 bytes) - Last traded price
  - **PreviousClosePrice** (4 bytes) - Previous day close
  - **TotalTradedQty** (4 bytes) - Total volume
  - **TotalTradedValue** (8 bytes) - Total turnover
  - **TotalTrades** (4 bytes) - Number of trades
  - **52WeekHigh** (4 bytes) - 52-week high price
  - **52WeekLow** (4 bytes) - 52-week low price
  - **NetChange** (4 bytes) - Price change from previous close
  - **PercentChange** (4 bytes) - Percentage change
  - And more fields (VWAP, delivery quantity, etc.)
- **Format**: Header → Multiple security records → Trailer
- **Decoder**: ❌ Not yet implemented

### Message Code: 6501 - BCAST_JRNL_VCT_MSG
- **Market Session**: All Sessions (also used in Post-Market)
- **Purpose**: Journal/VCT messages - system alerts, auction info, margin violations, bhav copy notifications
- **Packet Size**: 298 bytes
- **Fields**:
  - **TransactionCode** (2 bytes) - Always 6501
  - **BranchNumber** (2 bytes) - Branch identifier
  - **BrokerNumber** (5 bytes) - Broker code
  - **ActionCode** (3 bytes) - Message type:
    - 'SYS' = System message
    - 'AUI' = Auction Initiation
    - 'AUC' = Auction Complete
    - 'LIS' = Listing
    - 'MAR' = Margin violation
  - **MsgLength** (2 bytes) - Message text length
  - **Msg** (240 bytes) - Actual message content (e.g., "Security Bhav Copy being broadcast now")
- **Special Use in Post-Market**: Announces bhav copy broadcast start
- **Decoder**: ✅ Available (`message_6501_live.go`)

**Post-Market Features**:
- Bhav copy generation (security and index statistics)
- End-of-day reports and settlement files
- No trading activity
- Market statistics finalized
- Next day preparation

---

## � Decoder Implementation Status

### ✅ **Completed Decoders (18/35+ = 51.4%)**

| Code | Name | Session | Priority | Status |
|------|------|---------|----------|--------|
| **6501** | BCAST_JRNL_VCT_MSG | All | High | ✅ Complete |
| **6531** | BC_PREOPEN_SHUTDOWN_MSG | Preopen | Medium | ✅ Complete |
| **6541** | BC_CIRCUIT_CHECK | All | High | ✅ Complete |
| **7200** | BROADCAST_MBO_MBP | Regular | High | ✅ Complete |
| **7201** | BCAST_MW_ROUND_ROBIN | Regular | High | ✅ Complete |
| **7207** | BCAST_INDICES | Regular | High | ✅ Complete |
| **7208** | BCAST_ONLY_MBP | Regular | High | ✅ Complete |
| **7216** | BCAST_INDICES_VIX | Regular | Medium | ✅ Complete |
| **7306** | BCAST_PART_MSTR_CHG | All | Low | ✅ Complete |
| **8207** | BCAST_INDICATIVE_INDICES | Preopen | Medium | ✅ Complete |
| **9010** | BCAST_TURNOVER_EXCEEDED | All | Medium | ✅ Complete |
| **9011** | BROADCAST_BROKER_REACTIVATED | All | Medium | ✅ Complete |
| **18130** | BCAST_SECURITY_STATUS_CHG | All | Medium | ✅ Complete |
| **18201** | MARKET_STATS_REPORT_DATA | Post-Market | High | ✅ Complete |
| **18703** | BCAST_TICKER_AND_MKT_INDEX | Regular | High | ✅ Complete |
| **18707** | BCAST_SECURITY_STATUS_CHG_PREOPEN | Preopen | Medium | ✅ Complete |
| **18708** | BCAST_BUY_BACK | All | Low | ✅ Complete |
| **18720** | BCAST_SECURITY_MSTR_CHG | All | Medium | ✅ Complete |

### ❌ **Pending Decoders (20+ remaining)**

#### **High Priority (Core Trading Data)**
- **6511** - BC_OPEN_MESSAGE (Market open)
- **6521** - BC_CLOSE_MESSAGE (Market close)

#### **Medium Priority (Session Management)**
- **6571** - BC_NORMAL_MKT_PREOPEN_ENDED
- **6583** - BC_CLOSING_START
- **6584** - BC_CLOSING_END

#### **Low Priority (Special Features)**
- **6581** - BC_AUCTION_STATUS_CHANGE
- **7210** - BCAST_CALL_AUCTION_ORD_CXL_UPDATE
- **7214** - BCAST_CALL_AUCTION_MBP
- **7215** - BCAST_CA_MW
- **7304** - UPDATE_LOCALDB_DATA
- **7307** - UPDATE_LOCALDB_HEADER
- **7308** - UPDATE_LOCALDB_TRAILER
- **7321** - PARTIAL_SYSTEM_INFORMATION
- **7764** - BC_SYMBOL_STATUS_CHANGE_ACTION
- **18700** - BCAST_AUCTION_INQUIRY_OUT

### 🎯 **Coverage by Category**

| Category | Completed | Total | Coverage |
|----------|-----------|-------|----------|
| **Core Trading Data** | 6/8 | 75.0% | 🟢 Excellent |
| **System Messages** | 4/6 | 66.7% | 🟢 Good |
| **Session Management** | 4/7 | 57.1% | 🟡 Good |
| **Master Data** | 4/5 | 80.0% | 🟢 Excellent |
| **Special Features** | 0/9 | 0.0% | 🔴 None |

### 🏆 **Key Achievements**
- ✅ All major real-time trading data (18703, 7200, 7201, 7207, 7208)
- ✅ Complete system monitoring (6541, 6501, 9010, 9011)
- ✅ Master data management (18720, 18130, 7306, 18708)
- ✅ Index data coverage (7207, 7216)
- ✅ Market session basics (6531)

### 🎯 **Next Implementation Priority**
1. **6511** (BC_OPEN_MESSAGE) - Market open notification
2. **6521** (BC_CLOSE_MESSAGE) - Market close notification
3. **6571** (BC_NORMAL_MKT_PREOPEN_ENDED) - Preopen transition
4. **6583** (BC_CLOSING_START) - Closing session start
5. **6584** (BC_CLOSING_END) - Closing session end

---

## �🔧 Available Decoders

Currently implemented decoders in the `CM/` directory:

| Decoder File | Message Code | Description | Status |
|--------------|--------------|-------------|--------|
| `message_6501_live.go` | 6501 | Journal/VCT Messages | ✅ Ready |
| `message_6531_live.go` | 6531 | Preopen/Shutdown Messages | ✅ Ready |
| `message_6541_live.go` | 6541 | Heartbeat Monitor | ✅ Ready |
| `message_7200_live.go` | 7200 | Market By Order/Price (MBO/MBP) | ✅ Ready |
| `message_7201_live.go` | 7201 | Market Watch Round Robin | ✅ Ready |
| `message_7207_live.go` | 7207 | Stock Market Indices | ✅ Ready |
| `message_7208_live.go` | 7208 | Market By Price Only | ✅ Ready |
| `message_7216_live.go` | 7216 | India VIX Index | ✅ Ready |
| `message_9010_live.go` | 9010 | Broker Turnover Alert | ✅ Ready |
| `message_9011_live.go` | 9011 | Broker Reactivation | ✅ Ready |
| `message_18703_live.go` | 18703 | Live Ticker Data | ✅ Ready |
| `message_18720_live.go` | 18720 | Security Master Change | ✅ Ready |
| `message_18130_live.go` | 18130 | Security Status Change | ✅ Ready |
| `message_18201_live.go` | 18201 | Bhav Copy (End-of-day Statistics) | ✅ Ready |
| `message_18708_live.go` | 18708 | Buyback Information | ✅ Ready |
| `message_18707_live.go` | 18707 | Security Status Change (Preopen) | ✅ Ready |
| `message_7306_live.go` | 7306 | Participant Master Change | ✅ Ready |
| `message_8207_live.go` | 8207 | Indicative Indices (Preopen) | ✅ Ready |

### Usage Examples

```bash
# Live ticker data (trades)
cd CM
go run message_18703_live.go

# Market By Order/Price (full order book)
go run message_7200_live.go

# Market Watch (snapshot updates)
go run message_7201_live.go

# Stock indices (Nifty 50, Bank Nifty, etc.)
go run message_7207_live.go

# Market By Price only (5 depth levels)
go run message_7208_live.go

# India VIX volatility index
go run message_7216_live.go

# System messages and alerts
go run message_6501_live.go

# Preopen/market status changes
go run message_6531_live.go

# Connection health monitoring
go run message_6541_live.go

# Broker turnover limit monitoring
go run message_9010_live.go

# Monitor security master and status changes
go run message_18720_live.go  # Security master changes
go run message_18130_live.go  # Security status changes
go run message_18201_live.go  # Bhav copy (end-of-day statistics)
go run message_18708_live.go  # Buyback information
go run message_18707_live.go  # Security status changes (preopen)
go run message_7306_live.go   # Participant master changes

# Monitor preopen session (9:00-9:15 AM)
go run message_8207_live.go   # Indicative indices (preopen)
go run message_18707_live.go  # Security status changes (preopen)
```

---

## 📊 Message Frequency Reference

| Message Code | Frequency | Typical Count/Hour |
|--------------|-----------|-------------------|
| **18703** (Ticker) | Every trade | 50,000+ |
| **7207** (Indices) | Real-time | 3,600 (every second) |
| **7200** (MBO/MBP) | Order book changes | 10,000+ |
| **6541** (Heartbeat) | Every ~9 seconds | 400 |
| **6501** (Journal) | Event-driven | 50-100 |
| **6531** (Market Status) | Session changes | 5-10 |

---

## 🎯 Message Categories by Purpose

### Real-Time Trading Data
- **18703** - Live ticker (every trade)
- **7200** - Order book depth
- **7208** - Market by price
- **7207** - Index movements

### Market Session Control
- **6531** - Preopen start
- **6511** - Market open
- **6521** - Market close
- **6571** - Preopen ended
- **6583** - Closing session start
- **6584** - Closing session end

### System Monitoring
- **6541** - Heartbeat pulse
- **6501** - System messages
- **9010** - Turnover limits
- **9011** - Broker reactivation

### Master Data Updates
- **7306** - Participant changes
- **18720** - Security master changes
- **7764** - Security status changes

### Special Sessions
- **7214** - Call auction MBP
- **7215** - Call auction market watch
- **18700** - Auction inquiry
- **18708** - Buyback information

---

## 🌐 Multicast Channels

### After-Market Hours Testing
```
IP: 231.31.31.4
Port: 18901
```

### Live Market Hours
```
IP: 233.1.2.5
Port: 8222
```

---

## 📖 Protocol Reference

**Document**: NSE CM NNF Protocol v6.3 (October 2025)
**Location**: `Docs/bse/TP_CM_Trimmed_NNF_PROTOCOL_6.3.txt`

Key sections:
- **Chapter 7**: Broadcast messages and compression
- **Table 3**: BCAST_HEADER structure (40 bytes)
- **Table 4**: SEC_INFO structure (12 bytes)
- **Pages 203-204**: Complete message code listing

---

## 🔍 Important Notes

### Market Session Indicators

**Preopen Indicator** (in Order Flags):
- `0` = Regular market order
- `1` = Preopen order or Call Auction order

**Market Types**:
- `1` = Normal market
- `2` = Odd Lot
- `3` = Spot
- `4` = Auction
- `5` = Call Auction 1
- `6` = Call Auction 2

### Message Structure

All broadcast messages follow this structure:
```
[8-byte System Header] + [40-byte BCAST_HEADER] + [Variable Payload]
```

**Compression**: Most messages are LZO1Z compressed to save bandwidth.

---

## 📈 Trading Day Timeline

```
09:00 AM ─────── Preopen Start (6531)
   │             - Order collection
   │             - No matching
   │             - Indicative indices (8207)
   │
09:07 AM ─────── Preopen Order Entry Ends
   │             - Random closing (9:07-9:12)
   │
09:15 AM ─────── Regular Market Open (6511, 6571)
   │             ┌─────────────────────────┐
   │             │   Continuous Trading    │
   │             │   - Live ticks (18703)  │
   │             │   - Indices (7207)      │
   │             │   - MBO/MBP (7200)      │
   │             │   - Heartbeats (6541)   │
   │             └─────────────────────────┘
   │
03:30 PM ─────── Regular Market Close (6521)
   │             
   │             Closing Batch Price Calculation
   │
03:40 PM ─────── Closing Session Start (6583)
   │             - 20 minutes
   │             - Trading at closing price only
   │
04:00 PM ─────── Closing Session End (6584)
   │
   │             Post-Market Activities
   │             - Bhav copy (18201)
   │             - Settlement
   │
05:00 PM ─────── After-Market Hours Feed (Testing)
```

---

## 🚀 Quick Start

### 1. Monitor Live Market
```bash
# Terminal 1: Live trades
go run message_18703_live.go

# Terminal 2: Stock indices
go run message_7207_live.go

# Terminal 3: System messages
go run message_6501_live.go
```

### 2. Monitor Preopen Session (9:00-9:15 AM)
```bash
# Watch for preopen messages
go run message_6531_live.go

# Check for indicative indices
# (Decoder not yet created for 8207)
```

### 3. Monitor Closing Session (3:30-4:00 PM)
```bash
# Watch for closing session start/end
go run message_6531_live.go
```

---

## 📁 CSV Output

All decoders save data to:
```
CM/csv_output/message_<CODE>_<TIMESTAMP>.csv
```

Examples:
- `csv_output/message_18703_20241210_091530.csv` - Live trades
- `csv_output/message_7207_20241210_091530.csv` - Index data
- `csv_output/message_6501_20241210_091530.csv` - System messages
- `csv_output/message_6531_20241210_091530.csv` - Market status
- `csv_output/message_6541_20241210_091530.csv` - Heartbeats

---

## � Complete Message Code Reference with Fields

### System & Control Messages (All Sessions)

#### Message 6501 - BCAST_JRNL_VCT_MSG
- **Session**: All
- **Purpose**: System messages, auction alerts, margin violations
- **Size**: 298 bytes
- **Key Fields**: BranchNumber, BrokerNumber, ActionCode ('SYS'/'AUI'/'AUC'/'LIS'/'MAR'), MsgLength, Message
- **Decoder**: ✅ Yes

#### Message 6541 - BC_CIRCUIT_CHECK  
- **Session**: All
- **Purpose**: Heartbeat pulse (every ~9 seconds)
- **Size**: 40 bytes
- **Key Fields**: LogTime, BCSeqNo, TimeStamp2, MessageLength
- **Decoder**: ✅ Yes

#### Message 7764 - BC_SYMBOL_STATUS_CHANGE_ACTION
- **Session**: All
- **Purpose**: Security-level trading/market status changes
- **Size**: 58 bytes
- **Key Fields**: Symbol, Series, MarketType, ActionCode (6531/6571/6511/6521/6583/6584)
- **Decoder**: ❌ No

#### Message 9010 - BCAST_TURNOVER_EXCEEDED
- **Session**: All
- **Purpose**: Broker turnover limit exceeded notification
- **Size**: 77 bytes
- **Key Fields**: BrokerCode, WarningType (1=About to exceed, 2=Exceeded), Symbol, Series, TradeNumber, TradePrice, TradeVolume
- **Decoder**: ✅ Yes (`message_9010_live.go`)

#### Message 9011 - BROADCAST_BROKER_REACTIVATED
- **Session**: All
- **Purpose**: Broker reactivated after limit reset
- **Size**: 77 bytes
- **Key Fields**: BrokerCode
- **Decoder**: ✅ Yes (`message_9011_live.go`)

### Preopen Session Messages (9:00 AM - 9:15 AM)

#### Message 6531 - BC_PREOPEN_SHUTDOWN_MSG
- **Session**: Preopen
- **Purpose**: Market/security entered preopen state
- **Size**: 298 bytes
- **Key Fields**: Symbol, Series, InstrumentType, MarketType, BroadcastMessageLength, BroadcastMessage
- **Decoder**: ✅ Yes

#### Message 6571 - BC_NORMAL_MKT_PREOPEN_ENDED
- **Session**: Preopen → Regular
- **Purpose**: Preopen period ended, market opening
- **Size**: 298 bytes
- **Key Fields**: Same as 6531
- **Decoder**: ❌ No

#### Message 8207 - BCAST_INDICATIVE_INDICES
- **Session**: Preopen
- **Purpose**: Indicative (tentative) index values during preopen
- **Size**: 474 bytes
- **Key Fields**: IndexName (21 bytes), IndicativeIndexValue, High, Low, PercentChange, MarketCap (up to 6 indices)
- **Decoder**: ✅ Yes

#### Message 18707 - BCAST_SECURITY_STATUS_CHG_PREOPEN
- **Session**: Preopen
- **Purpose**: Security status changes during preopen
- **Size**: 442 bytes
- **Key Fields**: Symbol, Series, SecurityStatus, Eligibility, BookClosureDate, ExBonusDate, ExDividendDate
- **Decoder**: ✅ Yes

### Regular Market Messages (9:15 AM - 3:30 PM)

#### Message 6511 - BC_OPEN_MESSAGE
- **Session**: Regular
- **Purpose**: Market/security opened for trading
- **Size**: 298 bytes
- **Key Fields**: Same as 6531
- **Decoder**: ❌ No

#### Message 7200 - BROADCAST_MBO_MBP
- **Session**: Regular
- **Purpose**: Market By Order + Market By Price (full order book)
- **Size**: 482 bytes
- **Key Fields**: Symbol, Series, BestBuyPrice, BestSellPrice, TotalBuyQty, TotalSellQty, VolTraded, LastTradePrice, LastTradeQty, BidInfo[5], AskInfo[5], OpenPrice, HighPrice, LowPrice, ClosePrice
- **Decoder**: ✅ Yes

#### Message 7201 - BCAST_MW_ROUND_ROBIN
- **Session**: Regular
- **Purpose**: Market watch updates (snapshot)
- **Size**: 466 bytes
- **Key Fields**: Similar to 7200
- **Decoder**: ✅ Yes

#### Message 7207 - BCAST_INDICES
- **Session**: Regular
- **Purpose**: Real-time stock market indices
- **Size**: 474 bytes
- **Key Fields**: IndexName, IndexValue, HighIndexValue, LowIndexValue, OpeningIndex, ClosingIndex, PercentChange, YearlyHigh, YearlyLow, NoOfUpmoves, NoOfDownmoves, MarketCapitalisation, NetChangeIndicator (up to 6 indices)
- **Decoder**: ✅ Yes

#### Message 7208 - BCAST_ONLY_MBP
- **Session**: Regular
- **Purpose**: Market By Price only (no order details)
- **Size**: 566 bytes
- **Key Fields**: Similar to 7200 but without order count
- **Decoder**: ✅ Yes

#### Message 7216 - BCAST_INDICES_VIX
- **Session**: Regular
- **Purpose**: India VIX volatility index
- **Size**: 474 bytes
- **Key Fields**: Same structure as 7207
- **Decoder**: ✅ Yes

#### Message 18703 - BCAST_TICKER_AND_MKT_INDEX
- **Session**: Regular
- **Purpose**: Live ticker - every trade execution
- **Size**: 546 bytes
- **Key Fields**: Symbol, Series, LastTradePrice, LastTradeQty, LastTradeTime, VolTradeToday, OpenPrice, HighPrice, LowPrice, ClosePrice, NetChangeIndicator, PercentChange, TotalBuyQty, TotalSellQty, BestBuyPrice, BestSellPrice
- **Frequency**: High (50,000+ per hour)
- **Decoder**: ✅ Yes

### Post-Market/Closing Messages (3:30 PM - 4:00 PM+)

#### Message 6521 - BC_CLOSE_MESSAGE
- **Session**: Post-Market
- **Purpose**: Market closed notification
- **Size**: 298 bytes
- **Key Fields**: Same as 6531
- **Decoder**: ❌ No

#### Message 6583 - BC_CLOSING_START
- **Session**: Post-Market (Closing)
- **Purpose**: Closing session started (20 min trading at closing price)
- **Size**: 298 bytes
- **Key Fields**: Same as 6531
- **Decoder**: ❌ No

#### Message 6584 - BC_CLOSING_END
- **Session**: Post-Market (Closing)
- **Purpose**: Closing session ended
- **Size**: 298 bytes
- **Key Fields**: Same as 6531
- **Decoder**: ❌ No

#### Message 18201 - MARKET_STATS_REPORT_DATA
- **Session**: Post-Market
- **Purpose**: Bhav Copy - end-of-day statistics
- **Size**: Variable (Header 106B + Data 478B/security + Trailer 46B)
- **Key Fields**: Symbol, Series, OpenPrice, HighPrice, LowPrice, ClosePrice, LastTradePrice, PreviousClosePrice, TotalTradedQty, TotalTradedValue, TotalTrades, 52WeekHigh, 52WeekLow, NetChange, PercentChange, VWAP, DeliveryQty
- **Decoder**: ✅ Yes

### Auction Messages (Special Session)

#### Message 6581 - BC_AUCTION_STATUS_CHANGE
- **Session**: Auction
- **Purpose**: Auction status change notification
- **Size**: 302 bytes
- **Key Fields**: Symbol, Series, AuctionStatus, AuctionNumber
- **Decoder**: ❌ No

#### Message 7210 - BCAST_CALL_AUCTION_ORD_CXL_UPDATE
- **Session**: Auction
- **Purpose**: Call auction order cancel updates
- **Size**: 490 bytes
- **Key Fields**: Symbol, Series, OrderNumber, CancelReason
- **Decoder**: ❌ No

#### Message 7214 - BCAST_CALL_AUCTION_MBP
- **Session**: Auction
- **Purpose**: Call auction Market By Price
- **Size**: 538 bytes
- **Key Fields**: Similar to 7208 but for call auction
- **Decoder**: ❌ No

#### Message 7215 - BCAST_CA_MW
- **Session**: Auction
- **Purpose**: Call auction market watch
- **Size**: 482 bytes
- **Key Fields**: Auction-specific market watch data
- **Decoder**: ❌ No

#### Message 18700 - BCAST_AUCTION_INQUIRY_OUT
- **Session**: Auction
- **Purpose**: Auction inquiry response broadcast
- **Size**: 76 bytes
- **Key Fields**: Symbol, Series, AuctionNumber, AuctionQty, AuctionPrice
- **Decoder**: ❌ No

### Master Data Update Messages (All Sessions)

#### Message 18720 - BCAST_SECURITY_MSTR_CHG
- **Session**: All
- **Purpose**: Security master data changes
- **Size**: 260 bytes
- **Key Fields**: Symbol, Series, ISIN, TokenNumber, SecurityName, FaceValue, ISINNumber, InstrumentType, MarketLot, TickSize, IssueRate, IssueStartDate, LastTradingDate, ExpiryDate, ReadmitDate, PermittedToTrade
- **Decoder**: ✅ Yes

#### Message 18130 - BCAST_SECURITY_STATUS_CHG
- **Session**: All
- **Purpose**: Security status changes (eligibility, trading status)
- **Size**: 442 bytes
- **Key Fields**: Symbol, Series, SecurityStatus, Eligibility[6], BookClosureDate, ExBonusDate, ExDividendDate, NoDeliveryStartDate, NoDeliveryEndDate
- **Decoder**: ✅ Yes

#### Message 7306 - BCAST_PART_MSTR_CHG
- **Session**: All
- **Purpose**: Participant (broker/member) master changes
- **Size**: 84 bytes
- **Key Fields**: ParticipantId, ParticipantName, ParticipantStatus, SuspendedDate
- **Decoder**: ✅ Yes

#### Message 18708 - BCAST_BUY_BACK
- **Session**: All
- **Purpose**: Buyback information broadcast
- **Size**: 426 bytes
- **Key Fields**: Symbol, Series, BuybackStartDate, BuybackEndDate, BuybackPrice
- **Decoder**: ✅ Yes

---

## �📚 Additional Resources

- **Message Fields Reference**: See `MESSAGE_FIELDS_REFERENCE.md` for detailed field descriptions
- **Protocol Document**: `Docs/bse/TP_CM_Trimmed_NNF_PROTOCOL_6.3.txt`
- **Working Decoders**: All `.go` files in `CM/` directory

---

## 🎓 Message Code Summary by Session

| Session | Primary Messages | Purpose |
|---------|-----------------|----------|
| **Preopen** | 6531, 6571, 8207, 18707 | Session control, indicative data |
| **Regular** | 6511, 7200, 7207, 18703 | Open, live trading, indices, ticks |
| **Closing** | 6521, 6583, 6584 | Close, closing session |
| **Post-Market** | 18201, 6501 | Bhav copy, statistics |
| **All Sessions** | 6541, 6501, 7764, 18720 | Heartbeat, system alerts, master updates |

---

**Last Updated**: December 11, 2024  
**Protocol Version**: NSE CM NNF v6.3 (October 2025)  
**Total Broadcast Message Codes**: 35+  
**Implemented Decoders**: 18 out of 35+ (51.4% coverage)
**Available Decoders**: 18703, 7200, 7201, 7207, 7208, 7216, 6501, 6531, 6541, 9010, 9011, 18720, 18130, 18201, 18707, 18708, 7306, 8207
