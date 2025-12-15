# NSE CM Message Fields Reference

This document describes the data fields saved in CSV for each message decoder.

## Message 6501 - BCAST_JRNL_VCT_MSG (Journal/VCT Messages)

**Purpose**: System messages, auction notifications, margin violations

**Protocol Reference**: NSE CM NNF Protocol v6.3, Pages 79-80 (Table 23)

**Packet Structure**: 298 bytes total
- BCAST_HEADER: 40 bytes (offset 0-39)
- Data Fields: 258 bytes (offset 40-297)

**CSV Columns Saved** (7 columns):
1. **Timestamp** - Local system time when received
2. **TransactionCode** - Always 6501
3. **BranchNumber** - SHORT (2 bytes, offset 40-41) - Branch identifier
4. **BrokerNumber** - CHAR[5] (5 bytes, offset 42-46) - Broker code
5. **ActionCode** - CHAR[3] (3 bytes, offset 47-49) - Action type:
   - 'SYS' = System message
   - 'AUI' = Auction Initiation
   - 'AUC' = Auction Complete
   - 'LIS' = Listing
   - 'MAR' = Margin violation messages
6. **MsgLength** - SHORT (2 bytes, offset 56-57) - Length of message text
7. **Message** - CHAR[240] (240 bytes, offset 58-297) - Actual message content

**Example CSV Row**:
```
2024-12-10 15:23:45.123,6501,123,12345,SYS,45,System maintenance scheduled at 17:00
```

---

## Message 6531 - BC_PREOPEN_SHUTDOWN_MSG (Market Open/Close/Preopen)

**Purpose**: Market status change notifications (open, close, preopen)

**Protocol Reference**: NSE CM NNF Protocol v6.3, Pages 108-109 (Table 34)

**Packet Structure**: 298 bytes total
- BCAST_HEADER: 40 bytes (offset 0-39)
- SEC_INFO: 12 bytes (offset 40-51)
- Data Fields: 246 bytes (offset 52-297)

**CSV Columns Saved** (8 columns):
1. **Timestamp** - Local system time when received
2. **TransactionCode** - 6531 (preopen), 6511 (open), 6521 (close), etc.
3. **Symbol** - CHAR[6] (6 bytes, offset 40-45) - Security symbol (e.g., "RELIANCE", "TCS")
4. **Series** - CHAR[2] (2 bytes, offset 46-47) - Series code (e.g., "EQ", "BE")
5. **InstrumentType** - LONG (4 bytes, offset 48-51) - Instrument type code
6. **MarketType** - SHORT (2 bytes, offset 52-53) - Market type:
   - 1 = Normal
   - 2 = Odd Lot
   - 3 = Spot
   - 4 = Auction
   - 5 = Call Auction1
   - 6 = Call Auction2
7. **MsgLength** - SHORT (2 bytes, offset 56-57) - Length of broadcast message
8. **Message** - CHAR[240] (240 bytes, offset 58-297) - Broadcast message text

**Example CSV Row**:
```
2024-12-10 09:15:00.456,6531,TCS,EQ,0,1,38,Pre-open session started for security
```

---

## Message 6541 - BC_CIRCUIT_CHECK (Heartbeat Pulse)

**Purpose**: Connection health monitoring - sent every ~9 seconds when no other data

**Protocol Reference**: NSE CM NNF Protocol v6.3, Page 138

**Packet Structure**: 40 bytes total (only BCAST_HEADER, no additional data)

**CSV Columns Saved** (10 columns - all from BCAST_HEADER):
1. **Timestamp** - Local system time when received
2. **TransactionCode** - Always 6541
3. **LogTime** - LONG (4 bytes, offset 4-7) - Host generation time
4. **AlphaChar** - CHAR[2] (2 bytes, offset 8-9) - Usually blank for heartbeat
5. **ErrorCode** - SHORT (2 bytes, offset 12-13) - Should be 0 for heartbeat
6. **BCSeqNo** - LONG (4 bytes, offset 14-17) - Broadcast sequence number
7. **TimeStamp2** - CHAR[8] (8 bytes, offset 22-29) - Host timestamp
8. **MessageLength** - SHORT (2 bytes, offset 38-39) - Total message length (40)
9. **HeartbeatNumber** - Sequential counter (1, 2, 3, ...)
10. **SecondsSinceLastHeartbeat** - Time interval since previous heartbeat

**Example CSV Row**:
```
2024-12-10 15:23:45.789,6541,1234567890,,0,98765432,15234578,40,1,0.000
2024-12-10 15:23:54.823,6541,1234568901,,0,98765433,15234588,40,2,9.034
```

**Heartbeat Analysis**:
- Normal interval: 8-10 seconds (expected ~9 seconds)
- Abnormal: <8 or >10 seconds (connection issues or high data traffic)

---

## Message 7207 - BCAST_INDICES (Stock Market Indices)

**Purpose**: Real-time index data (Nifty 50, Bank Nifty, Sensex, etc.)

**Protocol Reference**: NSE CM NNF Protocol v6.3, Page 139 (Table 43)

**Packet Structure**: 474 bytes total
- BCAST_HEADER: 40 bytes (offset 0-39)
- Index Records: Up to 6 indices × 72 bytes each (offset 40-472)

**CSV Columns Saved** (16 columns per index):
1. **Timestamp** - Local system time
2. **TransactionCode** - Always 7207
3. **IndexName** - CHAR[21] - Index name
4. **IndexValue** - Current index value (in paise, divide by 100)
5. **HighIndexValue** - Day high
6. **LowIndexValue** - Day low
7. **OpeningIndex** - Opening value
8. **ClosingIndex** - Previous close
9. **PercentChange** - % change (in basis points, divide by 10000)
10. **YearlyHigh** - 52-week high
11. **YearlyLow** - 52-week low
12. **NoOfUpmoves** - Number of stocks up
13. **NoOfDownmoves** - Number of stocks down
14. **MarketCapitalisation** - DOUBLE (8 bytes) - Total market cap
15. **NetChangeIndicator** - CHAR - '+', '-', or ' ' (space)

**Example CSV Row**:
```
2024-12-10 15:23:45.123,7207,Nifty 50,19456.75,19523.40,19401.20,19450.00,19412.50,0.2287,20200.00,18800.00,28,22,1.23456789E+15,+
```

---

## Message 18703 - BCAST_TICKER_AND_MKT_INDEX (Live Ticker)

**Purpose**: Real-time trade tick data for individual securities

**Packet Structure**: Variable (trade records)

**CSV Columns**: Symbol, Series, LTP, Volume, Change%, etc.

---

## Comparison Summary

| Message | Purpose | Packet Size | Data Fields | Update Frequency |
|---------|---------|-------------|-------------|------------------|
| **6501** | Journal/System Messages | 298 bytes | BranchNo, BrokerNo, ActionCode, Message | Event-driven |
| **6531** | Market Open/Close/Preopen | 298 bytes | Symbol, Series, MarketType, Message | Market status changes |
| **6541** | Heartbeat Pulse | 40 bytes | BCAST_HEADER fields only | Every ~9 seconds |
| **7207** | Index Data | 474 bytes | Up to 6 indices with 15 fields each | Real-time (seconds) |
| **18703** | Live Ticker | Variable | Symbol, Price, Volume, etc. | Real-time (every trade) |

---

## Field Encoding Notes

### Numeric Scaling:
- **Index Values**: Stored in paise → divide by 100 for rupees
  - Example: 1945675 → 19456.75
- **Percentages**: Stored in basis points → divide by 10000
  - Example: 2287 → 0.2287%

### String Fields:
- All CHAR arrays are null-terminated and space-padded
- Trimming applied: `strings.TrimRight(string(field[:]), "\x00 ")`

### BigEndian Encoding:
- All numeric fields use BigEndian byte order (NSE standard)
- Use `binary.BigEndian.Uint16()`, `Uint32()`, etc.

---

## Usage Examples

### Message 6501 - System Messages
```bash
cd CM
go run message_6501_live.go
# Monitor for: Auction notifications, margin violations, system alerts
```

### Message 6531 - Market Status
```bash
go run message_6531_live.go
# Monitor for: Market open/close, preopen session, circuit breakers
```

### Message 6541 - Connection Health
```bash
go run message_6541_live.go
# Monitor for: Heartbeat intervals, connection stability
```

### Message 7207 - Index Tracking
```bash
go run message_7207_live.go
# Monitor for: Nifty 50, Bank Nifty, Sensex movements
```

---

## CSV Output Location

All decoders save CSV files to:
```
CM/csv_output/message_<CODE>_<TIMESTAMP>.csv
```

Example:
- `csv_output/message_6501_20241210_152345.csv`
- `csv_output/message_6531_20241210_091500.csv`
- `csv_output/message_6541_20241210_093000.csv`
- `csv_output/message_7207_20241210_101530.csv`

---

## Data Quality Notes

### Message 6501:
- ✅ All fields decoded and saved
- ActionCode indicates message type
- Message field contains human-readable text

### Message 6531:
- ✅ All fields decoded and saved
- Symbol/Series identify affected security
- MarketType shows which market segment
- Message contains status change details

### Message 6541:
- ✅ BCAST_HEADER fields decoded and saved
- BCSeqNo tracks broadcast sequence
- Heartbeat interval monitoring for connection health
- Normal interval: 8-10 seconds

### Message 7207:
- ✅ All index fields decoded and saved
- Up to 6 indices per packet
- 72-byte stride with reserved byte at offset 61
- MarketCapitalisation stored as DOUBLE at offset 62-69

---

**Last Updated**: December 10, 2024
**Protocol Version**: NSE CM NNF Protocol v6.3 (Oct 2025)
