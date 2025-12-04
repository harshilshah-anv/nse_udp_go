# NSE UDP Test Suite - Quick Start Guide

## 📋 Overview

A complete UDP packet analyzer for NSE (National Stock Exchange) broadcast messages with LZO1Z decompression support. Analyzes message statistics, compression ratios, and network traffic in real-time.

## 🚀 Quick Start

### Option 1: Using the Batch Script (Windows)
```cmd
run_test.bat
```
Then select option 3 to run both analyzer and sender.

### Option 2: Manual Execution

**Terminal 1 - Analyzer:**
```cmd
go run udp_test.go lzo1z_ultra.go
```

**Terminal 2 - Sender:**
```cmd
go run udp_sender_test.go
```

## 📁 Project Files

```
go-udp-reader/
├── udp_test.go                    # Main UDP analyzer (receiver)
├── udp_sender_test.go             # UDP packet simulator (sender)
├── lzo1z_ultra.go                 # LZO1Z decompression (ultra-optimized)
├── run_test.bat                   # Windows batch script
├── TEST_README.md                 # Detailed documentation
├── NSE_NNF_Broadcast_Analysis.md  # NSE protocol analysis
└── Docs/
    └── nse/
        └── TP_FO_Trimmed.txt      # NSE protocol specification
```

## 🎯 What It Does

### UDP Analyzer (udp_test.go)
✅ Captures UDP packets on port 9000  
✅ Parses NSE broadcast headers (40 bytes)  
✅ Identifies compressed vs uncompressed messages  
✅ Decompresses LZO1Z data using lzo1z_ultra.go  
✅ Calculates per-message-code statistics  
✅ Shows compression ratios and efficiency  
✅ Displays real-time and summary reports  

### UDP Sender (udp_sender_test.go)
✅ Simulates 14 different NSE message types  
✅ Sends 1000 test packets  
✅ Mix of compressed (7) and uncompressed (7) messages  
✅ Variable transmission rate  
✅ Includes error injection (5% rate)  
✅ Shows progress and completion stats  

## 📊 Statistics Provided

### Overall Metrics
- Total packets received
- Total bytes transferred
- Packet rate (packets/sec)
- Data rate (KB/sec)
- Compression success rate
- Decompression failure count

### Per-Message Code Metrics
- **Transaction Code** - Numeric identifier
- **Message Name** - Human-readable description
- **Packet Count** - Number of packets received
- **Compressed Size** - Total compressed data size
- **Raw Size** - Total decompressed data size
- **Compression Ratio** - Efficiency (e.g., 1.50x = 33% savings)
- **Error Counts** - Header errors / Decompression errors

### Compression Summary
- Total compressed data size
- Total raw (decompressed) data size
- Overall compression ratio
- Total bytes saved
- Bandwidth savings percentage

## 🗜️ Compressed Message Codes

These codes are automatically decompressed using LZO1Z:

| Code  | Message Name                          | Size  |
|-------|---------------------------------------|-------|
| 7200  | BCAST_MBO_MBP_UPDATE                  | 410B  |
| 7201  | BCAST_MW_ROUND_ROBIN                  | 472B  |
| 7202  | BCAST_TICKER_AND_MKT_INDEX            | 484B  |
| 7208  | BCAST_ONLY_MBP                        | 470B  |
| 7220  | BCAST_LIMIT_PRICE_PROTECTION_RANGE    | 344B  |
| 17201 | BCAST_ENHNCD_MW_ROUND_ROBIN           | 492B  |
| 17202 | BCAST_ENHNCD_TICKER_AND_MKT_INDEX     | 492B  |

## 📈 Example Output

```
====================================================================================================
UDP MESSAGE CODE STATISTICS
====================================================================================================

📊 OVERALL STATISTICS
  Runtime:                30s
  Total Packets:          1000
  Total Bytes:            456789 (0.44 MB)
  Compressed Packets:     700
  Decompressed Success:   695
  Decompression Failures: 5
  Avg Packet Rate:        33.33 packets/sec
  Avg Data Rate:          14.88 KB/sec

📈 MESSAGE CODE BREAKDOWN (14 unique codes)
----------------------------------------------------------------------------------------------------
Code   Name                                       Count   Compressed(KB)        Raw(KB)      Ratio
----------------------------------------------------------------------------------------------------
7208   BCAST_ONLY_MBP                          🗜️     150           68.55          102.83       1.50x
7200   BCAST_MBO_MBP_UPDATE                    🗜️     120           47.46           71.19       1.50x
7202   BCAST_TICKER_AND_MKT_INDEX              🗜️     110           51.68           77.52       1.50x
...

🗜️  COMPRESSION SUMMARY
  Total Compressed Data:   0.31 MB
  Total Raw Data:          0.47 MB
  Overall Compression:     1.50x
  Bytes Saved:             0.16 MB (33.3%)
```

## 🔧 Configuration

### Change UDP Port

**Analyzer:**
```cmd
go run udp_test.go lzo1z_ultra.go 8888
```

**Sender:**
```cmd
go run udp_sender_test.go 8888
```

### Adjust Packet Count

Edit `udp_sender_test.go`, line ~120:
```go
if packetCount >= 1000 {  // Change to desired count
    break
}
```

### Modify Decompression Buffer

Edit `udp_test.go`, line ~273:
```go
decompressed := make([]byte, 10000) // Increase if needed
```

## 🎨 Features Highlight

### Real-Time Monitoring
- Periodic progress updates (every 10 seconds)
- Live packet counting
- Non-blocking operation

### Comprehensive Statistics
- Message code frequency distribution
- Compression efficiency analysis
- Error tracking and reporting
- Bandwidth savings calculation

### Performance Optimized
- Ultra-fast LZO1Z decompression (270-300 MB/s)
- Concurrent packet processing
- Efficient memory management
- Thread-safe statistics updates

### Error Handling
- Malformed packet detection
- Decompression failure tracking
- Network error recovery
- Graceful shutdown (Ctrl+C)

## 📝 Notes

1. **Simulation Limitation**: The sender creates pseudo-compressed data rather than actual LZO compression. Real NSE packets would have proper LZO compression.

2. **Buffer Sizes**: Default buffer sizes are suitable for testing. Production use may require adjustment based on actual message sizes.

3. **Port Selection**: Default port is 9000. Ensure the port is not blocked by firewall.

4. **Localhost Only**: Sender uses 127.0.0.1. Modify for network testing.

## 🐛 Troubleshooting

**Problem**: Analyzer shows no packets  
**Solution**: Ensure sender is running and using same port

**Problem**: All decompression failures  
**Solution**: Normal for simulator - it sends uncompressed test data

**Problem**: Port already in use  
**Solution**: Change port or stop conflicting application

**Problem**: Build errors  
**Solution**: Ensure Go is installed and all files are in same directory

## 📚 Documentation

- **TEST_README.md** - Detailed technical documentation
- **NSE_NNF_Broadcast_Analysis.md** - NSE protocol specification
- **TP_FO_Trimmed.txt** - Complete NSE protocol document

## 🔬 Testing Scenarios

### Scenario 1: Basic Functionality
```cmd
# Terminal 1
go run udp_test.go lzo1z_ultra.go

# Terminal 2
go run udp_sender_test.go
```

### Scenario 2: Performance Testing
Modify sender to send 10,000 packets and measure throughput.

### Scenario 3: Error Analysis
Check error rates and decompression failures in statistics.

### Scenario 4: Compression Analysis
Compare compressed vs raw sizes for different message types.

## 💡 Tips

1. **Run analyzer first** - Start it before sender to capture all packets
2. **Wait for completion** - Let sender finish before stopping analyzer
3. **Multiple runs** - Run multiple times to see consistent patterns
4. **Monitor resources** - Watch CPU/memory usage during high packet rates
5. **Check statistics** - Review all sections of the output report

## 🎯 Next Steps

1. Run the test suite with `run_test.bat`
2. Review the statistics output
3. Experiment with different configurations
4. Study the protocol analysis document
5. Integrate with your NSE feed implementation

---

**Ready to test?** Run `run_test.bat` and select option 3!
