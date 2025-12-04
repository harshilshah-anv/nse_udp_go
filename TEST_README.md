# UDP Message Analyzer - Test Suite

This test suite analyzes NSE (National Stock Exchange) UDP broadcast messages and provides detailed statistics on message codes, compression ratios, and network traffic.

## Files

- **udp_test.go** - UDP packet receiver and analyzer
- **udp_sender_test.go** - UDP packet simulator for testing
- **lzo1z_ultra.go** - LZO1Z decompression implementation
- **NSE_NNF_Broadcast_Analysis.md** - Protocol documentation

## Features

### UDP Test Analyzer (udp_test.go)

✅ Real-time UDP packet capture and analysis  
✅ Automatic detection of compressed vs uncompressed messages  
✅ LZO1Z decompression using lzo1z_ultra.go  
✅ Per-message-code statistics:
  - Packet count
  - Total compressed size
  - Total raw (decompressed) size
  - Compression ratio
  - Error counts
  - First/last seen timestamps

✅ Overall statistics:
  - Total packets and bytes
  - Packet rate (packets/sec)
  - Data rate (KB/sec)
  - Compression efficiency
  - Decompression success/failure rates

### UDP Sender Simulator (udp_sender_test.go)

✅ Simulates NSE broadcast packets  
✅ Supports multiple message types (14 different codes)  
✅ Mix of compressed and uncompressed messages  
✅ Variable rate transmission (simulates market activity)  
✅ Configurable burst mode  
✅ Error injection (5% error rate)

## Supported Message Codes

### Compressed Messages (LZO1Z)
- **7200** - BCAST_MBO_MBP_UPDATE (Market By Order/MBP Update)
- **7201** - BCAST_MW_ROUND_ROBIN (Market Watch Round Robin)
- **7202** - BCAST_TICKER_AND_MKT_INDEX (Ticker and Market Index)
- **7208** - BCAST_ONLY_MBP (Market By Price Only)
- **7220** - BCAST_LIMIT_PRICE_PROTECTION_RANGE
- **17201** - BCAST_ENHNCD_MW_ROUND_ROBIN (Enhanced Market Watch)
- **17202** - BCAST_ENHNCD_TICKER_AND_MKT_INDEX (Enhanced Ticker)

### Uncompressed Messages
- **7203** - BCAST_INDUSTRY_INDEX_UPDATE
- **7206** - BCAST_SYSTEM_INFORMATION_OUT
- **7305** - BCAST_SECURITY_MSTR_CHG
- **7320** - BCAST_SECURITY_STATUS_CHG
- **6501** - BCAST_JRNL_VCT_MSG
- **6511** - BC_OPEN_MSG
- **6521** - BC_CLOSE_MSG

## Usage

### Running the UDP Analyzer

```bash
# Default port (9000)
go run udp_test.go lzo1z_ultra.go

# Custom port
go run udp_test.go lzo1z_ultra.go 8888
```

The analyzer will:
1. Start listening on the specified UDP port
2. Display periodic packet count updates
3. Process and analyze each packet
4. Attempt LZO decompression for compressed message types
5. Press `Ctrl+C` to stop and view detailed statistics

### Running the UDP Simulator

```bash
# Send to default port (9000)
go run udp_sender_test.go

# Send to custom port
go run udp_sender_test.go 8888
```

The simulator will:
1. Send 1000 simulated NSE broadcast packets
2. Mix of compressed and uncompressed message types
3. Variable transmission rate (10-60ms between packets)
4. Display progress every 100 packets
5. Show final statistics on completion

### Combined Testing

**Terminal 1** - Start the analyzer:
```bash
go run udp_test.go lzo1z_ultra.go
```

**Terminal 2** - Run the simulator:
```bash
go run udp_sender_test.go
```

Wait for simulator to complete, then press `Ctrl+C` in Terminal 1 to see statistics.

## Example Output

```
================================================================================
UDP MESSAGE CODE STATISTICS
================================================================================

📊 OVERALL STATISTICS
  Runtime:                45s
  Total Packets:          1000
  Total Bytes:            456789 (0.44 MB)
  Compressed Packets:     700
  Decompressed Success:   695
  Decompression Failures: 5
  Avg Packet Rate:        22.22 packets/sec
  Avg Data Rate:          9.93 KB/sec

📈 MESSAGE CODE BREAKDOWN (14 unique codes)
----------------------------------------------------------------------------------------------------
Code   Name                                       Count   Compressed(KB)        Raw(KB)      Ratio   Errors
----------------------------------------------------------------------------------------------------
7208   BCAST_ONLY_MBP                          🗜️     150           68.55          102.83       1.50x        -
7200   BCAST_MBO_MBP_UPDATE                    🗜️     120           47.46           71.19       1.50x        -
7202   BCAST_TICKER_AND_MKT_INDEX              🗜️     110           51.68           77.52       1.50x      2/1
7201   BCAST_MW_ROUND_ROBIN                    🗜️     100           45.31           67.97       1.50x        -
17202  BCAST_ENHNCD_TICKER_AND_MKT_INDEX       🗜️      90           42.89           64.34       1.50x        -
17201  BCAST_ENHNCD_MW_ROUND_ROBIN             🗜️      80           38.13           57.19       1.50x        -
7220   BCAST_LIMIT_PRICE_PROTECTION_RANGE      🗜️      50           16.60           24.90       1.50x        -
6501   BCAST_JRNL_VCT_MSG                           70           21.88           21.88          -         -
7305   BCAST_SECURITY_MSTR_CHG                      60           17.46           17.46          -         -
7320   BCAST_SECURITY_STATUS_CHG                    55           24.75           24.75          -       1/0
7203   BCAST_INDUSTRY_INDEX_UPDATE                  50           21.58           21.58          -         -
6511   BC_OPEN_MSG                                  35           10.94           10.94          -         -
6521   BC_CLOSE_MSG                                 20            6.25            6.25          -         -
7206   BCAST_SYSTEM_INFORMATION_OUT                 10            1.04            1.04          -         -
====================================================================================================

🗜️  COMPRESSION SUMMARY
  Total Compressed Data:   0.31 MB
  Total Raw Data:          0.47 MB
  Overall Compression:     1.50x
  Bytes Saved:             0.16 MB (33.3%)
```

## Statistics Explained

### Message Code Table Columns:
- **Code**: Transaction code number
- **Name**: Human-readable message name
- **🗜️**: Indicates compressed message types
- **Count**: Number of packets received
- **Compressed(KB)**: Total size of compressed data
- **Raw(KB)**: Total size after decompression
- **Ratio**: Compression ratio (Raw/Compressed)
- **Errors**: Format: `header_errors/decompression_errors`

### Compression Summary:
- **Total Compressed Data**: Sum of all compressed packet sizes
- **Total Raw Data**: Sum of all decompressed sizes
- **Overall Compression**: Average compression ratio across all messages
- **Bytes Saved**: Amount of bandwidth saved by compression

## Technical Details

### Broadcast Header Structure (40 bytes)
```
Offset  Size  Field               Type
------  ----  ------------------  ------
0       2     Reserved1           [2]byte
2       2     Reserved2           [2]byte
4       4     LogTime             uint32
8       2     AlphaChar           [2]byte
10      2     TransactionCode     uint16
12      2     ErrorCode           uint16
14      4     BCSeqNo             uint32
18      1     Reserved3           byte
19      3     Reserved4           [3]byte
22      8     TimeStamp2          [8]byte
30      8     Filler              [8]byte
38      2     MessageLength       uint16
```

### Compression Detection
Messages with these transaction codes are automatically detected as LZO1Z compressed:
- 7200, 7201, 7202, 7208, 7220
- 17201, 17202

### LZO Decompression
- Uses **lzo1z_ultra.go** - ultra-optimized pure Go implementation
- Handles overlapping copies and pattern repeats
- Target performance: 270-300 MB/s
- Supports payloads up to 10KB (configurable)

## Performance Considerations

- **Buffer Size**: 65535 bytes (max UDP packet size)
- **Decompression Buffer**: 10000 bytes (adjust based on max expected message size)
- **Packet Processing**: Concurrent with goroutine channel buffering (100 packets)
- **Statistics Updates**: Mutex-protected for thread safety
- **Memory Usage**: ~1MB per running instance

## Error Handling

The analyzer handles:
- ✅ Malformed packets (too small)
- ✅ Invalid broadcast headers
- ✅ Decompression failures
- ✅ Network errors
- ✅ Buffer overflow protection

## Limitations

1. **Simulated Compression**: The sender creates pseudo-compressed data (repeated patterns) rather than actual LZO compression
2. **Buffer Sizes**: Fixed buffer sizes may need adjustment for production use
3. **Single Port**: Analyzer listens on one port at a time
4. **Local Testing**: Sender uses localhost (127.0.0.1)

## Future Enhancements

- [ ] Real LZO compression in simulator
- [ ] Multiple port monitoring
- [ ] Export statistics to CSV/JSON
- [ ] Web dashboard for live monitoring
- [ ] Historical data storage
- [ ] Alert thresholds for errors
- [ ] Packet capture to PCAP format

## References

- **NSE NNF Protocol**: NSE_NNF_Broadcast_Analysis.md
- **LZO Algorithm**: http://www.oberhumer.com/opensource/lzo
- **NSE Documentation**: TP_FO_Trimmed.txt

## License

This test suite is for educational and testing purposes.
