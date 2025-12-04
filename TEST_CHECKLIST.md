# UDP Test Suite - Verification Checklist

## Pre-Test Verification

- [ ] Go is installed and in PATH (`go version`)
- [ ] All files are present in `d:\go-udp-reader\`
  - [ ] udp_test.go
  - [ ] udp_sender_test.go
  - [ ] lzo1z_ultra.go
  - [ ] run_test.bat
- [ ] Port 9000 is available (or choose alternate port)
- [ ] Windows Firewall allows UDP on selected port

## Test Execution Checklist

### Quick Test (Recommended)
- [ ] Run `run_test.bat`
- [ ] Select option 3 (Run both)
- [ ] Verify analyzer window shows "Listening on port 9000"
- [ ] Verify sender window starts sending packets
- [ ] Watch for "📤 Sent X packets" messages in sender
- [ ] Watch for "📦 Received X packets" messages in analyzer
- [ ] Wait for sender to complete (1000 packets)
- [ ] Press Ctrl+C in analyzer window
- [ ] Verify statistics report is displayed

### Manual Test
- [ ] Terminal 1: `go run udp_test.go lzo1z_ultra.go`
- [ ] Verify "🎧 Listening for UDP packets on port 9000"
- [ ] Terminal 2: `go run udp_sender_test.go`
- [ ] Verify "🚀 Sending simulated NSE broadcast packets"
- [ ] Monitor both terminals for progress
- [ ] Sender completes with "✅ Completed sending" message
- [ ] Stop analyzer with Ctrl+C
- [ ] Statistics report appears in Terminal 1

## Output Verification

### Analyzer Output Should Show:
- [ ] "UDP MESSAGE CODE STATISTICS" header
- [ ] Overall statistics section with:
  - [ ] Total packets count
  - [ ] Total bytes (MB)
  - [ ] Compressed packets count
  - [ ] Decompression success count
  - [ ] Packet rate (packets/sec)
  - [ ] Data rate (KB/sec)
- [ ] Message code breakdown table with:
  - [ ] Multiple message codes (7200, 7201, 7202, etc.)
  - [ ] 🗜️ emoji for compressed messages
  - [ ] Packet counts for each code
  - [ ] Compressed and raw sizes
  - [ ] Compression ratios (e.g., "1.50x")
- [ ] Compression summary section with:
  - [ ] Total compressed data
  - [ ] Total raw data
  - [ ] Overall compression ratio
  - [ ] Bytes saved

### Sender Output Should Show:
- [ ] "🚀 Sending simulated NSE broadcast packets"
- [ ] Progress updates every 100 packets
- [ ] Message type names and codes
- [ ] Packet rate (pkt/s)
- [ ] "✅ Completed sending X packets"
- [ ] Average rate in packets/second

## Statistics Validation

### Expected Values (1000 packets)
- [ ] Total packets: 1000
- [ ] Total bytes: 400-500 KB
- [ ] Compressed packets: 600-800
- [ ] Decompression success: 0-800 (simulator sends uncompressed)
- [ ] Message codes present: 10-14 different codes
- [ ] Packet rate: 20-50 packets/sec
- [ ] Data rate: 10-20 KB/sec

### Message Code Validation
Verify these codes appear:
- [ ] 7200 - BCAST_MBO_MBP_UPDATE
- [ ] 7201 - BCAST_MW_ROUND_ROBIN
- [ ] 7202 - BCAST_TICKER_AND_MKT_INDEX
- [ ] 7208 - BCAST_ONLY_MBP
- [ ] 7220 - BCAST_LIMIT_PRICE_PROTECTION_RANGE
- [ ] 17201 - BCAST_ENHNCD_MW_ROUND_ROBIN
- [ ] 17202 - BCAST_ENHNCD_TICKER_AND_MKT_INDEX
- [ ] 6501 - BCAST_JRNL_VCT_MSG
- [ ] 6511 - BC_OPEN_MSG
- [ ] 6521 - BC_CLOSE_MSG

### Compression Validation
For compressed messages (7200, 7201, 7202, 7208, 7220, 17201, 17202):
- [ ] Shows 🗜️ emoji indicator
- [ ] Has compression ratio displayed
- [ ] Raw size ≥ Compressed size
- [ ] Ratio is typically 1.0x - 2.0x

## Error Handling Verification

### Test Scenarios:
1. **Port Already in Use**
   - [ ] Start analyzer twice on same port
   - [ ] Verify error message: "failed to listen on UDP port"
   - [ ] Solution: Use different port

2. **No Sender Running**
   - [ ] Start only analyzer
   - [ ] Wait 30 seconds
   - [ ] Verify "Received 0 packets" in periodic updates
   - [ ] Start sender
   - [ ] Verify packets start arriving

3. **Decompression Errors**
   - [ ] Normal for simulator (sends uncompressed test data)
   - [ ] Verify "Decompression Failures" count in statistics
   - [ ] Should be same as "Compressed Packets" count

## Performance Verification

### Sender Performance:
- [ ] CPU usage < 10%
- [ ] Memory usage < 20 MB
- [ ] Completes in 20-60 seconds
- [ ] No errors during transmission
- [ ] Smooth progress updates

### Analyzer Performance:
- [ ] CPU usage < 20%
- [ ] Memory usage < 100 MB
- [ ] Responds to Ctrl+C immediately
- [ ] No packet loss reported
- [ ] Statistics generation < 1 second

## Documentation Verification

- [ ] QUICKSTART.md exists and is readable
- [ ] TEST_README.md exists and is readable
- [ ] ARCHITECTURE.md exists and is readable
- [ ] NSE_NNF_Broadcast_Analysis.md exists and is readable
- [ ] SUMMARY.md exists and is readable
- [ ] All documentation is consistent

## Build Verification (Optional)

### Build Binaries:
```cmd
go build -o udp_analyzer.exe udp_test.go lzo1z_ultra.go
go build -o udp_sender.exe udp_sender_test.go
```

- [ ] udp_analyzer.exe created successfully
- [ ] udp_sender.exe created successfully
- [ ] File size: analyzer ~2-3 MB, sender ~1-2 MB
- [ ] Binaries run without Go installed

### Run Built Binaries:
- [ ] `udp_analyzer.exe` starts and listens
- [ ] `udp_sender.exe` sends packets
- [ ] Both work identically to `go run` versions

## Troubleshooting Completed

If any issues encountered:
- [ ] Checked firewall settings
- [ ] Verified port availability
- [ ] Confirmed Go installation
- [ ] Reviewed error messages
- [ ] Consulted TEST_README.md troubleshooting section
- [ ] Issue resolved or documented

## Final Validation

### Overall Test Success Criteria:
- [ ] Sender successfully sends 1000 packets
- [ ] Analyzer receives all 1000 packets
- [ ] Statistics report generates correctly
- [ ] At least 10 different message codes present
- [ ] Compressed messages identified (🗜️ emoji)
- [ ] Compression ratios calculated
- [ ] No crashes or hangs
- [ ] Clean shutdown with Ctrl+C
- [ ] Output is readable and formatted correctly

### Test Result Summary:
```
Date: _______________
Time: _______________
Test Duration: _______________
Packets Sent: _______________
Packets Received: _______________
Packet Loss: _______________
Test Status: [ ] PASS  [ ] FAIL
```

### Notes:
```
_________________________________________________________________
_________________________________________________________________
_________________________________________________________________
_________________________________________________________________
```

## Sign-off

Tested by: _______________________  
Date: _______________  
Test Result: [ ] PASS  [ ] FAIL  

---

## Quick Status Check

Run this command to verify all files:
```cmd
dir /b udp_test.go udp_sender_test.go lzo1z_ultra.go run_test.bat
```

Expected output:
```
lzo1z_ultra.go
run_test.bat
udp_sender_test.go
udp_test.go
```

All 4 files should be listed. If any missing, check file location.

---

## Ready to Test?

If all pre-test items are checked:
```cmd
run_test.bat
```

Select option 3 and watch the magic happen! ✨
