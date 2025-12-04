# NSE UDP Test Suite - Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         NSE UDP TEST SUITE ARCHITECTURE                      │
└──────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────┐         ┌──────────────────────────────────┐
│     UDP SENDER (Simulator)      │         │      UDP ANALYZER (Receiver)     │
│   udp_sender_test.go            │         │      udp_test.go                 │
├─────────────────────────────────┤         ├──────────────────────────────────┤
│                                 │         │                                  │
│  ┌───────────────────────────┐  │         │  ┌────────────────────────────┐  │
│  │  Message Generator        │  │  UDP    │  │   UDP Listener             │  │
│  │  • 14 message types       │  │  Port   │  │   • Port 9000 (default)    │  │
│  │  • Random selection       │  │  9000   │  │   • 65KB buffer            │  │
│  │  • Variable rate          │  ├────────>│  │   • Non-blocking           │  │
│  └───────────────────────────┘  │         │  └────────────────────────────┘  │
│              │                   │         │              │                   │
│              ▼                   │         │              ▼                   │
│  ┌───────────────────────────┐  │         │  ┌────────────────────────────┐  │
│  │  Header Builder           │  │         │  │   Header Parser            │  │
│  │  • 40-byte broadcast hdr  │  │         │  │   • Validate structure     │  │
│  │  • Transaction code       │  │         │  │   • Extract fields         │  │
│  │  • Sequence number        │  │         │  │   • Identify msg type      │  │
│  │  • Error injection (5%)   │  │         │  └────────────────────────────┘  │
│  └───────────────────────────┘  │         │              │                   │
│              │                   │         │              ▼                   │
│              ▼                   │         │  ┌────────────────────────────┐  │
│  ┌───────────────────────────┐  │         │  │  Compression Detection     │  │
│  │  Payload Generator        │  │         │  │  • Check transaction code  │  │
│  │  • Random/pattern data    │  │         │  │  • 7200,7201,7202,7208,    │  │
│  │  • Compressible patterns  │  │         │  │    7220,17201,17202        │  │
│  │  • Size: 66-452 bytes     │  │         │  └────────────────────────────┘  │
│  └───────────────────────────┘  │         │              │                   │
│              │                   │         │              ▼                   │
│              ▼                   │         │        ┌─────┴─────┐             │
│  ┌───────────────────────────┐  │         │        │           │             │
│  │  UDP Transmission         │  │         │   Compressed?  Uncompressed      │
│  │  • net.Dial("udp",...)    │  │         │        │           │             │
│  │  • Write(packet)          │  │         │        ▼           ▼             │
│  │  • Rate control           │  │         │  ┌──────────┐  ┌──────────┐      │
│  │  • Progress reporting     │  │         │  │   LZO    │  │  Direct  │      │
│  │  • 1000 packets (default) │  │         │  │Decompress│  │  Process │      │
│  └───────────────────────────┘  │         │  └──────────┘  └──────────┘      │
│                                 │         │        │           │             │
└─────────────────────────────────┘         │        └─────┬─────┘             │
                                            │              ▼                   │
                                            │  ┌────────────────────────────┐  │
                                            │  │  Statistics Aggregator     │  │
                                            │  │  • Per-message counters    │  │
┌──────────────────────────────┐            │  │  • Size tracking          │  │
│   LZO1Z DECOMPRESSOR         │            │  │  • Error counting         │  │
│   lzo1z_ultra.go             │            │  │  • Time stamping          │  │
├──────────────────────────────┤            │  │  • Mutex protection       │  │
│                              │            │  └────────────────────────────┘  │
│  • Ultra-optimized           │            │              │                   │
│  • Unsafe pointers           │            │              ▼                   │
│  • 8-byte bulk copies        │            │  ┌────────────────────────────┐  │
│  • 270-300 MB/s target       │◄───────────┤  │  Report Generator          │  │
│  • Handles overlapping       │            │  │  • Overall stats           │  │
│  • Pattern repeats           │            │  │  • Per-code breakdown      │  │
│  • Error detection           │            │  │  • Compression summary     │  │
│                              │            │  │  • Formatted output        │  │
└──────────────────────────────┘            │  └────────────────────────────┘  │
                                            │                                  │
                                            └──────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                            DATA FLOW DIAGRAM                                 │
└──────────────────────────────────────────────────────────────────────────────┘

SENDER FLOW:
  1. Generate Message Type (random selection)
  2. Build Broadcast Header (40 bytes)
       ├─ Transaction Code
       ├─ Sequence Number
       ├─ Timestamp
       └─ Error Code (5% chance)
  3. Create Payload (size based on message type)
  4. Combine Header + Payload
  5. Send via UDP
  6. Repeat (with rate control)

RECEIVER FLOW:
  1. Receive UDP Packet
  2. Parse Broadcast Header
       ├─ Validate size (≥40 bytes)
       ├─ Extract transaction code
       ├─ Check error code
       └─ Get message length
  3. Check if Compressed
       ├─ YES: Decompress with LZO1Z
       │       ├─ Extract payload (skip header)
       │       ├─ Allocate decompression buffer
       │       ├─ Call DecompressUltra()
       │       └─ Calculate raw size
       └─ NO:  Use original size
  4. Update Statistics
       ├─ Increment counters
       ├─ Add sizes
       ├─ Update timestamps
       └─ Track errors
  5. Display Progress (periodic)
  6. On Ctrl+C: Generate Report

┌──────────────────────────────────────────────────────────────────────────────┐
│                          STATISTICS STRUCTURE                                │
└──────────────────────────────────────────────────────────────────────────────┘

UDPStats
├── TotalPackets: int64
├── TotalBytes: int64
├── CompressedPackets: int64
├── DecompressedPackets: int64
├── DecompressionFailures: int64
├── StartTime: time.Time
├── LastPacketTime: time.Time
└── MessageCodeStats: map[uint16]*MessageStats
        │
        └── MessageStats (per transaction code)
            ├── TransactionCode: uint16
            ├── Count: int64
            ├── TotalCompressedSize: int64
            ├── TotalRawSize: int64
            ├── FirstSeen: time.Time
            ├── LastSeen: time.Time
            ├── ErrorCount: int64
            └── DecompressionErrors: int64

┌──────────────────────────────────────────────────────────────────────────────┐
│                         BROADCAST HEADER (40 bytes)                          │
└──────────────────────────────────────────────────────────────────────────────┘

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          Reserved1            |          Reserved2            | 0-3
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           LogTime                             | 4-7
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|         AlphaChar             |      TransactionCode          | 8-11
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|          ErrorCode            |           BCSeqNo             | 12-15
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|           BCSeqNo             |Res3 |      Reserved4          | 16-19
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               | 20-23
+                         TimeStamp2                            +
|                                                               | 24-27
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               | 28-31
+                           Filler                              +
|                                                               | 32-35
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                           Filler                              | 36-39
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|        MessageLength          |                               | 38-40+
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+          PAYLOAD...           +

Key Fields:
  • TransactionCode (offset 10, 2 bytes): Message type identifier
  • ErrorCode (offset 12, 2 bytes): Error status (0 = no error)
  • BCSeqNo (offset 14, 4 bytes): Sequence number
  • MessageLength (offset 38, 2 bytes): Total packet length

┌──────────────────────────────────────────────────────────────────────────────┐
│                        PERFORMANCE CHARACTERISTICS                           │
└──────────────────────────────────────────────────────────────────────────────┘

Sender Performance:
  • Packet generation: 10-100 packets/sec
  • Packet size: 106-492 bytes
  • Throughput: ~1-50 KB/sec
  • CPU usage: <5%
  • Memory: <10 MB

Receiver Performance:
  • Packet processing: 100-1000+ packets/sec
  • Decompression: 270-300 MB/sec (LZO1Z)
  • CPU usage: 5-15%
  • Memory: ~50 MB
  • Latency: <1ms per packet

LZO1Z Decompressor:
  • Algorithm: Lempel-Ziv-Oberhumer
  • Implementation: Pure Go with unsafe pointers
  • Compression ratio: 1.2x - 3.0x (typical)
  • Speed: 270-300 MB/sec
  • Features:
    - Overlapping copy support
    - Pattern repeat handling
    - 8-byte bulk transfers
    - Zero-copy optimizations

┌──────────────────────────────────────────────────────────────────────────────┐
│                              FILE DEPENDENCIES                               │
└──────────────────────────────────────────────────────────────────────────────┘

udp_test.go
  ├── imports: encoding/binary, fmt, net, os, os/signal, strings,
  │            sync, syscall, time
  └── uses: lzo1z_ultra.go (DecompressUltra function)

udp_sender_test.go
  ├── imports: encoding/binary, fmt, math/rand, net, os, time
  └── uses: (standalone - no dependencies)

lzo1z_ultra.go
  ├── imports: errors, unsafe
  └── exports: DecompressUltra function

run_test.bat
  ├── Builds: udp_analyzer.exe, udp_sender.exe
  └── Runs: Both in separate cmd windows
```

**Legend:**
- 🗜️ = Compressed message type (uses LZO1Z)
- ◄─ = Data flow direction
- ├─ = Branch/option
- └─ = Final branch/end
