package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	// "time"
	"encoding/binary"
)

func main() {
	fmt.Println("🚀 Starting UDP Multicast Receiver")
	fmt.Println("📡 Target: 233.1.2.5:55655")
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		fmt.Println("\n🛑 Shutdown signal received...")
		cancel()
	}()
	
	// Start multicast receiver
	if err := startMulticastReceiver(ctx, "233.1.2.5", 55655); err != nil {
		fmt.Printf("❌ Error: %v\n", err)
		os.Exit(1)
	}
	
	fmt.Println("✅ Multicast receiver stopped")
}

func startMulticastReceiver(ctx context.Context, multicastIP string, port int) error {
	// Resolve multicast address
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", multicastIP, port))
	if err != nil {
		return fmt.Errorf("failed to resolve address: %v", err)
	}
	
	fmt.Printf("🔌 Listening on: %s\n", addr.String())
	
	// Create UDP connection
	conn, err := net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to create multicast listener: %v", err)
	}
	defer conn.Close()
	
	fmt.Printf("✅ Multicast receiver active\n")
	
	// Start receiving packets
	return receivePackets(ctx, conn)
}

func receivePackets(ctx context.Context, conn *net.UDPConn) error {
	fmt.Println("📊 Waiting for UDP packets... (Press Ctrl+C to stop)")
	
	buffer := make([]byte, 1024) // Standard UDP buffer
	packetCount := 0
	// startTime := time.Now()
	// i dont want statistics as of now just read packets
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, _, err := conn.ReadFromUDP(buffer)

			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Timeout is expected
				}
				return fmt.Errorf("read error: %v", err)
			}
			packetCount++
			// fmt.Printf("📥 Received %d bytes from %s\n", n, addr.String())
			parsePacket(buffer)
			// time.Sleep(10 * time.Millisecond) // to avoid flooding the output
		}
	}
}


// create function to parse packets 
func parsePacket(data []byte) {
	// first 2 bytes is CHAR cNetId [2]
	// SHORT iNoPackets
	// CHAR cPackData [512]
	
	
	// cNetId := data[0:2]
	// iNoOfPackets := data[2:4]
	cPackData := data[4:]
	// noPackets := binary.BigEndian.Uint16(iNoOfPackets)

	iCompLen := binary.BigEndian.Uint16(cPackData[0:2])

	// fmt.Printf("📋 NetID: %02x%02x | No. of Packets: %d\n", cNetId[0], cNetId[1], noPackets)	
	// fmt.Printf("   First Packet Compressed Length: %d bytes\n", iCompLen)
	if iCompLen > 0 {
		offset := 2
		decompressBuffer := make([]byte, 1024) // Pre-allocate decompression buffer (NSE typical size)
		// fmt.Printf("   uncompressing: \n")
			// Read compressed length for this packet
			
		
		compressedPacket := cPackData[offset : offset+int(iCompLen)]
		
		// Decompress using DecompressUltra (requires src and dst buffers)
		decompLen,err := DecompressUltra(compressedPacket, decompressBuffer)
		if err != nil {
			fmt.Printf("   ❌ Decompression error: %v\n", err)
		}

		decompressedPacket := decompressBuffer[:decompLen]
		

		message_code := binary.BigEndian.Uint16(decompressedPacket[18:20])			
		
		// fmt.Printf("   📬 Message Code: %d\n", message_code)
		if message_code == 7208 {
			token := binary.BigEndian.Uint32(decompressedPacket[50:54])
			
			// Only print if it's NIFTY token 37054
			if token == 37054 {
				fmt.Printf("🔥 NIFTY TOKEN 37054 FOUND! 🔥\n")
				// fmt.Printf("   📬 Message Code: %d | Token: %d\n", message_code, token)
				
				// Extract price information (typically at offset 54-58 after token)
				if len(decompressedPacket) >= 58 {
					// Try to extract LTP (Last Traded Price)
					ltpRaw := binary.BigEndian.Uint32(decompressedPacket[62:66])
					ltp := float64(ltpRaw) / 100.0 // NSE prices are in paise (divide by 100)
					
					fmt.Printf("   � Possible LTP: %.2f\n", ltp)
				}
				
				// Print hex dump of the packet for analysis
				// fmt.Println("   📋 Decompressed packet hex dump:")
				// printHexDump(decompressedPacket, 1)
			}
		}

		offset += 2 + int(iCompLen)
	}
}

// printHexDump prints a hex dump of the data in a readable format
func printHexDump(data []byte, packetNum int) {
	fmt.Printf("   📄 Hex Dump (Packet %d):\n", packetNum)
	
	bytesPerLine := 16
	for i := 0; i < len(data); i += bytesPerLine {
		// Print offset
		fmt.Printf("   %04x: ", i)
		
		// Print hex values
		for j := 0; j < bytesPerLine; j++ {
			if i+j < len(data) {
				fmt.Printf("%02x ", data[i+j])
			} else {
				fmt.Printf("   ")
			}
			
			// Add extra space in the middle for readability
			if j == 7 {
				fmt.Printf(" ")
			}
		}
		
		// Print ASCII representation
		fmt.Printf(" |")
		for j := 0; j < bytesPerLine && i+j < len(data); j++ {
			b := data[i+j]
			if b >= 32 && b <= 126 {
				fmt.Printf("%c", b)
			} else {
				fmt.Printf(".")
			}
		}
		fmt.Printf("|\n")
	}
	fmt.Printf("   Total: %d bytes\n", len(data))
}

