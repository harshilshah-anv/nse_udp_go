package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
)

func main() {
	// List of message files to fix
	messageFiles := []string{
		"message_6511_live.go",
		"message_6521_live.go", 
		"message_6571_live.go",
		"message_6581_live.go",
		"message_6583_live.go",
		"message_6584_live.go",
		"message_7206_live.go",
	}
	
	basePath := "d:\\go-udp-reader\\CM\\"
	
	for _, filename := range messageFiles {
		filePath := basePath + filename
		fmt.Printf("Fixing %s...\n", filename)
		
		// Read file
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Printf("Error reading %s: %v", filename, err)
			continue
		}
		
		contentStr := string(content)
		
		// Fix 1: Add context and sync to imports
		importRegex := regexp.MustCompile(`import \(\s*\n(\s*"encoding/binary"[\s\S]*?)\)`)
		contentStr = importRegex.ReplaceAllStringFunc(contentStr, func(match string) string {
			if !strings.Contains(match, "context") {
				return strings.Replace(match, `"encoding/binary"`, `"context"
	"encoding/binary"`, 1)
			}
			if !strings.Contains(match, `"sync"`) {
				return strings.Replace(match, `"sync/atomic"`, `"sync"
	"sync/atomic"`, 1)
			}
			return match
		})
		
		// Fix 2: Remove shutdownChan and packetChan from global vars
		varRegex := regexp.MustCompile(`(\s+// Control channels\s+startTime\s+time\.Time)\s+shutdownChan\s+chan bool\s+packetChan\s+chan \[\]byte`)
		contentStr = varRegex.ReplaceAllString(contentStr, `$1`)
		
		// Fix 3: Fix main function
		mainFuncRegex := regexp.MustCompile(`func main\(\) \{[\s\S]*?defer ticker\.Stop\(\)\s+for \{[\s\S]*?\}\s*\}`)
		contentStr = mainFuncRegex.ReplaceAllStringFunc(contentStr, func(match string) string {
			// Extract message code from filename
			codeRegex := regexp.MustCompile(`message_(\d+)_live\.go`)
			codeMatches := codeRegex.FindStringSubmatch(filename)
			if len(codeMatches) < 2 {
				return match
			}
			msgCode := codeMatches[1]
			
			// Generate new main function
			return generateMainFunction(msgCode)
		})
		
		// Fix 4: Update UDP Listener function
		listenerRegex := regexp.MustCompile(`func startUDPListener\(\) \{[\s\S]*?\}\s*}`)
		contentStr = listenerRegex.ReplaceAllStringFunc(contentStr, func(match string) string {
			return generateUDPListener()
		})
		
		// Fix 5: Update packet processor function
		processorRegex := regexp.MustCompile(`func processPackets\(\) \{[\s\S]*?\}\s*}`)
		contentStr = processorRegex.ReplaceAllStringFunc(contentStr, func(match string) string {
			return generatePacketProcessor()
		})
		
		// Write fixed content back
		err = ioutil.WriteFile(filePath, []byte(contentStr), 0644)
		if err != nil {
			log.Printf("Error writing %s: %v", filename, err)
			continue
		}
		
		fmt.Printf("✅ Fixed %s\n", filename)
	}
	
	fmt.Println("🎉 All files fixed successfully!")
}

func generateMainFunction(msgCode string) string {
	return fmt.Sprintf(`func main() {
	// Initialize tracking
	messageCodeCounts = make(map[uint16]int64)

	startTime = time.Now()

	fmt.Printf("\n" + strings.Repeat("=", 80) + "\n")
	fmt.Printf("NSE CM UDP Receiver - Message %s\n", msgCode)
	fmt.Printf(strings.Repeat("=", 80) + "\n")
	fmt.Printf("Listening for message code %s\n", msgCode)
	fmt.Printf("Press Ctrl+C to stop\n")
	fmt.Printf(strings.Repeat("=", 80) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("Failed to create csv_output directory: %%v", err)
		return
	}

	// Initialize CSV file for %s
	if err := initialize%sCSV(); err != nil {
		log.Fatalf("Failed to initialize CSV: %%v", err)
		return
	}
	defer func() {
		if csvWriter%s != nil {
			csvWriter%s.Flush()
		}
		if csvFile%s != nil {
			csvFile%s.Close()
		}
	}()

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create packet channel
	packetChan := make(chan []byte, 100)

	// Start goroutines
	wg.Add(2)
	go func() {
		defer wg.Done()
		startUDPListener(ctx, packetChan)
	}()
	go func() {
		defer wg.Done()
		processPackets(ctx, packetChan)
	}()

	// Print statistics every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Main loop
	for {
		select {
		case <-ticker.C:
			printStats()
		case <-sigChan:
			fmt.Printf("\n\n⏹️  Shutdown signal received. Stopping gracefully...\n")
			cancel() // Cancel context to signal all goroutines
			
			// Wait for goroutines to finish with timeout
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			
			select {
			case <-done:
				fmt.Printf("✅ All goroutines stopped successfully\n")
			case <-time.After(3 * time.Second):
				fmt.Printf("⚠️  Timeout waiting for goroutines to stop\n")
			}
			
			printFinalStats()
			fmt.Printf("👋 Program terminated successfully\n")
			return
		}
	}
}`, msgCode, msgCode, msgCode, msgCode, msgCode, msgCode, msgCode)
}

func generateUDPListener() string {
	return `func startUDPListener(ctx context.Context, packetChan chan<- []byte) {
	// Live Market Hours Multicast
	multicastIP := "233.1.2.5"
	port := 8222

	// After Market Hours Multicast
	//multicastIP := "231.31.31.4"
	//port := 18901

	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", multicastIP, port))
	if err != nil {
		log.Printf("Failed to resolve UDP address: %v", err)
		return
	}

	conn, err := net.ListenMulticastUDP("udp4", nil, addr)
	if err != nil {
		log.Printf("Failed to listen on multicast address: %v", err)
		return
	}
	defer conn.Close()

	// Set buffer size to 2MB for high throughput
	conn.SetReadBuffer(2 * 1024 * 1024)
	
	// Set read timeout to allow checking context cancellation
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))

	fmt.Printf("🌐 Listening on %s:%d\n\n", multicastIP, port)

	buffer := make([]byte, 65536)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 UDP listener stopping...\n")
			return
		default:
			// Set read timeout for non-blocking behavior
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			
			n, err := conn.Read(buffer)
			if err != nil {
				// Check if it's a timeout error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // Continue loop to check context
				}
				continue // Other errors, continue
			}

			atomic.AddInt64(&packetCount, 1)
			atomic.AddInt64(&totalBytes, int64(n))

			// Send copy of data to processing channel
			dataCopy := make([]byte, n)
			copy(dataCopy, buffer[:n])
			
			select {
			case packetChan <- dataCopy:
			case <-ctx.Done():
				fmt.Printf("🛑 UDP listener stopping due to context cancellation...\n")
				return
			default:
				// Drop packet if channel is full and context not cancelled
			}
		}
	}
}`
}

func generatePacketProcessor() string {
	return `func processPackets(ctx context.Context, packetChan <-chan []byte) {
	for {
		select {
		case data := <-packetChan:
			processUDPPacket(data)
		case <-ctx.Done():
			fmt.Printf("🛑 Packet processor stopping...\n")
			return
		}
	}
}`
}
