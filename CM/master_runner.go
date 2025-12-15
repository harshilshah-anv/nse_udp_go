// NSE Capital Market Multicast UDP Receiver - Master Runner
// 
// PURPOSE: Run all message code decoders simultaneously
// SCHEDULE: Decoders will start automatically at 8:58 AM tomorrow
// OUTPUT: Separate CSV files for each message type
//
// USAGE:
// ======
// Run: go run master_runner.go
// Output: csv_output/message_XXXX_TIMESTAMP.csv (one file per message type)
//
// This master runner starts all message decoders in parallel:
// - Market control messages (6511, 6521, 6531, 6541, 6571, 6581, 6583, 6584)
// - Price/quote messages (7200, 7201, 7202, 7208, 7211, 7220, 7340)
// - Auction messages (7206, 7207, 7216, 7306)
// - Other message types (8207, 9010, 9011, 17202, 18703, 18720)

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Message decoder configuration
type MessageDecoder struct {
	Code        string
	Description string
	Filename    string
	Process     *exec.Cmd
	Context     context.Context
	Cancel      context.CancelFunc
}

// All available message decoders
var messageDecoders = []MessageDecoder{
	// Market Control Messages
	{Code: "6511", Description: "BC_OPEN_MESSAGE (Market Open)", Filename: "message_6511_live.go"},
	{Code: "6521", Description: "BC_CLOSE_MESSAGE (Market Close)", Filename: "message_6521_live.go"},
	{Code: "6531", Description: "BC_PREOPEN_SHUTDOWN_MSG", Filename: "message_6531_live.go"},
	{Code: "6541", Description: "BC_CIRCUIT_CHECK (Heartbeat)", Filename: "message_6541_live.go"},
	{Code: "6571", Description: "BC_NORMAL_MKT_PREOPEN_ENDED", Filename: "message_6571_live.go"},
	{Code: "6581", Description: "BC_AUCTION_STATUS_CHANGE", Filename: "message_6581_live.go"},
	{Code: "6583", Description: "BC_CLOSING_START", Filename: "message_6583_live.go"},
	{Code: "6584", Description: "BC_CLOSING_END", Filename: "message_6584_live.go"},
	
	// Price & Quote Messages
	{Code: "7200", Description: "BCAST_ONLY_MBO (Market by Order)", Filename: "message_7200_live.go"},
	{Code: "7201", Description: "BCAST_SECURITY_STATUS", Filename: "message_7201_live.go"},
	{Code: "7202", Description: "BCAST_TICKER_AND_MKT_INDEX", Filename: "message_7202_live.go"},
	{Code: "7208", Description: "BCAST_LTP_REPLY", Filename: "message_7208_live.go"},
	{Code: "7211", Description: "BCAST_MW_ROUND_ROBIN", Filename: "message_7211_live.go"},
	{Code: "7220", Description: "BCAST_ONLY_MBP (Market by Price)", Filename: "message_7220_live.go"},
	{Code: "7340", Description: "BCAST_DELTA_REPLY", Filename: "message_7340_live.go"},
	
	// Auction Messages
	{Code: "7206", Description: "BCAST_AUCTION_TRD_MEMBERS", Filename: "message_7206_live.go"},
	{Code: "7207", Description: "BCAST_AUCTION_MKTWATCH", Filename: "message_7207_live.go"},
	{Code: "7216", Description: "BCAST_MKT_MVMT_MSG", Filename: "message_7216_live.go"},
	{Code: "7306", Description: "BCAST_INDICES_REPLY", Filename: "message_7306_live.go"},
	
	// Special Messages
	{Code: "8207", Description: "BCAST_OPEN_MSG", Filename: "message_8207_live.go"},
	{Code: "9010", Description: "BCAST_TRADE_DT_TM_MSG", Filename: "message_9010_live.go"},
	{Code: "9011", Description: "BCAST_TRADE_TIM_MSG", Filename: "message_9011_live.go"},
	{Code: "17202", Description: "FO_TICKER_REPLY", Filename: "message_17202_live.go"},
	{Code: "18703", Description: "BCAST_TRADING_SESSION_CHG_MSG", Filename: "message_18703_live.go"},
	{Code: "18720", Description: "BCAST_SESSION_CHG_INDICATOR", Filename: "message_18720_live.go"},
}

var (
	activeDecoders []MessageDecoder
	wg             sync.WaitGroup
	shutdownChan   chan bool
	startTime      time.Time
	scheduleMode   bool = false  // Set to true to schedule for 8:58 AM tomorrow
)

func main() {
	startTime = time.Now()
	shutdownChan = make(chan bool)

	// Check if we should schedule for 8:58 AM tomorrow
	if len(os.Args) > 1 && os.Args[1] == "--schedule-858" {
		scheduleMode = true
		createScheduledTasks()
		return
	}

	// Check if we should wait until 8:58 AM tomorrow
	if len(os.Args) > 1 && os.Args[1] == "--wait-858" {
		waitUntil858AM()
	}

	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("NSE CAPITAL MARKET MULTICAST UDP RECEIVER - MASTER RUNNER\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
	fmt.Printf("🚀 Starting all message decoders simultaneously\n")
	fmt.Printf("📅 Session Date: %s\n", time.Now().Format("2006-01-02"))
	fmt.Printf("⏰ Start Time: %s\n", time.Now().Format("15:04:05"))
	fmt.Printf("🌐 Multicast: 233.1.2.5:8222 (Live Market)\n")
	fmt.Printf("📁 Output: csv_output/ directory\n")
	fmt.Printf("⏹️  Press Ctrl+C to stop all decoders\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n\n")

	// Create output directory
	if err := os.MkdirAll("csv_output", 0755); err != nil {
		log.Fatalf("❌ Failed to create csv_output directory: %v", err)
	}

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {
		<-sigChan
		fmt.Printf("\n\n⏹️  Shutdown signal received. Stopping all decoders...\n")
		close(shutdownChan)
	}()

	// Check which decoders exist
	availableDecoders := checkAvailableDecoders()
	if len(availableDecoders) == 0 {
		log.Fatalf("❌ No message decoder files found in current directory")
	}

	fmt.Printf("🔍 Found %d message decoders:\n", len(availableDecoders))
	fmt.Printf(strings.Repeat("-", 100) + "\n")
	fmt.Printf("%-8s %-50s %-20s\n", "Code", "Description", "Status")
	fmt.Printf(strings.Repeat("-", 100) + "\n")

	// Start all available decoders
	for _, decoder := range availableDecoders {
		if startDecoder(&decoder) {
			activeDecoders = append(activeDecoders, decoder)
			fmt.Printf("%-8s %-50s ✅ STARTED\n", decoder.Code, decoder.Description)
		} else {
			fmt.Printf("%-8s %-50s ❌ FAILED\n", decoder.Code, decoder.Description)
		}
	}

	if len(activeDecoders) == 0 {
		log.Fatalf("❌ Failed to start any decoders")
	}

	fmt.Printf(strings.Repeat("-", 100) + "\n")
	fmt.Printf("🚀 Successfully started %d/%d decoders\n\n", len(activeDecoders), len(availableDecoders))

	// Monitor decoders
	go monitorDecoders()

	// Wait for shutdown signal
	<-shutdownChan

	// Stop all decoders
	stopAllDecoders()

	fmt.Printf("\n✅ All decoders stopped successfully\n")
	printFinalSummary()
}

func checkAvailableDecoders() []MessageDecoder {
	var available []MessageDecoder
	
	for _, decoder := range messageDecoders {
		if _, err := os.Stat(decoder.Filename); err == nil {
			available = append(available, decoder)
		}
	}
	
	return available
}

func startDecoder(decoder *MessageDecoder) bool {
	// Create context for this decoder
	ctx, cancel := context.WithCancel(context.Background())
	decoder.Context = ctx
	decoder.Cancel = cancel

	// Start the decoder process
	cmd := exec.CommandContext(ctx, "go", "run", decoder.Filename)
	cmd.Dir = "."
	cmd.Stdout = nil // Suppress output to avoid clutter
	cmd.Stderr = nil // Suppress errors to avoid clutter
	
	decoder.Process = cmd
	
	if err := cmd.Start(); err != nil {
		cancel()
		return false
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		cmd.Wait()
	}()

	return true
}

func stopAllDecoders() {
	fmt.Printf("🛑 Stopping %d active decoders...\n", len(activeDecoders))
	
	// Cancel all decoder contexts
	for i := range activeDecoders {
		if activeDecoders[i].Cancel != nil {
			activeDecoders[i].Cancel()
		}
	}

	// Wait for all processes to finish (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Printf("✅ All decoders stopped gracefully\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("⚠️  Timeout waiting for decoders to stop, forcing termination\n")
		// Force kill any remaining processes
		for _, decoder := range activeDecoders {
			if decoder.Process != nil && decoder.Process.Process != nil {
				decoder.Process.Process.Kill()
			}
		}
	}
}

func monitorDecoders() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-shutdownChan:
			return
		case <-ticker.C:
			printStatus()
		}
	}
}

func printStatus() {
	duration := time.Since(startTime)
	running := 0
	
	for _, decoder := range activeDecoders {
		if decoder.Process != nil && decoder.Process.ProcessState == nil {
			running++
		}
	}

	fmt.Printf("📊 Status: %v runtime | %d/%d decoders running | Output: csv_output/\n", 
		duration.Truncate(time.Second), running, len(activeDecoders))
}

func printFinalSummary() {
	duration := time.Since(startTime)
	
	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("FINAL SUMMARY - NSE MESSAGE DECODER MASTER RUNNER\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
	fmt.Printf("⏰ Session Duration: %v\n", duration.Truncate(time.Second))
	fmt.Printf("📅 Session Date: %s\n", startTime.Format("2006-01-02"))
	fmt.Printf("🕰️  Start Time: %s\n", startTime.Format("15:04:05"))
	fmt.Printf("🏁 End Time: %s\n", time.Now().Format("15:04:05"))
	fmt.Printf("🚀 Decoders Started: %d\n", len(activeDecoders))
	
	fmt.Printf("\n📁 OUTPUT FILES GENERATED:\n")
	fmt.Printf(strings.Repeat("-", 100) + "\n")
	
	// List all generated CSV files
	files, err := filepath.Glob("csv_output/message_*_*.csv")
	if err == nil && len(files) > 0 {
		fmt.Printf("Found %d CSV files in csv_output/:\n", len(files))
		for _, file := range files {
			info, _ := os.Stat(file)
			if info != nil {
				fmt.Printf("  📄 %s (%.1f KB)\n", filepath.Base(file), float64(info.Size())/1024)
			}
		}
	} else {
		fmt.Printf("⚠️  No CSV files found - check if market data was available\n")
	}

	fmt.Printf("\n🎯 DECODER SUMMARY:\n")
	fmt.Printf(strings.Repeat("-", 100) + "\n")
	fmt.Printf("%-8s %-50s %-15s\n", "Code", "Description", "Status")
	fmt.Printf(strings.Repeat("-", 100) + "\n")

	for _, decoder := range activeDecoders {
		status := "✅ COMPLETED"
		if decoder.Process != nil && decoder.Process.ProcessState != nil && !decoder.Process.ProcessState.Success() {
			status = "⚠️  ERROR"
		}
		fmt.Printf("%-8s %-50s %-15s\n", decoder.Code, decoder.Description, status)
	}

	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("✅ NSE MESSAGE CAPTURE SESSION COMPLETED\n")
	fmt.Printf("📊 Check csv_output/ directory for all captured messages\n")
	fmt.Printf("💡 Each message type has its own CSV file with timestamp\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
}

// =============================================================================
// SCHEDULING FUNCTIONS FOR 8:58 AM
// =============================================================================

func waitUntil858AM() {
	// Set target to 8:58 AM tomorrow
	tomorrow := time.Now().AddDate(0, 0, 1)
	target := time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 8, 58, 0, 0, tomorrow.Location())
	
	duration := time.Until(target)
	if duration < 0 {
		fmt.Printf("⚠️  8:58 AM has already passed. Will start immediately.\n")
		return
	}

	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("⏰ SCHEDULED START MODE - WAITING FOR 8:58 AM TOMORROW\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
	fmt.Printf("🕘 Target Start Time: %s\n", target.Format("2006-01-02 15:04:05"))
	fmt.Printf("⏳ Time Until Start: %v\n", duration.Truncate(time.Second))
	fmt.Printf("💤 Waiting... (Press Ctrl+C to cancel)\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n\n")

	// Setup signal handling during wait
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Show countdown every 30 seconds
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-time.After(duration):
			fmt.Printf("\n🎯 8:58 AM REACHED! Starting all decoders...\n\n")
			return
		case <-ticker.C:
			remaining := time.Until(target)
			if remaining > 0 {
				fmt.Printf("⏳ Time remaining: %v\n", remaining.Truncate(time.Second))
			}
		case <-sigChan:
			fmt.Printf("\n⏹️  Scheduling cancelled by user.\n")
			os.Exit(0)
		}
	}
}

func createScheduledTasks() {
	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("📅 CREATING SCHEDULED TASK FOR 8:58 AM TOMORROW\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
	
	tomorrow := time.Now().AddDate(0, 0, 1)
	targetDate := tomorrow.Format("01/02/2006")
	
	fmt.Printf("📅 Target Date: %s\n", targetDate)
	fmt.Printf("🕘 Target Time: 8:58 AM\n")
	fmt.Printf("📁 Working Directory: d:\\go-udp-reader\\CM\n")
	fmt.Printf("🎯 Command: go run master_runner.go --wait-858\n\n")

	// Create the scheduled task command for 8:58 AM tomorrow
	taskName := "NSE_Master_Runner_858AM"
	taskCmd := fmt.Sprintf(`schtasks /create /tn "%s" /tr "cmd /c cd /d d:\go-udp-reader\CM && go run master_runner.go --wait-858" /sc once /st 08:58 /sd %s /rl highest /f`, 
		taskName, targetDate)

	// Execute the command
	cmd := exec.Command("cmd", "/c", taskCmd)
	output, err := cmd.CombinedOutput()
	
	if err != nil {
		fmt.Printf("❌ Error creating scheduled task: %v\n", err)
		fmt.Printf("Output: %s\n", string(output))
		fmt.Printf("\n🔧 MANUAL SETUP INSTRUCTIONS:\n")
		fmt.Printf("1. Open Command Prompt as Administrator\n")
		fmt.Printf("2. Run this command:\n")
		fmt.Printf("%s\n", taskCmd)
		return
	}

	fmt.Printf("✅ Scheduled task created successfully!\n")
	fmt.Printf("📋 Task Name: %s\n", taskName)
	fmt.Printf("🔍 View task: schtasks /query /tn \"%s\"\n", taskName)
	fmt.Printf("▶️  Run now: schtasks /run /tn \"%s\"\n", taskName)
	fmt.Printf("🗑️  Delete: schtasks /delete /tn \"%s\" /f\n", taskName)

	fmt.Printf("\n" + strings.Repeat("=", 100) + "\n")
	fmt.Printf("🎉 SETUP COMPLETE!\n")
	fmt.Printf("⏰ NSE Master Runner will start automatically tomorrow at 8:58 AM\n")
	fmt.Printf("📊 All message decoders will capture data to csv_output/ directory\n")
	fmt.Printf(strings.Repeat("=", 100) + "\n")
}
