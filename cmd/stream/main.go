package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sosodev/duration"
	"github.com/tendant/nats-utils/processor"
)

type Config struct {
	NatsURL string
	Streams string
	Action  string
	// Phase 2: Retention policy configuration
	StreamName    string
	Subjects      string
	Retention     string
	MaxAge        string
	MaxBytes      int64
	MaxMsgs       int64
	MaxMsgSize    int
	Discard       string
	MaxConsumers  int
	Replicas      int
	Storage       string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.NatsURL, "nats-url", "nats://127.0.0.1:4222", "NATS server URL")
	flag.StringVar(&cfg.Streams, "streams", "", "JSON format stream mappings (e.g., '{\"AWS\":[\"aws.>\"],\"AUDIT\":[\"audit.>\"]}')")
	flag.StringVar(&cfg.Action, "action", "setup", "Action to perform: setup, info, create, update, recreate")
	// Phase 2 flags
	flag.StringVar(&cfg.StreamName, "name", "", "Stream name (for create/update actions)")
	flag.StringVar(&cfg.Subjects, "subjects", "", "Comma-separated list of subjects (for create/update actions)")
	flag.StringVar(&cfg.Retention, "retention", "limits", "Retention policy: limits, interest, workqueue")
	flag.StringVar(&cfg.MaxAge, "max-age", "", "Maximum age of messages (e.g., 24h, 7d)")
	flag.Int64Var(&cfg.MaxBytes, "max-bytes", 0, "Maximum size in bytes")
	flag.Int64Var(&cfg.MaxMsgs, "max-msgs", 0, "Maximum number of messages")
	maxMsgSize := flag.Int("max-msg-size", 0, "Maximum message size in bytes")
	flag.StringVar(&cfg.Discard, "discard", "old", "Discard policy: old, new")
	flag.IntVar(&cfg.MaxConsumers, "max-consumers", -1, "Maximum number of consumers")
	flag.IntVar(&cfg.Replicas, "replicas", 1, "Number of replicas")
	flag.StringVar(&cfg.Storage, "storage", "file", "Storage type: file, memory")
	flag.Parse()
	cfg.MaxMsgSize = *maxMsgSize

	switch cfg.Action {
	case "setup":
		if cfg.Streams == "" {
			slog.Error("--streams parameter is required for setup action")
			os.Exit(1)
		}
		setupStreams(cfg)
	case "info":
		showStreamInfo(cfg)
	case "create":
		if cfg.StreamName == "" || cfg.Subjects == "" {
			slog.Error("--name and --subjects are required for create action")
			os.Exit(1)
		}
		createOrUpdateStream(cfg, false)
	case "update":
		if cfg.StreamName == "" {
			slog.Error("--name is required for update action")
			os.Exit(1)
		}
		createOrUpdateStream(cfg, true)
	case "recreate":
		if cfg.StreamName == "" {
			slog.Error("--name is required for recreate action")
			os.Exit(1)
		}
		recreateStream(cfg)
	default:
		slog.Error("Invalid action", "action", cfg.Action)
		os.Exit(1)
	}
}

func setupStreams(cfg Config) {
	// Parse the streams JSON
	var streams map[string][]string
	if err := json.Unmarshal([]byte(cfg.Streams), &streams); err != nil {
		slog.Error("Failed to parse streams JSON", "error", err)
		os.Exit(1)
	}

	// Convert to processor.NatsConfig
	ncConfig := processor.NatsConfig{
		NatsURL: cfg.NatsURL,
	}

	// Call SetupStreams
	processor.SetupStreams(ncConfig, streams)
	slog.Info("Streams setup completed successfully")
}

func createOrUpdateStream(cfg Config, isUpdate bool) {
	// Create connection
	nc, err := processor.CreateNc(processor.NatsConfig{NatsURL: cfg.NatsURL})
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	// Create JetStream context
	js, err := processor.CreateJS(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	// Parse subjects
	subjects := strings.Split(cfg.Subjects, ",")
	for i := range subjects {
		subjects[i] = strings.TrimSpace(subjects[i])
	}

	// Build stream config
	streamConfig := jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: subjects,
	}

	// Set retention policy
	switch cfg.Retention {
	case "limits":
		streamConfig.Retention = jetstream.LimitsPolicy
	case "interest":
		streamConfig.Retention = jetstream.InterestPolicy
	case "workqueue":
		streamConfig.Retention = jetstream.WorkQueuePolicy
	default:
		streamConfig.Retention = jetstream.LimitsPolicy
	}

	// Set storage type
	switch cfg.Storage {
	case "memory":
		streamConfig.Storage = jetstream.MemoryStorage
	default:
		streamConfig.Storage = jetstream.FileStorage
	}

	// Set discard policy
	switch cfg.Discard {
	case "new":
		streamConfig.Discard = jetstream.DiscardNew
	default:
		streamConfig.Discard = jetstream.DiscardOld
	}

	// Set limits
	if cfg.MaxAge != "" {
		d, err := duration.Parse(cfg.MaxAge)
		if err != nil {
			slog.Error("Failed to parse max-age", "error", err)
			os.Exit(1)
		}
		streamConfig.MaxAge = d.ToTimeDuration()
	}

	if cfg.MaxBytes > 0 {
		streamConfig.MaxBytes = cfg.MaxBytes
	}

	if cfg.MaxMsgs > 0 {
		streamConfig.MaxMsgs = cfg.MaxMsgs
	}

	if cfg.MaxMsgSize > 0 {
		streamConfig.MaxMsgSize = int32(cfg.MaxMsgSize)
	}

	if cfg.MaxConsumers >= 0 {
		streamConfig.MaxConsumers = cfg.MaxConsumers
	}

	if cfg.Replicas > 0 {
		streamConfig.Replicas = cfg.Replicas
	}

	// Create or update stream
	ctx := context.Background()
	if isUpdate {
		// First, check if we're trying to change the retention policy
		var existingInfo *jetstream.StreamInfo
		existingStream, err := js.Stream(ctx, cfg.StreamName)
		if err == nil {
			existingInfo, err = existingStream.Info(ctx)
			if err == nil && existingInfo.Config.Retention != streamConfig.Retention {
				slog.Warn("Attempting to change retention policy", 
					"from", existingInfo.Config.Retention, 
					"to", streamConfig.Retention,
					"note", "NATS may not allow changing retention policy on existing streams")
			}
		}
		
		stream, err := js.UpdateStream(ctx, streamConfig)
		if err != nil {
			slog.Error("Failed to update stream", "error", err)
			
			// Check if this might be a retention policy change attempt
			if existingInfo != nil && existingInfo.Config.Retention != streamConfig.Retention {
				slog.Error("Cannot change retention policy via update", 
					"current", existingInfo.Config.Retention,
					"attempted", streamConfig.Retention,
					"solution", "Use --action=recreate to change retention policy (WARNING: This will delete all messages)")
			}
			os.Exit(1)
		}
		info, _ := stream.Info(ctx)
		printStreamInfo(info)
		slog.Info("Stream updated successfully", "name", cfg.StreamName)
	} else {
		stream, err := js.CreateStream(ctx, streamConfig)
		if err != nil {
			slog.Error("Failed to create stream", "error", err)
			os.Exit(1)
		}
		info, _ := stream.Info(ctx)
		printStreamInfo(info)
		slog.Info("Stream created successfully", "name", cfg.StreamName)
	}
}

func recreateStream(cfg Config) {
	// Create connection
	nc, err := processor.CreateNc(processor.NatsConfig{NatsURL: cfg.NatsURL})
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	// Create JetStream context
	js, err := processor.CreateJS(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Get existing stream info
	existingStream, err := js.Stream(ctx, cfg.StreamName)
	if err != nil {
		slog.Error("Failed to get existing stream", "error", err, "name", cfg.StreamName)
		os.Exit(1)
	}

	existingInfo, err := existingStream.Info(ctx)
	if err != nil {
		slog.Error("Failed to get stream info", "error", err)
		os.Exit(1)
	}

	// Use existing subjects if not provided
	subjects := strings.Split(cfg.Subjects, ",")
	if cfg.Subjects == "" {
		subjects = existingInfo.Config.Subjects
	} else {
		for i := range subjects {
			subjects[i] = strings.TrimSpace(subjects[i])
		}
	}

	// Build new stream config, preserving existing values where not specified
	streamConfig := jetstream.StreamConfig{
		Name:     cfg.StreamName,
		Subjects: subjects,
	}

	// Set retention policy
	switch cfg.Retention {
	case "limits":
		streamConfig.Retention = jetstream.LimitsPolicy
	case "interest":
		streamConfig.Retention = jetstream.InterestPolicy
	case "workqueue":
		streamConfig.Retention = jetstream.WorkQueuePolicy
	default:
		streamConfig.Retention = jetstream.LimitsPolicy
	}

	// Check if retention policy is changing
	if existingInfo.Config.Retention != streamConfig.Retention {
		slog.Warn("Changing retention policy requires recreating the stream",
			"from", existingInfo.Config.Retention,
			"to", streamConfig.Retention,
			"WARNING", "This will DELETE all existing messages!")
		
		// Ask for confirmation
		fmt.Printf("\n⚠️  WARNING: Recreating stream '%s' will DELETE all %d existing messages!\n", cfg.StreamName, existingInfo.State.Msgs)
		fmt.Printf("Current retention: %v → New retention: %v\n", existingInfo.Config.Retention, streamConfig.Retention)
		fmt.Printf("Are you sure you want to proceed? (yes/no): ")
		
		var response string
		fmt.Scanln(&response)
		if response != "yes" {
			slog.Info("Operation cancelled")
			os.Exit(0)
		}
	}

	// Set storage type
	if cfg.Storage != "" {
		switch cfg.Storage {
		case "memory":
			streamConfig.Storage = jetstream.MemoryStorage
		default:
			streamConfig.Storage = jetstream.FileStorage
		}
	} else {
		streamConfig.Storage = existingInfo.Config.Storage
	}

	// Set discard policy
	if cfg.Discard != "" {
		switch cfg.Discard {
		case "new":
			streamConfig.Discard = jetstream.DiscardNew
		default:
			streamConfig.Discard = jetstream.DiscardOld
		}
	} else {
		streamConfig.Discard = existingInfo.Config.Discard
	}

	// Set limits - use existing values if not specified
	if cfg.MaxAge != "" {
		d, err := duration.Parse(cfg.MaxAge)
		if err != nil {
			slog.Error("Failed to parse max-age", "error", err)
			os.Exit(1)
		}
		streamConfig.MaxAge = d.ToTimeDuration()
	} else {
		streamConfig.MaxAge = existingInfo.Config.MaxAge
	}

	if cfg.MaxBytes > 0 {
		streamConfig.MaxBytes = cfg.MaxBytes
	} else {
		streamConfig.MaxBytes = existingInfo.Config.MaxBytes
	}

	if cfg.MaxMsgs > 0 {
		streamConfig.MaxMsgs = cfg.MaxMsgs
	} else {
		streamConfig.MaxMsgs = existingInfo.Config.MaxMsgs
	}

	if cfg.MaxMsgSize > 0 {
		streamConfig.MaxMsgSize = int32(cfg.MaxMsgSize)
	} else {
		streamConfig.MaxMsgSize = existingInfo.Config.MaxMsgSize
	}

	if cfg.MaxConsumers >= 0 {
		streamConfig.MaxConsumers = cfg.MaxConsumers
	} else {
		streamConfig.MaxConsumers = existingInfo.Config.MaxConsumers
	}

	if cfg.Replicas > 0 {
		streamConfig.Replicas = cfg.Replicas
	} else {
		streamConfig.Replicas = existingInfo.Config.Replicas
	}

	// Delete the existing stream
	slog.Info("Deleting existing stream", "name", cfg.StreamName)
	err = js.DeleteStream(ctx, cfg.StreamName)
	if err != nil {
		slog.Error("Failed to delete stream", "error", err)
		os.Exit(1)
	}

	// Create new stream with updated config
	slog.Info("Creating new stream with updated configuration", "name", cfg.StreamName)
	stream, err := js.CreateStream(ctx, streamConfig)
	if err != nil {
		slog.Error("Failed to create stream", "error", err)
		os.Exit(1)
	}

	info, _ := stream.Info(ctx)
	printStreamInfo(info)
	slog.Info("Stream recreated successfully", "name", cfg.StreamName)
}

func showStreamInfo(cfg Config) {
	// Create connection
	nc, err := processor.CreateNc(processor.NatsConfig{NatsURL: cfg.NatsURL})
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		os.Exit(1)
	}
	defer nc.Drain()

	// Create JetStream context
	js, err := processor.CreateJS(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// List all streams
	streamsInfoLister := js.ListStreams(ctx)
	streamsInfo := make([]*jetstream.StreamInfo, 0)
	for streamInfo := range streamsInfoLister.Info() {
		streamsInfo = append(streamsInfo, streamInfo)
	}

	fmt.Println("\n=== JetStream Streams ===")
	fmt.Printf("%-20s %-30s %-15s %-10s %-10s %-12s\n", "Stream", "Subjects", "Messages", "Bytes", "Consumers", "Retention")
	fmt.Println(repeatChar("-", 97))

	for _, info := range streamsInfo {
		retentionStr := "limits"
		switch info.Config.Retention {
		case jetstream.InterestPolicy:
			retentionStr = "interest"
		case jetstream.WorkQueuePolicy:
			retentionStr = "workqueue"
		}

		fmt.Printf("%-20s %-30s %-15d %-10d %-10d %-12s\n",
			truncate(info.Config.Name, 20),
			truncate(fmt.Sprintf("%v", info.Config.Subjects), 30),
			info.State.Msgs,
			info.State.Bytes,
			info.State.Consumers,
			retentionStr,
		)

		// List consumers for this stream
		if info.State.Consumers > 0 {
			stream, err := js.Stream(ctx, info.Config.Name)
			if err != nil {
				slog.Error("Error getting stream", "error", err)
				continue
			}
			consumersLister := stream.ListConsumers(ctx)
			consumersInfo := make([]*jetstream.ConsumerInfo, 0)
			for consumerInfo := range consumersLister.Info() {
				consumersInfo = append(consumersInfo, consumerInfo)
			}

			fmt.Println("\n  Consumers:")
			fmt.Printf("  %-20s %-20s %-15s %-15s %-15s\n", "Name", "Filter Subjects", "Ack Pending", "Redelivered", "Num Pending")
			fmt.Println("  " + repeatChar("-", 85))
			for _, cInfo := range consumersInfo {
				fmt.Printf("  %-20s %-20s %-15d %-15d %-15d\n",
					truncate(cInfo.Name, 20),
					truncate(fmt.Sprintf("%v", cInfo.Config.FilterSubjects), 20),
					cInfo.NumAckPending,
					cInfo.NumRedelivered,
					cInfo.NumPending,
				)
			}
			fmt.Println()
		}
	}
}

func printStreamInfo(info *jetstream.StreamInfo) {
	fmt.Println("\n=== Stream Configuration ===")
	fmt.Printf("Name:          %s\n", info.Config.Name)
	fmt.Printf("Subjects:      %v\n", info.Config.Subjects)
	fmt.Printf("Retention:     %v\n", info.Config.Retention)
	fmt.Printf("Storage:       %v\n", info.Config.Storage)
	fmt.Printf("Replicas:      %d\n", info.Config.Replicas)
	fmt.Printf("MaxMsgs:       %d\n", info.Config.MaxMsgs)
	fmt.Printf("MaxBytes:      %d\n", info.Config.MaxBytes)
	fmt.Printf("MaxAge:        %v\n", info.Config.MaxAge)
	fmt.Printf("MaxMsgSize:    %d\n", info.Config.MaxMsgSize)
	fmt.Printf("MaxConsumers:  %d\n", info.Config.MaxConsumers)
	fmt.Printf("Discard:       %v\n", info.Config.Discard)

	fmt.Println("\n=== Stream State ===")
	fmt.Printf("Messages:      %d\n", info.State.Msgs)
	fmt.Printf("Bytes:         %d\n", info.State.Bytes)
	fmt.Printf("FirstSeq:      %d\n", info.State.FirstSeq)
	fmt.Printf("LastSeq:       %d\n", info.State.LastSeq)
	fmt.Printf("Consumers:     %d\n", info.State.Consumers)
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

func repeatChar(char string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += char
	}
	return result
}