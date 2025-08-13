package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sosodev/duration"
)

const (
	// MaxDeliver defines the maximum number of delivery attempts for a message.
	// Applies to any message that is re-sent due to ack policy.
	MaxDeliver int = 5
	// AckWait defines the maximum time to wait for an acknowledgement before resending a message.
	AckWait time.Duration = 30 * time.Second
)

func CreateNc(ncConfig NatsConfig, opts ...nats.Option) (*nats.Conn, error) {
	// Set up default options including our error handler
	defaultOpts := []nats.Option{
		nats.ErrorHandler(NatsErrHandler),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("NATS disconnected", "err", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
	}
	
	// Append any user-provided options
	allOpts := append(defaultOpts, opts...)
	
	nc, err := nats.Connect(ncConfig.NatsURL, allOpts...)
	// defer nc.Drain()

	slog.Info("natsurl", "NatsURL", ncConfig.NatsURL)

	if err != nil {
		slog.Error("Failed connecting to Nats Server", "NatsURL", ncConfig.NatsURL, "err", err)
		os.Exit(-1)
	} else {
		slog.Info("Connected to Nats Server!")
	}
	return nc, nil

}

func CreateJS(nc *nats.Conn) (jetstream.JetStream, error) {
	// Create a JetStream management interface
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed Connect to Stream Context! ", "err", err)
		return nil, err
	} else {
		slog.Info("Connected to Stream Context!")
	}

	return js, nil
}

// HandleConditionResult processes the result of a condition check and takes appropriate action
// based on the result. It returns true if the message should be processed, false otherwise.
func HandleConditionResult(msg jetstream.Msg, result ConditionResult) (shouldProcess bool, err error) {
	if !result.ShouldProcess {
		// Prepare log fields
		logFields := []interface{}{
			"delay", result.RetryDelay,
		}

		// Add reason if provided
		if result.Reason != "" {
			logFields = append(logFields, "reason", result.Reason)
		}

		// Add any metadata
		if len(result.Metadata) > 0 {
			for k, v := range result.Metadata {
				logFields = append(logFields, k, v)
			}
		}

		// Get message metadata for delivery count
		metadata, err := msg.Metadata()
		if err != nil {
			slog.Error("Error getting message metadata", "err", err)
			return false, err
		}

		// Add delivery count to log fields
		logFields = append(logFields, "delivery", metadata.NumDelivered)

		// Check if we should terminate processing permanently
		if result.TerminateIfFailed {
			slog.Info("Condition not met, terminating message processing", logFields...)
			if err := msg.Ack(); err != nil {
				slog.Error("Failed to ACK message for termination", "error", err)
				return false, err
			}
			return false, nil
		}

		// Check if we should retry with delay
		if result.RetryDelay > 0 {
			slog.Info("Condition not met, will retry after delay", logFields...)
			if err := msg.NakWithDelay(result.RetryDelay); err != nil {
				slog.Error("Failed to NAK message with delay",
					"delay", result.RetryDelay,
					"error", err)
				return false, err
			}
			return false, nil
		} else {
			slog.Info("Condition not met, will use default retry mechanism", logFields...)
			// Let the message go back to the stream without acknowledgment
			return false, nil
		}
	}

	// Prepare log fields for successful condition
	logFields := []interface{}{}

	// Add reason if provided
	if result.Reason != "" {
		logFields = append(logFields, "reason", result.Reason)
	}

	// Add any metadata
	if len(result.Metadata) > 0 {
		for k, v := range result.Metadata {
			logFields = append(logFields, k, v)
		}
	}

	// Condition met, proceed with processing
	slog.Info("Condition met, processing message", logFields...)
	return true, nil
}

// CreateOrUpdateConsumer creates or updates a consumer based on the provided input.
func CreateOrUpdateConsumer(ctx context.Context, js jetstream.JetStream, ci ConsumerConfig) (jetstream.Consumer, error) {
	dp := func() jetstream.DeliverPolicy {
		switch ci.DeliverPolicy {
		case "all":
			return jetstream.DeliverAllPolicy
		case "new":
			return jetstream.DeliverNewPolicy
		default:
			return jetstream.DeliverAllPolicy
		}
	}()

	aw := func() time.Duration {
		if ci.AckWait == "" {
			slog.Warn("AckWait is not set, using default", "AckWait", AckWait)
			return AckWait
		}
		d, err := duration.Parse(ci.AckWait)
		if err != nil {
			slog.Error("Failed to parse AckWait", "err", err)
			return AckWait
		}
		return d.ToTimeDuration()
	}()

	filterSubjects := strings.Split(ci.FilterSubjects, ",")
	consumerConfig := jetstream.ConsumerConfig{
		Durable:        ci.Name,
		FilterSubjects: filterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        aw,
	}

	if ci.MaxDeliver != 0 {
		consumerConfig.MaxDeliver = ci.MaxDeliver
	}

	if ci.MaxAckPending != 0 {
		consumerConfig.MaxAckPending = ci.MaxAckPending
	}

	// Check if the consumer already exists. If not, create a new one with the specified deliver policy.
	// An existing consumer cannot update its deliverPolicy.
	existingConsumer, err := js.Consumer(ctx, ci.StreamName, ci.Name)
	if err != nil {
		var jsErr jetstream.JetStreamError
		if errors.As(err, &jsErr) && jsErr.APIError().ErrorCode == jetstream.JSErrCodeConsumerNotFound {
			consumerConfig.DeliverPolicy = dp
		} else {
			slog.Error("Failed to check existing consumer", "error", err)
			return nil, err
		}
	} else {
		consumerConfig.DeliverPolicy = existingConsumer.CachedInfo().Config.DeliverPolicy
	}

	consumer, err := js.CreateOrUpdateConsumer(ctx, ci.StreamName, consumerConfig)
	if err != nil {
		slog.Error("Failed CreateOrUpdateConsumer", "err", err)
		return nil, err
	}

	// Fetch detailed consumer info
	info, err := consumer.Info(ctx)
	if err != nil {
		slog.Error("Failed to fetch consumer info", "error", err)
		return nil, err
	}

	// Log detailed consumer info
	slog.Info("Consumer created or updated",
		"name", info.Name,
		"stream", info.Stream,
		"timeCreated", info.Created,
		"durable", info.Config.Durable,
		"ackPolicy", info.Config.AckPolicy,
		"deliverPolicy", info.Config.DeliverPolicy,
		"maxDeliver", info.Config.MaxDeliver,
		// NumAckPending is the number of messages that have been delivered but
		// not yet acknowledged.
		"numAckPending", info.NumAckPending,
		// NumRedelivered counts the number of messages that have been
		// redelivered and not yet acknowledged. Each message is counted only
		// once, even if it has been redelivered multiple times. This count is
		// reset when the message is eventually acknowledged.
		"numRedelivered", info.NumRedelivered,
		// NumPending is the number of messages that match the consumer's
		// filter, but have not been delivered yet.
		"numPending", info.NumPending,
		"filterSubjects", info.Config.FilterSubjects,
		"delivered", info.Delivered,
	)

	return consumer, nil
}

func CreateStream(ctx context.Context, js jetstream.JetStream, streamName string, streamSubjects []string, recreate bool) (jetstream.JetStream, error) {
	slog.Info("Creating stream", "name", streamName, "subjects", streamSubjects)
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: streamSubjects,
	})
	if err != nil {
		if recreate {
			slog.Error("Failed AddStream! Updating...")

			_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
				Name:        streamName,
				Subjects:    streamSubjects,
				Description: fmt.Sprintf("Updated %s", streamName),
			})
		} else {
			slog.Error("Failed creating stream", "name", streamName, "err", err)
		}
	} else {
		slog.Info("Created stream", "name", streamName)
	}

	return js, err
}

// CreateStreamWithConfig creates or updates a stream with full configuration options
func CreateStreamWithConfig(ctx context.Context, js jetstream.JetStream, config jetstream.StreamConfig, recreate bool) (jetstream.Stream, error) {
	slog.Info("Creating stream with config", "name", config.Name, "subjects", config.Subjects)
	stream, err := js.CreateStream(ctx, config)
	if err != nil {
		if recreate {
			slog.Error("Failed to create stream! Updating...", "error", err)
			if config.Description == "" {
				config.Description = fmt.Sprintf("Updated %s", config.Name)
			}
			stream, err = js.UpdateStream(ctx, config)
			if err != nil {
				slog.Error("Failed to update stream", "name", config.Name, "error", err)
				return nil, err
			}
			slog.Info("Updated stream", "name", config.Name)
		} else {
			slog.Error("Failed creating stream", "name", config.Name, "error", err)
			return nil, err
		}
	} else {
		slog.Info("Created stream", "name", config.Name)
	}

	return stream, nil
}

func SetupStreams(ncConfig NatsConfig, streams map[string][]string) {
	nc, err := CreateNc(ncConfig)
	defer nc.Drain()
	if err != nil {
		return
	}

	js, err := CreateJS(nc)
	if err != nil {
		return
	}

	ctx := context.Background()
	for name, subjects := range streams {
		CreateStream(ctx, js, name, subjects, false)
	}
}

func LoadFromMap(to, from any) error {
	data, err := json.Marshal(from)
	if err == nil {
		err = json.Unmarshal(data, to)
	}
	return err
}

func ParseEvent(msg jetstream.Msg) (EventData, error) {
	// parse event
	var data EventData
	err := json.Unmarshal(msg.Data(), &data)
	if err != nil {
		errorMsgData := map[string]any{
			"subject":     msg.Subject(),
			"dataSnippet": string(msg.Data()),
		}

		// Attempt to get message metadata, if available
		if metadata, metadataErr := msg.Metadata(); metadataErr == nil {
			errorMsgData["stream"] = metadata.Stream
			errorMsgData["timestamp"] = metadata.Timestamp
			errorMsgData["numDelivered"] = metadata.NumDelivered
		} else {
			errorMsgData["metadataError"] = metadataErr.Error()
		}

		criticalErr := NewErrCritical("Error unmarshalling msg.Data", err, errorMsgData)
		slog.Error("Error unmarshalling msg.Data: ", "err", err)
		return data, criticalErr
	} else {
		return data, nil
	}
}
