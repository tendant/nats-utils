package processor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/exp/slog"
)

const (
	// MaxDeliver defines the maximum number of delivery attempts for a message.
	// Applies to any message that is re-sent due to ack policy.
	MaxDeliver int = 5
)

func CreateNc(ncConfig NatsConfig) (*nats.Conn, error) {
	nc, err := nats.Connect(ncConfig.NatsURL)
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
		slog.Error("Failed Connect to Stream Context! ", err)
		return nil, err
	}
	return js, nil
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

	filterSubjects := strings.Split(ci.FilterSubjects, ",")
	consumerConfig := jetstream.ConsumerConfig{
		Durable:        ci.Name,
		FilterSubjects: filterSubjects,
		AckPolicy:      jetstream.AckExplicitPolicy,
		MaxDeliver:     MaxDeliver,
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
