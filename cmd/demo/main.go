package main

import (
	"context"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tendant/nats-utils/processor"
	"golang.org/x/exp/slog"
)

type ProcessorConfig struct {
	processor.NatsConfig
	processor.ConsumerConfig
}

func main() {
	var config ProcessorConfig
	cleanenv.ReadEnv(&config)
	nc, err := processor.CreateNc(config.NatsConfig)
	defer nc.Drain()

	slog.Info("natsurl", "NatsURL", config.NatsURL)
	if err != nil {
		slog.Error("Failed connecting to Nats Server", "NatsURL", config.NatsURL, "err", err)
		os.Exit(-1)
	} else {
		slog.Info("Connected to Nats Server!")
	}

	js, err := processor.CreateJS(nc)
	if err != nil {
		slog.Error("Failed Connect to Stream Context! ", "err", err)
		os.Exit(-1)
	} else {
		slog.Info("Connect to Stream Context!")
	}

	consumer, err := processor.CreateOrUpdateConsumer(context.Background(), js, config.ConsumerConfig)
	if err != nil {
		slog.Error("Failed CreateOrUpdateConsumer", "err", err)
		os.Exit(-1)
	}

	proc := processor.NewProcessor(consumer, processFn)

	proc.Process()
}

type ExampleEvent struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func processFn(msg jetstream.Msg) error {
	slog.Debug("processFn: ", "msg", msg)

	// parse event from msg
	event, err := processor.ParseEvent(msg)
	if err != nil {
		return err
	}

	// parse data from event
	var ee ExampleEvent
	err = processor.LoadFromMap(&ee, &event.Data)
	if err != nil {
		return processor.NewErrIgnorable("erorr parsing event data", err)
	}

	slog.Info("parsed", "id", ee.Id, "name", ee.Name)
	return nil
}
