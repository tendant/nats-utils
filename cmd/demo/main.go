package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tendant/nats-utils/processor"
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
	if err != nil {
		os.Exit(-1)
	}

	js, err := processor.CreateJS(nc)
	if err != nil {
		os.Exit(-1)
	}

	consumer, err := processor.CreateOrUpdateConsumer(context.Background(), js, config.ConsumerConfig)
	if err != nil {
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
