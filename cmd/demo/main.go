package main

import (
	"context"

	"github.com/ilyakaznacheev/cleanenv"
	"github.com/tendant/nats-processor/processor"
	"golang.org/x/exp/slog"
)

type ProcessorConfig struct {
	processor.NatsConfig
	processor.ConsumerConfig
}

func main() {
	var config ProcessorConfig
	cleanenv.ReadEnv(&config)
	nc, _ := processor.CreateNc(config.NatsConfig)

	js, _ := processor.CreateJS(nc)

	consumer, _ := processor.CreateOrUpdateConsumer(context.Background(), js, config.ConsumerConfig)

	proc := processor.NewProcessor(consumer, processFn)

	proc.Process()

	nc.Drain()

}

func processFn(msg interface{}) error {
	slog.Debug("msg: ", "msg", msg)
	return nil
}
