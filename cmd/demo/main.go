package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/render"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/tendant/chi-demo/app"
	"github.com/tendant/nats-utils/processor"
)

type ProcessorConfig struct {
	processor.NatsConfig
	processor.ConsumerConfig
}

func main() {
	var config ProcessorConfig
	cleanenv.ReadEnv(&config)
	_ = cleanenv.ReadConfig(".env", &config)

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

	go func() {
		proc.Process()
	}()

	s := app.Default()
	s.R.Get("/healthz/ready", Ready)
	s.Run()
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

func Ready(w http.ResponseWriter, r *http.Request) {
	ready := true

	// Return
	if ready {
		render.PlainText(w, r, "Ready")
	} else {
		render.PlainText(w, r, "Not Ready")
	}
}
