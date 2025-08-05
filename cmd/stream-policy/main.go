package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/render"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/tendant/chi-demo/app"
	"github.com/tendant/nats-utils/processor"
)

type Config struct {
	NatsConfig   processor.NatsConfig   `yaml:"nats" env-prefix:"NATS_"`
	StreamConfig processor.StreamConfig `yaml:"stream" env-prefix:"STREAM_"`
	ServerConfig ServerConfig           `yaml:"server" env-prefix:"SERVER_"`
}

type NatsConfig struct {
	URL string `yaml:"url" env:"URL" env-default:"nats://localhost:4222"`
}

type ServerConfig struct {
	Port string `yaml:"port" env:"PORT" env-default:"8080"`
}

func main() {
	var cfg Config
	err := cleanenv.ReadEnv(&cfg)
	if err != nil {
		slog.Error("Failed to read config", "err", err)
		os.Exit(1)
	}

	// Initialize NATS connection
	nc, err := processor.CreateNc(cfg.NatsConfig)
	defer nc.Drain()
	if err != nil {
		os.Exit(-1)
	}

	// Create JetStream context
	js, err := processor.CreateJS(nc)
	if err != nil {
		os.Exit(-1)
	}

	// Update stream
	ctx := context.Background()
	_, err = processor.CreateOrUpdateStream(ctx, js, processor.StreamConfig{
		Name:            cfg.StreamConfig.Name,
		Subjects:        cfg.StreamConfig.Subjects,
		RetentionPolicy: "workqueue",
	})
	if err != nil {
		slog.Error("Failed to create/update stream", "err", err)
		os.Exit(1)
	}

	// Start HTTP server for health checks
	s := app.Default()
	s.R.Get("/healthz/ready", Ready)
	s.R.Get("/healthz/live", Live)

	slog.Info("Starting stream policy service", "port", cfg.ServerConfig.Port)
	if err := http.ListenAndServe(":"+cfg.ServerConfig.Port, s.R); err != nil {
		slog.Error("HTTP server failed", "err", err)
		os.Exit(1)
	}
}

func Ready(w http.ResponseWriter, r *http.Request) {
	// Add your readiness logic here
	ready := true
	if ready {
		render.PlainText(w, r, "Ready")
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		render.PlainText(w, r, "Not Ready")
	}
}

func Live(w http.ResponseWriter, r *http.Request) {
	// Add your liveness logic here
	render.PlainText(w, r, "Alive")
}
