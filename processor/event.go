package processor

import (
	"log/slog"
	"time"
)

type EventData struct {
	EventSpecversion     string         `json:"specversion"`
	EventType            string         `json:"type"`
	EventSource          string         `json:"source"`
	EventId              string         `json:"id"`
	EventTime            time.Time      `json:"time"`
	Eventdatacontenttype string         `json:"datacontenttype"`
	Data                 map[string]any `json:"data"`
	Subject              string         `json:"subject"`
}

func (e EventData) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", e.EventId),
		slog.String("type", e.EventType),
		slog.String("subject", e.Subject),
		slog.String("source", e.EventSource),
		slog.Time("time", e.EventTime),
		slog.String("specversion", e.EventSpecversion),
		slog.String("datacontenttype", e.Eventdatacontenttype),
		slog.Any("data", e.Data),
	)
}
