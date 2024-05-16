package processor

import (
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
