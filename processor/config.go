package processor

type NatsConfig struct {
	NatsURL string `env:"NATS_URL" env-default:"nats://127.0.0.1:4222"`
	SinkURL string `env:"SINK_URL" env-default:"http://localhost:14000/events"`
}

type StreamConfig struct {
	Name            string   `env:"STREAM_NAME" env-default:"DEMO"`
	RetentionPolicy string   `env:"RETENTION_POLICY" env-default:"limits"`
	Subjects        []string `env:"STREAM_SUBJECTS" env-default:"demo.>"`
}

type ConsumerConfig struct {
	Name           string `env:"CONSUMER_NAME" env-default:"demo-durable"`
	FilterSubjects string `env:"FILTER_SUBJECTS" env-default:"demo.sub1,demo.sub2"`
	DeliverPolicy  string `env:"DELIVER_POLICY" env-default:"new"`
	StreamName     string `env:"STREAM_NAME" env-default:"DEMO"`
	MaxDeliver     int    `env:"MAX_DELIVER" env-default:"5"`
	MaxAckPending  int    `env:"MAX_ACK_PENDING" env-default:"1000"`
	AckWait        string `env:"ACK_WAIT" env-default:"PT30S"`
}
