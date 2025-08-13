# NATS Stream CLI Tool

A command-line tool for managing NATS JetStream streams with support for setting up multiple streams, creating/updating individual streams with retention policies, and displaying stream information.

## Installation

```bash
cd /Users/bd/Workspace/Torpago/nats-utils
go build -o stream cmd/stream/main.go
```

## Usage

### Phase 1: Basic Stream Setup

Set up multiple streams at once using JSON format:

```bash
./stream --action=setup --streams='{"AWS":["aws.>"],"AUDIT":["audit.>"],"RISK":["risk.>"]}'
```

### Phase 2: Create/Update Streams with Retention Policies

Create a new stream with specific retention policy:

```bash
# Create a stream with limits retention policy
./stream --action=create --name=TEST --subjects="test.>" --retention=limits --max-age=24h --max-msgs=1000000

# Create a workqueue stream (each message consumed only once)
./stream --action=create --name=WORKQUEUE --subjects="work.>" --retention=workqueue

# Create an interest-based stream (messages retained while consumers exist)
./stream --action=create --name=INTEREST --subjects="interest.>" --retention=interest
```

Update an existing stream:

```bash
./stream --action=update --name=TEST --max-age=7d --max-bytes=1073741824
```

Change retention policy (requires recreating the stream - **WARNING: This deletes all messages!**):

```bash
# Change from limits to workqueue retention
./stream --action=recreate --name=TEST --retention=workqueue

# The tool will warn you and ask for confirmation before deleting messages
```

### Phase 3: Display Stream and Consumer Information

Show all streams and their consumers:

```bash
./stream --action=info
```

## Command-Line Options

### Common Options
- `--nats-url`: NATS server URL (default: "nats://127.0.0.1:4222")
- `--action`: Action to perform: setup, info, create, update, recreate (default: "setup")

### Setup Action Options
- `--streams`: JSON format stream mappings (required for setup action)

### Create/Update Action Options
- `--name`: Stream name (required)
- `--subjects`: Comma-separated list of subjects (required for create)
- `--retention`: Retention policy: limits, interest, workqueue (default: "limits")
- `--max-age`: Maximum age of messages (e.g., 24h, 7d)
- `--max-bytes`: Maximum size in bytes
- `--max-msgs`: Maximum number of messages
- `--max-msg-size`: Maximum message size in bytes
- `--discard`: Discard policy: old, new (default: "old")
- `--max-consumers`: Maximum number of consumers
- `--replicas`: Number of replicas (default: 1)
- `--storage`: Storage type: file, memory (default: "file")

## Examples

### Example 1: Setup streams from message-queue

```bash
./stream --action=setup --streams='{"AWS":["aws.>"],"AUDIT":["audit.>"],"NOTICE":["notice.>"],"RISK":["risk.>"],"MARQETA":["marqeta.>"],"MANAGEMENT":["management.>"]}'
```

### Example 2: Create a high-volume stream with retention limits

```bash
./stream --action=create \
  --name=HIGH_VOLUME \
  --subjects="events.>" \
  --retention=limits \
  --max-age=72h \
  --max-bytes=10737418240 \
  --max-msgs=10000000 \
  --discard=old
```

### Example 3: Create a memory-based workqueue

```bash
./stream --action=create \
  --name=FAST_QUEUE \
  --subjects="queue.>" \
  --retention=workqueue \
  --storage=memory \
  --replicas=3
```

## Retention Policies Explained

1. **limits**: Traditional retention based on age, size, or message count limits
2. **interest**: Messages retained only while there are active consumers interested in them
3. **workqueue**: Each message can be consumed by only one consumer (FIFO queue)

## Notes

- The tool uses the existing `processor.SetupStreams` for the setup action
- For create/update actions, it directly uses the NATS JetStream API with full configuration options
- The info action displays both stream and consumer information in a formatted table