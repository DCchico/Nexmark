# Nexmark Query 8 - OpenFaaS Function (Streaming Implementation with Ordered Processing)

This OpenFaaS function implements Nexmark Query 8 as a streaming query that monitors new users within a 12-hour sliding window, triggered by each bid event, with **in-order correctness** guaranteed through Redis PubSub synchronization.

## Original Query

```sql
SELECT Rstream(P.id, P.name, A.reserve)
FROM Person [RANGE 12 HOUR] P, Auction [RANGE 12 HOUR] A
WHERE P.id = A.seller;
```

## What is Nexmark Query 8?

Nexmark Query 8 monitors auction data streams to find new users (persons) who are also sellers of auctions within a 12-hour sliding window. The function:

- **Maintains State**: Uses Redis to persist data across function invocations
- **Sliding Window**: Keeps a 12-hour window of person and auction data
- **Bid-Triggered**: Processes each bid event to update the window
- **Real-time Matching**: Finds person-seller matches within the window
- **Automatic Cleanup**: Removes expired data outside the window
- **In-Order Processing**: Ensures sequential processing using Redis PubSub synchronization
- **State Persistence**: Uses pickle serialization for complex object storage

## Key Features

### Ordered Processing
- **Sequence IDs**: Each event has a unique sequence ID for ordering
- **Redis PubSub**: Waits for previous sequence completion before processing
- **State Synchronization**: Ensures data consistency across invocations
- **Timeout Handling**: Configurable wait times for missing sequences

### State Management
- **Pickle Serialization**: Stores complex Python objects efficiently
- **Automatic Cleanup**: Removes expired data from 12-hour window
- **Metrics Tracking**: Detailed performance and timing metrics
- **Error Recovery**: Robust error handling and recovery mechanisms

### Streaming Architecture
- **Event-Driven**: Triggered by individual bid events
- **Real-time Processing**: Immediate response to new data
- **Scalable**: Handles high-throughput streaming data
- **Fault-Tolerant**: Survives function restarts and failures

### 12-Hour Window Initialization
- **Full Window Population**: Starts with complete 12-hour window (43,200 seconds)
- **1-Second Granularity**: Each entry represents one second of data
- **Auto-Initialization**: Automatically initializes window on first event
- **Memory Efficient**: ~25 MB for full window (conservative estimate: ~50 MB)
- **Progress Tracking**: Real-time initialization progress monitoring

## Architecture

```
Bid Event → Sequence ID → Redis PubSub Wait → State Retrieval → Processing → State Storage → Results
    ↓           ↓              ↓                ↓              ↓            ↓            ↓
  Trigger   Ordering    Synchronization    Previous State   Window Update  Persist    Return
```

## Data Format

### Ordered Bid Event Input

```json
{
  "stream_id": "bid_stream",
  "sequence_id": 1640995200000,
  "bid_event": {
    "person": {
      "id": "person_123",
      "name": "John Doe",
      "email": "john@example.com"
    },
    "auction": {
      "id": "auction_456",
      "seller_id": "person_123",
      "reserve": 150.0,
      "type": "Regular"
    },
    "bid_amount": 200.0,
    "bid_time": 1640995200
  }
}
```

### XML Data Input (Backward Compatible)

```json
{
  "xml_data": "<?xml version=\"1.0\"?><site><person id=\"person_1\">...</person><open_auctions><open_auction id=\"auction_1\">...</open_auction></open_auctions></site>"
}
```

## Deployment

### Prerequisites

- OpenFaaS CLI installed
- OpenFaaS gateway running
- Docker installed
- Redis server running

### Setup Redis

```bash
# Start Redis container
docker run -d -p 6379:6379 --name redis-nexmark redis:alpine

# Or use local Redis
redis-server
```

### Build and Deploy

1. **Build the function:**
   ```bash
   faas-cli build nexmark-q8
   ```

2. **Deploy to OpenFaaS:**
   ```bash
   faas-cli deploy nexmark-q8
   ```

3. **Set Redis environment variables:**
   ```bash
   faas-cli deploy nexmark-q8 --env REDIS_HOST=localhost --env REDIS_PORT=6379 --env REDIS_DB=0
   ```

4. **Invoke the function:**
   ```bash
   # Using ordered bid event
   echo '{"stream_id": "test_stream", "sequence_id": 123456, "bid_event": {"person": {"id": "person_1", "name": "John"}, "auction": {"id": "auction_1", "seller_id": "person_1", "reserve": 100}}}' | faas-cli invoke nexmark-q8
   
   # Using XML data
   echo '{"xml_data": "<?xml version=\"1.0\"?><site>...</site>"}' | faas-cli invoke nexmark-q8
   ```

## Testing

### Generate Test Data

```bash
cd nexmark-q8
python test_data.py
```

### Test Ordered Processing

```bash
# Generate and test ordered bid events
python -c "
import test_data
import json
event = test_data.generate_bid_event('person_1', 'auction_1', 123456, 'test_stream')
event['bid_event']['auction']['seller_id'] = 'person_1'
print(json.dumps(event))
" | faas-cli invoke nexmark-q8
```

### Test Ordered Sequence

```bash
# Process a sequence of ordered events
python -c "import test_data; import json; events = test_data.generate_ordered_test_sequence(5); [print(json.dumps(e)) for e in events]" | while read line; do echo "$line" | faas-cli invoke nexmark-q8; done
```

### Test Window Initialization

```bash
# Check window status
echo '{"command": "status", "stream_id": "test_stream"}' | faas-cli invoke nexmark-q8

# Initialize 12-hour window
echo '{"command": "initialize", "stream_id": "test_stream"}' | faas-cli invoke nexmark-q8

# Auto-initialize if needed
echo '{"command": "auto_initialize", "stream_id": "test_stream"}' | faas-cli invoke nexmark-q8

# Run comprehensive initialization test
python test_initialization.py
```

### Test with NEXMark Generator

```bash
# Generate auction data and pipe to function
java NEXMarkGenerator -gen-calls 100 -prettyprint false | \
  faas-cli invoke nexmark-q8
```

## Output Format

The function returns a JSON response with the following structure:

```json
{
  "query": "Nexmark Query 8 - Monitor New Users (Ordered Processing)",
  "timestamp": 1640995200,
  "stream_id": "test_stream",
  "sequence_id": 1640995200000,
  "window_hours": 12,
  "new_matches": [
    {
      "person_id": "person_123",
      "person_name": "John Doe",
      "auction_id": "auction_456",
      "reserve": 150.0,
      "person_timestamp": 1640995200,
      "auction_timestamp": 1640995200
    }
  ],
  "window_statistics": {
    "persons_in_window": 25,
    "auctions_in_window": 15,
    "window_start": 1640952000,
    "window_end": 1640995200
  },
  "state_statistics": {
    "total_persons": 50,
    "total_auctions": 30,
    "total_matches": 5
  },
  "processing_time": 0.123,
  "total_processing_time": 0.456,
  "metrics": {
    "timings": {
      "retrieval": 0.045,
      "processing": 0.078,
      "storage": 0.034
    },
    "sizes": {
      "retrieved_kb": 12.5,
      "stored_kb": 13.2
    },
    "retrieval_details": {
      "already_available": 1,
      "initial_check_time": 2.1,
      "deserialization_time": 1.8
    },
    "store_details": {
      "pickle_time": 1.2,
      "redis_write_time": 3.4,
      "serialized_size": 13568
    }
  }
}
```

## Configuration

### Environment Variables

- `REDIS_HOST`: Redis server host (default: localhost)
- `REDIS_PORT`: Redis server port (default: 6379)
- `REDIS_DB`: Redis database number (default: 0)
- `LOG_LEVEL`: Logging level (default: INFO)

### Function Configuration

Edit `stack.yaml` to modify function settings:

```yaml
functions:
  nexmark-q8:
    lang: python3-http
    handler: ./nexmark-q8
    image: nexmark-q8:latest
    environment:
      REDIS_HOST: localhost
      REDIS_PORT: 6379
      REDIS_DB: 0
      LOG_LEVEL: INFO
    limits:
      memory: 512Mi
      cpu: 500m
```

## Redis Data Structure

The function uses the following Redis key patterns:

- `nexmark:person:{person_id}` - Person data with timestamp
- `nexmark:auction:{auction_id}` - Auction data with timestamp
- `nexmark:state:{stream_id}:{sequence_id}` - Serialized state objects
- `nexmark:sequence:{stream_id}:latest` - Latest sequence pointer
- All keys have TTL set to window duration + buffer

## Ordered Processing Details

### Sequence Management
- Each event must have a unique `sequence_id`
- Function waits for previous sequence completion
- Uses Redis PubSub for synchronization
- Configurable timeout for missing sequences

### State Persistence
- Complex objects serialized with pickle
- Automatic cleanup of expired data
- State survives function restarts
- Detailed metrics for performance monitoring

### Synchronization
- Redis keyspace notifications for coordination
- Wait-cycles with timeout handling
- Immediate availability detection
- Deserialization performance tracking

## Performance Considerations

- **Memory Usage**: Scales with number of persons/auctions in window
- **Processing Time**: O(n*m) where n=persons, m=auctions in window
- **Redis Operations**: Optimized with hash operations and TTL
- **Window Cleanup**: Automatic cleanup of expired data
- **State Persistence**: Redis ensures state survives function restarts
- **Ordered Processing**: PubSub synchronization adds minimal overhead
- **Serialization**: Pickle provides efficient object storage

## Data Size and Memory Requirements

### 12-Hour Window Specifications
- **Duration**: 12 hours (43,200 seconds)
- **Granularity**: 1 second per entry
- **Total Entries**: 43,200
- **Estimated Persons**: 4,320 (1 per 10 seconds)
- **Estimated Auctions**: 43,200 (1 per second)
- **Estimated Bids**: 432,000 (10 per second)

### Memory Requirements
- **Person Data**: ~0.73 MB (177 bytes per person)
- **Auction Data**: ~8.90 MB (216 bytes per auction)
- **State Data**: ~6.72 MB (163 bytes per state)
- **Redis Overhead**: 50%
- **Total Memory**: ~24.52 MB (0.02 GB)
- **Conservative Estimate**: ~49.03 MB (0.05 GB)

### Alternative Scenarios
- **High Activity (10x)**: ~245.15 MB (0.24 GB)
- **Low Activity (0.1x)**: ~2.45 MB
- **Conservative (2x buffer)**: ~49.03 MB (0.05 GB)

### Performance Metrics
- **Redis Operations per Second**: ~432,000
- **State Retrievals per Second**: ~432,000
- **State Stores per Second**: ~432,000
- **Memory Bandwidth**: ~0.01 MB/s (hourly)

**Recommendation**: Use at least 2-4 GB of memory for Redis to handle the 12-hour window comfortably.

## Error Handling

The function handles various error conditions:
- Redis connection failures
- Invalid input data
- Missing required fields
- Network timeouts
- Data parsing errors
- Sequence synchronization timeouts
- Serialization failures

Error responses include:
- HTTP 400 for invalid input
- HTTP 500 for internal errors
- Detailed error messages for debugging
- Partial metrics for failed operations

## Monitoring and Logging

The function includes comprehensive logging:
- Redis connection status
- Window cleanup operations
- Match detection events
- Performance metrics
- Error details
- Sequence synchronization events
- State retrieval and storage metrics

View logs with:
```bash
faas-cli logs nexmark-q8
```

## Integration Examples

### Kafka Integration with Ordered Processing

```bash
# Consume from Kafka and invoke function with sequence IDs
kafka-console-consumer --bootstrap-server localhost:9092 --topic bids | \
  while read line; do
    # Add sequence ID to each event
    sequence_id=$(date +%s%3N)  # Millisecond timestamp
    echo "$line" | jq --arg seq "$sequence_id" '.sequence_id = ($seq | tonumber)' | \
      faas-cli invoke nexmark-q8
  done
```

### NEXMark Generator Integration

```bash
# Generate data and process in real-time with ordering
java NEXMarkGenerator -gen-calls 1000 -prettyprint false | \
  while IFS= read -r line; do
    sequence_id=$(date +%s%3N)
    echo "{\"stream_id\": \"nexmark_stream\", \"sequence_id\": $sequence_id, \"xml_data\": \"$line\"}" | \
      faas-cli invoke nexmark-q8
  done
```

### Multi-Stream Processing

```bash
# Process multiple streams with different sequence IDs
for stream in "stream_a" "stream_b" "stream_c"; do
  for i in {1..10}; do
    sequence_id=$(($(date +%s%3N) + i))
    echo "{\"stream_id\": \"$stream\", \"sequence_id\": $sequence_id, \"bid_event\": {...}}" | \
      faas-cli invoke nexmark-q8 &
  done
done
wait
```

## Contributing

To extend the function:
1. Modify `handler.py` to add new window processing logic
2. Update `requirements.txt` for new dependencies
3. Add tests in `handler_test.py`
4. Update this README with new features

## License

This function is part of the NEXMark benchmark implementation and follows the same licensing terms as the original NEXMark Generator. 