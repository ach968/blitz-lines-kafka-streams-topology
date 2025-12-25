# Kafka Streams Odds Join Topology

Kafka Streams application for joining sportsbook odds using KTable-KTable joins. Joins sharp book (Pinnacle) odds against soft books (Kalshi, Bovada) and outputs matched pairs for downstream processing by the C++ SIMD engine.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         KAFKA TOPICS (Input)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  pinny-transformed  │  kalshi-transformed  │  bovada-transformed            │
│  (sharp book)       │  (soft book)         │  (soft book)                   │
└─────────────────────────────────────────────────────────────────────────────┘
              │                    │                    │
              ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA STREAMS TOPOLOGY (Java 17)                         │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                      │
│  │  KTable      │  │  KTable      │  │  KTable      │                      │
│  │  Pinnacle    │  │  Kalshi      │  │  Bovada      │                      │
│  │  (RocksDB)   │  │  (RocksDB)   │  │  (RocksDB)   │                      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                      │
│         │                 │                 │                               │
│         │    ┌────────────┴────────────┐    │                               │
│         └───►│   KTable-KTable Join    │◄───┘                               │
│              │   by composite key:     │                                    │
│              │   {gameId|marketType|   │                                    │
│              │    selection|lineValue} │                                    │
│              └────────────┬────────────┘                                    │
│                           │                                                 │
│                           ▼                                                 │
│              ┌────────────────────────┐                                     │
│              │   Output JoinedOdds    │                                     │
│              │   (sharp + soft pair)  │                                     │
│              └────────────┬───────────┘                                     │
└───────────────────────────┼─────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      KAFKA TOPIC (Output)                                   │
│                      joined-odds                                            │
└─────────────────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    C++ SIMD ENGINE (Downstream)                             │
│  • Calculates EV% from joined odds                                          │
│  • Kelly criterion stake sizing                                             │
│  • Publishes to WebSocket dashboard                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

## How It Works

1. **Input Topics**: Consumes odds from three sportsbook topics:
   - `pinny-transformed` - Pinnacle (sharp book, fair value proxy)
   - `kalshi-transformed` - Kalshi (soft book)
   - `bovada-transformed` - Bovada (soft book)

2. **KTables**: Each sportsbook's data is materialized into a KTable with RocksDB state store, keyed by composite join key: `{gameId}|{marketType}|{selection}|{lineValue}`

3. **KTable Joins**: Soft book KTables are joined against the sharp book (Pinnacle) KTable to match odds for identical betting lines

4. **Output**: Joined odds pairs (sharp + soft) are published to `joined-odds` topic

5. **Downstream**: C++ SIMD engine consumes joined odds and performs:
   - EV% calculation: `(softOdds × (1/sharpOdds) - 1) × 100`
   - Kelly criterion stake sizing
   - WebSocket publishing to dashboard

## Prerequisites

- Java 17+
- Maven 3.8+
- Docker (for containerized deployment)
- Running Kafka cluster with topics created

## Building

```bash
# Build the fat JAR
mvn clean package

# Run tests
mvn test

# Build without tests
mvn clean package -DskipTests
```

## Running Locally

```bash
# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Run the application
java -jar target/kafka-streams-topology-1.0.0.jar
```

## Configuration

All configuration is via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `APPLICATION_ID` | `ev-opportunity-streams` | Kafka Streams application ID |
| `STATE_DIR` | `/tmp/kafka-streams` | RocksDB state store directory |
| `COMMIT_INTERVAL_MS` | `500` | How often to commit offsets |
| `CACHE_MAX_BYTES` | `10485760` (10MB) | Max cache for record buffering |
| `NUM_STREAM_THREADS` | `2` | Number of stream threads |
| `REPLICATION_FACTOR` | `1` | Replication factor for internal topics |
| `JAVA_OPTS` | `-Xmx2G -XX:+UseG1GC` | JVM options |

## Docker

```bash
# Build image
docker build -t blitz-lines/kafka-streams-topology:latest .

# Run container
docker run -d \
  --name ev-streams \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e NUM_STREAM_THREADS=2 \
  -v kafka-streams-state:/var/kafka-streams \
  blitz-lines/kafka-streams-topology:latest
```

## Docker Compose Integration

Add to your `docker-compose.yaml`:

```yaml
kafka-streams-join:
  build:
    context: ./kafka-streams-topology
    dockerfile: Dockerfile
  depends_on:
    - kafka
  environment:
    KAFKA_BOOTSTRAP_SERVERS: kafka:29092
    NUM_STREAM_THREADS: 2
    JAVA_OPTS: "-Xmx2G -XX:+UseG1GC"
  volumes:
    - kafka-streams-state:/var/kafka-streams
```

## Input Message Format (TransformedLine)

```json
{
  "timestamp": 1702756800000,
  "sportsbook": "pinnacle",
  "gameId": "football-nfl-2024-12-17-eagles-cowboys",
  "sport": "football",
  "marketType": "spread",
  "teamA": "Eagles",
  "teamB": "Cowboys",
  "selection": "Eagles -3.5",
  "lineValue": -3.5,
  "odds": 1.91,
  "dataSource": "pinnacle-api"
}
```

## Output Message Format (JoinedOdds)

```json
{
  "time": 1702756800500,
  "game_id": "football-nfl-2024-12-17-eagles-cowboys",
  "sport": "football",
  "market_type": "spread",
  "team_a": "Eagles",
  "team_b": "Cowboys",
  "selection": "Eagles -3.5",
  "line_value": -3.5,
  "sharp_book": "pinnacle",
  "soft_book": "kalshi",
  "sharp_odds": 1.91,
  "soft_odds": 2.05
}
```

The C++ SIMD engine downstream will calculate:
- **EV%**: `(soft_odds × (1/sharp_odds) - 1) × 100`
- **Kelly stake**: `(b×p - q) / b` where `b = soft_odds - 1`, `p = 1/sharp_odds`, `q = 1 - p`

## Performance Tuning

| Component | Parameter | Recommended Value | Notes |
|-----------|-----------|-------------------|-------|
| Kafka Streams | `commit.interval.ms` | 500 | Balance latency vs. throughput |
| RocksDB | Cache size | 512MB | Tune based on state store size |
| JVM | Heap | 2GB | Adjust based on traffic volume |
| Threads | `num.stream.threads` | CPU cores | Match to available vCPUs |

## Monitoring

The application logs state transitions and +EV detections:

```
INFO  EVOpportunityStreamsApp - State transition: CREATED -> RUNNING
INFO  EVOpportunityTopology - +EV Opportunity detected: pinnacle vs kalshi | Eagles -3.5 spread | EV: 7.33%
```

For production monitoring, integrate with:
- Prometheus JMX exporter for metrics
- Kafka Streams built-in metrics via JMX

## Testing

Unit tests use `TopologyTestDriver` to verify join logic without a running Kafka cluster:

```bash
mvn test
```

## Project Structure

```
kafka-streams-topology/
├── pom.xml                              # Maven build configuration
├── Dockerfile                           # Multi-stage Docker build
├── README.md                            # This file
└── src/
    ├── main/
    │   ├── java/com/blitzlines/streams/
    │   │   ├── EVOpportunityStreamsApp.java    # Main entry point
    │   │   ├── model/
    │   │   │   ├── TransformedLine.java        # Input POJO (from sportsbooks)
    │   │   │   └── JoinedOdds.java             # Output POJO (joined pair)
    │   │   ├── serde/
    │   │   │   ├── JsonSerde.java              # JSON serializer/deserializer
    │   │   │   └── StreamSerdes.java           # Serde factory
    │   │   └── topology/
    │   │       └── EVOpportunityTopology.java  # KTable join topology
    │   └── resources/
    │       └── logback.xml                     # Logging configuration
    └── test/
        └── java/com/blitzlines/streams/topology/
            └── EVOpportunityTopologyTest.java  # Unit tests
```

## Next Steps

After deploying this Kafka Streams topology:

1. **Verify joins**: Check `joined-odds` topic for output
2. **Deploy C++ SIMD engine**: Consume from `joined-odds`, calculate EV% and Kelly
3. **Connect dashboard**: WebSocket integration for real-time alerts

## Troubleshooting

### No output on joined-odds topic
- Verify input topics have matching `gameId|marketType|selection|lineValue` keys
- Check that Pinnacle data is present (sharp book required for joins)
- Review logs for deserialization errors

### High latency
- Reduce `commit.interval.ms`
- Increase `num.stream.threads`
- Check RocksDB cache hit rate

### Out of memory
- Increase JVM heap (`-Xmx`)
- Enable RocksDB compression
- Check for unbounded state growth
