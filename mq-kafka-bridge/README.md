# MQ-Kafka Bridge

A Spring Boot application that bridges IBM MQ and Apache Kafka for real-time banking transaction events. This service consumes fixed-length COBOL copybook formatted messages from IBM MQ, transforms them into Avro-serialized events, and publishes them to Kafka topics.

## Features

- **Message Consumption**: Reads transaction events from IBM MQ queues
- **Data Transformation**: Parses COBOL copybook format into structured Java objects
- **Avro Serialization**: Converts events to Avro format for efficient Kafka publishing
- **Spring Integration**: Uses Spring Integration for message flow orchestration
- **Health Monitoring**: Includes Spring Boot Actuator for application health checks

## Architecture

The application follows an event-driven architecture:

1. **Inbound Channel**: Polls IBM MQ for new transaction messages
2. **Message Filtering**: Applies business rules to filter relevant messages
3. **Transformation**: Converts raw COBOL data to `MqTransactionEvent` POJO, then to Avro `TransactionEvent`
4. **Outbound Channel**: Publishes Avro events to Kafka topics with transaction ID as key

## Technologies

- **Java 17**
- **Spring Boot 3.x**
- **Spring Integration**
- **IBM MQ JMS**
- **Apache Kafka**
- **Apache Avro**
- **Confluent Schema Registry**
- **Maven**

## Prerequisites

- Java 17 or higher
- Maven 3.6+
- Docker and Docker Compose (for running MQ and Kafka locally)

## Quick Start

### 1. Start Infrastructure

Use the provided Docker Compose files to start IBM MQ and Kafka:

```bash
# Start IBM MQ
cd docker/ibmmq
docker-compose up -d

# Start Kafka (with Zookeeper and Schema Registry)
cd ../kafka
docker-compose up -d
```

### 2. Configure Application

Update `src/main/resources/config/application-local.yml` with your MQ and Kafka connection details if needed.

### 3. Build and Run

```bash
# Build the application
mvn clean package

# Run the application
java -jar target/mq-kafka-bridge-1.0.0-SNAPSHOT.jar
```

The application will start on port 8080 by default.

## Configuration

Key configuration properties (in `application-local.yml`):

- **IBM MQ**:
  - `ibm.mq.conn-name`: MQ connection string
  - `ibm.mq.queue-manager`: Queue manager name
  - `ibm.mq.channel`: Connection channel
  - `ibm.mq.user` / `ibm.mq.password`: Credentials

- **Kafka**:
  - `spring.kafka.bootstrap-servers`: Kafka broker addresses
  - `spring.kafka.properties.[schema.registry.url]`: Schema Registry URL

- **Channels**:
  - `inbound-channel.queue-name`: Source MQ queue
  - `outbound-channel.topic-name`: Target Kafka topic

## Message Format

### Input (COBOL Copybook)
The application expects fixed-length records from MQ matching the `MQTXNEVT` copybook:

```
01  MQ-TRANSACTION-RECORD.
   05 MQ-RECORD-TYPE             PIC X(3).     *> 'TXN'
   05 MQ-TRANSACTION-ID          PIC 9(10).    *> 10-digit ID
   05 MQ-ACCOUNT-NUMBER          PIC X(10).    *> Account number
   05 MQ-TRANSACTION-TYPE        PIC X(1).     *> 'D' or 'C'
   05 MQ-AMOUNT                  PIC 9(8)V99.  *> Amount with decimals
   05 MQ-CURRENCY                PIC X(3).     *> Currency code
   05 MQ-TRANSACTION-TIMESTAMP   PIC 9(14).    *> YYYYMMDDHHMMSS
   05 MQ-MERCHANT-NAME           PIC X(15).    *> Merchant name
   05 MQ-CHANNEL                 PIC X(3).     *> Channel (POS/ATM/etc.)
```

**Sample Input:**
```
TXN0000009876ACC2001234C0000004500CAD20250612123000AMAZON         ONL
```

### Output (Avro)
Messages are published to Kafka as Avro `TransactionEvent` records with headers and payload.

## Monitoring

The application exposes health and metrics endpoints via Spring Boot Actuator:

- Health check: `http://localhost:8080/actuator/health`
- Metrics: `http://localhost:8080/actuator/metrics`
- Info: `http://localhost:8080/actuator/info`

## Development

### Running Tests

```bash
mvn test
```

### Code Quality

The project uses:
- Spotless for code formatting
- JaCoCo for test coverage reporting

## Related Projects

- [Banking Event Platform](https://github.com/saundharpreet/banking-event-platform) - Parent project
- [Top Cat POM](https://github.com/saundharpreet/top-cat-pom) - Parent Maven configuration