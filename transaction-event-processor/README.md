# Transaction Event Processor

A Spring Boot microservice that processes real-time and end-of-day transaction events from Kafka, transforms them using Apache Avro schemas, and persists them to PostgreSQL.

## Overview

The Transaction Event Processor is a high-performance event streaming application built with Spring Boot and Spring Integration. It consumes transaction events from Kafka topics, applies message transformations, and stores normalized transaction data in a PostgreSQL database.

### Key Features

- **Real-time Event Processing**: Processes real-time transaction events and EOD (End-of-Day) batch transactions
- **Apache Avro Serialization**: Uses Avro schemas for robust event serialization/deserialization with Kafka
- **Spring Integration Framework**: Sophisticated message routing and transformation pipelines
- **JPA Data Persistence**: Stores transactions in PostgreSQL with optimized indexes
- **Health Monitoring**: Spring Boot Actuator endpoints for health checks and metrics
- **Kafka Integration**: Native Spring Kafka and Confluent serializers for Avro support

## Architecture

### Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Framework | Spring Boot | 3.x |
| Message Stream | Apache Kafka | Latest |
| Serialization | Apache Avro | 1.12.1 |
| Database | PostgreSQL | Latest |
| ORM | Spring Data JPA | 3.x |
| Message Integration | Spring Integration | Latest |
| Build Tool | Maven | 3.9.11+ |
| Java | OpenJDK | 21 |

## Getting Started

### Prerequisites

- Java 21 or higher
- Maven 3.9.11 or higher
- PostgreSQL 12 or higher
- Apache Kafka 3.x or higher
- Docker & Docker Compose (optional, for local development)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/saundharpreet/transaction-event-processor.git
   cd transaction-event-processor
   ```

2. **Build the project**
   ```bash
   mvn clean install
   ```

3. **Configure application properties**
   
   Edit `src/main/resources/config/application-local.yml` with your environment:
   ```yaml
   spring:
     datasource:
       url: jdbc:postgresql://localhost:5432/transaction_db
       username: postgres
       password: your_password
     kafka:
       bootstrap-servers: localhost:9092
       schema-registry-url: http://localhost:8081
   ```

4. **Run the application**
   ```bash
   mvn spring-boot:run
   ```

   The application will start on `http://localhost:8080`

## Event Data Models

### Transaction Event

Real-time transaction events containing transaction details:

```json
{
  "headers": {
    "eventId": "evt-12345",
    "sourceSystem": "MQ",
    "topicName": "transactions",
    "eventType": "TRANSACTION",
    "eventTimestamp": 1646908200000,
    "payloadSchemaVersion": "1.0"
  },
  "payload": {
    "transactionId": "txn-001",
    "accountNumber": "ACC-12345",
    "transactionType": "DEBIT",
    "amount": 150.50,
    "currency": "CAD",
    "transactionTimestamp": 1646908200000,
    "merchantName": "Sample Store",
    "channel": "POS"
  }
}
```

### EOD Transaction Event

End-of-day batch transaction events with same structure but for batch processing.

## Processing Pipeline

```
Kafka Topic (TransactionEvent)
        ↓
Kafka Consumer
        ↓
Message Routing (InboundChannelConfig)
        ↓
MessageTransformService
(Transforms Avro → Transaction entity)
        ↓
JPA Repository
        ↓
PostgreSQL Database
```

### Data Flow

1. **Inbound**: Kafka topics publish serialized Avro transaction events
2. **Deserialization**: Spring Kafka with Confluent Avro serializer deserializes events
3. **Transformation**: `MessageTransformService` maps Avro events to JPA entities
4. **Persistence**: Spring Integration JPA outbound adapter persists to PostgreSQL
5. **Monitoring**: Actuator endpoints provide health and metrics

## API Endpoints

### Health Check
```bash
GET http://localhost:8080/actuator/health
```

Response:
```json
{
  "status": "UP",
  "components": {
    "db": { "status": "UP" },
    "kafka": { "status": "UP" }
  }
}
```

### Application Metrics
```bash
GET http://localhost:8080/actuator/metrics
```

### Detailed Metrics
```bash
GET http://localhost:8080/actuator/metrics/{metric.name}
```

## 🧪 Testing

### Run All Tests
```bash
mvn test
```

### Test Coverage
```bash
mvn clean jacoco:report
```

Coverage report available at: `target/site/jacoco/index.html`

### Test Suites

- **TransactionMapperTest**: 8 test methods
  - Tests Avro to Transaction entity mapping
  - Tests all enum types and null handling
  
- **MessageTransformServiceTest**: 6 test methods
  - Tests message transformation with Mockito
  - Verifies header preservation
  
- **TransactionTest**: 14 test methods
  - Tests JPA entity behavior
  - Tests equals/hashCode/toString contracts
  
- **TransactionEventProcessorApplicationTests**: Integration test
  - Verifies application context loads successfully

**Total: 28 unit tests**

## Configuration

### Kafka Configuration

```yaml
spring:
  kafka:
    bootstrap-servers: ${KAFKA_BROKERS:localhost:9092}
    schema-registry-url: ${SCHEMA_REGISTRY_URL:http://localhost:8081}
    consumer:
      group-id: transaction-event-processor-group
      auto-offset-reset: earliest
      enable-auto-commit: false
    producer:
      acks: all
      retries: 3
```

### Database Configuration

```yaml
spring:
  datasource:
    url: jdbc:postgresql://${DB_HOST:localhost}:${DB_PORT:5432}/${DB_NAME:transaction_db}
    username: ${DB_USER:postgres}
    password: ${DB_PASSWORD:password}
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5
      connection-timeout: 30000
  jpa:
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
        format_sql: true
```

## 🗄️ Database Schema

### Transactions Table

```sql
CREATE TABLE transactions (
  id BIGSERIAL PRIMARY KEY,
  event_id VARCHAR(255) NOT NULL UNIQUE,
  transaction_id VARCHAR(255) NOT NULL,
  account_number VARCHAR(255) NOT NULL,
  transaction_type VARCHAR(50) NOT NULL,
  amount NUMERIC(18, 2) NOT NULL,
  currency VARCHAR(3) NOT NULL,
  merchant_name VARCHAR(255),
  channel VARCHAR(50),
  transaction_timestamp TIMESTAMP NOT NULL,
  source_system VARCHAR(255) NOT NULL,
  topic_name VARCHAR(255),
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT uk_event_id UNIQUE (event_id),
  INDEX idx_transactions_account (account_number),
  INDEX idx_transactions_txn_time (transaction_timestamp),
  INDEX idx_transactions_transaction_id (transaction_id)
);
```

## Development Guide

### Building the Project

```bash
# Clean build with all tests
mvn clean install

# Skip tests during build
mvn clean install -DskipTests

# Build and create JAR
mvn clean package
```

### Code Formatting

The project uses Spotless Maven plugin for code formatting following Eclipse Java Style:

```bash
# Check formatting
mvn spotless:check

# Apply formatting
mvn spotless:apply
```

### Generate Avro Classes

Avro classes are auto-generated from schema files during build:

```bash
mvn generate-sources
```

Generated classes are located in: `target/generated-sources/avro/`

## Monitoring & Observability

### Spring Boot Actuator Endpoints

| Endpoint | Purpose |
|----------|---------|
| `/actuator/health` | Application health status |
| `/actuator/metrics` | Available metrics |
| `/actuator/env` | Environment properties |
| `/actuator/loggers` | Log level management |
| `/actuator/info` | Application information |

### Kafka Metrics

Monitor Kafka consumer/producer metrics:
- Consumer lag
- Messages processed per second
- Error rates

### Database Metrics

- Connection pool utilization
- Query execution times
- Transaction throughput

## 📝 Avro Schemas

### transaction-event.avsc
Defines the real-time transaction event structure with event headers and payload.

### eod-transaction-event.avsc
Defines the end-of-day batch transaction event structure.

Both schemas use standard event envelope pattern with metadata headers and business data payload.

## 📄 License

This project is part of the banking-event-platform and follows the organization's licensing.

## Related Projects

- [Banking Event Platform](https://github.com/saundharpreet/banking-event-platform) - Parent project
- [Top Cat POM](https://github.com/saundharpreet/top-cat-pom) - Parent Maven configuration
