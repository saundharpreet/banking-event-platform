# Transaction File Ingestor

A robust, enterprise-grade Spring Boot application designed to ingest end-of-day (EOD) transaction files from SFTP servers, process them using batch processing, and publish events to Apache Kafka for downstream consumption. This service is part of the Banking Event Platform ecosystem.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Frameworks & Technologies](#frameworks--technologies)
- [Key Features](#key-features)
- [Distributed Processing & Multi-Instance Support](#distributed-processing--multi-instance-support)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Development](#development)

---

## Overview

The Transaction File Ingestor is a microservice that:
- **Retrieves** transaction files from remote SFTP servers
- **Processes** files using Spring Batch for efficient, scalable batch operations
- **Transforms** raw transaction data into structured Avro events
- **Publishes** events to Apache Kafka for real-time consumption by downstream services
- **Maintains** metadata and state for reliable file tracking and reprocessing
- **Supports** horizontal scaling with multiple instances using distributed file synchronization
- **Coordinates** file processing across instances to prevent duplicate processing

This service is optimized for high-volume transaction processing with built-in fault tolerance, idempotency, comprehensive monitoring, and seamless multi-instance deployment support.

---

## Architecture

![Project Architecture](architecture-diagram.png)

---

## Frameworks & Technologies

### Spring Boot 3.x
**Why Selected:** Spring Boot is the industry standard for building production-ready Java applications with minimal boilerplate.
- **Rapid Development:** Convention over configuration allows fast iteration
- **Embedded Tomcat:** No separate application server needed
- **Auto-Configuration:** Intelligent defaults reduce manual setup
- **Production-Ready:** Built-in health checks, metrics, and monitoring
- **Ecosystem:** Seamless integration with Spring ecosystem components

### Spring Integration
**Why Selected:** Provides enterprise integration patterns for connecting disparate systems.
- **SFTP Adapter:** Reliable file retrieval with configurable polling
- **Message-Driven Workflows:** Decoupled, event-driven architecture
- **Metadata Store:** Distributed file tracking across multiple instances—prevents duplicate processing
- **Distributed Synchronization:** Persistent state management using PostgreSQL ensures consistency across instances
- **Channel Adapters:** Clean separation between inbound and outbound flows
- **Error Handling:** Built-in retry logic and error channels

### Spring Batch
**Why Selected:** Purpose-built framework for batch processing large volumes of data efficiently.
- **Chunked Processing:** Configurable chunk sizes for memory-efficient processing
- **Job Management:** Robust job execution, restart, and recovery capabilities
- **Item Processing:** Clean reader → processor → writer pattern
- **Transaction Management:** Atomic operations with rollback capabilities
- **Performance:** Optimized for high-throughput scenarios
- **Fault Tolerance:** Automatic retry and skip policies

### Apache Kafka & Spring Kafka
**Why Selected:** Distributed event streaming platform for real-time data pipelines.
- **High Throughput:** Processes millions of events per second
- **Durability:** Persists events for fault tolerance and replay
- **Scalability:** Horizontal scaling through partitioning
- **Real-Time:** Enables downstream services to react immediately
- **Integration:** Seamless Spring Kafka integration for producer/consumer logic

### Apache Avro
**Why Selected:** Schema-first, language-neutral data serialization format.
- **Schema Evolution:** Supports backward and forward compatibility
- **Compact Serialization:** Smaller payload sizes than JSON/XML
- **Schema Registry:** Confluent schema registry for schema management
- **Type Safety:** Compile-time schema validation and code generation
- **Cross-Language:** Works seamlessly across different programming languages

### PostgreSQL
**Why Selected:** Robust, open-source relational database for data persistence.
- **ACID Compliance:** Ensures data integrity for batch operations
- **Spring JDBC Integration:** Simple, template-based database access
- **Batch Infrastructure:** Spring Batch's own job repository and execution context
- **Metadata Store:** Reliable tracking of processed files
- **Scalability:** Proven handling of enterprise workloads

### Liquibase
**Why Selected:** Database change management and migration tool.
- **Version Control:** Track all database schema changes
- **Reproducibility:** Ensure consistent database state across environments
- **Rollback Support:** Safely revert schema changes if needed
- **Multiple DB Support:** Works with PostgreSQL and other databases
- **CI/CD Integration:** Automated migrations in deployment pipelines

### Spring Boot Actuator
**Why Selected:** Provides production-ready monitoring and management endpoints.
- **Health Checks:** Built-in `/health` endpoint for liveness/readiness probes
- **Metrics:** JMeter integration for performance monitoring
- **Custom Endpoints:** Expose application-specific metrics
- **Container-Ready:** Perfect for Kubernetes deployments with probe integration

### Apache Maven
**Why Selected:** Industry-standard build automation tool for Java projects.
- **Dependency Management:** Centralized, transitive dependency handling
- **Plugin Ecosystem:** Avro code generation, Spring Boot packaging, etc.
- **Build Reproducibility:** Consistent builds across environments
- **Multi-Module Support:** Enables future modular architecture expansion

---

## Key Features

### 1. **Reliable File Ingestion**
- Polls SFTP server at configurable intervals
- Automatic duplicate detection using metadata store
- Configurable filename patterns and directory filtering
- **Multi-instance safe:** Coordinates file processing across distributed instances

### 2. **High-Performance Batch Processing**
- Configurable chunk sizes for memory efficiency
- Parallel processing capabilities
- Graceful error handling with skip and retry policies
- **Distributed execution:** Scales horizontally with multiple concurrent instances

### 3. **Event-Driven Architecture**
- Spring Integration channels for decoupled message flow
- Inbound channel for SFTP file retrieval
- Outbound channel for Kafka event publishing
- **Instance-aware:** Each instance writes to shared Kafka cluster for unified event stream

### 4. **Schema-First Data Design**
- Avro schemas define contract between services
- Automatic code generation from schema definitions
- Schema versioning and evolution support

### 5. **Comprehensive Monitoring**
- Spring Boot Actuator endpoints for health and metrics
- Structured logging for debugging and auditing
- Database-backed job execution tracking
- **Distributed observability:** Correlated logs across instances via event IDs

### 6. **Database Migration Management**
- Liquibase changelogs for version-controlled migrations
- Automatic schema initialization on application startup
- Batch metadata store for job tracking
- **Shared state management:** PostgreSQL ensures consistency across distributed instances

---

## Distributed Processing & Multi-Instance Support

The Transaction File Ingestor is designed for horizontal scalability with built-in support for multiple concurrent instances:

### 🔄 Multi-Instance Coordination

**Metadata Store Synchronization**
- Spring Integration's **metadata store** tracks processed files across all instances
- Stores file processing state in PostgreSQL (distributed, persistent storage)
- File tracking uses globally unique identifiers and timestamps
- Prevents duplicate file processing even when multiple instances are running

**How It Works:**
1. When an instance retrieves a file, it records the file name and processing state in the metadata store
2. Before processing a file, all instances check the metadata store for previous processing
3. If a file has already been processed by any instance, it is skipped
4. Upon successful processing, the completion status is recorded with the instance ID and timestamp

### ⚖️ Load Balancing

**Distributed Polling**
- Multiple instances can poll the SFTP server simultaneously
- Spring Integration's polling mechanism is stateless and instance-independent
- Each instance independently checks for new files based on configured polling intervals
- No polling coordination needed—metadata store ensures no duplicate processing

**File Distribution:**
- Files are naturally distributed across instances based on polling timing
- Heavy-load files are processed independently by instances that detect them
- Processing throughput scales linearly with the number of instances
- Configurable chunk sizes allow fine-tuning per instance capacity

### 🔒 Consistency Guarantees

**Idempotent Processing**
- Metadata store prevents re-processing of already-handled files
- Transaction-based operations ensure atomicity
- PostgreSQL ACID compliance provides consistency across instances
- Failed processing attempts can be safely retried

**Instance Failure Recovery**
- If an instance fails during file processing, the metadata store maintains the state
- Other instances continue processing remaining files
- Failed job can be restarted on a different instance without data loss
- Spring Batch's job repository tracks execution history for auditing

### 📊 Monitoring Multi-Instance Deployments

**Per-Instance Metrics**
- Each instance exposes `/actuator/metrics` endpoint with instance-specific data
- Spring Boot provides server.port=0 for dynamic port assignment in Kubernetes
- Instance identifiers can be set via `spring.application.instance-id`

**Centralized Observability**
- All instances write events to the same Kafka topic
- Kafka naturally provides ordering guarantees per partition
- Distributed traces correlate logs across instances via event IDs
- PostgreSQL metadata store provides centralized audit trail

### 🚀 Deployment Scenarios

**Scale-Up Example (1 to 3 instances):**
```bash
# Instance 1
docker run -e spring.profiles.active=local -e spring.application.instance-id=ingestor-1 transaction-file-ingestor

# Instance 2
docker run -e spring.profiles.active=local -e spring.application.instance-id=ingestor-2 transaction-file-ingestor

# Instance 3
docker run -e spring.profiles.active=local -e spring.application.instance-id=ingestor-3 transaction-file-ingestor
```

All three instances:
- Share the same PostgreSQL metadata store
- Share the same Kafka cluster
- Share the same SFTP server connection
- Coordinate file processing through metadata store
- Independently poll for new files

**Kubernetes Deployment:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: transaction-file-ingestor
spec:
  replicas: 3  # Automatically scaled to 3 instances
  selector:
    matchLabels:
      app: transaction-file-ingestor
  template:
    metadata:
      labels:
        app: transaction-file-ingestor
    spec:
      containers:
      - name: ingestor
        image: transaction-file-ingestor:1.0.0
        env:
        - name: SPRING_PROFILES_ACTIVE
          value: "local"
        - name: SPRING_APPLICATION_INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 8080
        livenessProbe:
          httpGet:
            path: /actuator/health/liveness
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /actuator/health/readiness
            port: 8080
          initialDelaySeconds: 20
          periodSeconds: 5
```

---

## Project Structure

```
transaction-file-ingestor/
├── src/
│   ├── main/
│   │   ├── avro/
│   │   │   └── eod-transaction-event.avsc    # Avro schema definition
│   │   ├── java/com/harpreetsaund/
│   │   │   └── transactionfileingestor/
│   │   │       ├── config/                    # Spring configurations
│   │   │       ├── listener/                  # Event listeners
│   │   │       ├── mapper/                    # Data mapping logic
│   │   │       ├── model/                     # Domain models
│   │   │       ├── processor/                 # Batch item processors
│   │   │       └── service/                   # Business logic
│   │   └── resources/
│   │       ├── config/                        # Application properties
│   │       └── db/                            # Database migrations
│   └── test/
│       └── java/                              # Integration and unit tests
├── docker/
│   ├── postgres/                              # PostgreSQL container
│   └── sftp/                                  # SFTP test server
├── pom.xml                                    # Maven project configuration
└── README.md                                  # This file
```

---

## Getting Started

### Prerequisites

- Java 17 or higher
- Maven 3.8.0 or higher
- Docker & Docker Compose (for local development)
- PostgreSQL 14+ (or use Docker image)
- Apache Kafka with Schema Registry (or use Docker image)

### Quick Start with Docker Compose

1. **Start Infrastructure Services:**
   ```bash
   # Start PostgreSQL
   cd docker/postgres
   docker-compose up -d
   
   # Start SFTP Server
   cd docker/sftp
   docker-compose up -d
   
   # Start Kafka (if using separate compose file)
   cd docker/kafka
   docker-compose up -d
   ```

2. **Build the Application:**
   ```bash
   mvn clean package
   ```

3. **Run the Application:**
   ```bash
   mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=local"
   ```

4. **Verify Health:**
   ```bash
   curl http://localhost:8080/actuator/health
   ```

### Configuration

The application uses Spring Boot's externalized configuration with profile-specific files:

- `application.yml` - Common configuration
- `application-local.yml` - Local development overrides

## Development

### Building the Project

```bash
# Clean build
mvn clean package

# Build without running tests
mvn clean package -DskipTests

# Build with specific profile
mvn clean package -Dspring.profiles.active=local
```

### Running Tests

```bash
# Run all tests
mvn test

# Run specific test class
mvn test -Dtest=TransactionFileIngestorApplicationTests

# Run with coverage
mvn clean test jacoco:report
```

### Code Generation

The Avro Maven plugin automatically generates Java classes from schema definitions:

```bash
mvn generate-sources
```

Generated classes are placed in:
```
target/generated-sources/avro/com/harpreetsaund/transaction/avro/
```

### Database Migrations

Liquibase migrations are in:
```
src/main/resources/db/changelog/
```

To create a new migration:
1. Add a new changeset in `db.changelog-master.yaml`
2. Run the application (automatic migration on startup)
3. Or manually run: `mvn liquibase:update`

### Code Style

The project uses Eclipse Java formatting standards defined in `eclipse-java-style.xml`. Configure your IDE to use this style guide.

---

## Technology Selection Justification

| Technology | Problem Solved | Rationale |
|-----------|---|---|
| **Spring Boot** | Framework complexity, boilerplate code | Industry standard with vast ecosystem and excellent documentation |
| **Spring Integration** | System integration complexity | Purpose-built for enterprise integration patterns |
| **Spring Batch** | Batch processing requirements | Optimized for high-volume data processing with transaction support |
| **Apache Kafka** | Event streaming, real-time publishing | Distributed, scalable, durable event platform |
| **Apache Avro** | Schema management, compact serialization | Schema evolution, language-neutral, efficient serialization |
| **PostgreSQL** | Data persistence | ACID compliance, excellent Spring integration, proven reliability |
| **Liquibase** | Database change management | Version-controlled migrations, multi-environment support |
| **Spring Actuator** | Production monitoring | Built-in health checks, metrics, Kubernetes-ready |
| **Maven** | Build automation | Industry standard, extensive plugin ecosystem |

---

## API Endpoints

### Health & Monitoring

- `GET /actuator/health` - Application health status
- `GET /actuator/health/liveness` - Kubernetes liveness probe
- `GET /actuator/health/readiness` - Kubernetes readiness probe
- `GET /actuator/metrics` - Application metrics

---