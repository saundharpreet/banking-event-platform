# Transaction File Ingestor

A robust, enterprise-grade Spring Boot application designed to ingest end-of-day (EOD) transaction files from SFTP servers, process them via Spring Batch, and publish Avro-encoded events to Apache Kafka. This service is a core component of the Banking Event Platform ecosystem.

## Key Features

* **Reliable Ingestion:** Automated SFTP polling with duplicate detection via a distributed metadata store.
* **High-Performance Batching:** Chunk-oriented processing with configurable skip/retry policies and parallel execution.
* **Event-Driven Architecture:** Decoupled flow using Spring Integration and Kafka for real-time downstream consumption.
* **Type-Safe Serialization:** Schema-first design using Apache Avro for strict data contracts and evolution.
* **Horizontal Scalability:** Native support for multi-instance deployments with synchronized state management.

## Tech Stack & Justification

| Technology | Role | Why It Was Chosen |
| :--- | :--- | :--- |
| **Spring Boot 3.x** | Core Framework | Rapid development, embedded runtime, and production-ready metrics. |
| **Spring Batch** | ETL Engine | Handles massive datasets with restartability and transaction management. |
| **Spring Integration** | Orchestration | Manages SFTP polling and message routing via enterprise patterns. |
| **Apache Kafka** | Event Streaming | High-throughput, durable distribution of transaction events. |
| **Apache Avro** | Serialization | Compact binary format with schema registry support for evolution. |
| **PostgreSQL** | Persistence | ACID-compliant state tracking for the Batch Job Repository. |
| **Liquibase** | DB Migration | Version-controlled schema changes across all environments. |

## Engineering Highlight: Distributed Race Condition Fix

This project identified and resolved a critical race condition in the **Spring Integration** framework affecting clustered deployments.

### The Challenge
In a horizontally scaled environment, multiple instances would occasionally attempt to "claim" the same SFTP file at the exact same millisecond. This caused `DuplicateKeyException` (PostgreSQL) or `CannotAcquireLockException` (MySQL), resulting in the entire polling cycle crashing rather than simply skipping the file.

### The Open Source Contribution
I documented this behavior and provided a reproducible case to the Spring team, leading to a framework-level fix (introduced in **v6.4.0**) that utilizes native database "upsert" logic (`ON CONFLICT DO NOTHING`).

* **Official Fix:** Backported to Spring Integration 6.3.4 and 6.2.9.
* **Case Discussion:** [Stack Overflow #78979169](https://stackoverflow.com/questions/78979169/)

## Performance Benchmarks

Local performance test results processing a standard EOD dataset:

| Metric                      | Result                         |
|:----------------------------|:-------------------------------|
| **Total Transactions Read** | 50,000 (Chunks)                |
| **Kafka Events Written**    | 5,000,000 (100x Multiplier)    |
| **Total Execution Time**    | 5m 53s                         |
| **Average Throughput**      | ~142 items/second              |
| **Error Rate**              | 0.0% (Zero skips or rollbacks) |

> **Note:** Each transaction generates an average of 100 Kafka events, demonstrating the service's ability to handle high-volume write amplification efficiently.

## Distributed Processing & Scaling

The Ingestor is designed to scale horizontally across `n` instances:

* **Shared State:** All instances point to a shared PostgreSQL metadata store. This ensures that if Instance A picks up a file, Instance B immediately recognizes it as "In Progress" or "Complete."
* **Idempotency:** The metadata store prevents duplicate processing even if polling intervals overlap.
* **Fault Tolerance:** If an instance fails mid-process, the Job Repository maintains the last successful chunk, allowing a different instance to resume the work without data loss.

## Project Structure

```text
transaction-file-ingestor/
├── src/main/avro/           # Avro schema definitions (.avsc)
├── src/main/java/           # Core logic (Config, Processors, Services)
├── src/main/resources/      # Application properties and Liquibase changelogs
├── docker/                  # Local infrastructure (Postgres, Kafka, SFTP)
└── pom.xml                  # Maven dependencies and Avro code-gen
```

## Related Projects

- [Banking Event Platform](https://github.com/saundharpreet/banking-event-platform) - Parent project
- [Top Cat POM](https://github.com/saundharpreet/top-cat-pom) - Parent Maven configuration