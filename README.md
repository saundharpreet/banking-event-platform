# Event Driven Financial Platform

## Overview

Event Driven Financial Platform is an enterprise grade event driven microservices system designed to simulate real world banking transaction processing using modern distributed architecture patterns.

This platform demonstrates how batch and real time financial data can be ingested, processed, stored, and exposed through APIs using Kafka as the central event streaming backbone.

The system follows industry standard practices where Kafka acts as the single source of truth and downstream services build projections such as databases and APIs.

This project is built as a proof of concept to showcase end to end integration using Spring Boot, Apache Kafka, PostgreSQL, and REST APIs.

## Architecture Principles

- Event Driven Architecture  
- Kafka as central event streaming platform  
- Loose coupling between services  
- Scalable microservices design  
- Replayable and fault tolerant system  
- Cloud ready deployment design  

## Core Components

This repository contains multiple microservices and supporting modules.

### eod-transaction-file-processor

Reads batch EOD transaction files and publishes transaction events to Kafka.

### transaction-event-bridge

Consumes real time messages from MQ and publishes them to Kafka.

### transaction-event-processor

Consumes Kafka events and persists them into the database.

This service acts as the only database writer.

### transaction-api-service

Provides REST APIs to access and manage transaction and account data.

### transaction-api-client

Client library for interacting with the transaction API service.

## High Level Flow

### Batch Flow

![Diagram](architecture-diagram.png)

## Technology Stack

- Java  
- Spring Boot  
- Spring Kafka  
- Spring Data JPA  
- PostgreSQL  
- Apache Kafka  
- Docker  
- Kubernetes  
- REST APIs  

## Key Features

- Batch transaction ingestion  
- Real time transaction streaming  
- Event driven processing  
- Centralized event backbone using Kafka  
- Database projection from Kafka events  
- RESTful APIs for data access  
- Scalable microservice architecture  
- Cloud deployment ready  

## Repository Structure

- event-driven-financial-platform

- eod-transaction-file-processor
- transaction-event-bridge
- transaction-event-processor
- transaction-api-service
- transaction-api-client

- database-scripts
- docker-config
- kubernetes-deployments
- shared-libraries

## Purpose of This Project

This project is designed to demonstrate practical implementation of enterprise integration patterns commonly used in banking and financial systems.

It can be used for:

- Learning event driven architecture  
- Practicing Spring Boot and Kafka integration  
- Understanding batch and real time processing  
- Preparing for backend and distributed systems interviews  
- Demonstrating production style architecture  

## Future Enhancements

- Authentication and Authorization  
- Monitoring and Observability  
- Dead Letter Queue handling  
- Schema Registry integration  
- Cloud deployment  
- Frontend dashboard  

## Author

Harpreet Saund  

Senior Software Developer  

Specializing in Spring Boot, Kafka, and Distributed Systems
