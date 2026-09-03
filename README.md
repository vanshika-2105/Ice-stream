#  Ice-Stream — Real-Time Data Quality & Lakehouse Observability Platform

> A real-time streaming data pipeline that detects data-quality issues, processes checkout events, stores reliable data in an Apache Iceberg lakehouse, and provides a live monitoring dashboard.

---

##  Project Overview

**Ice-Stream** is a real-time data engineering and observability platform designed to monitor the quality and health of a streaming data pipeline.

The system simulates an e-commerce checkout platform where checkout events are continuously generated and processed in real time.

The platform will:

- Generate real-time checkout events
- Stream events through Apache Kafka
- Process events using Apache Flink
- Validate incoming data using data-quality rules
- Detect invalid or suspicious records
- Calculate real-time data-quality metrics
- Generate alerts when quality thresholds are violated
- Store processed data in Apache Iceberg
- Use MinIO as local object storage
- Expose alerts and metrics through a FastAPI backend
- Send real-time updates using WebSockets
- Display pipeline health and data-quality information on a React dashboard

The complete project will be developed over **20 days** by a team of three members.

---

#  Project Objective

The primary objective is to build a complete real-time data pipeline with built-in data-quality monitoring.

The final system should allow a user to open a dashboard and see:

- Whether Kafka is healthy
- Whether Flink is processing events
- Whether Iceberg storage is working
- Number of events processed
- Number of valid events
- Number of invalid events
- Data-quality score
- Types of data-quality errors
- Real-time alerts
- Recent pipeline activity

---

#  Problem Statement

Modern applications generate huge amounts of streaming data.

However, streaming data can contain problems such as:

- Missing fields
- Invalid values
- Incorrect data types
- Negative prices
- Invalid quantities
- Invalid timestamps
- Duplicate events
- Unexpected values
- Sudden drops in data quality

If these problems are detected only after the data has been stored, it can be difficult to identify when and where the problem occurred.

Ice-Stream solves this by monitoring the data **while it is moving through the pipeline**.

---

#  High-Level Architecture

```text
                         ┌──────────────────────┐
                         │   Checkout Producer  │
                         │      (Python)        │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │        Kafka         │
                         │  checkout-events     │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │        Flink         │
                         │ Stream Processing    │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Quality Engine     │
                         │                      │
                         │ Validation Rules     │
                         │ Quality Metrics      │
                         │ Error Detection      │
                         └──────────┬───────────┘
                                    │
                     ┌──────────────┴──────────────┐
                     │                             │
                     ▼                             ▼
              ┌─────────────┐              ┌──────────────┐
              │    VALID    │              │   INVALID    │
              │   EVENTS    │              │    EVENTS    │
              └──────┬──────┘              └──────┬───────┘
                     │                             │
                     ▼                             ▼
              ┌─────────────┐              ┌──────────────┐
              │   Iceberg   │              │ Alert Server │
              │   Tables    │              │   FastAPI    │
              └──────┬──────┘              └──────┬───────┘
                     │                             │
                     │                             ▼
                     │                       ┌──────────────┐
                     │                       │  WebSocket   │
                     │                       └──────┬───────┘
                     │                              │
                     └──────────────┬───────────────┘
                                    ▼
                         ┌──────────────────────┐
                         │   React Dashboard    │
                         │                      │
                         │ Pipeline Health      │
                         │ Quality Metrics      │
                         │ Alerts               │
                         │ Event Statistics     │
                         └──────────────────────┘
 # Complete Data Flow

The final system will work as follows:

1. Python Producer
       ↓
2. Generate Checkout Event
       ↓
3. Kafka
       ↓
4. Flink
       ↓
5. Parse + Transform
       ↓
6. Data Quality Validation
       ↓
7. Calculate Quality Metrics
       ↓
8. Separate Valid / Invalid Events
       ↓
9. Store Valid Data in Iceberg
       ↓
10. Generate Alerts for Problems
       ↓
11. FastAPI Alert Server
       ↓
12. WebSocket
       ↓
13. React Dashboard
 Technologies Used


Backend / Data Engineering

Technology                	Purpose
Python	Producer,     validation and backend logic
Apache Kafka	        Real-time event streaming
Apache Flink	        Real-time stream processing
Apache Iceberg       	Lakehouse table format
MinIO	                Local S3-compatible object storage
FastAPI             	Backend API and alert server
WebSockets          	Real-time dashboard communication

Frontend

Technology	         Purpose
React	            Dashboard UI
Vite	            React development/build tool
React Flow       	Pipeline visualization
JavaScript       	Frontend logic

Development

Technology	                Purpose
Docker               	Run infrastructure locally
Docker Compose  	     Manage multiple services
Git	                   Version control
GitHub	               Collaboration and source control
Pytest	               Backend/quality testing
