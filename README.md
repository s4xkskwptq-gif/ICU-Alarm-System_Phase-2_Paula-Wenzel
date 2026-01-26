# ICU-Alarm-System_Phase-2_Paula-Wenzel
Prototype demonstrating an ICU alarm pipeline with streaming, prioritization, and visualization.

This repository contains a prototype built as part of Portfolio Phase 2 – Data-Intensive Systems. The project showcases the processing of time-referenced streaming data using a simulated ICU alarm use case.
Synthetic ICU vital sign alarms (events) are generated, validated, enriched, and delivered in an event-driven microservice architecture. This is accomplished by using a prioritization algorithm. Apache Kafka serves 
as the central event stream, so the microservices can be decoupled. This ensures a scalable, real-time processing. Alarm states are then stored in PostgreSQL, with active and historical alarm states separated to 
enable live monitoring and auditing. In the end a FastAPI service provides REST interfaces to manage and query alarms, while a simple web dashboard serves to visualize alarms in real time and show prioritized 
alarms.

All data used in this project is fully synthetic. The system is not a medical system and is for educational and architectural demonstration purposes only. It does not imply a ready-for-use clinical or production 
medical system.
 
## Architecture

**Core idea:** Kafka in the middle → services do not depend on each other directly.

**Pipeline:**


Alarm Generator
   → Validator Consumer 
      → Alarm Processor
         → Validator Alerts
            → Priority Engine
               → Validator Prioritized
                  → Raw & Alarm Storage
                     → Alarm Service


**Main components:**

* **Kafka (KRaft mode)** – central event backbone
* **PostgreSQL** – persistence (ACTIVE + HISTORY tables)
* **FastAPI** – delivery layer (REST API + dashboard)
* **Docker Compose** – reproducible local deployment



## Services & Responsibilities

| Layer                        | Service                           | Responsibility                                     |
| ---------------------------- | --------------------------------- | -------------------------------------------------- |
| Ingestion                    | `alarm-generator`                 | Emits synthetic ICU events                         |
| Validation                   | `alarm-processor`                 | Validates & enriches raw events                    |
| Processing                   | `alarm-processor`                 | Detects alarms, classifies severity & data quality |
| Aggregation / Prioritization | `priority-engine`                 | Context-aware scoring & prioritization             |
| Storage                      | `alarm-storage`                   | ACTIVE upsert + HISTORY append                     |
| Delivery                     | `alarm-service`                   | REST API + web dashboard                           |



## Data Storage Design

* **ACTIVE table**
  One row per `(patient_id, alarm_type, data_quality)` → represents the *current state*
* **HISTORY table**
  Append-only → full audit trail over time

Clinical and artifact alarms are stored **separately** to avoid suppressing clinical alarms with technical artifacts.



## Running the System

### Prerequisites

Docker Desktop

### Start

docker compose up --build


### Access

Once all containers are running:

* **Dashboard**
  [http://localhost:8000](http://localhost:8000)

* **API Health Check**
  [http://localhost:8000/health](http://localhost:8000/health)

* **Active Alarms (API)**
  [http://localhost:8000/alerts/active](http://localhost:8000/alerts/active)

The dashboard is served directly by the FastAPI service inside Docker.



## Kafka Topics

| Topic                | Purpose                          |
| -------------------- | -------------------------------- |
| `raw_alarms`         | Synthetic ICU events             |
| `enriched_alarms`    | Validated & classified alarms    |
| `prioritized_alarms` | Context-aware prioritized alarms |

Topics are auto-created to keep setup simple.



## API Endpoints (Selection)

| Endpoint                    | Description                    |
| --------------------------- | ------------------------------ |
| `GET /alerts/active`        | Current active alarms          |
| `GET /alerts/history`       | Historical alarms (pagination) |
| `GET /alerts/{id}`          | Alarm details                  |
| `POST /alerts/{id}/ack`     | Acknowledge alarm              |
| `POST /alerts/{id}/resolve` | Resolve alarm                  |

Filtering by priority, ward, alarm type, score and data quality is supported.



## Reliability, Scalability & Maintainability

* Kafka decouples producers and consumers
* Consumer groups enable horizontal scaling
* Stable database initialization
* Restart policies for all services
* Health checks to control startup order
* Environment variables for configuration
* Clear service boundaries and schemas



## Security, Governance & Data Protection

* No real patient data (synthetic only, at this point)
* Configuration via environment variables
* Dedicated database user
* Minimal privileges (demo scope)
* No credentials committed outside Docker context
