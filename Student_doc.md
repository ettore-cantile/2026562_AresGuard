# SYSTEM DESCRIPTION

AresGuard is a distributed, event-driven automation platform designed to monitor and manage a fragile Mars habitat. The system ingests data from heterogeneous REST sensors (polling), normalizes the telemetry into a Unified Event Schema, evaluates automated safety rules through a dedicated engine, and provides a real-time dashboard. 

The dashboard serves three main personas: 
* The **Mission Specialist** (for real-time monitoring and manual overrides).
* The **Automation Engineer** (to configure safety rules).
* The **Safety Officer** (to audit system health and automatic interventions).

---

# USER STORIES

1. **As a Mission Specialist**, I want to view the continuously updated temperature of the greenhouse so that I can monitor crop health without manually refreshing the dashboard.
2. **As a Mission Specialist**, I want to check the current water tank level on the dashboard so that I know how much water is left for hydroponic irrigation.
3. **As a Mission Specialist**, I want a dedicated button to manually turn on the hall ventilation so that I can rapidly cycle the air if I detect smoke or stale air.
4. **As a Mission Specialist**, I want the dashboard to visually highlight critical CO2 levels in red and trigger a system-wide warning when the concentration exceeds 1000 ppm, so that I can immediately take safety measures or evacuate the hall.
5. **As an Automation Engineer**, I want the system to continuously gather data from all available sensors so that the automation logic always evaluates the most recent habitat conditions.
6. **As an Automation Engineer**, I want to automatically convert all incoming sensor data into a single, unified format so that the core system logic is decoupled from specific device dialects.
7. **As an Automation Engineer**, I want to define an automatic rule that turns on the hall ventilation if the CO2 level exceeds a safe threshold so that dangerous carbon dioxide buildup is prevented.
8. **As an Automation Engineer**, I want all created automation rules to be securely saved so that I don't have to reconfigure the habitat's safety protocols if the system restarts.
9. **As a Safety Officer**, I want the dashboard to visually alert the crew if the corridor pressure drops below a safe threshold so that everyone is warned of a potential hull breach.
10. **As a Safety Officer**, I want to consult a historical log showing when and why an actuator was automatically triggered so that I can audit the system's "autopilot" behavior after an incident.

---

# CONTAINERS

## CONTAINER_NAME: Frontend

### DESCRIPTION 
Provides the User Interface for the Mission Specialist, the Automation Engineer, and the Safety Officer. It visualizes real-time telemetry, handles manual overrides, manages rule creation, and displays audit logs and live charts.

### USER STORIES
1, 2, 3, 4, 7, 9, 10

### PORTS 
`3000:80`

### PERSISTENCE EVALUATION
The Frontend container does not include a database. It stores minor UI preferences (like the active tab) in the browser's LocalStorage.

### EXTERNAL SERVICES CONNECTIONS
Connects directly to the **API Gateway** (via HTTP and WebSockets) and the **Simulator** (for manual forced fetches). Uses *Chart.js* via CDN for data visualization.

### MICROSERVICES

#### MICROSERVICE: aresguard_frontend
* **TYPE:** frontend
* **DESCRIPTION:** Serves the Single Page Application (SPA) dashboard built with HTML, CSS, and Vanilla JS, hosted on an Nginx alpine server.
* **PORTS:** `80` (Internal) / `3000` (External)
* **TECHNOLOGICAL SPECIFICATION:**
  * HTML5, CSS3, Vanilla JavaScript.
  * WebSockets for real-time updates.
  * Nginx:alpine as the web server.
  * Chart.js for real-time analytics.

---

## CONTAINER_NAME: API_Gateway

### DESCRIPTION 
Acts as the central communication hub. It exposes REST endpoints for rule management, proxies commands to the simulator, maintains the in-memory state of the latest sensor readings, and broadcasts updates to connected clients via WebSockets.

### USER STORIES
1, 2, 3, 8

### PORTS 
`8000:8000`

### PERSISTENCE EVALUATION
Maintains an in-memory cache (`sensor_state_cache`) for the latest telemetry data to provide instant full-state sync upon client connection. Connects to PostgreSQL to read, write, update, and delete automation rules.

### EXTERNAL SERVICES CONNECTIONS
Connects to **RabbitMQ** to consume the normalized telemetry stream. Connects to **PostgreSQL**. Connects to the **Simulator** to proxy actuator POST commands.

### MICROSERVICES

#### MICROSERVICE: aresguard_api_gateway
* **TYPE:** backend / middleware
* **DESCRIPTION:** Central API and WebSocket manager.
* **PORTS:** `8000`
* **TECHNOLOGICAL SPECIFICATION:**
  * Python 3.9
  * FastAPI (Web framework)
  * Uvicorn (ASGI server)
  * aio-pika (Asynchronous RabbitMQ client)
  * psycopg2-binary (PostgreSQL adapter)
* **SERVICE ARCHITECTURE:**
  Asynchronous event loop consuming from RabbitMQ while simultaneously serving REST requests and broadcasting over WebSockets.
* **ENDPOINTS:**

  | HTTP METHOD | URL | Description |
  | :--- | :--- | :--- |
  | **GET** | `/api/state` | Returns the full in-memory cache of sensors |
  | **GET** | `/api/sensors/{sensor_id}` | Proxies manual fetch requests to the Simulator |
  | **POST** | `/api/commands/{actuator_id}` | Proxies manual commands to the Simulator |
  | **GET** | `/api/rules` | Retrieves all saved automation rules |
  | **POST** | `/api/rules` | Saves a new rule (Upsert logic) |
  | **PUT** | `/api/rules/{rule_id}` | Updates an existing rule |
  | **DELETE** | `/api/rules/{rule_id}` | Deletes a rule |
  | **WS** | `/ws` | WebSocket stream for real-time live updates |

---

## CONTAINER_NAME: Ingestion

### DESCRIPTION 
Responsible for fetching data from legacy devices (REST polling), flattening the heterogeneous JSON payloads into a Unified Event Schema, and publishing them to the message broker.

### USER STORIES
5, 6

### PORTS 
*None exposed externally.*

### PERSISTENCE EVALUATION
Stateless service. No database required.

### EXTERNAL SERVICES CONNECTIONS
Polls the **Mars Simulator API**. Publishes to **RabbitMQ**.

### MICROSERVICES

#### MICROSERVICE: aresguard_ingestion
* **TYPE:** backend / worker
* **DESCRIPTION:** The data normalizer and polling scheduler.
* **TECHNOLOGICAL SPECIFICATION:**
  * Python 3.9
  * Requests (HTTP client)
  * Pika (Synchronous RabbitMQ client)

---

## CONTAINER_NAME: Rule_Engine

### DESCRIPTION 
The "brain" of the automation platform. It consumes the unified event stream from the broker, evaluates incoming telemetry against persisted rules, triggers actuators if thresholds are met, and records the intervention in the audit trail.

### USER STORIES
7, 8, 10

### PORTS 
*None exposed externally.*

### PERSISTENCE EVALUATION
Connects to PostgreSQL to evaluate rules in real-time and to insert audit logs. Maintains a minor internal dictionary cache (`last_action_cache`) to prevent actuator flapping/spamming.

### EXTERNAL SERVICES CONNECTIONS
Consumes from **RabbitMQ**. Posts commands to the **Mars Simulator**. Connects to **PostgreSQL**.

### MICROSERVICES

#### MICROSERVICE: aresguard_rule_engine
* **TYPE:** backend / worker
* **DESCRIPTION:** Event processor and decision maker.
* **TECHNOLOGICAL SPECIFICATION:**
  * Python 3.9
  * Pika (AMQP client)
  * psycopg2-binary (Database adapter)

---

## CONTAINER_NAME: Database

### DESCRIPTION 
The persistent storage layer for the platform. Stores automation rules and the historical audit trail of actions triggered by the Rule Engine.

### USER STORIES
8, 10

### PORTS 
`5432:5432`

### MICROSERVICES

#### MICROSERVICE: aresguard_db
* **TYPE:** database
* **DESCRIPTION:** Relational database.
* **PORTS:** `5432`
* **TECHNOLOGICAL SPECIFICATION:**
  * PostgreSQL 15 (Alpine)
* **DB STRUCTURE:** 

  **_sensor_data_** : | **id** | sensor_id | value | unit | timestamp |

  **_rules_** : | **id** | sensor_id | operator | threshold | actuator_id | action_value |
  
  **_audit_logs_** : | **id** | command_id | timestamp | sensor_id | sensor_value | actuator_id | action_taken |

---

## CONTAINER_NAME: Broker

### DESCRIPTION 
The central Message Broker implementing the Event-Driven Architecture.

### USER STORIES
1, 5, 6, 7

### PORTS 
`5672:5672` (AMQP)
`15672:15672` (Management UI)

### MICROSERVICES

#### MICROSERVICE: aresguard_broker
* **TYPE:** message broker
* **DESCRIPTION:** Routes normalized telemetry from Ingestion to the API Gateway and Rule Engine via Fanout exchange.
* **TECHNOLOGICAL SPECIFICATION:**
  * RabbitMQ 3 (Management)

---

## CONTAINER_NAME: Simulator

### DESCRIPTION 
The simulated Mars Habitat IoT environment provided by the assignment.

### PORTS 
`8080:8080`

### MICROSERVICES

#### MICROSERVICE: mars_simulator
* **TYPE:** simulator
* **DESCRIPTION:** Emulates legacy REST sensors and actuators.
* **TECHNOLOGICAL SPECIFICATION:**
  * Provided Docker Image (`mars-iot-simulator:multiarch_v1`)