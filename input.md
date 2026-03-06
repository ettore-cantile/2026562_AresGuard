# SYSTEM DESCRIPTION:

## 1. Short Pitch & System Overview
**Context:** We are in 2036. Following a "promotion" to Mars Operations at SpaceY, survival depends on managing fragmented data and heterogeneous sensors in a fragile, partially destroyed habitat.

**Problem:** The current automation stack is compromised. Devices speak incompatible dialects, and the REST sensors must be polled continuously to extract vital thermodynamic and life-support data. 

**Solution:** AresGuard is a distributed, event-driven automation platform designed to save the crew. It ingests data from heterogeneous REST sensors (polling), normalizes every vital sign into a Unified Event Schema, evaluates automated safety rules, and provides a centralized real-time dashboard to prevent fatal thermodynamic consequences. 

*Note: As a 2-person team, AresGuard focuses exclusively on polling REST devices and implementing 10 core user stories, omitting telemetry streams, as permitted by the mission guidelines.*

## 2. Unified Event & Command Schemas
To fully decouple ingestion, processing, and actuation, AresGuard implements a dual-structure approach. The ingestion service normalizes heterogeneous sensor data into a standard **Sensors Event Schema**, while the Rule Engine and Dashboard issue actions using a standard **Actuator Command Schema**.

### 2.1 Event Schema (Sensors)
Heterogeneous REST payloads (`rest.scalar.v1`, `rest.chemistry.v1`, etc.) are flattened into atomic events. For example, the `measurements` array in chemistry sensors is unpacked into individual single-metric events to ensure a uniform format.

```json
{
  "event_id": "string (UUID v4)",
  "timestamp": "string (ISO-8601)",
  "source": {
    "identifier": "string (Unique ID)",
    "protocol": "string"
  },
  "payload": {
    "value": "dynamic (number/boolean/string)",
    "unit": "string (SI Unit)",
    "category": "string"
  },
  "metadata": {
    "version": "string (Schema version)",
    "tags": "array (Labels)"
  }
}
```

### 2.2 Command Schema (Actuators)
When an automation rule evaluates to true , or a manual override is requested from the dashboard, the system generates a standardized command to invoke the actuator API.

```json
{
  "command_id": "string (UUID v4)",
  "timestamp": "string (ISO-8601)",
  "target": {
    "actuator_id": "string (Unique ID)",
    "action": "string"
  },
  "params": {
    "value": "dynamic (number/boolean/string)",
    "duration": "number (optional, seconds)"
  },
  "issued_by": "string"
}
```

## 3. Automation Rule Model
AresGuard implements a persistent rule engine that evaluates conditions dynamically upon event arrival. The system employs simple `IF-THEN` logic, as shown below.

**Rule Syntax:**
`IF <sensor_name> <operator> <value> [unit] THEN set <actuator_name> to ON|OFF` 

**Supported Operators:** `<`, `<=`, `=`, `>`, `>=` 

**Example:**
`IF greenhouse_temperature > 28 °C THEN set cooling_fan to ON` 

## 4. User Personas
1. **Mission Specialist (End User):** The astronaut relying on the dashboard to survive. Wants clear data, situational awareness, and quick manual controls.
2. **Automation Engineer (Technical):** Configures the "brain" of the system. Manages rule logic and data normalization.
3. **Safety Officer (Supervisor):** Monitors system health, connection statuses, and audits historical automated actions.

<br>

---

<br>

# USER STORIES:

| ID | Persona | User Story | Acceptance Criteria (AC) & Non-Functional Requirements (NFR) |
| :--- | :--- | :--- | :--- |
| **US01** | Mission Specialist | **As a** Mission Specialist, **I want to** view the continuously updated temperature of the greenhouse **so that** I can monitor crop health without manually refreshing the dashboard. | **AC:** The dashboard must display the latest temperature automatically. <br>**NFR:** Data must be fetched via scheduled REST polling. The latest state must be cached in-memory. |
| **US02** | Mission Specialist | **As a** Mission Specialist, **I want to** check the current water tank level on the dashboard **so that** I know how much water is left for hydroponic irrigation. | **AC:** The system must automatically read and display the current water level. <br>**NFR:** The system must poll the `water_tank_level` REST sensor. |
| **US03** | Mission Specialist | **As a** Mission Specialist, **I want** a dedicated button to manually turn on the hall ventilation **so that** I can rapidly cycle the air if I detect smoke or stale air. | **AC:** The UI must provide an interactive control to activate the ventilation and visually confirm the action. <br>**NFR:** The action must trigger a REST POST request to the actuator API. |
| **US04** | Mission Specialist | **As a** Mission Specialist, **I want** the dashboard to visually highlight critical CO2 levels in red and trigger a system-wide warning when the concentration exceeds 1000 ppm, **so that** I can immediately take safety measures or evacuate the hall. | **AC:** The system must monitor the co2_hall sensor value in real-time.If the value is below 1000 ppm, the status indicator must remain Green (Normal).f the value exceeds 1000 ppm, the specific sensor card must change its border/background to Red and the status text must switch to "CRITICAL". <br>**NFR:** The alert logic must be processed on the frontend based on the data fetched from the rest.scalar.v1 sensor schema. |
| **US05** | Automation Engineer | **As an** Automation Engineer, **I want** the system to continuously gather data from all available sensors **so that** the automation logic always evaluates the most recent habitat conditions. | **AC:** The system must ingest readings from all connected devices without manual triggers. <br>**NFR:** An ingestion microservice must poll the endpoints listed in `/api/sensors`. |
| **US06** | Automation Engineer | **As an** Automation Engineer, **I want to** automatically convert all incoming sensor data into a single, unified format **so that** the core system logic is decoupled from specific device dialects. | **AC:** All metrics must be mapped to a standard structure regardless of their original source. <br>**NFR:** The ingestion service must normalize heterogeneous payloads into the standard internal schema and update the central system state (In-Memory Cache/DB). |
| **US07** | Automation Engineer | **As an** Automation Engineer, **I want to** define an automatic rule that turns on the hall ventilation if the CO2 level exceeds a safe threshold **so that** dangerous carbon dioxide buildup is prevented. | **AC:** The system must allow the creation of automated conditional responses for CO2 levels. <br>**NFR:** The rule engine must support `IF co2_hall > [value] THEN set hall_ventilation to ON` logic. |
| **US08** | Automation Engineer | **As an** Automation Engineer, **I want** all created automation rules to be securely saved **so that** I don't have to reconfigure the habitat's safety protocols if the system restarts. | **AC:** Configured rules must remain active even after a complete system reboot. <br>**NFR:** Rules must be persisted in a database to survive container restarts. |
| **US09** | Safety Officer | **As a** Safety Officer, **I want** the dashboard to visually alert the crew if the corridor pressure drops below a safe threshold **so that** everyone is warned of a potential hull breach. | **AC:** The UI must dynamically change color states based on pressure limits. <br>**NFR:** Frontend must apply conditional styling based on the `corridor_pressure` value. |
| **US10** | Safety Officer | **As a** Safety Officer, **I want to** consult a historical log showing when and why an actuator was automatically triggered **so that** I can audit the system's "autopilot" behavior after an incident. | **AC:** The system must provide a readable history of all automated interventions. <br>**NFR:** The rule engine must log triggered actions to the database, separating processing from presentation. |