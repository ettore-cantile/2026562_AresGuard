CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    value VARCHAR(50) NOT NULL,
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS rules (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(100) NOT NULL,
    operator VARCHAR(5) NOT NULL,
    threshold VARCHAR(50) NOT NULL,
    actuator_id VARCHAR(100) NOT NULL,
    action_value VARCHAR(50) NOT NULL,
    CONSTRAINT unique_rule UNIQUE (sensor_id, actuator_id, operator)
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    command_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    sensor_id VARCHAR(100) NOT NULL,
    sensor_value VARCHAR(50) NOT NULL,
    actuator_id VARCHAR(100) NOT NULL,
    action_taken VARCHAR(50) NOT NULL
);