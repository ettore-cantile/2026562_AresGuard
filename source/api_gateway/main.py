import asyncio
import json
import os
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import aio_pika
import psycopg2
from psycopg2.extras import RealDictCursor

BROKER_HOST = os.getenv("BROKER_HOST", "aresguard_broker")
BROKER_USER = os.getenv("RABBITMQ_DEFAULT_USER", "ares")
BROKER_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "mars2036")
EXCHANGE_NAME = "ares_telemetry_stream"
SIMULATOR_URL = os.getenv("SIMULATOR_URL", "http://mars_simulator:8080/api/actuators")
DATABASE_URL = os.getenv("DATABASE_URL", "host=aresguard_db dbname=aresguard user=ares password=mars2036")

sensor_state_cache = {}

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        await websocket.send_json({"type": "FULL_STATE", "data": sensor_state_cache})

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()

async def consume_rabbitmq():
    print(f"[GATEWAY] Connecting to RabbitMQ at {BROKER_HOST}...", flush=True)
    while True:
        try:
            connection = await aio_pika.connect_robust(
                f"amqp://{BROKER_USER}:{BROKER_PASS}@{BROKER_HOST}/"
            )
            async with connection:
                channel = await connection.channel()
                exchange = await channel.declare_exchange(EXCHANGE_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
                queue = await channel.declare_queue('', exclusive=True)
                await queue.bind(exchange)
                print("[GATEWAY] RabbitMQ Connected & Listening!", flush=True)
                
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            event = json.loads(message.body.decode())
                            sensor_id = event["source"]["identifier"]
                            sensor_state_cache[sensor_id] = event
                            await manager.broadcast({"type": "LIVE_UPDATE", "data": event})
        except Exception as e:
            print(f"[GATEWAY ERR] RabbitMQ Error: {e}", flush=True)
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    consumer_task = asyncio.create_task(consume_rabbitmq())
    yield
    consumer_task.cancel()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "AresGuard API Gateway Online"}

@app.get("/api/state")
def get_state():
    return sensor_state_cache

@app.get("/api/sensors/{sensor_id}")
def get_sensor_data(sensor_id: str):
    target_url = SIMULATOR_URL.replace("actuators", "sensors") + f"/{sensor_id}"
    
    print(f"[PROXY DEBUG] Frontend asked for: {sensor_id}", flush=True)
    
    try:
        resp = requests.get(target_url, timeout=3)
        
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"[PROXY ERROR] Simulator returned error: {resp.text}", flush=True)
            raise HTTPException(status_code=resp.status_code, detail="Simulator Error")
            
    except Exception as e:
        print(f"[PROXY CRITICAL] Connection failed: {e}", flush=True)
        raise HTTPException(status_code=500, detail=f"Proxy Error: {str(e)}")

@app.post("/api/commands/{actuator_id}")
def send_command(actuator_id: str, command: dict):
    try:
        res = requests.post(f"{SIMULATOR_URL}/{actuator_id}", json=command, timeout=3)
        return {"status": "sent", "simulator_status_code": res.status_code, "response": res.text}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/rules")
def get_rules():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT id AS rule_id, sensor_id, operator, threshold, actuator_id, action_value AS action FROM rules")
        rules = cur.fetchall()
        conn.close()
        return rules
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/rules")
def create_rule(rule: dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # --- QUERY CORRETTA (UPSERT) ---
        # Usa il vincolo UNIQUE (sensor_id, actuator_id, operator) definito in init.sql
        # Se esiste già una regola con lo stesso operatore, aggiorna soglia e azione.
        cur.execute(
            """
            INSERT INTO rules (sensor_id, operator, threshold, actuator_id, action_value) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sensor_id, actuator_id, operator) 
            DO UPDATE SET 
                threshold = EXCLUDED.threshold, 
                action_value = EXCLUDED.action_value
            RETURNING (xmax = 0) AS is_insert;
            """,
            (rule['sensor_id'], rule['operator'], str(rule['threshold']), rule['actuator_id'], rule['action'])
        )
        
        result = cur.fetchone()
        is_insert = result[0] if result else True
        
        conn.commit()
        conn.close()
        
        action_type = "inserted" if is_insert else "updated"
        return {"status": "success", "action": action_type}
    except Exception as e:
        print(f"[API ERROR] Rule creation failed: {e}", flush=True)
        return {"error": str(e)}

@app.put("/api/rules/{rule_id}")
def update_rule(rule_id: int, rule: dict):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute(
            """
            UPDATE rules 
            SET sensor_id = %s, operator = %s, threshold = %s, actuator_id = %s, action_value = %s
            WHERE id = %s
            """,
            (rule['sensor_id'], rule['operator'], str(rule['threshold']), rule['actuator_id'], rule['action'], rule_id)
        )
        
        conn.commit()
        conn.close()
        return {"status": "updated"}
    except Exception as e:
        return {"error": str(e)}

@app.delete("/api/rules/{rule_id}")
def delete_rule(rule_id: int):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM rules WHERE id = %s", (rule_id,))
        conn.commit()
        conn.close()
        return {"status": "deleted"}
    except Exception as e:
        return {"error": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)