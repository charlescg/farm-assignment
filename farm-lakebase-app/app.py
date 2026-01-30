from flask import Flask, request, jsonify
import psycopg
from datetime import datetime
from models import Cow, Sensor, CowSensor
import uuid
from database import get_connection, SCHEMA_NAME
from psycopg import sql
import os

def generar_uuid():
    """
    Genera un UUID versión 4 (aleatorio) como cadena.
    """
    return str(uuid.uuid4())


def create_cow(cow: Cow):
    """Add a new cow item."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("INSERT INTO {}.cows (id, name, birthdate) VALUES (%s, %s, %s)").format(sql.Identifier(SCHEMA_NAME)), (cow.id, cow.name, cow.birthdate))
                conn.commit()
                return True
    except Exception as e:
        print(f"Add todo error: {e}")
        return False
    
def create_sensor(sensor: Sensor):
    """Add a new sensor item."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("INSERT INTO {}.sensors (id, unit) VALUES (%s, %s)").format(sql.Identifier(SCHEMA_NAME)), (sensor.id, sensor.unit))
                conn.commit()
                return True
    except Exception as e:
        print(f"Add todo error: {e}")
        return False

def get_cow_latest_sensor_data(cow_id: str):
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql.SQL("""SELECT   id,
                                                name, 
                                                birthdate, 
                                                milk_time_serie_event, 
                                                milk_sensor_id, 
                                                milk_value, 
                                                weight_time_serie_event, 
                                                weight_sensor_id, 
                                                weight_value 
                                        FROM {}.cow_lastest_sensor_data_sync_view 
                                        where id = %s
                                    """).format(sql.Identifier(SCHEMA_NAME)),(cow_id,))
                
                
                row = cur.fetchone()
                print("recupera datos")
                if row is None:
                    return jsonify({"detail": "Cow not found"}), 404
                print("tine datos")
                colnames = [desc[0] for desc in cur.description]
                result = dict(zip(colnames, row))
                print("result")
                return result
    except Exception as e:
        print(f"Get cow_latest_sensor_data error: {e}")
        return []

# Initialize Flask app
app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key')


def parse_date(value):
    """Convierte 'YYYY-MM-DD' a date; devuelve None si viene vacío."""
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None
        
@app.post("/api/cows")
def post_cow_route():
    """Body JSON: { "name": "Matilda", "birthdate": "2020-07-11" }"""
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    birthdate = parse_date(data.get("birthdate"))

    errors = {}
    if not name:
        errors["name"] = "name is required"
    if "birthdate" in data and data.get("birthdate") and birthdate is None:
        errors["birthdate"] = "birthdate must be YYYY-MM-DD"

    if errors:
        return jsonify({"errors": errors}), 400
    
    
    cow = Cow(id = generar_uuid(), name=name, birthdate=birthdate)
    
    create_cow(cow)
    
    return jsonify(cow.to_dict()), 201

@app.post("/api/sensors")
def post_sensor_route():
    """Body JSON: { "unit": "kg"}"""
    data = request.get_json(silent=True) or {}
    unit = (data.get("unit") or "").strip()


    errors = {}
    if not unit:
        errors["unit"] = "unit is required"

    if errors:
        return jsonify({"errors": errors}), 400
    
    
    sensor = Sensor(id = generar_uuid(), unit=unit)
    
    create_sensor(sensor)
    
    return jsonify(sensor.to_dict()), 201

@app.get("/api/cows/<cow_id>")
def get_cow_latest_sensor_data_route(cow_id: str):

    errors = {}
    if not cow_id:
        errors["cow_id"] = "cow_id is required"

    if errors:
        return jsonify({"errors": errors}), 400
    
    cow_data = get_cow_latest_sensor_data(cow_id)
    
    return jsonify(cow_data), 201


if __name__ == '__main__':
    host = os.getenv('FLASK_RUN_HOST', '0.0.0.0')
    port = int(os.getenv('FLASK_RUN_PORT', 8000))

    app.run(debug=True, host=host, port=port)
    print(f"Flask app running on http://{host}:{port}")
