from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
from collections import defaultdict

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale

import json

class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket

index_es_name = "sensors"
def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return db_sensor

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def insertMongodb(mongodb_client: MongoDBClient, sensor_document):
    try:
        mongodb_client.getDatabase('P2Documentales')
        mongodb_client.getCollection('sensors')
        mongodb_client.collection.insert_one(sensor_document)
    except Exception as e:
        print(f"Error al insertar en MongoDB: {e}")
        raise HTTPException(status_code=500, detail="Failed to insert sensor data into MongoDB")


def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    data_dict = data.dict()
    redis_key = f"sensor:{sensor_id}:data"
    data_json = json.dumps(data_dict)
    success = redis.set(redis_key, data_json)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to set data in Redis")


def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor

def deleteSensorRedis(redis: RedisClient, sensor_id: int):
    redis_key = f"sensor:{sensor_id}:data"
    redis.delete(redis_key)

def deleteSensorMongodb(mongodb_client: MongoDBClient, sensor_id: int, es: ElasticsearchClient):
    mongodb_client.getDatabase('P2Documentales')
    mongodb_client.getCollection('sensors')
    mongodb_client.collection.delete_one({"id": sensor_id})
    query = {
            "query": {
                "match": {
                    'id': sensor_id
                }
            }
    }
    es.delete_document_by_field(index_es_name, query)


# change
def get_data(redis: RedisClient, sensor_id: int) -> schemas.Sensor:
    redis_key = f"sensor:{sensor_id}:data"
    stored_data = redis.get(redis_key)
    if stored_data is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    stored_data = json.loads(stored_data.decode("utf-8"))
    return stored_data

def getInfoSensorMDB(mongodb_client: MongoDBClient, sensor_id: int):

    mongodb_client.getDatabase('P2Documentales')
    mongodb_client.getCollection('sensors')
    document= mongodb_client.find_one({"id": sensor_id})
    if document:
        sensor_dict = dict(document)
        sensor_dict.pop('_id', None)
        longitude, latitude = sensor_dict["location"]["coordinates"]
        sensor_data = {
            "latitude": latitude,
            "longitude": longitude,
            "type": sensor_dict["type"],
            "mac_address": sensor_dict["mac_address"],
            "manufacturer": sensor_dict["manufacturer"],
            "model": sensor_dict["model"],
            "serie_number": sensor_dict["serie_number"],
            "firmware_version": sensor_dict["firmware_version"],
            "description": sensor_dict["description"],
        }
        return sensor_data
    else:
        return None

# 
def get_sensors_near(mongodb_client: MongoDBClient,  db:Session, redis:RedisClient,  latitude: float, longitude: float, radius: int):
    mongodb_client.getDatabase("P2Documentales")
    collection = mongodb_client.getCollection("sensors")
    # 2dsphere index on the "location" field to enable geospatial queries
    collection.create_index([("location", "2dsphere")])
    # Construct a GeoJSON query to find sensors near a given point within a specified radius
    geoJSON = {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                    "$maxDistance": radius
                },
                
            }
        }
    }
    # Execute the query to find nearby sensors and convert into a list of dictionaries
    nearby_sensors = list(mongodb_client.findByQuery(geoJSON))
    sensors = []
    # Iterate through each sensor found in the MongoDB query
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_sensor(db=db, sensor_id=doc["id"]).__dict__
        sensorRedis= get_data(redis, doc["id"])
        # If sensor data is successfully retrieved, combine it with the Redis data
        if sensor is not None:
            sensor = {**sensor, **sensorRedis} 
            sensors.append(sensor)        
    if sensors is not None:
        return sensors
    return []

def insertElasticsearch(es: ElasticsearchClient,  es_doc: dict, ):
    # Check if the index exists
    if not es.client.indices.exists(index=index_es_name):
        es.create_index(index_name=index_es_name)
        mapping = {
        'properties': {
            "id": {'type': 'integer'},
            "name": {'type': 'text'},
            "description": {'type': 'text'},
            "type": {'type': 'text'}
         }}
        es.create_mapping(index_name=index_es_name, mapping=mapping)
 
    es.index_document(index_es_name, es_doc)

def searchElasticsearch(es: ElasticsearchClient, query_dict: dict ,search_type: str, size: int, mongodb_client: MongoDBClient):
    # Build the Elasticsearch query based on the search type. --- "match_phrase".
    if search_type == "match" or search_type == "prefix":
        search_body = {"query": {search_type: query_dict}, "size": size}
    elif search_type == "similar":
        fields = list(query_dict.keys())  
        like_text = list( query_dict.values() )  
        search_body = {
            "query": {
                "multi_match": {
                    "query": like_text[0],  
                    "fields": fields,  
                    "operator": "and",
                    "fuzziness": "auto",
                }
            },
            "size": size,  
        }
    else:
        raise HTTPException(
            status_code=400, detail=f"Unsupported search_type: {search_type}"
        )
    results = es.search(index_name=index_es_name, query=search_body)
    sensors_data = []
    # Iterate through the search results
    for sensor in results["hits"]["hits"]:
        # Retrieve additional sensor information from MongoDB using the sensor's ID
        infoSensor =getInfoSensorMDB(mongodb_client, sensor["_source"]["id"])
        if infoSensor:
            # Merge the Elasticsearch and MongoDB sensor data and add it to the sensors_data list
            sensor_data = {**{"id": sensor["_source"]["id"],"name": sensor["_source"]["name"]}, **(infoSensor)}
            sensors_data.append(sensor_data)
    return sensors_data


def insert_sensor_data_to_timescale(sensor_id:int , data:schemas.SensorData, timescale:Timescale):
    data_dict = {
            "sensor_id": sensor_id,
            "velocity": data.velocity if hasattr(data, 'velocity') else None,
            "temperature": data.temperature if hasattr(data, 'temperature') else None,
            "humidity": data.humidity if hasattr(data, 'humidity') else None,
            "battery_level": data.battery_level,
            "last_seen": data.last_seen 
        }
    columns = list(data_dict.keys())#[col for col in sensor_data if sensor_data[col] is not None]
    placeholders = ["%s"] * len(columns)
    values = [data_dict[col] for col in columns]

    query = f"""
        INSERT INTO sensor_data ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})"""
    try:    
        timescale.enable_autocommit(True)
        timescale.execute(query, values)
        timescale.enable_autocommit(False)
        return {"message": "Data recorded successfully"}
    except Exception as e:
        timescale.enable_autocommit(False)
        raise HTTPException(status_code=500, detail="Failed to record sensor data")
    
      
def get_view_data(sensor_id:int, from_:str, to:str, bucket:str,timescale:Timescale):
    # Validate the bucket parameter to ensure it is one of the predefined sizes
    if bucket not in ['hour', 'day', 'week', 'month', 'year']:
            raise HTTPException(status_code=400, detail="Invalid bucket size")
    view_name = f"{bucket}_aggregates"
    # Adjust the 'from_' date to the start of the week if the bucket size is 'week'
    if bucket == "week":
            from_date = datetime.fromisoformat(from_[:-1])  # Remove the 'Z'
            from_ = from_date - timedelta(days=from_date.weekday())
    # Try to refresh the continuous aggregate view to ensure data is up to date
    try:
        now = datetime.now().isoformat(timespec='milliseconds')
        timescale.enable_autocommit(True)
        query_refresh = f"CALL refresh_continuous_aggregate('{view_name}', '2019-01-01T00:00:00.000Z', '{now}Z');"
        timescale.execute(query_refresh)
    except Exception as e:
            timescale.enable_autocommit(False)
            raise HTTPException(status_code=500, detail=f"Failed to refresh aggregate view: {str(e)}")
    query = f"""
        SELECT *
        FROM {view_name}
        WHERE sensor_id = %s AND {bucket} BETWEEN %s AND %s; """
    try:
        results = timescale.fetch_all(query, (sensor_id, from_, to))
        timescale.enable_autocommit(False)
        if not results:
            return []
        return [dict(zip(["sensor_id", "time_bucket", "avg_velocity", "avg_temperature", "avg_humidity", "avg_battery"], result)) for result in results]
    except Exception as e:
        timescale.enable_autocommit(False)
        raise HTTPException(status_code=500, detail=f"Failed to fetch aggregated data: {str(e)}")

def get_temperature_values(db, cassandra_client, mongodb_client):
    try:
        result = cassandra_client.execute(
            """
            SELECT * FROM sensor.sensor_temperatures;
            """
        )
        temperature_data = defaultdict(list)
        for row in result:
            temperature_data[row.sensor_id].append(row.temperature)

        response_data = {"sensors": []}
        for sensor_id, temperatures in temperature_data.items():
            if temperatures:
                max_temp = max(temperatures)
                min_temp = min(temperatures)
                avg_temp = sum(temperatures) / len(temperatures)
                db_sensor = get_sensor(db, sensor_id)  # Supposing get_sensor is a function in this repository
                infoSensor = getInfoSensorMDB(mongodb_client, sensor_id)  # Supposing getInfoSensorMDB is also here
                response_data["sensors"].append({
                    **{"id": sensor_id, "name": db_sensor.name},
                    **(infoSensor),
                    **{"values": [{
                        "max_temperature": max_temp,
                        "min_temperature": min_temp,
                        "average_temperature": avg_temp
                    }]}
                })
        return response_data
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve temperature values")

def get_sensors_quantity(cassandra_client):
    try:
        query = "SELECT sensor_type, COUNT(sensor_id) AS quantity FROM sensor.sensor_counts GROUP BY sensor_type;"
        result = cassandra_client.execute(query)
        sensors_count = [{"type": row.sensor_type, "quantity": row.quantity} for row in result]
        return {"sensors": sensors_count}
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve sensor counts")

def get_low_battery_sensors(cassandra_client, db, mongodb_client):
    try:
        query = """
        SELECT sensor_id, battery_level FROM sensor.sensors_low_battery
        WHERE battery_range = 'low'
        """
        result = cassandra_client.execute(query)
        response_data = {"sensors": []}
        for sensor in result:
            db_sensor = get_sensor(db, sensor.sensor_id)  
            infoSensor = getInfoSensorMDB(mongodb_client, sensor.sensor_id) 
            response_data["sensors"].append({
                **{"id": sensor.sensor_id, "name": db_sensor.name},
                **(infoSensor),
                **{"battery_level": sensor.battery_level}
            })
        return response_data
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve low battery sensor data")

def classify_battery_level(battery_level):
    if battery_level < 0.2:
        return 'low'
    elif 0.2 <= battery_level < 0.5:
        return 'normal'
    else:
        return 'high'

def insert_sensor_data_cassandra(cassandra_client, sensor_id, data):
    try:
        if data.temperature is not None:
            typeSensor= "Temperatura"
            cassandra_client.execute(
                """
                INSERT INTO sensor.sensor_temperatures (sensor_id, temperature, last_seen)
                VALUES (%(sensor_id)s, %(temperature)s, %(last_seen)s)
                """,
                {'sensor_id': sensor_id, 'temperature': data.temperature, 'last_seen': data.last_seen}
            )
        else:
            typeSensor="Velocitat"
            battery_range = classify_battery_level(data.battery_level)
            cassandra_client.execute(
                """
                INSERT INTO sensor.sensors_low_battery (battery_range, sensor_id, battery_level)
                VALUES (%(battery_range)s, %(sensor_id)s, %(battery_level)s)
                """,
                {'battery_range': battery_range, 'sensor_id': sensor_id, 'battery_level': data.battery_level}
            )
        cassandra_client.execute(
                "INSERT INTO sensor.sensor_counts (sensor_type, sensor_id) VALUES (%s, %s)",
                [typeSensor, sensor_id]
            )
    except Exception as e:
        print(f"Error inserting data into Cassandra: {str(e)}")
        raise
