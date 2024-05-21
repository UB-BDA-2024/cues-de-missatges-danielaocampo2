import json

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from shared.database import SessionLocal
from shared.publisher import Publisher
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.sensors.repository import DataCommand
from shared.timescale import Timescale
from shared.sensors import repository, schemas
import json
from typing import Optional
from collections import defaultdict

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_timescale():
    ts = Timescale()
    try:
        yield ts
    finally:
        ts.close()

# Dependency to get redis client

def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client

def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")
    try:
        yield mongodb
    finally:
        mongodb.close()


publisher = Publisher()

router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)



# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near",summary="Get sensors near a location",description="Returns a list of sensors located within a specified radius of a given latitude and longitude.",)
def get_sensors_near(latitude: float, longitude: float,radius:int, db: Session = Depends(get_db),mongodb_client: MongoDBClient = Depends(get_mongodb_client),redis_client: RedisClient = Depends(get_redis_client)):
    return repository.get_sensors_near(mongodb_client=mongodb_client,db=db,redis=redis_client,latitude=latitude,longitude=longitude,radius=radius,)



# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
# @router.get("/search")
# def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), es: ElasticsearchClient = Depends(get_elastic_search)):
#     try:
#         query_dict = json.loads(query)
#         # Convert all values in the dictionary to lowercase for case-insensitive search
#         query_dict = {k: v.lower() for k, v in query_dict.items()}
#     except json.JSONDecodeError:
#         raise HTTPException(
#             status_code=400, detail="Invalid JSON format in query parameter"
#         )
#     sensors_data=repository.searchElasticsearch(es, query_dict, search_type, size, mongodb_client)
#     return sensors_data


# 🙋🏽‍♀️ Add here the route to get the temperature values of a sensor

@router.get("/temperature/values")
def get_temperature_values(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    try:
        return repository.get_temperature_values(db, cassandra_client, mongodb_client)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve temperature values")
    

@router.get("/quantity_by_type")
def get_sensors_quantity(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    try:
        return repository.get_sensors_quantity(cassandra_client)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve sensor counts")

@router.get("/low_battery")
def get_low_battery_sensors(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    try:
        return repository.get_low_battery_sensors(cassandra_client, db, mongodb_client)
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve low battery sensor data")


# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)): #es: ElasticsearchClient = Depends(get_elastic_search)
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(
            status_code=400, detail="Sensor with same name already registered"
        )
    newSensor = repository.create_sensor(db=db, sensor=sensor)
    sensor_document = {
        "id": newSensor.id,
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude],
        },
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        "description": sensor.description,
    }
    repository.insertMongodb(mongodb_client=mongodb_client, sensor_document=sensor_document)
    sensorIndex = {
        "id": newSensor.id,
        "name": sensor.name,
        "description": sensor.description,
        "type": sensor.type,
    }
    # repository.insertElasticsearch(es, sensorIndex)
    response = {**{"id": newSensor.id}, **(sensor.__dict__)}
    return response

# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    infoSensor = repository.getInfoSensorMDB(mongodb_client, sensor_id)
    response = {**{"id": db_sensor.id,"name": db_sensor.name}, **(infoSensor)}
    return response

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),  redis_client:RedisClient = Depends(get_redis_client)): #es: ElasticsearchClient = Depends(get_elastic_search)
    db_sensor = repository.get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    try:
        repository.deleteSensorRedis(redis_client, sensor_id)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error: {str(e)}"
        )
    try:
        repository.deleteSensorMongodb(mongodb_client, sensor_id, es)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to delete sensor data in MongoDB: {str(e)}"
        )
    return repository.delete_sensor(db=db, sensor_id=sensor_id)
    
 #   return repository.delete_sensor(db=db, sensor_id=sensor_id)
    


# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData,db: Session = Depends(get_db) ,redis_client: RedisClient = Depends(get_redis_client), cassandra_client: CassandraClient = Depends(get_cassandra_client), timescale: Timescale = Depends(get_timescale)): # timescale: Timescale = Depends(get_timescale)
    try:
        repository.get_sensor(db, sensor_id)
        repository.record_data(redis=redis_client, sensor_id=sensor_id, data=data)
        repository.insert_sensor_data_cassandra(cassandra_client, sensor_id, data)
        repository.insert_sensor_data_to_timescale(sensor_id, data, timescale)
        return {"message": "Data recorded successfully in both Redis and TimescaleDB and Cassandra."}
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to record data")
    #return repository.record_data(redis=redis_client, sensor_id=sensor_id, data=data)


# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int,from_: Optional[str] = None, to: Optional[str] = None, bucket: Optional[str] = None, db: Session = Depends(get_db), redis_client: RedisClient = Depends(get_redis_client), timescale: Timescale = Depends(get_timescale)):    # timescale: Timescale = Depends(get_timescale)
    db_sensor=repository.get_sensor(db, sensor_id)
    try:
        # If aggregation parameters are not provided, retrieve the latest sensor data for redis
        if not from_ or not to or not bucket:
            data = repository.get_data(redis=redis_client, sensor_id=sensor_id)
            if db_sensor is not None:
                data["id"] = db_sensor.id
                data["name"] = db_sensor.name
            return data
        # If aggregation parameters are provided, retrieve aggregated data
        data = repository.get_view_data(sensor_id, from_, to, bucket,timescale)
        if not data:
            raise HTTPException(status_code=404, detail="No data found for the given parameters")
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
