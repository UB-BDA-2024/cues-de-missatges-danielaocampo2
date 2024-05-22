from pydantic import BaseModel

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: str
    last_seen: str
    type: str
    mac_address: str
    battery_level: float
    temperature: float
    humidity: float
    velocity: float
    description: str
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description:str

class SensorData(BaseModel):
    velocity: float | None = None
    temperature: float | None = None
    humidity: float | None = None
    battery_level: float| None = None
    last_seen: str
