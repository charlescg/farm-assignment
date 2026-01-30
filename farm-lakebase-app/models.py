
from flask_sqlalchemy import SQLAlchemy
from datetime import date

db = SQLAlchemy()

class Cow(db.Model):
    __tablename__ = "cows"

    id = db.Column(db.String, primary_key=True)
    name = db.Column(db.String(120), nullable=False, index=True)
    birthdate = db.Column(db.Date, nullable=True)

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "birthdate": self.birthdate.isoformat() if self.birthdate else None
        }

class Sensor(db.Model):
    __tablename__ = "sensors"

    id = db.Column(db.String, primary_key=True)
    unit = db.Column(db.String(5), nullable=False, index=True)

    def to_dict(self):
        return {
            "id": self.id,
            "unit": self.unit
        }
        
class CowSensor:
    id: int
    name: str
    latest_sensor_data: dict[str, Sensor]
    
    
    def __init__(self, id: int, name: str, latest_sensor_data: dict[str, Sensor]) -> None:
        self.id = id
        self.name = name
        self.latest_sensor_data = latest_sensor_data
        
    def to_dict(self):
        return {
            "id": self.id,
            "unit": self.unit,
            "latest_sensor_data": self.latest_sensor_data
        }

    
    
    




