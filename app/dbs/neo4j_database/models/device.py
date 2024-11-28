from dataclasses import dataclass
from typing import Optional

@dataclass
class Device:
    device_id: int
    name:str
    brand : str
    model : str
    os : str
    latitude: float
    longitude: float
    altitude_meters: int
    accuracy_meters: int
