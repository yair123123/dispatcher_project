from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class RelInteraction:
    method: int
    bluetooth_version : str
    signal_strength_dbm : int
    distance_meters : float
    duration_seconds : int
    timestamp : datetime
