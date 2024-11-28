from dataclasses import dataclass
from typing import Optional

@dataclass
class Cinema:
    device_id: int
    brand : str
    model : str
    os : str
