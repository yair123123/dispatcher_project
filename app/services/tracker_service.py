from typing import Dict, List, Any

from returns.maybe import Nothing

from app.dbs.neo4j_database.models.device import Device
from app.dbs.neo4j_database.models.rel_interaction import RelInteraction
from app.dbs.neo4j_database.repository.device_repository import get_device_by_id, insert_device, relation_devices


def convert_to_device(device_data: Dict[str,Any]):
    return Device(
        device_id=device_data['id'],
        name=device_data['name'],
        brand=device_data['brand'],
        model=device_data['model'],
        os=device_data['os'],
        latitude=device_data['location']['latitude'],
        longitude=device_data['location']['longitude'],
        altitude_meters=device_data['location']['altitude_meters'],
        accuracy_meters=device_data['location']['accuracy_meters']
    )


def convert_to_interaction(interaction_data: Dict[str,Any]):
    del interaction_data["from_device"]
    del interaction_data["to_device"]
    return RelInteraction(**interaction_data)


def normalize_devices_and_interaction(devices: Dict[str, List[Dict[str,Any]]]):
    devices_model = [convert_to_device(device) for device in devices["devices"]]
    interaction_model = convert_to_interaction(devices["interaction"])
    devices = [insert_to_database_if_not_exit(devices_model[0]), insert_to_database_if_not_exit(devices_model[1])]

    return create_interaction(devices, interaction_model) if devices[0] != devices[1] else {}


def insert_to_database_if_not_exit(device: Device):
    res = get_device_by_id(device.device_id)
    return insert_device(device).unwrap() if res is Nothing or res is {} else res.unwrap()


def create_interaction(devices: List[Device], interaction: RelInteraction):
    device_a, device_b = devices
    return relation_devices(device_a.device_id, device_b.device_id, interaction)
