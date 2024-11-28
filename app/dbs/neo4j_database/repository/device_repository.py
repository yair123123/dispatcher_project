from functools import partial
from typing import Dict

from returns.maybe import Maybe,Nothing
from toolz import pipe

from app.dbs.neo4j_database.database import driver
from app.dbs.neo4j_database.models.device import Device
from app.dbs.neo4j_database.models.rel_interaction import RelInteraction


def convert_to_model_device(letter: str, record):
    def convert(device: Dict) -> Device:
        return Device(**device)
    return pipe(
        record,
        lambda x: x.map(lambda y: y.get(f"{letter}")),
        lambda x: x.map(lambda y: dict(y) if y else {}),
        lambda x: x.map(lambda y: convert(y) if y else None)
    )


def get_device_by_id(device_id:int):
    with driver.session() as session:
        query = """
        match (d:Device{device_id:$device_id})
        return d
        """
        params = {
            "device_id": device_id
        }
        res = Maybe.from_optional(session.run(query, params).single())
    return pipe(
        res,
        partial(convert_to_model_device, "d")
    )
def insert_device(device: Device):
    with driver.session() as session:
        query = """
            create (d:Device{ 
            device_id:$device_id,
            name:$name,
            os:$os,
            model:$model,
            brand:$brand,
            latitude: $latitude,
            longitude: $longitude,
            altitude_meters: $altitude,
            accuracy_meters: $accuracy})
            return d
        """
        param = {
            "name":device.name,
            "device_id": device.device_id,
            "os": device.os,
            "model": device.model,
            "brand": device.brand,
            "latitude": device.latitude,
            "longitude": device.longitude,
            "altitude": device.altitude_meters,
            "accuracy": device.accuracy_meters
        }
        res = Maybe.from_optional(session.run(query, param).single())
        return pipe(
            res,
            partial(convert_to_model_device, "d")
        )


def relation_devices(device_a_id: int, device_b_id: int, interaction: RelInteraction):
    with driver.session() as session:
        query = """
        MATCH (d1:Device {device_id: $device_a_id}), (d2:Device {device_id: $device_b_id})
        MERGE (d1)-[rel:CONNECTED { 
            method: $method,
            bluetooth_version: $bluetooth_version,
            signal_strength_dbm: $signal_strength_dbm,
            distance_meters: $distance_meters,
            duration_seconds: $duration_seconds,
            timestamp: $timestamp}]->(d2)
        RETURN d1, rel, d2
        """
        params = {
            "device_a_id": device_a_id,
            "device_b_id": device_b_id,
            "method": interaction.method,
            "bluetooth_version": interaction.bluetooth_version,
            "signal_strength_dbm": interaction.signal_strength_dbm,
            "distance_meters": interaction.distance_meters,
            "duration_seconds": interaction.duration_seconds,
            "timestamp": interaction.timestamp,
        }
        return session.run(query, params).data()
def get_interaction_with_bluetooth():
    with driver.session() as session:
        query = """
MATCH path = (d:Device)-[:CONNECTED {method: "Bluetooth"}]->(d2:Device)
WHERE NOT d = d2
return d AS startDevice, 
d2 AS endDevice, 
length(path) AS pathLength, 
nodes(path) AS devices
        """
        results =  session.run(query)
        return [
            {
                "startDevice": record["startDevice"],
                "endDevice": record["endDevice"],
                "pathLength": record["pathLength"],
                "devices": [dict(device) for device in record["devices"]],
            }
            for record in results
        ]
def get_devices_with_strong_signal():
    with driver.session() as session:
        query = """
        MATCH path = (d:Device)-[r:CONNECTED]->(d2:Device)
        WHERE r.signal_strength_dbm >= -60
        return d 
        """
        return session.run(query).data()

def get_count_connection_by_device_id(device_id:int):
    query = """
    MATCH (:Device {id: $device_id})-[:CONNECTED]->(connected:Device)
    RETURN count(connected) AS connectionsCount
    """
    with driver.session() as session:
        result = session.run(query, parameters={"device_id":device_id})
        record = result.single()
        count = record["connectionsCount"] if  record else 0
    return {"device_id": device_id, "connections_count": count}

def find_connection_between_two_devices(device1_id,device2_id):
    query = """
    MATCH (d1:Device {id: $device1_id})-[:CONNECTED]-(d2:Device {id: $device2_id})
    RETURN count(*) > 0 AS areConnected
    """
    params={
        "device1_id":device1_id,
        "device2_id":device2_id
    }

    with driver.session() as session:
        result = session.run(query, device1_id=device1_id, device2_id=device2_id)
        record = result.single()
        are_connected = record["areConnected"] if record else False

    return {
        "device1_id": device1_id,
        "device2_id": device2_id,
        "are_connected": are_connected
    }