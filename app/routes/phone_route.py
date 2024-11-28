from flask import blueprints, request, jsonify

from app.dbs.neo4j_database.repository.device_repository import get_interaction_with_bluetooth, \
    get_devices_with_strong_signal, get_count_connection_by_device_id, find_connection_between_two_devices, \
    get_recent_connection_by_device_id
from app.services.tracker_service import normalize_devices_and_interaction
from flask import blueprints, request, jsonify

from app.dbs.neo4j_database.repository.device_repository import get_interaction_with_bluetooth, \
    get_devices_with_strong_signal, get_count_connection_by_device_id, find_connection_between_two_devices, \
    get_recent_connection_by_device_id
from app.services.tracker_service import normalize_devices_and_interaction

phone_tracker_blueprint = blueprints.Blueprint("phone", __name__)


@phone_tracker_blueprint.route("/", methods=["POST"])
def post_all_tracker():
    res = normalize_devices_and_interaction(request.json)
    return jsonify(res), 200


@phone_tracker_blueprint.route("/all_bluetooth", methods=["GET"])
def get_tracker_with_bluetooth():
    res = get_interaction_with_bluetooth()
    return jsonify(res), 200


@phone_tracker_blueprint.route('/strong_signal', methods=['GET'])
def find_devices_with_strong_signal():
    res = get_devices_with_strong_signal()
    return jsonify(res), 200


@phone_tracker_blueprint.route('/connections_count/<string:device_id>', methods=['GET'])
def count_connected_devices(device_id):
    return jsonify(get_count_connection_by_device_id(device_id))


@phone_tracker_blueprint.route('/find_connection/<string:device_a_id>/<string:device_b_id>', methods=['GET'])
def find_connection(device_a_id, device_b_id) :
    return jsonify(find_connection_between_two_devices(device_a_id, device_b_id))



@phone_tracker_blueprint.route('/recent_connections/<string:device_id>', methods=['GET'])
def find_recent_connection_by_device_id(device_id: str):
    return jsonify(get_recent_connection_by_device_id(device_id))
