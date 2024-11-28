from flask import blueprints, request, jsonify

phone_tracker_blueprint = blueprints.Blueprint("phone", __name__)

@phone_tracker_blueprint.route("/", methods=["POST"])
def get_all_tracker():
    print(request.json)
    return jsonify({}),200