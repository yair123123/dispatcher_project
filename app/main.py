from flask import Flask

from app.routes.phone_route import phone_tracker_blueprint

app = Flask(__name__)
app.register_blueprint(phone_tracker_blueprint, url_prefix="/api/phone_tracker")
if __name__ == '__main__':
    app.run()
