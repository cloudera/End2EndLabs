from flask import Flask, jsonify
from app.generator import generate_cdr

def create_app():
    app = Flask(__name__)

    @app.route("/cdr", methods=["GET"])
    def get_cdr():
        return jsonify(generate_cdr())

    @app.route("/health", methods=["GET"])
    def health():
        return {"status": "ok"}

    return app