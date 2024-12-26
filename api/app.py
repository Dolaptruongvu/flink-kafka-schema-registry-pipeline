# flask_api.py

from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route("/batch", methods=["POST"])
def handle_batch():
    data = request.json  # data is a list/array of records
    if not isinstance(data, list):
        return jsonify({"status": "error", "message": "Invalid data format, expected a list."}), 400

    print(f"Received batch (size={len(data)}):")
    for idx, msg in enumerate(data):
        print(f"  [{idx}] {msg}")
    # Return JSON response indicating success
    return jsonify({"status": "ok", "received_count": len(data)}), 200

if __name__ == "__main__":
    # Run Flask on port 8080
    app.run(host="0.0.0.0", port=8080)
