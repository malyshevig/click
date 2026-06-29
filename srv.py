from flask import Flask, jsonify, request

app = Flask(__name__)


@app.get("/payment")
def payment():
    payment = request.args.get("payment", type=str)
    return jsonify({})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)
