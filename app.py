
import os
import subprocess
import sys
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify

app = Flask(__name__)
lock = threading.Lock()
status = {"state": "ready", "last_run": None, "exit_code": None}


def run_scanner():
    try:
        result = subprocess.run(
            [sys.executable, "scanner.py"],
            timeout=120,
            check=False
        )
        status["exit_code"] = result.returncode
        status["last_run"] = datetime.now(timezone.utc).isoformat()
        status["state"] = "finished" if result.returncode == 0 else "failed"
    except Exception as exc:
        status["state"] = "failed"
        status["error"] = str(exc)
    finally:
        lock.release()


@app.route("/")
def home():
    return jsonify({
        "service": "Crypto Arbitrage Scanner",
        **status
    })


@app.route("/scan", methods=["POST"])
def scan():
    if not lock.acquire(blocking=False):
        return jsonify({"error": "Scan already running"}), 409

    status["state"] = "running"
    threading.Thread(target=run_scanner, daemon=True).start()
    return jsonify({"message": "Scan started"}), 202


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 10000))
    )
