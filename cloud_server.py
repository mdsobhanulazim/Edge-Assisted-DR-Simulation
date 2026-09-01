"""V4.1 simulated primary cloud ordering service."""
from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request

try:
    import psutil
except ImportError:  # Resource evidence is optional at runtime.
    psutil = None

app = Flask(__name__)
BASE_DIR = Path(__file__).resolve().parent
DB_NAME = BASE_DIR / "cloud.db"

META_COLUMNS = [
    ("experiment_id", "TEXT"),
    ("batch_id", "TEXT"),
    ("test_case_id", "TEXT"),
    ("run_id", "INTEGER"),
    ("request_number", "INTEGER"),
    ("phase", "TEXT"),
]


def get_now() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def init_db() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_order_id TEXT UNIQUE,
                customer_name TEXT NOT NULL,
                order_details TEXT NOT NULL,
                created_at TEXT NOT NULL,
                source TEXT NOT NULL,
                restored_from_edge INTEGER DEFAULT 0,
                synced_from_edge_at TEXT,
                experiment_id TEXT,
                batch_id TEXT,
                test_case_id TEXT,
                run_id INTEGER,
                request_number INTEGER,
                phase TEXT
            )
            """
        )
        cursor.execute("PRAGMA table_info(orders)")
        columns = [row[1] for row in cursor.fetchall()]
        migrations = {
            "client_order_id": "TEXT",
            "restored_from_edge": "INTEGER DEFAULT 0",
            "synced_from_edge_at": "TEXT",
            **dict(META_COLUMNS),
        }
        for column, definition in migrations.items():
            if column not in columns:
                cursor.execute(f"ALTER TABLE orders ADD COLUMN {column} {definition}")

        cursor.execute(
            "SELECT id, customer_name, order_details, created_at FROM orders "
            "WHERE client_order_id IS NULL OR client_order_id = ''"
        )
        for legacy_id, customer_name, order_details, created_at in cursor.fetchall():
            legacy_key = f"cloud-legacy-{legacy_id}-{abs(hash((customer_name, order_details, created_at))) % 1000000}"
            cursor.execute("UPDATE orders SET client_order_id = ? WHERE id = ?", (legacy_key, legacy_id))

        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cloud_client_order_id ON orders(client_order_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cloud_experiment_id ON orders(experiment_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cloud_batch_id ON orders(batch_id)")
        conn.commit()


def metadata_from(data: dict) -> tuple:
    return (
        data.get("experiment_id"), data.get("batch_id"), data.get("test_case_id"),
        data.get("run_id"), data.get("request_number"), data.get("phase"),
    )


def resource_metrics() -> dict:
    result = {
        "process_rss_mb": None,
        "cpu_percent_snapshot": None,
        "database_file_size_bytes": DB_NAME.stat().st_size if DB_NAME.exists() else 0,
        "record_count": 0,
    }
    try:
        with sqlite3.connect(DB_NAME) as conn:
            result["record_count"] = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    except sqlite3.Error:
        pass
    if psutil is not None:
        try:
            process = psutil.Process(os.getpid())
            result["process_rss_mb"] = round(process.memory_info().rss / (1024 * 1024), 3)
            result["cpu_percent_snapshot"] = round(process.cpu_percent(interval=0.05), 3)
        except (psutil.Error, OSError):
            pass
    return result


@app.route("/")
def home():
    return jsonify({"message": "Cloud Server is running", "status": "available", "version": "v4.1"})


@app.route("/metrics")
def metrics():
    return jsonify(resource_metrics())


@app.route("/add_order", methods=["POST"])
def add_order():
    data = request.get_json() or {}
    customer_name = data.get("customer_name")
    order_details = data.get("order_details")
    client_order_id = data.get("client_order_id") or str(uuid.uuid4())
    if not customer_name or not order_details:
        return jsonify({"error": "customer_name and order_details are required"}), 400

    created_at = data.get("created_at") or get_now()
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO orders
            (client_order_id, customer_name, order_details, created_at, source,
             restored_from_edge, synced_from_edge_at,
             experiment_id, batch_id, test_case_id, run_id, request_number, phase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_order_id, customer_name, order_details, created_at, "cloud", 0, None,
                *metadata_from(data),
            ),
        )
        inserted = cursor.rowcount
        conn.commit()
        row = cursor.execute("SELECT id FROM orders WHERE client_order_id = ?", (client_order_id,)).fetchone()
        order_id = row[0]

    return jsonify({
        "message": "Order added to cloud server" if inserted else "Order already exists in cloud",
        "id": order_id,
        "client_order_id": client_order_id,
        "created_at": created_at,
        "source": "cloud",
        "inserted": bool(inserted),
    }), 201 if inserted else 200


@app.route("/sync_order_from_edge", methods=["POST"])
def sync_order_from_edge():
    data = request.get_json() or {}
    required = ["client_order_id", "customer_name", "order_details", "created_at"]
    if any(not data.get(key) for key in required):
        return jsonify({"error": ", ".join(required) + " are required"}), 400

    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO orders
            (client_order_id, customer_name, order_details, created_at, source,
             restored_from_edge, synced_from_edge_at,
             experiment_id, batch_id, test_case_id, run_id, request_number, phase)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["client_order_id"], data["customer_name"], data["order_details"], data["created_at"],
                "edge_recovered", 1, get_now(), *metadata_from(data),
            ),
        )
        inserted = cursor.rowcount
        conn.commit()
        order_id = cursor.execute(
            "SELECT id FROM orders WHERE client_order_id = ?", (data["client_order_id"],)
        ).fetchone()[0]

    return jsonify({
        "message": "Edge order synchronized back to cloud" if inserted else "Order already exists in cloud",
        "id": order_id,
        "client_order_id": data["client_order_id"],
        "inserted": bool(inserted),
    }), 201 if inserted else 200


@app.route("/orders", methods=["GET"])
def get_orders():
    with sqlite3.connect(DB_NAME) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, client_order_id, customer_name, order_details, created_at, source,
                   COALESCE(restored_from_edge, 0) AS restored_from_edge, synced_from_edge_at,
                   experiment_id, batch_id, test_case_id, run_id, request_number, phase
            FROM orders ORDER BY id
            """
        ).fetchall()
    return jsonify([dict(row) for row in rows])


if __name__ == "__main__":
    init_db()
    app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False, threaded=True)
