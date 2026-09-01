"""Local graphical control dashboard for the V4.1 edge-assisted DR simulation.

The dashboard runs on port 5050 and manages the existing cloud (5000) and edge
(5001) Flask services as child processes. It does not replace the V4 experiment
logic; it calls the same scenario, synchronization, consistency, logging, and
analysis functions used by the command-line package.
"""
from __future__ import annotations

import atexit
import csv
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
import zipfile
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import requests
from flask import Flask, abort, jsonify, render_template, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)

# Imports must occur after changing to BASE_DIR because the original V4 scripts
# intentionally use project-relative result paths.
import scenario_runner_v4_1 as runner  # noqa: E402
from sync_service import sync_cloud_to_edge  # noqa: E402

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

DASHBOARD_PORT = 5050
STATE_FILE = BASE_DIR / ".v4_1_dashboard_processes.json"
LOG_DIR = BASE_DIR / "dashboard_logs"
ARCHIVE_DIR = BASE_DIR / "archives"
RESULT_ROOT = BASE_DIR / "v4_1_results"
RAW_RESULTS = RESULT_ROOT / "raw" / "v4_1_raw_experiment_results.csv"
REQUEST_EVENTS = RESULT_ROOT / "raw" / "v4_1_request_event_log.csv"
CHART_DIR = RESULT_ROOT / "charts"

SERVICES = {
    "cloud": {
        "label": "Cloud Server",
        "script": "cloud_server.py",
        "url": "http://127.0.0.1:5000/",
        "port": 5000,
        "log": LOG_DIR / "cloud_server.log",
    },
    "edge": {
        "label": "Edge Server",
        "script": "edge_server.py",
        "url": "http://127.0.0.1:5001/",
        "port": 5001,
        "log": LOG_DIR / "edge_server.log",
    },
}

SCENARIOS = {
    "TC01": {
        "scenario": "normal_operation",
        "label": "TC01 Normal operation",
        "display_label": "TC01 — Normal operation",
        # TC01 is the pure normal-cloud baseline. Edge stays available for the
        # topology, but a cloud failure is recorded rather than masked by failover.
        "failover": False,
        "expected_cloud": "available",
        "expected_edge": "available",
        "service_state": {"cloud": True, "edge": True},
    },
    "TC02": {
        "scenario": "cloud_failure_with_stateful_edge_failover",
        "label": "TC02 Cloud failure with stateful edge failover",
        "display_label": "TC02 — Cloud outage with edge failover",
        "failover": True,
        "expected_cloud": "failed",
        "expected_edge": "available",
        "service_state": {"cloud": False, "edge": True},
    },
    "TC03": {
        "scenario": "cloud_only_failure_no_failover",
        "label": "TC03 Cloud failure without failover",
        "display_label": "TC03 — Cloud outage without edge failover",
        "failover": False,
        "expected_cloud": "failed",
        "expected_edge": "not_used",
        "service_state": {"cloud": False, "edge": True},
    },
    "TC04": {
        "scenario": "complete_service_failure",
        "label": "TC04 Complete service failure",
        "display_label": "TC04 — Complete service outage",
        "failover": True,
        "expected_cloud": "failed",
        "expected_edge": "failed",
        "service_state": {"cloud": False, "edge": False},
    },
    "TC07": {
        "scenario": "end_to_end_recovery_lifecycle",
        "label": "TC07 End-to-end recovery lifecycle",
        "display_label": "TC07 — Full recovery lifecycle validation",
        "failover": True,
        "expected_cloud": "available -> failed -> restored",
        "expected_edge": "available",
        "service_state": {"cloud": True, "edge": True},
        "lifecycle": True,
    },
}

PROCESS_LOCK = threading.RLock()
# Serialises dashboard-side SQLite reads with destructive reset operations.
# This prevents Windows from seeing cloud.db/edge.db as in-use while reset deletes them.
DB_ACCESS_LOCK = threading.RLock()
PROCESSES: dict[str, subprocess.Popen[Any] | None] = {"cloud": None, "edge": None}
LOG_HANDLES: dict[str, Any] = {"cloud": None, "edge": None}
EVENT_LOG: deque[dict[str, str]] = deque(maxlen=250)
JOB_LOCK = threading.RLock()
JOB: dict[str, Any] = {
    "active": False,
    "name": "",
    "message": "Ready",
    "started_at": None,
    "finished_at": None,
    "success": None,
    "error": "",
}


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log_event(message: str, level: str = "info") -> None:
    EVENT_LOG.appendleft({"timestamp": now_text(), "level": level, "message": message})
    print(f"[{now_text()}] {level.upper()}: {message}", flush=True)


def read_state() -> dict[str, int]:
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        return {key: int(value) for key, value in data.items() if key in SERVICES}
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}


def write_state() -> None:
    data: dict[str, int] = {}
    stored = read_state()
    for name, process in PROCESSES.items():
        if process is not None and process.poll() is None:
            data[name] = process.pid
        elif name in stored and pid_exists(stored[name]):
            data[name] = stored[name]
    try:
        STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except OSError:
        pass


def pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def listening_pids(port: int) -> set[int]:
    """Return process IDs listening on a TCP port.

    This is mainly used to stop a server that was launched by an earlier dashboard
    session or from a PowerShell window. Windows uses ``netstat -ano``; Linux/macOS
    use common local command-line tools when available.
    """
    pids: set[int] = set()
    try:
        if os.name == "nt":
            completed = subprocess.run(
                ["netstat", "-ano", "-p", "tcp"],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            for line in completed.stdout.splitlines():
                parts = line.split()
                if len(parts) < 5 or parts[0].upper() != "TCP":
                    continue
                local_address, state, pid_text = parts[1], parts[3].upper(), parts[4]
                if state != "LISTENING":
                    continue
                if local_address.rsplit(":", 1)[-1] != str(port):
                    continue
                if pid_text.isdigit() and int(pid_text) != os.getpid():
                    pids.add(int(pid_text))
        else:
            # ``fuser`` is available on most Linux distributions and returns only PIDs.
            completed = subprocess.run(
                ["fuser", f"{port}/tcp"],
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
            for token in (completed.stdout + " " + completed.stderr).replace(":", " " ).split():
                if token.isdigit() and int(token) != os.getpid():
                    pids.add(int(token))
    except (OSError, subprocess.SubprocessError, ValueError):
        pass
    return pids


def service_online(name: str, timeout: float = 0.45) -> bool:
    try:
        response = requests.get(SERVICES[name]["url"], timeout=timeout)
        return response.status_code == 200
    except requests.RequestException:
        return False


def managed_pid(name: str) -> int | None:
    process = PROCESSES.get(name)
    if process is not None and process.poll() is None:
        return process.pid
    pid = read_state().get(name)
    return pid if pid and pid_exists(pid) else None


def start_service(name: str) -> dict[str, Any]:
    if name not in SERVICES:
        raise ValueError("Unknown service")
    with PROCESS_LOCK:
        if service_online(name):
            return {"success": True, "message": f"{SERVICES[name]['label']} is already online."}

        # Clear a stale listener that owns the port but does not answer the health check.
        stale_pids = listening_pids(int(SERVICES[name]["port"]))
        for pid in stale_pids:
            terminate_pid(pid)
        if stale_pids:
            time.sleep(0.4)

        old_process = PROCESSES.get(name)
        if old_process is not None and old_process.poll() is not None:
            PROCESSES[name] = None
            old_handle = LOG_HANDLES.get(name)
            if old_handle:
                try:
                    old_handle.close()
                except OSError:
                    pass
            LOG_HANDLES[name] = None

        LOG_DIR.mkdir(exist_ok=True)
        log_handle = open(SERVICES[name]["log"], "a", encoding="utf-8", buffering=1)
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        kwargs: dict[str, Any] = {
            "cwd": str(BASE_DIR),
            "stdout": log_handle,
            "stderr": subprocess.STDOUT,
            "env": env,
        }
        if os.name == "nt":
            kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        else:
            kwargs["start_new_session"] = True

        process = subprocess.Popen([sys.executable, SERVICES[name]["script"]], **kwargs)
        PROCESSES[name] = process
        LOG_HANDLES[name] = log_handle
        write_state()

    deadline = time.time() + 8
    while time.time() < deadline:
        if service_online(name):
            log_event(f"{SERVICES[name]['label']} started on port {SERVICES[name]['port']}.", "success")
            return {"success": True, "message": f"{SERVICES[name]['label']} started."}
        if process.poll() is not None:
            break
        time.sleep(0.2)

    log_event(f"{SERVICES[name]['label']} did not become available. Check its dashboard log.", "error")
    return {"success": False, "message": f"{SERVICES[name]['label']} failed to start. Check dashboard_logs."}


def terminate_pid(pid: int) -> None:
    if pid <= 0 or pid == os.getpid():
        return
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    else:
        try:
            os.killpg(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass


def stop_service(name: str) -> dict[str, Any]:
    """Stop a cloud/edge service whether dashboard-managed or externally started.

    A previous dashboard version only knew the PID of processes launched in the
    current session. This version also detects the process listening on the service
    port, which makes the Stop button behave like Ctrl+C for an older PowerShell or
    dashboard-launched instance.
    """
    if name not in SERVICES:
        raise ValueError("Unknown service")

    with PROCESS_LOCK:
        process = PROCESSES.get(name)
        candidate_pids: set[int] = set()
        if process is not None and process.poll() is None:
            candidate_pids.add(process.pid)
        stored_pid = read_state().get(name)
        if stored_pid and pid_exists(stored_pid):
            candidate_pids.add(stored_pid)
        candidate_pids.update(listening_pids(int(SERVICES[name]["port"])))

        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2.5)
            except subprocess.TimeoutExpired:
                process.kill()
                try:
                    process.wait(timeout=1.5)
                except subprocess.TimeoutExpired:
                    pass

        for pid in sorted(candidate_pids):
            if process is None or pid != process.pid or pid_exists(pid):
                terminate_pid(pid)

        PROCESSES[name] = None
        handle = LOG_HANDLES.get(name)
        if handle:
            try:
                handle.close()
            except OSError:
                pass
        LOG_HANDLES[name] = None

        stored = read_state()
        stored.pop(name, None)
        try:
            STATE_FILE.write_text(json.dumps(stored, indent=2), encoding="utf-8")
        except OSError:
            pass

    deadline = time.time() + 5
    while time.time() < deadline:
        if not service_online(name):
            log_event(f"{SERVICES[name]['label']} stopped.", "success")
            return {"success": True, "message": f"{SERVICES[name]['label']} stopped."}
        # A child or stale external listener may still own the port; retry once detected.
        for pid in listening_pids(int(SERVICES[name]["port"])):
            terminate_pid(pid)
        time.sleep(0.2)

    message = (
        f"{SERVICES[name]['label']} is still online. Close any separate PowerShell "
        f"window running {SERVICES[name]['script']} and try Stop again."
    )
    log_event(message, "warning")
    return {"success": False, "message": message}


def set_service_state(name: str, should_be_online: bool) -> dict[str, Any]:
    return start_service(name) if should_be_online else stop_service(name)


def wait_for_service_state(name: str, expected_online: bool, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    consecutive_matches = 0
    while time.time() < deadline:
        current = service_online(name, timeout=0.35)
        if current == expected_online:
            consecutive_matches += 1
            if consecutive_matches >= 2:
                return True
        else:
            consecutive_matches = 0
        time.sleep(0.2)
    return service_online(name, timeout=0.5) == expected_online


def prepare_scenario(test_case_id: str) -> dict[str, bool]:
    """Put cloud and edge into the exact initial state required by a test case.

    The method stops unwanted services first, starts required services, then verifies
    both ports. A failed verification raises an error instead of silently starting a
    test with the wrong topology. Manual Start/Stop controls remain available after
    preparation while the workload is running.
    """
    config = SCENARIOS[test_case_id]
    desired_state: dict[str, bool] = dict(config["service_state"])
    update_job(message=f"Preparing server state for {config['label']}")
    log_event(f"Preparing exact server state for {config['label']}.")

    # Stop unwanted services first so a stale listener cannot affect the scenario.
    for name in ("cloud", "edge"):
        if not desired_state[name]:
            result = stop_service(name)
            if not result["success"]:
                raise RuntimeError(result["message"])

    # Start edge first where required, then cloud. This avoids a brief cloud-only state.
    for name in ("edge", "cloud"):
        if desired_state[name]:
            result = start_service(name)
            if not result["success"]:
                raise RuntimeError(result["message"])

    # Verify and perform one corrective retry for each service.
    for name in ("cloud", "edge"):
        expected = desired_state[name]
        if not wait_for_service_state(name, expected, timeout=5.0):
            log_event(
                f"Corrective retry: setting {SERVICES[name]['label']} to "
                f"{'ONLINE' if expected else 'OFFLINE'}.",
                "warning",
            )
            result = set_service_state(name, expected)
            if not result["success"] or not wait_for_service_state(name, expected, timeout=6.0):
                actual = "ONLINE" if service_online(name) else "OFFLINE"
                required = "ONLINE" if expected else "OFFLINE"
                raise RuntimeError(
                    f"Automatic preparation failed for {config['label']}: "
                    f"{SERVICES[name]['label']} is {actual}, required {required}."
                )

    actual_state = {name: service_online(name) for name in ("cloud", "edge")}
    if actual_state != desired_state:
        raise RuntimeError(
            f"Automatic preparation verification failed for {config['label']}: "
            f"required={desired_state}, actual={actual_state}."
        )
    log_event(
        f"Prepared {config['label']}: cloud={'ONLINE' if actual_state['cloud'] else 'OFFLINE'}, "
        f"edge={'ONLINE' if actual_state['edge'] else 'OFFLINE'}.",
        "success",
    )
    return actual_state


def update_job(**changes: Any) -> None:
    with JOB_LOCK:
        JOB.update(changes)


def start_job(name: str, function: Callable[[], Any]) -> tuple[bool, str]:
    with JOB_LOCK:
        if JOB["active"]:
            return False, f"Another operation is already running: {JOB['name']}"
        JOB.update(
            {
                "active": True,
                "name": name,
                "message": "Starting…",
                "started_at": now_text(),
                "finished_at": None,
                "success": None,
                "error": "",
            }
        )

    def worker() -> None:
        try:
            log_event(f"Started: {name}")
            update_job(message="Running")
            result = function()
            update_job(active=False, message="Completed", finished_at=now_text(), success=True, error="")
            log_event(f"Completed: {name}", "success")
            # Keep a compact serialisable result for the browser.
            if result is not None:
                try:
                    update_job(result=json.loads(json.dumps(result, default=str)))
                except (TypeError, ValueError):
                    update_job(result=str(result))
        except Exception as exc:  # noqa: BLE001 - dashboard must report background failures
            update_job(active=False, message="Failed", finished_at=now_text(), success=False, error=str(exc))
            log_event(f"Failed: {name}: {exc}", "error")

    threading.Thread(target=worker, name=f"v4-1-dashboard-{name}", daemon=True).start()
    return True, "Operation started."


def db_snapshot(name: str) -> dict[str, Any]:
    db_path = BASE_DIR / f"{name}.db"
    snapshot = runner.get_db_snapshot(str(db_path))
    snapshot["exists"] = db_path.exists()
    return snapshot


def read_database_orders(source: str, limit: int = 100) -> list[dict[str, Any]]:
    db_path = BASE_DIR / f"{source}.db"
    if source not in {"cloud", "edge"} or not db_path.exists():
        return []

    conn: sqlite3.Connection | None = None
    with DB_ACCESS_LOCK:
        try:
            conn = sqlite3.connect(db_path, timeout=1)
            conn.row_factory = sqlite3.Row
            columns = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
            if not columns:
                return []

            selected = [
                "id",
                "client_order_id",
                "customer_name",
                "order_details",
                "created_at",
                "source",
                "experiment_id",
                "batch_id",
                "test_case_id",
                "run_id",
                "request_number",
                "phase",
            ]
            if source == "cloud":
                selected.extend(["restored_from_edge", "synced_from_edge_at"])
            else:
                selected.extend(["pending_cloud_sync", "synced_to_cloud_at"])

            selected = [column for column in selected if column in columns]
            query = f"SELECT {', '.join(selected)} FROM orders ORDER BY id DESC LIMIT ?"
            return [dict(row) for row in conn.execute(query, (limit,)).fetchall()]
        except sqlite3.Error as exc:
            log_event(f"Could not read {source} orders: {exc}", "warning")
            return []
        finally:
            if conn is not None:
                conn.close()


def read_csv_tail(path: Path, limit: int) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with open(path, newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
        return list(reversed(rows[-limit:]))
    except (OSError, csv.Error):
        return []


def tail_text(path: Path, lines: int = 100) -> list[str]:
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            return deque(handle, maxlen=lines)  # type: ignore[return-value]
    except OSError:
        return []


def run_scenario_task(payload: dict[str, Any]) -> Any:
    test_case_id = payload["test_case_id"]
    config = SCENARIOS[test_case_id]
    update_job(message=f"Running {config['label']}")

    if test_case_id == "TC07":
        # TC07 is deliberately automated: it must move through NORMAL -> outage ->
        # EDGE_DEGRADED -> RECOVERY -> NORMAL without manual intervention.
        rows = runner.run_recovery_cycles(
            payload["repetitions"],
            payload["total_requests"],  # interpreted as requests per lifecycle phase
            payload["request_interval"],
            service_controller=lambda name, online: set_service_state(name, online),
        )
        return {"runs": rows}

    if payload.get("auto_manage", True):
        prepare_scenario(test_case_id)
    rows = runner.run_repeated(
        test_case_id,
        config["scenario"],
        payload["repetitions"],
        payload["total_requests"],
        payload["request_interval"],
        config["failover"],
        config["expected_cloud"],
        config["expected_edge"],
    )
    return {"runs": rows}


def run_recovery_task() -> Any:
    start_service("edge")
    start_service("cloud")
    update_job(message="Synchronising pending edge records to restored cloud")
    return runner.run_recovery_sync_test(wait_for_confirmation=False)


def run_cloud_to_edge_task() -> Any:
    start_service("cloud")
    start_service("edge")
    update_job(message="Synchronising cloud records to edge")
    return sync_cloud_to_edge()


def run_analysis_task() -> dict[str, Any]:
    update_job(message="Generating clean datasets, summaries and charts")
    env = os.environ.copy()
    env["MPLBACKEND"] = "Agg"
    completed = subprocess.run(
        [sys.executable, "analyse_v4_1_results.py"],
        cwd=BASE_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )
    output = (completed.stdout + "\n" + completed.stderr).strip()
    for line in output.splitlines()[-15:]:
        if line.strip():
            log_event(f"Analysis: {line.strip()}")
    if completed.returncode != 0:
        raise RuntimeError(f"Analysis failed with exit code {completed.returncode}")
    charts = sorted(path.name for path in CHART_DIR.glob("*.png")) if CHART_DIR.exists() else []
    return {"return_code": completed.returncode, "charts": charts}


def archive_evidence_task() -> dict[str, Any]:
    ARCHIVE_DIR.mkdir(exist_ok=True)
    archive_path = ARCHIVE_DIR / f"v4_1_evidence_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    update_job(message="Archiving databases and experiment evidence")
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for filename in ["cloud.db", "edge.db"]:
            path = BASE_DIR / filename
            if path.exists():
                archive.write(path, arcname=filename)
        if RESULT_ROOT.exists():
            for path in RESULT_ROOT.rglob("*"):
                if path.is_file():
                    archive.write(path, arcname=str(path.relative_to(BASE_DIR)))
    return {"archive": archive_path.name, "size_bytes": archive_path.stat().st_size}


def _delete_with_retries(path: Path, attempts: int = 8, delay: float = 0.35) -> None:
    """Delete a file after short retries for transient Windows file-handle release."""
    if not path.exists():
        return

    last_error: OSError | None = None
    for attempt in range(1, attempts + 1):
        try:
            path.unlink()
            return
        except OSError as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(delay)

    if last_error is not None:
        raise last_error


def _rmtree_with_retries(path: Path, attempts: int = 8, delay: float = 0.35) -> None:
    """Remove a result folder with retries in case a recently served chart is still open."""
    if not path.exists():
        return

    last_error: OSError | None = None
    for attempt in range(1, attempts + 1):
        try:
            shutil.rmtree(path)
            return
        except OSError as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(delay)

    if last_error is not None:
        raise last_error


def reset_task(restart_services: bool) -> dict[str, Any]:
    update_job(message="Stopping services")

    # Block dashboard database readers for the whole stop/delete phase. Without this,
    # the 2-second status poll can reopen cloud.db while Windows is trying to delete it.
    with DB_ACCESS_LOCK:
        cloud_stop = stop_service("cloud")
        edge_stop = stop_service("edge")
        if not cloud_stop["success"]:
            raise RuntimeError(cloud_stop["message"])
        if not edge_stop["success"]:
            raise RuntimeError(edge_stop["message"])

        # Give Windows a brief moment to release SQLite handles from stopped services.
        time.sleep(0.5)

        update_job(message="Deleting experiment databases and result files")
        for filename in ["cloud.db", "edge.db", "metrics_log.csv"]:
            _delete_with_retries(BASE_DIR / filename)

        # Remove SQLite sidecar files too if a previous process left any behind.
        for sidecar in [
            "cloud.db-journal", "cloud.db-wal", "cloud.db-shm",
            "edge.db-journal", "edge.db-wal", "edge.db-shm",
        ]:
            _delete_with_retries(BASE_DIR / sidecar)

        _rmtree_with_retries(RESULT_ROOT)

    log_event("Databases and v4_1_results were deleted for a clean experiment.", "warning")

    if restart_services:
        update_job(message="Recreating clean databases")
        edge_start = start_service("edge")
        cloud_start = start_service("cloud")
        if not edge_start["success"]:
            raise RuntimeError(edge_start["message"])
        if not cloud_start["success"]:
            raise RuntimeError(cloud_start["message"])

    return {"reset": True, "services_restarted": restart_services}


@app.get("/")
def dashboard() -> str:
    return render_template("dashboard.html", scenarios=SCENARIOS)


@app.get("/api/status")
def api_status():
    # Keep all dashboard-side SQLite reads in one short critical section so a reset
    # cannot delete the database while the browser's periodic status poll is reading it.
    with DB_ACCESS_LOCK:
        try:
            database_metrics = runner.get_global_database_metrics()
        except Exception:  # noqa: BLE001
            database_metrics = {
                "cloud_record_count": 0,
                "edge_record_count": 0,
                "pending_edge_sync_count": 0,
                "cloud_db_size_bytes": 0,
                "edge_db_size_bytes": 0,
            }
        cloud_snapshot = db_snapshot("cloud")
        edge_snapshot = db_snapshot("edge")

    services = {}
    for name, config in SERVICES.items():
        online = service_online(name)
        services[name] = {
            "label": config["label"],
            "online": online,
            "port": config["port"],
            "managed_pid": managed_pid(name),
        }
    latest_results = read_csv_tail(RAW_RESULTS, 1)
    return jsonify(
        {
            "timestamp": now_text(),
            "services": services,
            "metrics": {
                **database_metrics,
                "cloud": cloud_snapshot,
                "edge": edge_snapshot,
            },
            "latest_result": latest_results[0] if latest_results else None,
            "job": dict(JOB),
            "events": list(EVENT_LOG)[:25],
        }
    )


@app.post("/api/services/<name>/<action>")
def api_service_action(name: str, action: str):
    if name not in SERVICES or action not in {"start", "stop"}:
        abort(404)

    # Manual controls remain available during scenario workloads, but not during a
    # destructive reset because restarting a node mid-delete can reopen cloud.db/edge.db.
    with JOB_LOCK:
        if JOB["active"] and JOB["name"] == "Clean V4.1 reset":
            return jsonify({
                "success": False,
                "message": "Service controls are temporarily disabled while the clean reset is running.",
            }), 409

    result = start_service(name) if action == "start" else stop_service(name)
    return jsonify(result), 200 if result["success"] else 500


@app.post("/api/scenarios/run")
def api_run_scenario():
    payload = request.get_json(silent=True) or {}
    test_case_id = str(payload.get("test_case_id", "TC01")).upper()
    if test_case_id not in SCENARIOS:
        return jsonify({"success": False, "message": "Unknown test case."}), 400
    try:
        repetitions = int(payload.get("repetitions", 1))
        total_requests = int(payload.get("total_requests", 20))
        request_interval = float(payload.get("request_interval", 0.2))
    except (TypeError, ValueError):
        return jsonify({"success": False, "message": "Scenario parameters must be numeric."}), 400
    if not 1 <= repetitions <= 100:
        return jsonify({"success": False, "message": "Repetitions must be between 1 and 100."}), 400
    if not 1 <= total_requests <= 10000:
        return jsonify({"success": False, "message": "Orders per run/phase must be between 1 and 10,000."}), 400
    if not 0.01 <= request_interval <= 60:
        return jsonify({"success": False, "message": "Request interval must be between 0.01 and 60 seconds."}), 400
    clean_payload = {
        "test_case_id": test_case_id,
        "repetitions": repetitions,
        "total_requests": total_requests,
        "request_interval": request_interval,
        "auto_manage": bool(payload.get("auto_manage", True)),
    }
    started, message = start_job(SCENARIOS[test_case_id]["label"], lambda: run_scenario_task(clean_payload))
    return jsonify({"success": started, "message": message}), 202 if started else 409


@app.post("/api/actions/<action>")
def api_action(action: str):
    actions: dict[str, tuple[str, Callable[[], Any]]] = {
        "consistency": ("TC05 scoped RPO-related exposure / recovery check", runner.run_consistency_check_only),
        "recovery-sync": ("TC06 scoped edge-to-cloud recovery sync", run_recovery_task),
        "cloud-to-edge-sync": ("Cloud-to-edge normal sync", run_cloud_to_edge_task),
        "analyse": ("Analyse V4.1 results", run_analysis_task),
        "archive": ("Archive V4.1 evidence", archive_evidence_task),
    }
    if action == "reset":
        payload = request.get_json(silent=True) or {}
        restart_services = bool(payload.get("restart_services", True))
        started, message = start_job("Clean V4.1 reset", lambda: reset_task(restart_services))
        return jsonify({"success": started, "message": message}), 202 if started else 409
    if action not in actions:
        abort(404)
    name, function = actions[action]
    started, message = start_job(name, function)
    return jsonify({"success": started, "message": message}), 202 if started else 409


@app.get("/api/orders")
def api_orders():
    source = request.args.get("source", "cloud").lower()
    try:
        limit = min(max(int(request.args.get("limit", "100")), 1), 500)
    except ValueError:
        limit = 100
    if source not in {"cloud", "edge"}:
        return jsonify({"success": False, "message": "source must be cloud or edge"}), 400
    return jsonify({"source": source, "orders": read_database_orders(source, limit)})


@app.get("/api/results")
def api_results():
    try:
        limit = min(max(int(request.args.get("limit", "30")), 1), 200)
    except ValueError:
        limit = 30
    return jsonify({"results": read_csv_tail(RAW_RESULTS, limit)})


@app.get("/api/request-events")
def api_request_events():
    try:
        limit = min(max(int(request.args.get("limit", "50")), 1), 500)
    except ValueError:
        limit = 50
    return jsonify({"events": read_csv_tail(REQUEST_EVENTS, limit)})


@app.get("/api/service-logs/<name>")
def api_service_logs(name: str):
    if name not in SERVICES:
        abort(404)
    return jsonify({"service": name, "lines": list(tail_text(SERVICES[name]["log"], 100))})


@app.get("/api/charts")
def api_charts():
    charts = sorted(path.name for path in CHART_DIR.glob("*.png")) if CHART_DIR.exists() else []
    return jsonify({"charts": charts})


@app.get("/charts/<path:filename>")
def charts(filename: str):
    if not CHART_DIR.exists():
        abort(404)
    return send_from_directory(CHART_DIR, filename)


@app.get("/archives/<path:filename>")
def archives(filename: str):
    if not ARCHIVE_DIR.exists():
        abort(404)
    return send_from_directory(ARCHIVE_DIR, filename, as_attachment=True)


def cleanup() -> None:
    # Only stop processes registered by this dashboard. Existing externally started
    # services are not targeted unless their PID was written by this dashboard.
    for name in ["cloud", "edge"]:
        process = PROCESSES.get(name)
        if process is not None and process.poll() is None:
            try:
                process.terminate()
            except OSError:
                pass


atexit.register(cleanup)


if __name__ == "__main__":
    LOG_DIR.mkdir(exist_ok=True)
    log_event("V4.1 dashboard started at http://127.0.0.1:5050", "success")
    threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{DASHBOARD_PORT}")).start()
    app.run(host="127.0.0.1", port=DASHBOARD_PORT, debug=False, use_reloader=False, threaded=True)
