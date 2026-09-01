"""V4.1 research experiment runner for the edge-assisted DR simulation.

Key V4.1 changes:
- stateful failover within each run (NORMAL -> EDGE_DEGRADED)
- observed failover recovery separated from organisational RTO
- experiment/batch-scoped recovery metrics to prevent historical contamination
- recovery scope verified from transactions actually persisted in edge.db
- separate connection/read HTTP timeouts to reduce false acknowledgement failures
- configured versus achieved request rate
- RPO-related exposure as accepted edge records not yet represented in cloud
- recovery completeness and replica convergence as separate measures
- lightweight process/database resource observations
- TC07 automated end-to-end recovery lifecycle (when invoked by dashboard)
"""
from __future__ import annotations

from contextlib import closing
import csv
import math
import os
import sqlite3
import statistics
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import requests

from sync_service import sync_cloud_to_edge, sync_edge_to_cloud_after_restoration

VERSION = "4.1"
CLOUD_URL = "http://127.0.0.1:5000/add_order"
EDGE_URL = "http://127.0.0.1:5001/add_order"
CLOUD_METRICS_URL = "http://127.0.0.1:5000/metrics"
EDGE_METRICS_URL = "http://127.0.0.1:5001/metrics"
CLOUD_DB = "cloud.db"
EDGE_DB = "edge.db"

RESULT_DIR = Path("v4_1_results/raw")
RESULT_FILE = RESULT_DIR / "v4_1_raw_experiment_results.csv"
REQUEST_LOG_FILE = RESULT_DIR / "v4_1_request_event_log.csv"
DB_SNAPSHOT_FILE = RESULT_DIR / "v4_1_database_snapshots.csv"
SYNC_EVENT_FILE = RESULT_DIR / "v4_1_sync_event_log.csv"
LIFECYCLE_EVENT_FILE = RESULT_DIR / "v4_1_lifecycle_event_log.csv"

FIELDNAMES = [
    "timestamp", "version", "batch_id", "experiment_id", "test_case_id", "scenario", "run_id", "lifecycle_phase",
    "total_requests", "request_interval_seconds", "configured_requests_per_second", "achieved_requests_per_second",
    "workload_duration_seconds", "workload_level", "successful_requests", "failed_requests", "cloud_success", "edge_success",
    "failover_enabled", "expected_cloud_status", "expected_edge_status", "failover_transition_count",
    "observed_failover_recovery_seconds", "cloud_failure_detection_seconds", "mean_total_request_latency_seconds",
    "p95_total_request_latency_seconds", "availability_percent", "recovery_scope_records", "rpo_related_exposure_records",
    "pending_edge_sync_count", "oldest_pending_age_seconds", "recovery_completeness_percent", "replica_convergence_percent",
    "missing_from_cloud_count", "duplicate_client_order_ids", "pre_recovery_rpo_exposure_records",
    "pre_recovery_pending_sync_count", "post_recovery_rpo_exposure_records", "cloud_record_count", "edge_record_count",
    "recovery_sync_time_seconds", "records_synced_back_to_cloud", "cloud_process_rss_mb", "edge_process_rss_mb",
    "cloud_cpu_percent_snapshot", "edge_cpu_percent_snapshot", "cloud_db_size_bytes", "edge_db_size_bytes",
    "lifecycle_transition_count", "notes",
]

REQUEST_FIELDNAMES = [
    "event_timestamp", "version", "batch_id", "experiment_id", "test_case_id", "scenario", "run_id", "phase",
    "request_number", "client_order_id", "request_interval_seconds", "operating_state_before", "operating_state_after",
    "failover_enabled", "expected_cloud_status", "expected_edge_status", "cloud_http_status", "cloud_latency_seconds",
    "cloud_error", "cloud_failure_detected", "cloud_failure_detection_seconds", "edge_http_status", "edge_latency_seconds",
    "edge_error", "failover_triggered", "observed_failover_recovery_seconds", "final_success", "final_target",
    "total_request_latency_seconds",
]

SNAPSHOT_FIELDNAMES = [
    "snapshot_timestamp", "version", "batch_id", "experiment_id", "test_case_id", "scenario", "run_id", "snapshot_stage",
    "database", "record_count", "pending_sync_count", "restored_from_edge_count", "unique_client_order_ids",
    "duplicate_client_order_ids", "database_file_size_bytes",
]

SYNC_FIELDNAMES = [
    "event_timestamp", "version", "batch_id", "experiment_id", "test_case_id", "scenario", "run_id", "client_order_id",
    "sync_direction", "success", "http_status", "duration_seconds", "error",
]

LIFECYCLE_FIELDNAMES = [
    "event_timestamp", "version", "batch_id", "experiment_id", "run_id", "event_number", "from_state", "to_state",
    "event", "duration_seconds", "details",
]


def ensure_csv_file(path: Path, fieldnames: list[str]) -> None:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as handle:
            csv.DictWriter(handle, fieldnames=fieldnames).writeheader()


def append_csv(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    ensure_csv_file(path, fieldnames)
    normalized = {field: row.get(field, "") for field in fieldnames}
    with path.open("a", newline="", encoding="utf-8") as handle:
        csv.DictWriter(handle, fieldnames=fieldnames).writerow(normalized)


def percentile95(values: list[float]) -> float | str:
    if not values:
        return "N/A"
    ordered = sorted(values)
    index = max(0, math.ceil(0.95 * len(ordered)) - 1)
    return round(ordered[index], 6)


def generate_order(
    order_number: int,
    scenario_name: str,
    run_id: int,
    *,
    experiment_id: str,
    batch_id: str,
    test_case_id: str,
    phase: str = "workload",
) -> dict[str, Any]:
    client_order_id = f"v41-{test_case_id}-{run_id}-{order_number}-{uuid.uuid4().hex[:8]}"
    return {
        "client_order_id": client_order_id,
        "customer_name": f"V4.1 {test_case_id} Run {run_id} Customer {order_number}",
        "order_details": f"V4.1 test order {order_number} for {scenario_name}, run {run_id}, phase {phase}",
        "created_at": datetime.now().isoformat(timespec="milliseconds"),
        "experiment_id": experiment_id,
        "batch_id": batch_id,
        "test_case_id": test_case_id,
        "run_id": run_id,
        "request_number": order_number,
        "phase": phase,
    }


def post_order(
    url: str,
    order: dict[str, Any],
    connect_timeout: float = 2.0,
    read_timeout: float = 5.0,
) -> dict[str, Any]:
    """POST one order using separate connection and response timeouts.

    The shorter connection timeout preserves fast detection of an unavailable
    service, while the longer read timeout reduces false client-side failures when
    a live Flask service has accepted/persisted a transaction but needs slightly
    longer to return its HTTP response.
    """
    start = time.perf_counter()
    try:
        response = requests.post(
            url,
            json=order,
            timeout=(connect_timeout, read_timeout),
        )
        ok = response.status_code in (200, 201)
        return {
            "ok": ok,
            "status_code": response.status_code,
            "latency": round(time.perf_counter() - start, 6),
            "error": "" if ok else response.text[:300],
        }
    except requests.RequestException as error:
        return {
            "ok": False,
            "status_code": None,
            "latency": round(time.perf_counter() - start, 6),
            "error": f"{type(error).__name__}: {error}",
        }


def empty_attempt(reason: str = "not_attempted") -> dict[str, Any]:
    return {"ok": False, "status_code": None, "latency": None, "error": reason}


def send_cloud_only(order: dict[str, Any], state: str = "NORMAL") -> dict[str, Any]:
    total_start = time.perf_counter()
    cloud = post_order(CLOUD_URL, order)
    return {
        "success": cloud["ok"],
        "target": "cloud" if cloud["ok"] else "none",
        "cloud": cloud,
        "edge": empty_attempt(),
        "state_before": state,
        "state_after": "NORMAL" if cloud["ok"] else state,
        "cloud_failure_detected": not cloud["ok"],
        "cloud_failure_detection_seconds": cloud["latency"] if not cloud["ok"] else None,
        "failover_triggered": False,
        "observed_failover_recovery_seconds": None,
        "total_latency": round(time.perf_counter() - total_start, 6),
    }


def send_with_stateful_failover(order: dict[str, Any], state: str) -> dict[str, Any]:
    """Send one request using a persistent per-run operating state.

    The first failed cloud attempt in NORMAL triggers an edge attempt. If the edge
    accepts the request, the session enters EDGE_DEGRADED and later requests are
    sent directly to edge instead of repeatedly probing cloud.
    """
    total_start = time.perf_counter()
    state_before = state
    cloud = empty_attempt()
    edge = empty_attempt()
    cloud_failure_detected = False
    cloud_detection = None
    failover_triggered = False
    failover_recovery = None

    if state == "NORMAL":
        cloud = post_order(CLOUD_URL, order)
        if cloud["ok"]:
            return {
                "success": True, "target": "cloud", "cloud": cloud, "edge": edge,
                "state_before": state_before, "state_after": "NORMAL",
                "cloud_failure_detected": False, "cloud_failure_detection_seconds": None,
                "failover_triggered": False, "observed_failover_recovery_seconds": None,
                "total_latency": round(time.perf_counter() - total_start, 6),
            }
        cloud_failure_detected = True
        cloud_detection = cloud["latency"]
        failover_triggered = True
        edge = post_order(EDGE_URL, order)
        if edge["ok"]:
            failover_recovery = round(time.perf_counter() - total_start, 6)
            state_after = "EDGE_DEGRADED"
            success = True
            target = "edge"
        else:
            state_after = "SERVICE_UNAVAILABLE"
            success = False
            target = "none"
    else:
        # Once cloud failure is known, do not pay the cloud-failure-detection cost
        # again during the same controlled outage run.
        edge = post_order(EDGE_URL, order)
        if edge["ok"]:
            state_after = "EDGE_DEGRADED"
            success = True
            target = "edge"
        else:
            state_after = "SERVICE_UNAVAILABLE"
            success = False
            target = "none"

    return {
        "success": success,
        "target": target,
        "cloud": cloud,
        "edge": edge,
        "state_before": state_before,
        "state_after": state_after,
        "cloud_failure_detected": cloud_failure_detected,
        "cloud_failure_detection_seconds": cloud_detection,
        "failover_triggered": failover_triggered,
        "observed_failover_recovery_seconds": failover_recovery,
        "total_latency": round(time.perf_counter() - total_start, 6),
    }


def get_db_records(db_name: str) -> list[dict[str, Any]]:
    if not os.path.exists(db_name):
        return []
    try:
        with closing(sqlite3.connect(db_name, timeout=1)) as conn:
            conn.row_factory = sqlite3.Row
            columns = [row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()]
            select_columns = [
                "client_order_id",
                "created_at" if "created_at" in columns else "NULL AS created_at",
                "experiment_id" if "experiment_id" in columns else "NULL AS experiment_id",
                "batch_id" if "batch_id" in columns else "NULL AS batch_id",
            ]
            if "pending_cloud_sync" in columns:
                select_columns.append("COALESCE(pending_cloud_sync, 0) AS pending_cloud_sync")
            else:
                select_columns.append("0 AS pending_cloud_sync")
            rows = conn.execute(
                f"SELECT {', '.join(select_columns)} FROM orders WHERE client_order_id IS NOT NULL"
            ).fetchall()
            return [dict(row) for row in rows]
    except sqlite3.Error:
        return []


def get_db_snapshot(db_name: str) -> dict[str, Any]:
    result = {
        "record_count": 0,
        "pending_sync_count": 0,
        "restored_from_edge_count": 0,
        "unique_client_order_ids": 0,
        "duplicate_client_order_ids": 0,
        "database_file_size_bytes": os.path.getsize(db_name) if os.path.exists(db_name) else 0,
    }
    if not os.path.exists(db_name):
        return result
    try:
        with closing(sqlite3.connect(db_name, timeout=1)) as conn:
            columns = [row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()]
            result["record_count"] = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            if "client_order_id" in columns:
                result["unique_client_order_ids"] = conn.execute(
                    "SELECT COUNT(DISTINCT client_order_id) FROM orders WHERE client_order_id IS NOT NULL"
                ).fetchone()[0]
                result["duplicate_client_order_ids"] = max(
                    0, result["record_count"] - result["unique_client_order_ids"]
                )
            if "pending_cloud_sync" in columns:
                result["pending_sync_count"] = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE COALESCE(pending_cloud_sync, 0)=1"
                ).fetchone()[0]
            if "restored_from_edge" in columns:
                result["restored_from_edge_count"] = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE COALESCE(restored_from_edge, 0)=1"
                ).fetchone()[0]
    except sqlite3.Error:
        pass
    return result


def capture_db_snapshots(
    batch_id: str,
    experiment_id: str,
    test_case_id: str,
    scenario: str,
    run_id: int,
    stage: str,
) -> None:
    for database, db_name in [("cloud", CLOUD_DB), ("edge", EDGE_DB)]:
        append_csv(DB_SNAPSHOT_FILE, SNAPSHOT_FIELDNAMES, {
            "snapshot_timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "version": VERSION,
            "batch_id": batch_id,
            "experiment_id": experiment_id,
            "test_case_id": test_case_id,
            "scenario": scenario,
            "run_id": run_id,
            "snapshot_stage": stage,
            "database": database,
            **get_db_snapshot(db_name),
        })


def _parse_created_at(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for parser in (
        lambda: datetime.fromisoformat(text),
        lambda: datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return parser()
        except ValueError:
            continue
    return None


def calculate_scope_metrics(client_order_ids: set[str] | list[str] | None) -> dict[str, Any]:
    """Calculate recovery measures only for the selected accepted-edge records."""
    scope = {str(value) for value in (client_order_ids or []) if value}
    cloud_records = get_db_records(CLOUD_DB)
    edge_records = get_db_records(EDGE_DB)
    cloud_ids = {str(row["client_order_id"]) for row in cloud_records}
    edge_ids = {str(row["client_order_id"]) for row in edge_records}

    cloud_snapshot = get_db_snapshot(CLOUD_DB)
    edge_snapshot = get_db_snapshot(EDGE_DB)
    if not scope:
        return {
            "recovery_scope_records": 0,
            "rpo_related_exposure_records": 0,
            "pending_edge_sync_count": 0,
            "oldest_pending_age_seconds": "N/A",
            "recovery_completeness_percent": "N/A",
            "replica_convergence_percent": "N/A",
            "missing_from_cloud_count": 0,
            "duplicate_client_order_ids": 0,
            "cloud_record_count": cloud_snapshot["record_count"],
            "edge_record_count": edge_snapshot["record_count"],
        }

    cloud_in_scope = scope & cloud_ids
    edge_in_scope = scope & edge_ids
    missing_from_cloud = scope - cloud_ids
    pending_rows = [
        row for row in edge_records
        if str(row["client_order_id"]) in scope and int(row.get("pending_cloud_sync", 0) or 0) == 1
    ]
    pending_ids = {str(row["client_order_id"]) for row in pending_rows}
    converged = {item for item in scope if item in cloud_ids and item in edge_ids and item not in pending_ids}

    ages = []
    now = datetime.now()
    for row in pending_rows:
        created = _parse_created_at(row.get("created_at"))
        if created:
            ages.append(max(0.0, (now - created).total_seconds()))

    # Count duplicates in the scoped IDs even though UNIQUE indexes should keep this zero.
    duplicate_count = 0
    for records in (cloud_records, edge_records):
        counts: dict[str, int] = {}
        for row in records:
            key = str(row["client_order_id"])
            if key in scope:
                counts[key] = counts.get(key, 0) + 1
        duplicate_count += sum(max(0, count - 1) for count in counts.values())

    return {
        "recovery_scope_records": len(scope),
        # RPO-related exposure is the number of accepted outage records not yet
        # represented in the restored primary cloud. A stale pending flag does not
        # imply data loss if the record is already present in cloud.
        "rpo_related_exposure_records": len(missing_from_cloud),
        "pending_edge_sync_count": len(pending_ids),
        "oldest_pending_age_seconds": round(max(ages), 3) if ages else "N/A",
        "recovery_completeness_percent": round(len(cloud_in_scope) / len(scope) * 100, 2),
        "replica_convergence_percent": round(len(converged) / len(scope) * 100, 2),
        "missing_from_cloud_count": len(missing_from_cloud),
        "duplicate_client_order_ids": duplicate_count,
        "cloud_record_count": cloud_snapshot["record_count"],
        "edge_record_count": edge_snapshot["record_count"],
    }


def get_global_database_metrics() -> dict[str, Any]:
    """Dashboard-only global counts; not used as the scoped consistency result."""
    cloud = get_db_snapshot(CLOUD_DB)
    edge = get_db_snapshot(EDGE_DB)
    return {
        "cloud_record_count": cloud["record_count"],
        "edge_record_count": edge["record_count"],
        "pending_edge_sync_count": edge["pending_sync_count"],
        "cloud_db_size_bytes": cloud["database_file_size_bytes"],
        "edge_db_size_bytes": edge["database_file_size_bytes"],
    }


def calculate_consistency():
    """Compatibility helper for older callers.

    V4.1 no longer treats global union/intersection as the research consistency
    metric. The returned 'consistency' is therefore N/A; callers should use
    calculate_scope_metrics() for recovery completeness/convergence.
    """
    metrics = get_global_database_metrics()
    return (
        metrics["pending_edge_sync_count"],
        "N/A",
        metrics["cloud_record_count"],
        metrics["edge_record_count"],
        metrics["pending_edge_sync_count"],
    )


def get_service_resource_metrics(url: str) -> dict[str, Any]:
    try:
        response = requests.get(url, timeout=0.8)
        if response.status_code == 200:
            return response.json()
    except requests.RequestException:
        pass
    return {
        "process_rss_mb": None,
        "cpu_percent_snapshot": None,
        "database_file_size_bytes": None,
        "record_count": None,
    }


def sample_resource_metrics() -> dict[str, Any]:
    cloud = get_service_resource_metrics(CLOUD_METRICS_URL)
    edge = get_service_resource_metrics(EDGE_METRICS_URL)
    return {
        "cloud_process_rss_mb": cloud.get("process_rss_mb"),
        "edge_process_rss_mb": edge.get("process_rss_mb"),
        "cloud_cpu_percent_snapshot": cloud.get("cpu_percent_snapshot"),
        "edge_cpu_percent_snapshot": edge.get("cpu_percent_snapshot"),
        "cloud_db_size_bytes": cloud.get("database_file_size_bytes") if cloud.get("database_file_size_bytes") is not None else get_db_snapshot(CLOUD_DB)["database_file_size_bytes"],
        "edge_db_size_bytes": edge.get("database_file_size_bytes") if edge.get("database_file_size_bytes") is not None else get_db_snapshot(EDGE_DB)["database_file_size_bytes"],
    }


def merge_resource_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    result = {}
    for key in [
        "cloud_process_rss_mb", "edge_process_rss_mb", "cloud_cpu_percent_snapshot", "edge_cpu_percent_snapshot",
        "cloud_db_size_bytes", "edge_db_size_bytes",
    ]:
        numeric = [float(sample[key]) for sample in samples if sample.get(key) not in (None, "", "N/A")]
        result[key] = round(max(numeric), 3) if numeric else "N/A"
    return result


def classify_workload(request_interval: float) -> str:
    if request_interval <= 0.1:
        return "high"
    if request_interval <= 0.2:
        return "medium"
    if request_interval >= 1.0:
        return "low"
    return "custom"


def log_request_event(
    *,
    batch_id: str,
    experiment_id: str,
    test_case_id: str,
    scenario: str,
    run_id: int,
    phase: str,
    request_number: int,
    order: dict[str, Any],
    request_interval: float,
    failover_enabled: bool,
    expected_cloud_status: str,
    expected_edge_status: str,
    result: dict[str, Any],
) -> None:
    append_csv(REQUEST_LOG_FILE, REQUEST_FIELDNAMES, {
        "event_timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "version": VERSION,
        "batch_id": batch_id,
        "experiment_id": experiment_id,
        "test_case_id": test_case_id,
        "scenario": scenario,
        "run_id": run_id,
        "phase": phase,
        "request_number": request_number,
        "client_order_id": order["client_order_id"],
        "request_interval_seconds": request_interval,
        "operating_state_before": result["state_before"],
        "operating_state_after": result["state_after"],
        "failover_enabled": failover_enabled,
        "expected_cloud_status": expected_cloud_status,
        "expected_edge_status": expected_edge_status,
        "cloud_http_status": result["cloud"]["status_code"],
        "cloud_latency_seconds": result["cloud"]["latency"],
        "cloud_error": result["cloud"]["error"],
        "cloud_failure_detected": result["cloud_failure_detected"],
        "cloud_failure_detection_seconds": result["cloud_failure_detection_seconds"],
        "edge_http_status": result["edge"]["status_code"],
        "edge_latency_seconds": result["edge"]["latency"],
        "edge_error": result["edge"]["error"],
        "failover_triggered": result["failover_triggered"],
        "observed_failover_recovery_seconds": result["observed_failover_recovery_seconds"],
        "final_success": result["success"],
        "final_target": result["target"],
        "total_request_latency_seconds": result["total_latency"],
    })


def base_result_row() -> dict[str, Any]:
    return {field: "N/A" for field in FIELDNAMES}


def run_single_experiment(
    test_case_id: str,
    scenario: str,
    run_id: int,
    total_requests: int,
    request_interval: float,
    failover_enabled: bool,
    expected_cloud_status: str,
    expected_edge_status: str,
    *,
    batch_id: str | None = None,
) -> dict[str, Any]:
    batch_id = batch_id or f"{test_case_id}-batch-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    experiment_id = f"{test_case_id}-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}-run{run_id}-{uuid.uuid4().hex[:6]}"
    capture_db_snapshots(batch_id, experiment_id, test_case_id, scenario, run_id, "before_run")

    print("=" * 76)
    print(f"V4.1 | {test_case_id} | {scenario} | Run {run_id} | Batch {batch_id}")
    configured_rate = round(1 / request_interval, 3)
    print(f"Requests: {total_requests} | Interval: {request_interval}s | Configured rate: {configured_rate} req/s")
    print(f"Failover enabled: {failover_enabled} | Expected cloud={expected_cloud_status}, edge={expected_edge_status}")
    print("=" * 76)

    state = "NORMAL"
    successful = failed = cloud_success = edge_success = 0
    failover_values: list[float] = []
    detection_values: list[float] = []
    total_latencies: list[float] = []
    edge_scope_ids: set[str] = set()
    workload_start = time.perf_counter()

    for request_number in range(1, total_requests + 1):
        order = generate_order(
            request_number, scenario, run_id,
            experiment_id=experiment_id, batch_id=batch_id, test_case_id=test_case_id, phase="workload",
        )
        result = send_with_stateful_failover(order, state) if failover_enabled else send_cloud_only(order, state)
        state = result["state_after"]
        log_request_event(
            batch_id=batch_id, experiment_id=experiment_id, test_case_id=test_case_id,
            scenario=scenario, run_id=run_id, phase="workload", request_number=request_number,
            order=order, request_interval=request_interval, failover_enabled=failover_enabled,
            expected_cloud_status=expected_cloud_status, expected_edge_status=expected_edge_status,
            result=result,
        )
        total_latencies.append(float(result["total_latency"]))
        if result["cloud_failure_detection_seconds"] is not None:
            detection_values.append(float(result["cloud_failure_detection_seconds"]))
        if result["observed_failover_recovery_seconds"] is not None:
            failover_values.append(float(result["observed_failover_recovery_seconds"]))

        if result["success"]:
            successful += 1
            if result["target"] == "cloud":
                cloud_success += 1
                print(f"Request {request_number}: SUCCESS via CLOUD | state={state}")
            else:
                edge_success += 1
                edge_scope_ids.add(order["client_order_id"])
                marker = " | FAILOVER TRANSITION" if result["failover_triggered"] else ""
                print(f"Request {request_number}: SUCCESS via EDGE | state={state}{marker}")
        else:
            failed += 1
            print(f"Request {request_number}: FAILED | state={state}")

        if request_number < total_requests:
            time.sleep(request_interval)

    workload_duration = round(time.perf_counter() - workload_start, 6)
    achieved_rate = round(total_requests / workload_duration, 3) if workload_duration > 0 else "N/A"
    availability = round(successful / total_requests * 100, 2) if total_requests else 0.0

    # Include any relevant order that was actually committed to edge.db even if
    # the HTTP client timed out before receiving the acknowledgement.
    time.sleep(0.2)
    edge_scope_ids |= get_pending_edge_scope_ids(
        batch_id=batch_id,
        experiment_id=experiment_id,
        test_case_id=test_case_id,
        run_id=run_id,
        phase="workload",
    )

    scope_metrics = calculate_scope_metrics(edge_scope_ids)
    resources = sample_resource_metrics()
    capture_db_snapshots(batch_id, experiment_id, test_case_id, scenario, run_id, "after_run")

    row = base_result_row()
    row.update({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "version": VERSION,
        "batch_id": batch_id,
        "experiment_id": experiment_id,
        "test_case_id": test_case_id,
        "scenario": scenario,
        "run_id": run_id,
        "lifecycle_phase": "single_scenario",
        "total_requests": total_requests,
        "request_interval_seconds": request_interval,
        "configured_requests_per_second": configured_rate,
        "achieved_requests_per_second": achieved_rate,
        "workload_duration_seconds": workload_duration,
        "workload_level": classify_workload(request_interval),
        "successful_requests": successful,
        "failed_requests": failed,
        "cloud_success": cloud_success,
        "edge_success": edge_success,
        "failover_enabled": failover_enabled,
        "expected_cloud_status": expected_cloud_status,
        "expected_edge_status": expected_edge_status,
        "failover_transition_count": len(failover_values),
        "observed_failover_recovery_seconds": round(statistics.mean(failover_values), 6) if failover_values else "N/A",
        "cloud_failure_detection_seconds": round(statistics.mean(detection_values), 6) if detection_values else "N/A",
        "mean_total_request_latency_seconds": round(statistics.mean(total_latencies), 6) if total_latencies else "N/A",
        "p95_total_request_latency_seconds": percentile95(total_latencies),
        "availability_percent": availability,
        **scope_metrics,
        "pre_recovery_rpo_exposure_records": scope_metrics["rpo_related_exposure_records"],
        "pre_recovery_pending_sync_count": scope_metrics["pending_edge_sync_count"],
        "post_recovery_rpo_exposure_records": "N/A",
        "recovery_sync_time_seconds": "N/A",
        "records_synced_back_to_cloud": "N/A",
        **resources,
        "lifecycle_transition_count": 1 if failover_values else 0,
        "notes": (
            "Observed failover recovery is a technical indicator, not organisational RTO; "
            f"final_state={state}; edge_recovery_scope={len(edge_scope_ids)}"
        ),
    })
    append_csv(RESULT_FILE, FIELDNAMES, row)

    print("-" * 76)
    print(f"Availability: {availability}%")
    print(f"Configured / achieved rate: {configured_rate} / {achieved_rate} req/s")
    print(f"Observed failover recovery: {row['observed_failover_recovery_seconds']} s")
    print(f"RPO-related exposure: {scope_metrics['rpo_related_exposure_records']} record(s)")
    print(f"Recovery completeness: {scope_metrics['recovery_completeness_percent']}%")
    print(f"Replica convergence: {scope_metrics['replica_convergence_percent']}%")
    print("-" * 76)
    return row


def run_repeated(
    test_case_id: str,
    scenario: str,
    repetitions: int,
    total_requests: int,
    request_interval: float,
    failover_enabled: bool,
    expected_cloud_status: str,
    expected_edge_status: str,
) -> list[dict[str, Any]]:
    batch_id = f"{test_case_id}-batch-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    rows = []
    for run_id in range(1, repetitions + 1):
        rows.append(run_single_experiment(
            test_case_id, scenario, run_id, total_requests, request_interval,
            failover_enabled, expected_cloud_status, expected_edge_status, batch_id=batch_id,
        ))
        if run_id < repetitions:
            time.sleep(0.5)
    return rows


def _as_true(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def get_pending_edge_scope_ids(
    *,
    batch_id: str | None = None,
    experiment_id: str | None = None,
    test_case_id: str | None = None,
    run_id: int | None = None,
    phase: str | None = None,
) -> set[str]:
    """Return persisted pending edge transactions for the requested experiment scope.

    Recovery is based on records actually committed to edge.db, not only on the
    client-side HTTP acknowledgement. This prevents a transaction from being
    omitted from recovery if the server stored it but the client timed out while
    waiting for the response.
    """
    if not os.path.exists(EDGE_DB):
        return set()

    try:
        with closing(sqlite3.connect(EDGE_DB, timeout=2)) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
            if not columns or "client_order_id" not in columns:
                return set()

            conditions = ["client_order_id IS NOT NULL"]
            parameters: list[Any] = []

            if "pending_cloud_sync" in columns:
                conditions.append("COALESCE(pending_cloud_sync, 0) = 1")

            filters = [
                ("batch_id", batch_id),
                ("experiment_id", experiment_id),
                ("test_case_id", test_case_id),
                ("run_id", run_id),
                ("phase", phase),
            ]
            for column, value in filters:
                if value is None:
                    continue
                # A requested scope field must exist; otherwise do not risk
                # broadening the recovery scope to unrelated records.
                if column not in columns:
                    return set()
                conditions.append(f"{column} = ?")
                parameters.append(value)

            rows = conn.execute(
                f"""
                SELECT client_order_id
                FROM orders
                WHERE {' AND '.join(conditions)}
                """,
                parameters,
            ).fetchall()
            return {str(row[0]) for row in rows if row[0]}
    except sqlite3.Error:
        return set()


def latest_recovery_scope() -> dict[str, Any] | None:
    """Return persisted pending records from the latest TC02 recovery batch."""
    if not os.path.exists(EDGE_DB):
        return None

    try:
        with closing(sqlite3.connect(EDGE_DB, timeout=2)) as conn:
            conn.row_factory = sqlite3.Row
            columns = {row[1] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
            required = {"client_order_id", "batch_id", "test_case_id"}
            if not required.issubset(columns):
                return None

            experiment_select = "experiment_id" if "experiment_id" in columns else "NULL AS experiment_id"
            latest = conn.execute(
                f"""
                SELECT batch_id, {experiment_select}, test_case_id
                FROM orders
                WHERE test_case_id = 'TC02'
                  AND client_order_id IS NOT NULL
                  {'AND COALESCE(pending_cloud_sync, 0) = 1' if 'pending_cloud_sync' in columns else ''}
                  AND batch_id IS NOT NULL
                ORDER BY rowid DESC
                LIMIT 1
                """
            ).fetchone()

        if not latest:
            return None

        batch_id = str(latest["batch_id"])
        scope_ids = get_pending_edge_scope_ids(
            batch_id=batch_id,
            test_case_id="TC02",
        )
        if not scope_ids:
            return None

        return {
            "batch_id": batch_id,
            "client_order_ids": scope_ids,
            "source_test_case": "TC02",
            "source_experiment_id": latest["experiment_id"],
        }
    except sqlite3.Error:
        return None


def _write_recovery_row(
    *,
    test_case_id: str,
    scenario: str,
    batch_id: str,
    experiment_id: str,
    run_id: int,
    scope_ids: set[str],
    before: dict[str, Any],
    after: dict[str, Any] | None = None,
    sync_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected = after or before
    row = base_result_row()
    row.update({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "version": VERSION,
        "batch_id": batch_id,
        "experiment_id": experiment_id,
        "test_case_id": test_case_id,
        "scenario": scenario,
        "run_id": run_id,
        "lifecycle_phase": "recovery_check" if test_case_id == "TC05" else "recovery_sync",
        "total_requests": 0,
        "successful_requests": 0,
        "failed_requests": (sync_result or {}).get("failed", 0),
        "cloud_success": 0,
        "edge_success": 0,
        "availability_percent": "N/A",
        **selected,
        "pre_recovery_rpo_exposure_records": before["rpo_related_exposure_records"],
        "pre_recovery_pending_sync_count": before["pending_edge_sync_count"],
        "post_recovery_rpo_exposure_records": (after or before)["rpo_related_exposure_records"],
        "recovery_sync_time_seconds": (sync_result or {}).get("duration_seconds", "N/A"),
        "records_synced_back_to_cloud": (sync_result or {}).get("synced", "N/A"),
        **sample_resource_metrics(),
        "lifecycle_transition_count": 0,
        "notes": f"Scoped to {len(scope_ids)} accepted edge record(s) from batch {batch_id}",
    })
    append_csv(RESULT_FILE, FIELDNAMES, row)
    return row


def run_consistency_check_only() -> dict[str, Any]:
    scope = latest_recovery_scope()
    if not scope:
        raise RuntimeError("No successful edge-assisted batch is available. Run TC02 or TC07 first.")
    batch_id = scope["batch_id"]
    scope_ids = set(scope["client_order_ids"])
    run_id = int(datetime.now().strftime("%H%M%S"))
    experiment_id = f"TC05-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}-{uuid.uuid4().hex[:6]}"
    metrics = calculate_scope_metrics(scope_ids)
    capture_db_snapshots(batch_id, experiment_id, "TC05", "scoped_recovery_state_check", run_id, "scope_check")
    row = _write_recovery_row(
        test_case_id="TC05", scenario="scoped_recovery_state_check", batch_id=batch_id,
        experiment_id=experiment_id, run_id=run_id, scope_ids=scope_ids, before=metrics,
    )
    print(f"TC05 scope: {len(scope_ids)} record(s) from {batch_id}")
    print(f"RPO-related exposure: {metrics['rpo_related_exposure_records']}")
    print(f"Pending sync: {metrics['pending_edge_sync_count']}")
    print(f"Recovery completeness: {metrics['recovery_completeness_percent']}%")
    print(f"Replica convergence: {metrics['replica_convergence_percent']}%")
    return row


def _log_sync_events(
    result: dict[str, Any], *, batch_id: str, experiment_id: str, test_case_id: str,
    scenario: str, run_id: int,
) -> None:
    for event in result.get("events", []):
        append_csv(SYNC_EVENT_FILE, SYNC_FIELDNAMES, {
            "event_timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "version": VERSION,
            "batch_id": batch_id,
            "experiment_id": experiment_id,
            "test_case_id": test_case_id,
            "scenario": scenario,
            "run_id": run_id,
            "client_order_id": event.get("client_order_id"),
            "sync_direction": "edge_to_cloud",
            "success": event.get("success"),
            "http_status": event.get("http_status"),
            "duration_seconds": event.get("duration_seconds"),
            "error": event.get("error", ""),
        })


def run_recovery_sync_test(wait_for_confirmation: bool = True) -> dict[str, Any]:
    scope = latest_recovery_scope()
    if not scope:
        raise RuntimeError("No successful edge-assisted batch is available. Run TC02 or TC07 first.")
    if wait_for_confirmation:
        input("Start CLOUD and keep EDGE running, then press Enter to run scoped recovery sync...")
    batch_id = scope["batch_id"]
    scope_ids = set(scope["client_order_ids"])
    run_id = int(datetime.now().strftime("%H%M%S"))
    experiment_id = f"TC06-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}-{uuid.uuid4().hex[:6]}"
    scenario = "cloud_restoration_scoped_edge_to_cloud_sync"
    before = calculate_scope_metrics(scope_ids)
    capture_db_snapshots(batch_id, experiment_id, "TC06", scenario, run_id, "before_sync")
    result = sync_edge_to_cloud_after_restoration(client_order_ids=scope_ids)
    _log_sync_events(result, batch_id=batch_id, experiment_id=experiment_id, test_case_id="TC06", scenario=scenario, run_id=run_id)
    after = calculate_scope_metrics(scope_ids)
    capture_db_snapshots(batch_id, experiment_id, "TC06", scenario, run_id, "after_sync")
    row = _write_recovery_row(
        test_case_id="TC06", scenario=scenario, batch_id=batch_id, experiment_id=experiment_id,
        run_id=run_id, scope_ids=scope_ids, before=before, after=after, sync_result=result,
    )
    print(f"Post-sync recovery completeness: {after['recovery_completeness_percent']}%")
    print(f"Post-sync replica convergence: {after['replica_convergence_percent']}%")
    print(f"Post-sync RPO-related exposure: {after['rpo_related_exposure_records']}")
    return {"sync_result": result, "row": row, "before": before, "after": after}


def log_lifecycle_event(
    *, batch_id: str, experiment_id: str, run_id: int, event_number: int,
    from_state: str, to_state: str, event: str, duration_seconds: Any = "N/A", details: str = "",
) -> None:
    append_csv(LIFECYCLE_EVENT_FILE, LIFECYCLE_FIELDNAMES, {
        "event_timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "version": VERSION, "batch_id": batch_id, "experiment_id": experiment_id, "run_id": run_id,
        "event_number": event_number, "from_state": from_state, "to_state": to_state,
        "event": event, "duration_seconds": duration_seconds, "details": details,
    })


def _control_service(
    service_controller: Callable[[str, bool], Any] | None,
    name: str,
    online: bool,
) -> None:
    if service_controller is None:
        input(f"Set {name.upper()} {'ONLINE' if online else 'OFFLINE'}, then press Enter...")
        return
    result = service_controller(name, online)
    if isinstance(result, dict) and not result.get("success", True):
        raise RuntimeError(result.get("message", f"Could not set {name} state"))


def _run_lifecycle_requests(
    *,
    batch_id: str,
    experiment_id: str,
    run_id: int,
    phase: str,
    scenario: str,
    total_requests: int,
    request_interval: float,
    state: str,
    failover_enabled: bool,
    expected_cloud: str,
    expected_edge: str,
) -> tuple[str, list[dict[str, Any]], set[str]]:
    results: list[dict[str, Any]] = []
    edge_ids: set[str] = set()
    for request_number in range(1, total_requests + 1):
        order = generate_order(
            request_number, scenario, run_id,
            experiment_id=experiment_id, batch_id=batch_id, test_case_id="TC07", phase=phase,
        )
        result = send_with_stateful_failover(order, state) if failover_enabled else send_cloud_only(order, state)
        state = result["state_after"]
        log_request_event(
            batch_id=batch_id, experiment_id=experiment_id, test_case_id="TC07", scenario=scenario,
            run_id=run_id, phase=phase, request_number=request_number, order=order,
            request_interval=request_interval, failover_enabled=failover_enabled,
            expected_cloud_status=expected_cloud, expected_edge_status=expected_edge, result=result,
        )
        results.append(result)
        if result["success"] and result["target"] == "edge":
            edge_ids.add(order["client_order_id"])
        if request_number < total_requests:
            time.sleep(request_interval)
    return state, results, edge_ids


def run_end_to_end_recovery_cycle(
    run_id: int,
    requests_per_phase: int,
    request_interval: float,
    *,
    batch_id: str,
    service_controller: Callable[[str, bool], Any] | None = None,
) -> dict[str, Any]:
    """TC07: NORMAL -> EDGE_DEGRADED -> RECOVERY -> NORMAL."""
    scenario = "end_to_end_recovery_lifecycle"
    experiment_id = f"TC07-{datetime.now().strftime('%Y%m%dT%H%M%S%f')}-run{run_id}-{uuid.uuid4().hex[:6]}"
    resource_samples: list[dict[str, Any]] = []
    capture_db_snapshots(batch_id, experiment_id, "TC07", scenario, run_id, "cycle_start")

    event_number = 1
    _control_service(service_controller, "edge", True)
    _control_service(service_controller, "cloud", True)
    state = "NORMAL"
    log_lifecycle_event(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, event_number=event_number,
        from_state="START", to_state="NORMAL", event="services_verified_online",
    )
    event_number += 1

    cycle_start = time.perf_counter()
    state, baseline_results, _ = _run_lifecycle_requests(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, phase="normal_baseline",
        scenario=scenario, total_requests=requests_per_phase, request_interval=request_interval,
        state=state, failover_enabled=False, expected_cloud="available", expected_edge="available",
    )
    resource_samples.append(sample_resource_metrics())

    _control_service(service_controller, "cloud", False)
    log_lifecycle_event(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, event_number=event_number,
        from_state="NORMAL", to_state="NORMAL", event="cloud_failure_injected",
        details="Logical state remains NORMAL until the first request detects the failure.",
    )
    event_number += 1

    outage_state_before = state
    state, outage_results, outage_ids = _run_lifecycle_requests(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, phase="cloud_outage",
        scenario=scenario, total_requests=requests_per_phase, request_interval=request_interval,
        state=state, failover_enabled=True, expected_cloud="failed", expected_edge="available",
    )

    # Recovery scope comes from persisted edge state. Union with acknowledged
    # edge IDs as a defensive fallback if SQLite is briefly busy.
    time.sleep(0.2)
    outage_ids |= get_pending_edge_scope_ids(
        batch_id=batch_id,
        experiment_id=experiment_id,
        test_case_id="TC07",
        run_id=run_id,
        phase="cloud_outage",
    )

    failover_event = next((r for r in outage_results if r.get("observed_failover_recovery_seconds") is not None), None)
    if failover_event:
        log_lifecycle_event(
            batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, event_number=event_number,
            from_state=outage_state_before, to_state="EDGE_DEGRADED", event="failover_completed",
            duration_seconds=failover_event["observed_failover_recovery_seconds"],
            details=f"cloud_failure_detection={failover_event['cloud_failure_detection_seconds']}s",
        )
        event_number += 1
    resource_samples.append(sample_resource_metrics())
    before = calculate_scope_metrics(outage_ids)
    capture_db_snapshots(batch_id, experiment_id, "TC07", scenario, run_id, "edge_degraded_before_recovery")

    log_lifecycle_event(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, event_number=event_number,
        from_state=state, to_state="RECOVERY", event="cloud_restoration_started",
    )
    event_number += 1
    _control_service(service_controller, "cloud", True)
    sync_result = sync_edge_to_cloud_after_restoration(client_order_ids=outage_ids)
    _log_sync_events(sync_result, batch_id=batch_id, experiment_id=experiment_id, test_case_id="TC07", scenario=scenario, run_id=run_id)
    after = calculate_scope_metrics(outage_ids)
    log_lifecycle_event(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, event_number=event_number,
        from_state="RECOVERY", to_state="NORMAL", event="recovery_sync_verified",
        duration_seconds=sync_result.get("duration_seconds", "N/A"),
        details=(
            f"recovery_completeness={after['recovery_completeness_percent']}%; "
            f"replica_convergence={after['replica_convergence_percent']}%"
        ),
    )
    event_number += 1
    state = "NORMAL"
    resource_samples.append(sample_resource_metrics())
    capture_db_snapshots(batch_id, experiment_id, "TC07", scenario, run_id, "after_recovery_sync")

    state, post_results, _ = _run_lifecycle_requests(
        batch_id=batch_id, experiment_id=experiment_id, run_id=run_id, phase="post_recovery_verification",
        scenario=scenario, total_requests=requests_per_phase, request_interval=request_interval,
        state=state, failover_enabled=False, expected_cloud="available", expected_edge="available",
    )
    resource_samples.append(sample_resource_metrics())
    capture_db_snapshots(batch_id, experiment_id, "TC07", scenario, run_id, "cycle_end")

    all_results = baseline_results + outage_results + post_results
    total_requests = len(all_results)
    successful = sum(1 for result in all_results if result["success"])
    failed = total_requests - successful
    cloud_success = sum(1 for result in all_results if result["success"] and result["target"] == "cloud")
    edge_success = sum(1 for result in all_results if result["success"] and result["target"] == "edge")
    latencies = [float(result["total_latency"]) for result in all_results]
    failovers = [float(result["observed_failover_recovery_seconds"]) for result in all_results if result["observed_failover_recovery_seconds"] is not None]
    detections = [float(result["cloud_failure_detection_seconds"]) for result in all_results if result["cloud_failure_detection_seconds"] is not None]
    cycle_duration = round(time.perf_counter() - cycle_start, 6)
    configured_rate = round(1 / request_interval, 3)
    achieved_rate = round(total_requests / cycle_duration, 3) if cycle_duration > 0 else "N/A"
    resources = merge_resource_samples(resource_samples)

    row = base_result_row()
    row.update({
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "version": VERSION,
        "batch_id": batch_id,
        "experiment_id": experiment_id,
        "test_case_id": "TC07",
        "scenario": scenario,
        "run_id": run_id,
        "lifecycle_phase": "normal_to_failure_to_recovery",
        "total_requests": total_requests,
        "request_interval_seconds": request_interval,
        "configured_requests_per_second": configured_rate,
        "achieved_requests_per_second": achieved_rate,
        "workload_duration_seconds": cycle_duration,
        "workload_level": classify_workload(request_interval),
        "successful_requests": successful,
        "failed_requests": failed,
        "cloud_success": cloud_success,
        "edge_success": edge_success,
        "failover_enabled": True,
        "expected_cloud_status": "available -> failed -> restored",
        "expected_edge_status": "available",
        "failover_transition_count": len(failovers),
        "observed_failover_recovery_seconds": round(statistics.mean(failovers), 6) if failovers else "N/A",
        "cloud_failure_detection_seconds": round(statistics.mean(detections), 6) if detections else "N/A",
        "mean_total_request_latency_seconds": round(statistics.mean(latencies), 6),
        "p95_total_request_latency_seconds": percentile95(latencies),
        "availability_percent": round(successful / total_requests * 100, 2) if total_requests else 0,
        **after,
        # For TC07 the principal RPO-related exposure is the maximum observed just
        # before recovery. Post-recovery exposure is recorded separately.
        "rpo_related_exposure_records": before["rpo_related_exposure_records"],
        "pre_recovery_rpo_exposure_records": before["rpo_related_exposure_records"],
        "pre_recovery_pending_sync_count": before["pending_edge_sync_count"],
        "post_recovery_rpo_exposure_records": after["rpo_related_exposure_records"],
        "recovery_sync_time_seconds": sync_result.get("duration_seconds", "N/A"),
        "records_synced_back_to_cloud": sync_result.get("synced", 0),
        **resources,
        "lifecycle_transition_count": event_number - 1,
        "notes": (
            f"TC07 requests_per_phase={requests_per_phase}; final_state={state}; "
            "scoped recovery metrics use only outage records accepted by edge"
        ),
    })
    append_csv(RESULT_FILE, FIELDNAMES, row)
    print("TC07 complete:")
    print(f"  Pre-recovery RPO-related exposure: {before['rpo_related_exposure_records']}")
    print(f"  Post-recovery RPO-related exposure: {after['rpo_related_exposure_records']}")
    print(f"  Recovery completeness: {after['recovery_completeness_percent']}%")
    print(f"  Replica convergence: {after['replica_convergence_percent']}%")
    return row


def run_recovery_cycles(
    repetitions: int,
    requests_per_phase: int,
    request_interval: float,
    *,
    service_controller: Callable[[str, bool], Any] | None = None,
) -> list[dict[str, Any]]:
    batch_id = f"TC07-batch-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"
    rows = []
    for run_id in range(1, repetitions + 1):
        rows.append(run_end_to_end_recovery_cycle(
            run_id, requests_per_phase, request_interval,
            batch_id=batch_id, service_controller=service_controller,
        ))
        if run_id < repetitions:
            time.sleep(0.5)
    return rows


def read_positive_float(prompt: str, default: float) -> float:
    while True:
        raw = input(prompt).strip()
        if not raw:
            return default
        try:
            value = float(raw)
            if value > 0:
                return value
        except ValueError:
            pass
        print("Please enter a number greater than 0, e.g. 1.0, 0.2 or 0.1.")


def menu() -> None:
    while True:
        print("\nV4.1 RESEARCH EXPERIMENT RUNNER")
        print("1. TC01 Normal operation: cloud ON, edge ON")
        print("2. TC02 Cloud failure with stateful edge failover: cloud OFF, edge ON")
        print("3. TC03 Cloud-only failure: cloud OFF, failover disabled")
        print("4. TC04 Complete service failure: cloud OFF, edge OFF")
        print("5. TC05 Scoped RPO-related exposure / recovery-state check")
        print("6. TC06 Scoped cloud restoration: edge -> cloud recovery sync")
        print("7. TC07 End-to-end lifecycle (dashboard recommended for automation)")
        print("8. Optional normal cloud -> edge synchronization")
        print("9. Exit")
        choice = input("Choose option: ").strip()

        if choice in {"1", "2", "3", "4"}:
            reps = int(input("Repetitions [1]: ") or "1")
            reqs = int(input("Requests per run [25]: ") or "25")
            interval = read_positive_float("Seconds between requests [0.2]: ", 0.2)
            configs = {
                "1": ("TC01", "normal_operation", False, "available", "available", "Start CLOUD and EDGE"),
                "2": ("TC02", "cloud_failure_with_stateful_edge_failover", True, "failed", "available", "STOP CLOUD; keep EDGE running"),
                "3": ("TC03", "cloud_only_failure_no_failover", False, "failed", "not_used", "STOP CLOUD; EDGE is not used"),
                "4": ("TC04", "complete_service_failure", True, "failed", "failed", "STOP CLOUD and EDGE"),
            }
            tc, scenario, failover, cloud, edge, prompt = configs[choice]
            input(f"{prompt}, then press Enter...")
            run_repeated(tc, scenario, reps, reqs, interval, failover, cloud, edge)
        elif choice == "5":
            run_consistency_check_only()
        elif choice == "6":
            run_recovery_sync_test()
        elif choice == "7":
            reps = int(input("Lifecycle repetitions [1]: ") or "1")
            reqs = int(input("Requests per lifecycle phase [10]: ") or "10")
            interval = read_positive_float("Seconds between requests [0.2]: ", 0.2)
            run_recovery_cycles(reps, reqs, interval, service_controller=None)
        elif choice == "8":
            input("Start CLOUD and EDGE, then press Enter...")
            print(sync_cloud_to_edge())
        elif choice == "9":
            break
        else:
            print("Invalid choice.")


if __name__ == "__main__":
    menu()
