"""V4.1 synchronization service.

The recovery direction can be scoped to a batch or experiment so historical
records do not contaminate the recovery evaluation.
"""
from __future__ import annotations

import time
from typing import Any

import requests

CLOUD_ORDERS_URL = "http://127.0.0.1:5000/orders"
EDGE_ORDERS_URL = "http://127.0.0.1:5001/orders"
EDGE_SYNC_URL = "http://127.0.0.1:5001/sync_order"
CLOUD_SYNC_FROM_EDGE_URL = "http://127.0.0.1:5000/sync_order_from_edge"
EDGE_MARK_SYNCED_URL = "http://127.0.0.1:5001/mark_synced_to_cloud"


def sync_cloud_to_edge() -> dict[str, Any]:
    """Normal cloud-to-edge replication helper (not part of formal failover timing)."""
    try:
        cloud_response = requests.get(CLOUD_ORDERS_URL, timeout=5)
        if cloud_response.status_code != 200:
            return {"success": False, "checked": 0, "synced": 0, "failed": 1, "events": []}

        cloud_orders = cloud_response.json()
        synced_count = failed_count = 0
        events = []
        for order in cloud_orders:
            event_start = time.perf_counter()
            try:
                edge_response = requests.post(EDGE_SYNC_URL, json=order, timeout=5)
                ok = edge_response.status_code in (200, 201)
                duration = round(time.perf_counter() - event_start, 6)
                events.append({
                    "client_order_id": order.get("client_order_id"),
                    "success": ok,
                    "http_status": edge_response.status_code,
                    "duration_seconds": duration,
                    "error": "" if ok else edge_response.text[:300],
                })
                if ok:
                    synced_count += 1
                else:
                    failed_count += 1
            except requests.RequestException as exc:
                failed_count += 1
                events.append({
                    "client_order_id": order.get("client_order_id"),
                    "success": False,
                    "http_status": None,
                    "duration_seconds": round(time.perf_counter() - event_start, 6),
                    "error": f"{type(exc).__name__}: {exc}",
                })
        return {
            "success": failed_count == 0,
            "checked": len(cloud_orders),
            "synced": synced_count,
            "failed": failed_count,
            "events": events,
        }
    except requests.RequestException as exc:
        return {"success": False, "checked": 0, "synced": 0, "failed": 1, "events": [], "error": str(exc)}


def _matches_scope(order: dict, batch_id: str | None, experiment_id: str | None, client_order_ids: set[str] | None) -> bool:
    if int(order.get("pending_cloud_sync", 0) or 0) != 1:
        return False
    if client_order_ids is not None and str(order.get("client_order_id")) not in client_order_ids:
        return False
    if batch_id is not None and order.get("batch_id") != batch_id:
        return False
    if experiment_id is not None and order.get("experiment_id") != experiment_id:
        return False
    return True


def sync_edge_to_cloud_after_restoration(
    *,
    batch_id: str | None = None,
    experiment_id: str | None = None,
    client_order_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Copy pending edge records to cloud after restoration.

    A scope is strongly recommended for research runs. With no scope the function
    still synchronizes all pending records, preserving the original demonstration
    behaviour.
    """
    start_time = time.perf_counter()
    try:
        edge_response = requests.get(EDGE_ORDERS_URL, timeout=5)
        if edge_response.status_code != 200:
            return {
                "success": False, "checked": 0, "synced": 0, "failed": 1,
                "duration_seconds": round(time.perf_counter() - start_time, 6), "events": [],
            }

        edge_orders = edge_response.json()
        pending_orders = [
            order for order in edge_orders
            if _matches_scope(order, batch_id, experiment_id, client_order_ids)
        ]
        synced_count = failed_count = 0
        sync_events: list[dict[str, Any]] = []

        print("=" * 72)
        print("V4.1 EDGE-TO-CLOUD RECOVERY SYNCHRONIZATION")
        print("=" * 72)
        print(f"Pending records in selected scope: {len(pending_orders)}")
        if batch_id:
            print(f"Batch scope: {batch_id}")
        if experiment_id:
            print(f"Experiment scope: {experiment_id}")
        print("-" * 72)

        for order in pending_orders:
            event_start = time.perf_counter()
            try:
                cloud_response = requests.post(CLOUD_SYNC_FROM_EDGE_URL, json=order, timeout=5)
                if cloud_response.status_code in (200, 201):
                    mark_response = requests.post(
                        EDGE_MARK_SYNCED_URL,
                        json={"client_order_id": order.get("client_order_id")},
                        timeout=5,
                    )
                    if mark_response.status_code == 200:
                        synced_count += 1
                        success = True
                        error = ""
                        status = cloud_response.status_code
                    else:
                        failed_count += 1
                        success = False
                        error = "cloud_accepted_but_edge_mark_failed"
                        status = mark_response.status_code
                else:
                    failed_count += 1
                    success = False
                    error = cloud_response.text[:300]
                    status = cloud_response.status_code
            except requests.RequestException as exc:
                failed_count += 1
                success = False
                error = f"{type(exc).__name__}: {exc}"
                status = None

            event_duration = round(time.perf_counter() - event_start, 6)
            sync_events.append({
                "client_order_id": order.get("client_order_id"),
                "success": success,
                "http_status": status,
                "duration_seconds": event_duration,
                "error": error,
            })
            print(
                f"{order.get('client_order_id')}: "
                f"{'SYNCED' if success else 'FAILED'} ({event_duration}s)"
            )

        duration = round(time.perf_counter() - start_time, 6)
        print("-" * 72)
        print(f"Records synced back to cloud: {synced_count}")
        print(f"Failed sync attempts: {failed_count}")
        print(f"Recovery synchronization time: {duration} seconds")
        print("=" * 72)
        return {
            "success": failed_count == 0,
            "checked": len(pending_orders),
            "synced": synced_count,
            "failed": failed_count,
            "duration_seconds": duration,
            "events": sync_events,
            "scope_batch_id": batch_id,
            "scope_experiment_id": experiment_id,
        }

    except requests.RequestException as exc:
        return {
            "success": False, "checked": 0, "synced": 0, "failed": 1,
            "duration_seconds": round(time.perf_counter() - start_time, 6),
            "events": [], "error": f"{type(exc).__name__}: {exc}",
        }
