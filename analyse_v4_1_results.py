"""Generate clean V4.1 datasets, summaries, charts and data-quality evidence."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

BASE = Path(__file__).resolve().parent
RESULT_ROOT = BASE / "v4_1_results"
RAW_DIR = RESULT_ROOT / "raw"
CLEAN_DIR = RESULT_ROOT / "clean"
SUMMARY_DIR = RESULT_ROOT / "summaries"
CHART_DIR = RESULT_ROOT / "charts"
EXPORT_DIR = RESULT_ROOT / "database_exports"
for directory in [CLEAN_DIR, SUMMARY_DIR, CHART_DIR, EXPORT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

RUN_FILE = RAW_DIR / "v4_1_raw_experiment_results.csv"
REQUEST_FILE = RAW_DIR / "v4_1_request_event_log.csv"
SNAPSHOT_FILE = RAW_DIR / "v4_1_database_snapshots.csv"
SYNC_FILE = RAW_DIR / "v4_1_sync_event_log.csv"
LIFECYCLE_FILE = RAW_DIR / "v4_1_lifecycle_event_log.csv"
CLOUD_DB = BASE / "cloud.db"
EDGE_DB = BASE / "edge.db"

SCENARIO_LABELS = {
    "TC01": "Normal Operation",
    "TC02": "Cloud Failure + Stateful Edge Failover",
    "TC03": "Cloud Failure without Failover",
    "TC04": "Complete Service Failure",
    "TC05": "Scoped Recovery-State Check",
    "TC06": "Scoped Cloud Recovery Sync",
    "TC07": "End-to-End Recovery Lifecycle",
}

NUMERIC_RUN_COLUMNS = [
    "run_id", "total_requests", "request_interval_seconds", "configured_requests_per_second",
    "achieved_requests_per_second", "workload_duration_seconds", "successful_requests", "failed_requests",
    "cloud_success", "edge_success", "failover_transition_count", "observed_failover_recovery_seconds",
    "cloud_failure_detection_seconds", "mean_total_request_latency_seconds", "p95_total_request_latency_seconds",
    "availability_percent", "recovery_scope_records", "rpo_related_exposure_records", "pending_edge_sync_count",
    "oldest_pending_age_seconds", "recovery_completeness_percent", "replica_convergence_percent",
    "missing_from_cloud_count", "duplicate_client_order_ids", "pre_recovery_rpo_exposure_records",
    "pre_recovery_pending_sync_count", "post_recovery_rpo_exposure_records", "cloud_record_count",
    "edge_record_count", "recovery_sync_time_seconds", "records_synced_back_to_cloud", "cloud_process_rss_mb",
    "edge_process_rss_mb", "cloud_cpu_percent_snapshot", "edge_cpu_percent_snapshot", "cloud_db_size_bytes",
    "edge_db_size_bytes", "lifecycle_transition_count",
]


def read_csv_optional(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def clean_runs() -> pd.DataFrame:
    if not RUN_FILE.exists():
        raise FileNotFoundError("No V4.1 run-level results found. Run experiments first.")
    df = pd.read_csv(RUN_FILE)
    for column in NUMERIC_RUN_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["scenario_display"] = df["test_case_id"].map(SCENARIO_LABELS).fillna(df.get("scenario"))
    df["request_success_rate_percent"] = (
        df["successful_requests"] / df["total_requests"] * 100
    ).where(df["total_requests"] > 0)
    df["edge_share_of_success_percent"] = (
        df["edge_success"] / df["successful_requests"] * 100
    ).where(df["successful_requests"] > 0)
    df["configured_achieved_rate_ratio_percent"] = (
        df["achieved_requests_per_second"] / df["configured_requests_per_second"] * 100
    ).where(df["configured_requests_per_second"] > 0)
    df["data_quality_issue"] = ""
    request_mismatch = (
        df["successful_requests"].fillna(0) + df["failed_requests"].fillna(0)
    ) != df["total_requests"].fillna(0)
    df.loc[request_mismatch, "data_quality_issue"] += "request_total_mismatch;"
    target_mismatch = (
        df["cloud_success"].fillna(0) + df["edge_success"].fillna(0)
    ) != df["successful_requests"].fillna(0)
    df.loc[target_mismatch, "data_quality_issue"] += "success_target_mismatch;"
    negative_recovery = df["rpo_related_exposure_records"].fillna(0) < 0
    df.loc[negative_recovery, "data_quality_issue"] += "negative_rpo_exposure;"
    impossible_completeness = df["recovery_completeness_percent"].dropna().gt(100)
    df.loc[impossible_completeness.index[impossible_completeness], "data_quality_issue"] += "recovery_completeness_gt_100;"
    return df


def clean_requests() -> pd.DataFrame:
    df = read_csv_optional(REQUEST_FILE)
    if df.empty:
        return df
    numeric = [
        "run_id", "request_number", "request_interval_seconds", "cloud_http_status", "cloud_latency_seconds",
        "cloud_failure_detection_seconds", "edge_http_status", "edge_latency_seconds",
        "observed_failover_recovery_seconds", "total_request_latency_seconds",
    ]
    for column in numeric:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "event_timestamp" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    for column in ["final_success", "cloud_failure_detected", "failover_triggered"]:
        if column in df.columns:
            df[column] = df[column].astype(str).str.lower().map({"true": True, "false": False})
    df["scenario_display"] = df["test_case_id"].map(SCENARIO_LABELS).fillna(df.get("scenario"))
    return df


def export_database(path: Path, name: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        with sqlite3.connect(path) as conn:
            df = pd.read_sql_query("SELECT * FROM orders ORDER BY id", conn)
    except (sqlite3.Error, pd.errors.DatabaseError):
        return pd.DataFrame()
    df.insert(0, "database", name)
    df.to_csv(EXPORT_DIR / f"{name}_orders_export.csv", index=False)
    return df


def build_global_reconciliation(cloud: pd.DataFrame, edge: pd.DataFrame) -> pd.DataFrame:
    """Diagnostic inventory only; not used as V4.1 research consistency metric."""
    if cloud.empty and edge.empty:
        return pd.DataFrame()
    cloud_ids = set(cloud.get("client_order_id", pd.Series(dtype=str)).dropna().astype(str))
    edge_ids = set(edge.get("client_order_id", pd.Series(dtype=str)).dropna().astype(str))
    rows = []
    for key in sorted(cloud_ids | edge_ids):
        c = cloud[cloud.get("client_order_id", pd.Series(index=cloud.index, dtype=str)).astype(str) == key]
        e = edge[edge.get("client_order_id", pd.Series(index=edge.index, dtype=str)).astype(str) == key]
        pending = int(e["pending_cloud_sync"].fillna(0).max()) if not e.empty and "pending_cloud_sync" in e else 0
        rows.append({
            "client_order_id": key,
            "present_in_cloud": not c.empty,
            "present_in_edge": not e.empty,
            "pending_cloud_sync": pending,
            "batch_id": (e["batch_id"].dropna().astype(str).iloc[0] if not e.empty and "batch_id" in e and e["batch_id"].notna().any()
                         else c["batch_id"].dropna().astype(str).iloc[0] if not c.empty and "batch_id" in c and c["batch_id"].notna().any() else ""),
            "diagnostic_status": (
                "matched_and_synced" if not c.empty and not e.empty and pending == 0
                else "pending_edge_to_cloud" if not e.empty and pending == 1
                else "cloud_only" if not c.empty else "edge_only"
            ),
        })
    result = pd.DataFrame(rows)
    result.to_csv(EXPORT_DIR / "global_record_inventory_diagnostic.csv", index=False)
    return result


def build_run_summary(runs: pd.DataFrame) -> pd.DataFrame:
    primary = runs[runs["test_case_id"].isin(["TC01", "TC02", "TC03", "TC04", "TC07"])].copy()
    if primary.empty:
        return pd.DataFrame()
    return primary.groupby(["test_case_id", "scenario_display"], dropna=False).agg(
        runs=("run_id", "count"),
        total_requests=("total_requests", "sum"),
        successful_requests=("successful_requests", "sum"),
        failed_requests=("failed_requests", "sum"),
        mean_availability_percent=("availability_percent", "mean"),
        availability_stddev=("availability_percent", "std"),
        mean_configured_rate=("configured_requests_per_second", "mean"),
        mean_achieved_rate=("achieved_requests_per_second", "mean"),
        mean_failover_recovery_seconds=("observed_failover_recovery_seconds", "mean"),
        p95_run_failover_recovery_seconds=("observed_failover_recovery_seconds", lambda s: s.quantile(0.95)),
        mean_cloud_failure_detection_seconds=("cloud_failure_detection_seconds", "mean"),
        mean_total_request_latency_seconds=("mean_total_request_latency_seconds", "mean"),
        p95_total_request_latency_seconds=("p95_total_request_latency_seconds", "mean"),
        max_rpo_related_exposure_records=("rpo_related_exposure_records", "max"),
        max_pending_sync_records=("pre_recovery_pending_sync_count", "max"),
        mean_recovery_completeness_percent=("recovery_completeness_percent", "mean"),
        mean_replica_convergence_percent=("replica_convergence_percent", "mean"),
        mean_recovery_sync_seconds=("recovery_sync_time_seconds", "mean"),
        max_edge_process_rss_mb=("edge_process_rss_mb", "max"),
        max_edge_db_size_bytes=("edge_db_size_bytes", "max"),
    ).reset_index()


def build_recovery_summary(runs: pd.DataFrame) -> pd.DataFrame:
    recovery = runs[runs["test_case_id"].isin(["TC05", "TC06", "TC07"])].copy()
    if recovery.empty:
        return pd.DataFrame()
    columns = [
        "timestamp", "test_case_id", "batch_id", "experiment_id", "recovery_scope_records",
        "pre_recovery_rpo_exposure_records", "pre_recovery_pending_sync_count",
        "post_recovery_rpo_exposure_records", "recovery_completeness_percent",
        "replica_convergence_percent", "recovery_sync_time_seconds", "records_synced_back_to_cloud",
        "missing_from_cloud_count", "duplicate_client_order_ids",
    ]
    return recovery[[column for column in columns if column in recovery.columns]].copy()


def build_request_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    return events.groupby(["test_case_id", "scenario_display", "phase"], dropna=False).agg(
        request_events=("client_order_id", "count"),
        successful_events=("final_success", "sum"),
        mean_total_latency_seconds=("total_request_latency_seconds", "mean"),
        p95_total_latency_seconds=("total_request_latency_seconds", lambda s: s.quantile(0.95)),
        mean_cloud_latency_seconds=("cloud_latency_seconds", "mean"),
        mean_edge_latency_seconds=("edge_latency_seconds", "mean"),
        mean_failover_recovery_seconds=("observed_failover_recovery_seconds", "mean"),
        mean_cloud_failure_detection_seconds=("cloud_failure_detection_seconds", "mean"),
    ).reset_index()


def save_charts(summary: pd.DataFrame, runs: pd.DataFrame, lifecycle: pd.DataFrame) -> None:
    if not summary.empty:
        def bar(column: str, title: str, ylabel: str, filename: str, upper: float | None = None):
            plot = summary.dropna(subset=[column])
            if plot.empty:
                return
            plt.figure(figsize=(11, 6))
            plt.bar(plot["scenario_display"], plot[column])
            plt.title(title)
            plt.ylabel(ylabel)
            if upper is not None:
                plt.ylim(0, upper)
            plt.xticks(rotation=25, ha="right")
            plt.tight_layout()
            plt.savefig(CHART_DIR / filename, dpi=180)
            plt.close()

        bar("mean_availability_percent", "Mean Transaction Availability by Scenario", "Availability (%)", "v4_1_mean_availability.png", 105)
        bar("mean_failover_recovery_seconds", "Observed Failover Recovery by Scenario", "Seconds", "v4_1_observed_failover_recovery.png")
        bar("max_rpo_related_exposure_records", "Maximum RPO-Related Exposure", "Accepted edge records not yet in cloud", "v4_1_rpo_related_exposure.png")
        bar("max_edge_process_rss_mb", "Observed Edge Process Memory", "Maximum RSS (MB)", "v4_1_edge_memory_observation.png")

        rate = summary.dropna(subset=["mean_configured_rate", "mean_achieved_rate"])
        if not rate.empty:
            x = range(len(rate))
            width = 0.38
            plt.figure(figsize=(11, 6))
            plt.bar([i - width / 2 for i in x], rate["mean_configured_rate"], width=width, label="Configured")
            plt.bar([i + width / 2 for i in x], rate["mean_achieved_rate"], width=width, label="Achieved")
            plt.xticks(list(x), rate["scenario_display"], rotation=25, ha="right")
            plt.ylabel("Requests/second")
            plt.title("Configured versus Achieved Request Rate")
            plt.legend()
            plt.tight_layout()
            plt.savefig(CHART_DIR / "v4_1_configured_vs_achieved_rate.png", dpi=180)
            plt.close()

    recovery = runs[runs["test_case_id"].isin(["TC06", "TC07"])].dropna(subset=["recovery_completeness_percent"])
    if not recovery.empty:
        plot = recovery.groupby("test_case_id", as_index=False).agg(
            recovery_completeness_percent=("recovery_completeness_percent", "mean"),
            replica_convergence_percent=("replica_convergence_percent", "mean"),
        )
        x = range(len(plot))
        width = 0.38
        plt.figure(figsize=(9, 5.5))
        plt.bar([i - width / 2 for i in x], plot["recovery_completeness_percent"], width=width, label="Recovery completeness")
        plt.bar([i + width / 2 for i in x], plot["replica_convergence_percent"], width=width, label="Replica convergence")
        plt.xticks(list(x), plot["test_case_id"])
        plt.ylim(0, 105)
        plt.ylabel("Percent")
        plt.title("Post-Recovery Completeness and Convergence")
        plt.legend()
        plt.tight_layout()
        plt.savefig(CHART_DIR / "v4_1_recovery_completeness_convergence.png", dpi=180)
        plt.close()

    if not lifecycle.empty and {"event_number", "to_state", "experiment_id"}.issubset(lifecycle.columns):
        latest_id = lifecycle["experiment_id"].dropna().astype(str).iloc[-1] if lifecycle["experiment_id"].notna().any() else None
        if latest_id:
            cycle = lifecycle[lifecycle["experiment_id"].astype(str) == latest_id].copy()
            if not cycle.empty:
                cycle["event_number"] = pd.to_numeric(cycle["event_number"], errors="coerce")
                cycle = cycle.dropna(subset=["event_number"]).sort_values("event_number")
                states = list(dict.fromkeys(cycle["to_state"].astype(str).tolist()))
                mapping = {state: idx for idx, state in enumerate(states)}
                y = [mapping[state] for state in cycle["to_state"].astype(str)]
                plt.figure(figsize=(11, 5))
                plt.step(cycle["event_number"], y, where="post")
                plt.scatter(cycle["event_number"], y)
                plt.yticks(list(mapping.values()), list(mapping.keys()))
                plt.xlabel("Lifecycle event")
                plt.ylabel("Operating state")
                plt.title("Latest TC07 Recovery Lifecycle")
                plt.tight_layout()
                plt.savefig(CHART_DIR / "v4_1_tc07_lifecycle_states.png", dpi=180)
                plt.close()


def write_quality_report(runs: pd.DataFrame, events: pd.DataFrame, inventory: pd.DataFrame) -> None:
    report = {
        "version": "4.1",
        "run_rows": int(len(runs)),
        "request_event_rows": int(len(events)),
        "run_rows_with_quality_flags": int(runs["data_quality_issue"].ne("").sum()),
        "duplicate_database_client_order_ids": int(runs["duplicate_client_order_ids"].fillna(0).max()) if not runs.empty else 0,
        "global_inventory_rows": int(len(inventory)),
        "note": "Global inventory is diagnostic only; research recovery metrics are batch/experiment scoped.",
    }
    (SUMMARY_DIR / "v4_1_data_quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")


def main() -> None:
    runs = clean_runs()
    events = clean_requests()
    snapshots = read_csv_optional(SNAPSHOT_FILE)
    sync_events = read_csv_optional(SYNC_FILE)
    lifecycle = read_csv_optional(LIFECYCLE_FILE)

    runs.to_csv(CLEAN_DIR / "v4_1_clean_experiment_results.csv", index=False)
    if not events.empty:
        events.to_csv(CLEAN_DIR / "v4_1_clean_request_event_log.csv", index=False)
    if not snapshots.empty:
        snapshots.to_csv(CLEAN_DIR / "v4_1_clean_database_snapshots.csv", index=False)
    if not sync_events.empty:
        sync_events.to_csv(CLEAN_DIR / "v4_1_clean_sync_event_log.csv", index=False)
    if not lifecycle.empty:
        lifecycle.to_csv(CLEAN_DIR / "v4_1_clean_lifecycle_event_log.csv", index=False)

    cloud = export_database(CLOUD_DB, "cloud")
    edge = export_database(EDGE_DB, "edge")
    inventory = build_global_reconciliation(cloud, edge)

    run_summary = build_run_summary(runs)
    recovery_summary = build_recovery_summary(runs)
    request_summary = build_request_summary(events)
    if not run_summary.empty:
        run_summary.to_csv(SUMMARY_DIR / "v4_1_summary_by_scenario.csv", index=False)
    if not recovery_summary.empty:
        recovery_summary.to_csv(SUMMARY_DIR / "v4_1_recovery_summary.csv", index=False)
    if not request_summary.empty:
        request_summary.to_csv(SUMMARY_DIR / "v4_1_request_latency_summary.csv", index=False)

    save_charts(run_summary, runs, lifecycle)
    write_quality_report(runs, events, inventory)

    print("V4.1 analysis complete.")
    print(f"Clean run results: {CLEAN_DIR / 'v4_1_clean_experiment_results.csv'}")
    print(f"Scenario summary: {SUMMARY_DIR / 'v4_1_summary_by_scenario.csv'}")
    print(f"Recovery summary: {SUMMARY_DIR / 'v4_1_recovery_summary.csv'}")
    print(f"Data-quality report: {SUMMARY_DIR / 'v4_1_data_quality_report.json'}")
    print(f"Charts: {CHART_DIR}")


if __name__ == "__main__":
    main()
