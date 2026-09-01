"""Research-quality post-experiment evaluation for V4.1.

This script is analysis-only. It never writes to raw experiment files or databases.
It intentionally separates the primary comparative experiment (TC01-TC04) from
TC07 lifecycle validation, and produces dissertation-oriented tables and charts.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

BASE = Path(__file__).resolve().parent
RESULT_ROOT = BASE / "v4_1_results"
RAW_DIR = RESULT_ROOT / "raw"
OUT_DIR = RESULT_ROOT / "research_evaluation"
TABLE_DIR = OUT_DIR / "tables"
CHART_DIR = OUT_DIR / "charts"
for d in (OUT_DIR, TABLE_DIR, CHART_DIR):
    d.mkdir(parents=True, exist_ok=True)

RUN_FILE = RAW_DIR / "v4_1_raw_experiment_results.csv"
REQUEST_FILE = RAW_DIR / "v4_1_request_event_log.csv"
LIFECYCLE_FILE = RAW_DIR / "v4_1_lifecycle_event_log.csv"

PRIMARY = ["TC01", "TC02", "TC03", "TC04"]
LABELS = {
    "TC01": "Normal operation",
    "TC02": "Cloud outage + edge assistance",
    "TC03": "Cloud outage, no failover",
    "TC04": "Complete service failure",
    "TC05": "Pre-recovery assessment",
    "TC06": "Recovery synchronisation",
    "TC07": "End-to-end lifecycle",
}

NUMERIC_RUN = [
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

NUMERIC_REQUEST = [
    "run_id", "request_number", "request_interval_seconds", "cloud_http_status", "cloud_latency_seconds",
    "cloud_failure_detection_seconds", "edge_http_status", "edge_latency_seconds",
    "observed_failover_recovery_seconds", "total_request_latency_seconds",
]

# 95% two-sided Student t critical values, df 1..30. For larger df, normal approximation.
T95 = {
    1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571, 6: 2.447, 7: 2.365, 8: 2.306,
    9: 2.262, 10: 2.228, 11: 2.201, 12: 2.179, 13: 2.160, 14: 2.145, 15: 2.131,
    16: 2.120, 17: 2.110, 18: 2.101, 19: 2.093, 20: 2.086, 21: 2.080, 22: 2.074,
    23: 2.069, 24: 2.064, 25: 2.060, 26: 2.056, 27: 2.052, 28: 2.048, 29: 2.045, 30: 2.042,
}


def _read(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def load_runs() -> pd.DataFrame:
    if not RUN_FILE.exists():
        raise FileNotFoundError(f"Run-level results not found: {RUN_FILE}. Complete the formal experiment first.")
    df = pd.read_csv(RUN_FILE)
    for c in NUMERIC_RUN:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["scenario_label"] = df["test_case_id"].map(LABELS).fillna(df.get("scenario"))
    return df


def load_requests() -> pd.DataFrame:
    df = _read(REQUEST_FILE)
    if df.empty:
        return df
    for c in NUMERIC_REQUEST:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("final_success", "cloud_failure_detected", "failover_triggered"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.lower().map({"true": True, "false": False})
    if "event_timestamp" in df.columns:
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce")
    df["scenario_label"] = df["test_case_id"].map(LABELS).fillna(df.get("scenario"))
    return df


def ci95(series: pd.Series) -> tuple[float | None, float | None]:
    x = pd.to_numeric(series, errors="coerce").dropna()
    n = len(x)
    if n < 2:
        return (None, None)
    mean = float(x.mean())
    sd = float(x.std(ddof=1))
    tcrit = T95.get(n - 1, 1.96)
    half = tcrit * sd / math.sqrt(n)
    return (mean - half, mean + half)


def describe_metric(group: pd.DataFrame, column: str, prefix: str) -> dict:
    x = pd.to_numeric(group[column], errors="coerce").dropna() if column in group else pd.Series(dtype=float)
    if x.empty:
        return {
            f"{prefix}_n": 0, f"{prefix}_mean": None, f"{prefix}_sd": None, f"{prefix}_median": None,
            f"{prefix}_min": None, f"{prefix}_max": None, f"{prefix}_ci95_low": None, f"{prefix}_ci95_high": None,
        }
    low, high = ci95(x)
    return {
        f"{prefix}_n": int(len(x)),
        f"{prefix}_mean": float(x.mean()),
        f"{prefix}_sd": float(x.std(ddof=1)) if len(x) > 1 else 0.0,
        f"{prefix}_median": float(x.median()),
        f"{prefix}_min": float(x.min()),
        f"{prefix}_max": float(x.max()),
        f"{prefix}_ci95_low": low,
        f"{prefix}_ci95_high": high,
    }


def primary_summary(runs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    primary = runs[runs["test_case_id"].isin(PRIMARY)].copy()
    for tc in PRIMARY:
        g = primary[primary["test_case_id"] == tc].copy()
        if g.empty:
            continue
        row = {
            "test_case_id": tc,
            "scenario": LABELS[tc],
            "runs": int(len(g)),
            "requests_total": int(g["total_requests"].fillna(0).sum()),
            "successful_requests": int(g["successful_requests"].fillna(0).sum()),
            "failed_requests": int(g["failed_requests"].fillna(0).sum()),
            "cloud_success": int(g["cloud_success"].fillna(0).sum()),
            "edge_success": int(g["edge_success"].fillna(0).sum()),
        }
        row.update(describe_metric(g, "availability_percent", "availability_pct"))
        row.update(describe_metric(g, "achieved_requests_per_second", "achieved_rate_rps"))
        row.update(describe_metric(g, "mean_total_request_latency_seconds", "mean_request_latency_s"))
        row.update(describe_metric(g, "p95_total_request_latency_seconds", "run_p95_latency_s"))
        row.update(describe_metric(g, "edge_process_rss_mb", "edge_rss_mb"))
        rows.append(row)
    return pd.DataFrame(rows)


def tc02_failover_summary(runs: pd.DataFrame) -> pd.DataFrame:
    g = runs[runs["test_case_id"] == "TC02"].copy().sort_values("run_id")
    cols = [c for c in [
        "run_id", "total_requests", "failover_transition_count", "cloud_failure_detection_seconds",
        "observed_failover_recovery_seconds", "mean_total_request_latency_seconds", "p95_total_request_latency_seconds",
        "availability_percent", "achieved_requests_per_second", "rpo_related_exposure_records",
        "edge_process_rss_mb", "edge_db_size_bytes",
    ] if c in g.columns]
    return g[cols]


def tc02_descriptive(runs: pd.DataFrame) -> pd.DataFrame:
    g = runs[runs["test_case_id"] == "TC02"].copy()
    rows = []
    for col, label in [
        ("cloud_failure_detection_seconds", "Cloud failure detection"),
        ("observed_failover_recovery_seconds", "Observed failover recovery"),
        ("mean_total_request_latency_seconds", "Mean request latency"),
        ("achieved_requests_per_second", "Achieved request rate"),
    ]:
        if col not in g.columns:
            continue
        stats = describe_metric(g, col, "x")
        rows.append({
            "metric": label,
            "n": stats["x_n"], "mean": stats["x_mean"], "sd": stats["x_sd"],
            "median": stats["x_median"], "min": stats["x_min"], "max": stats["x_max"],
            "ci95_low": stats["x_ci95_low"], "ci95_high": stats["x_ci95_high"],
        })
    return pd.DataFrame(rows)


def recovery_table(runs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    tc05 = runs[runs["test_case_id"] == "TC05"].copy()
    tc06 = runs[runs["test_case_id"] == "TC06"].copy()
    if not tc05.empty:
        r = tc05.iloc[-1]
        rows.append({
            "stage": "TC05 pre-recovery assessment",
            "recovery_scope_records": r.get("recovery_scope_records"),
            "rpo_related_exposure_records": r.get("rpo_related_exposure_records"),
            "pending_edge_sync_count": r.get("pending_edge_sync_count"),
            "recovery_completeness_percent": r.get("recovery_completeness_percent"),
            "replica_convergence_percent": r.get("replica_convergence_percent"),
            "records_synced": 0,
            "sync_duration_seconds": None,
            "missing_from_cloud_count": r.get("missing_from_cloud_count"),
            "duplicate_client_order_ids": r.get("duplicate_client_order_ids"),
        })
    if not tc06.empty:
        r = tc06.iloc[-1]
        rows.append({
            "stage": "TC06 post-recovery synchronisation",
            "recovery_scope_records": r.get("recovery_scope_records"),
            "rpo_related_exposure_records": r.get("post_recovery_rpo_exposure_records") if pd.notna(r.get("post_recovery_rpo_exposure_records")) else r.get("rpo_related_exposure_records"),
            "pending_edge_sync_count": r.get("pending_edge_sync_count"),
            "recovery_completeness_percent": r.get("recovery_completeness_percent"),
            "replica_convergence_percent": r.get("replica_convergence_percent"),
            "records_synced": r.get("records_synced_back_to_cloud"),
            "sync_duration_seconds": r.get("recovery_sync_time_seconds"),
            "missing_from_cloud_count": r.get("missing_from_cloud_count"),
            "duplicate_client_order_ids": r.get("duplicate_client_order_ids"),
        })
    return pd.DataFrame(rows)


def tc07_table(runs: pd.DataFrame) -> pd.DataFrame:
    g = runs[runs["test_case_id"] == "TC07"].copy().sort_values("run_id")
    cols = [c for c in [
        "run_id", "total_requests", "availability_percent", "failover_transition_count",
        "observed_failover_recovery_seconds", "cloud_failure_detection_seconds",
        "pre_recovery_rpo_exposure_records", "post_recovery_rpo_exposure_records",
        "records_synced_back_to_cloud", "recovery_sync_time_seconds", "recovery_completeness_percent",
        "replica_convergence_percent", "missing_from_cloud_count", "duplicate_client_order_ids",
        "mean_total_request_latency_seconds", "edge_process_rss_mb",
    ] if c in g.columns]
    return g[cols]


def request_phase_summary(events: pd.DataFrame) -> pd.DataFrame:
    if events.empty:
        return pd.DataFrame()
    use = events[events["test_case_id"].isin(PRIMARY + ["TC07"])].copy()
    rows = []
    for (tc, phase), g in use.groupby(["test_case_id", "phase"], dropna=False):
        latency = pd.to_numeric(g.get("total_request_latency_seconds"), errors="coerce").dropna()
        rows.append({
            "test_case_id": tc,
            "scenario": LABELS.get(tc, tc),
            "phase": phase,
            "request_events": int(len(g)),
            "successful_events": int(g["final_success"].fillna(False).sum()) if "final_success" in g else None,
            "mean_total_latency_seconds": float(latency.mean()) if not latency.empty else None,
            "median_total_latency_seconds": float(latency.median()) if not latency.empty else None,
            "p95_total_latency_seconds": float(latency.quantile(0.95)) if not latency.empty else None,
            "mean_cloud_latency_seconds": pd.to_numeric(g.get("cloud_latency_seconds"), errors="coerce").mean(),
            "mean_edge_latency_seconds": pd.to_numeric(g.get("edge_latency_seconds"), errors="coerce").mean(),
        })
    return pd.DataFrame(rows)


def protocol_checks(runs: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    checks = []
    def add(check, status, observed, expected, note=""):
        checks.append({"check": check, "status": status, "observed": observed, "expected": expected, "note": note})

    for tc in PRIMARY:
        g = runs[runs["test_case_id"] == tc]
        add(f"{tc} run count", "PASS" if len(g) == 10 else "WARN", len(g), 10)
        if not g.empty:
            reqs = sorted(set(pd.to_numeric(g["total_requests"], errors="coerce").dropna().astype(int)))
            add(f"{tc} requests per run consistent", "PASS" if len(reqs) == 1 else "WARN", str(reqs), "one fixed value")
            ints = sorted(set(pd.to_numeric(g["request_interval_seconds"], errors="coerce").dropna()))
            add(f"{tc} interval consistent", "PASS" if len(ints) == 1 else "WARN", str(ints), "one fixed value")

    tc02 = runs[runs["test_case_id"] == "TC02"]
    if not tc02.empty:
        vals = pd.to_numeric(tc02["failover_transition_count"], errors="coerce")
        add("TC02 exactly one failover transition per run", "PASS" if vals.eq(1).all() else "WARN", vals.tolist(), "all 1")

    for tc in ["TC01", "TC03", "TC04"]:
        g = runs[runs["test_case_id"] == tc]
        if not g.empty:
            vals = pd.to_numeric(g["failover_transition_count"], errors="coerce").fillna(0)
            add(f"{tc} no failover transition", "PASS" if vals.eq(0).all() else "WARN", vals.tolist(), "all 0")

    for tc in ["TC05", "TC06"]:
        n = len(runs[runs["test_case_id"] == tc])
        add(f"{tc} execution count", "PASS" if n == 1 else "WARN", n, 1)

    tc07 = runs[runs["test_case_id"] == "TC07"]
    if not tc07.empty:
        add("TC07 run count", "PASS" if len(tc07) == 10 else "WARN", len(tc07), 10)
        vals = pd.to_numeric(tc07["failover_transition_count"], errors="coerce")
        add("TC07 one failover transition per lifecycle", "PASS" if vals.eq(1).all() else "WARN", vals.tolist(), "all 1")

    dup = pd.to_numeric(runs.get("duplicate_client_order_ids"), errors="coerce").dropna()
    if not dup.empty:
        add("No duplicate client_order_id detected", "PASS" if dup.max() == 0 else "WARN", float(dup.max()), 0)

    if not events.empty:
        add("Request event rows present", "PASS", len(events), "matches formal workload subject to recorded failures")
    return pd.DataFrame(checks)


def chart_primary_availability(summary: pd.DataFrame):
    if summary.empty: return
    p = summary.dropna(subset=["availability_pct_mean"])
    if p.empty: return
    plt.figure(figsize=(9.5, 5.6))
    plt.bar(p["test_case_id"], p["availability_pct_mean"])
    plt.ylim(0, 105)
    plt.xlabel("Primary scenario")
    plt.ylabel("Mean transaction availability (%)")
    plt.title("Primary Experiment: Transaction Availability")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "01_primary_transaction_availability.png", dpi=220)
    plt.close()


def chart_primary_rate(runs: pd.DataFrame):
    g = runs[runs["test_case_id"].isin(PRIMARY)].copy()
    if g.empty: return
    plt.figure(figsize=(9.5, 5.6))
    positions = list(range(1, 5))
    data = [pd.to_numeric(g[g["test_case_id"] == tc]["achieved_requests_per_second"], errors="coerce").dropna() for tc in PRIMARY]
    if not any(len(x) for x in data): return
    plt.boxplot(data, tick_labels=PRIMARY, showmeans=True)
    configured = pd.to_numeric(g["configured_requests_per_second"], errors="coerce").dropna()
    if not configured.empty:
        plt.axhline(configured.median(), linestyle="--", label="Nominal interval-derived rate")
        plt.legend()
    plt.xlabel("Primary scenario")
    plt.ylabel("Achieved requests/second")
    plt.title("Primary Experiment: Achieved Request Rate Across Runs")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "02_primary_achieved_request_rate.png", dpi=220)
    plt.close()


def chart_primary_latency(runs: pd.DataFrame):
    g = runs[runs["test_case_id"].isin(PRIMARY)].copy()
    if g.empty: return
    data = [pd.to_numeric(g[g["test_case_id"] == tc]["mean_total_request_latency_seconds"], errors="coerce").dropna() for tc in PRIMARY]
    if not any(len(x) for x in data): return
    plt.figure(figsize=(9.5, 5.6))
    plt.boxplot(data, tick_labels=PRIMARY, showmeans=True)
    plt.xlabel("Primary scenario")
    plt.ylabel("Run mean request latency (s)")
    plt.title("Primary Experiment: Request Latency Across Runs")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "03_primary_request_latency.png", dpi=220)
    plt.close()


def chart_tc02_failover(runs: pd.DataFrame):
    g = runs[runs["test_case_id"] == "TC02"].copy().sort_values("run_id")
    if g.empty: return
    y = pd.to_numeric(g["observed_failover_recovery_seconds"], errors="coerce")
    valid = g[y.notna()].copy()
    y = y[y.notna()]
    if y.empty: return
    mean = float(y.mean())
    low, high = ci95(y)
    plt.figure(figsize=(9.5, 5.6))
    plt.plot(valid["run_id"], y, marker="o", label="Observed per run")
    plt.axhline(mean, linestyle="--", label=f"Mean = {mean:.4f} s")
    if low is not None and high is not None:
        plt.axhspan(low, high, alpha=0.15, label="95% CI of mean")
    plt.xticks(valid["run_id"].astype(int))
    plt.xlabel("TC02 run")
    plt.ylabel("Observed failover recovery time (s)")
    plt.title("TC02: Observed Failover Recovery Across Repeated Runs")
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART_DIR / "04_tc02_failover_recovery_distribution.png", dpi=220)
    plt.close()


def chart_recovery_exposure(recovery: pd.DataFrame):
    if recovery.empty or len(recovery) < 1: return
    p = recovery.dropna(subset=["rpo_related_exposure_records"])
    if p.empty: return
    plt.figure(figsize=(8.5, 5.4))
    plt.bar(p["stage"], p["rpo_related_exposure_records"])
    plt.ylabel("RPO-related exposure (records)")
    plt.title("Scoped Recovery: Exposure Before and After Synchronisation")
    plt.xticks(rotation=15, ha="right")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "05_recovery_exposure_before_after.png", dpi=220)
    plt.close()


def chart_recovery_quality(recovery: pd.DataFrame):
    if recovery.empty: return
    p = recovery.dropna(subset=["recovery_completeness_percent", "replica_convergence_percent"], how="all")
    if p.empty: return
    x = range(len(p)); width = 0.36
    plt.figure(figsize=(8.5, 5.4))
    plt.bar([i - width/2 for i in x], p["recovery_completeness_percent"], width=width, label="Recovery completeness")
    plt.bar([i + width/2 for i in x], p["replica_convergence_percent"], width=width, label="Replica convergence")
    plt.xticks(list(x), p["stage"], rotation=15, ha="right")
    plt.ylim(0, 105)
    plt.ylabel("Percent")
    plt.title("Scoped Recovery: Completeness and Convergence")
    plt.legend()
    plt.tight_layout()
    plt.savefig(CHART_DIR / "06_recovery_completeness_convergence.png", dpi=220)
    plt.close()


def chart_tc07_validation(tc07: pd.DataFrame):
    if tc07.empty: return
    g = tc07.sort_values("run_id")
    if {"pre_recovery_rpo_exposure_records", "post_recovery_rpo_exposure_records"}.issubset(g.columns):
        plt.figure(figsize=(9.5, 5.6))
        plt.plot(g["run_id"], g["pre_recovery_rpo_exposure_records"], marker="o", label="Pre-recovery exposure")
        plt.plot(g["run_id"], g["post_recovery_rpo_exposure_records"], marker="o", label="Post-recovery exposure")
        plt.xticks(g["run_id"].astype(int))
        plt.xlabel("TC07 lifecycle repetition")
        plt.ylabel("RPO-related exposure (records)")
        plt.title("TC07: Recovery Exposure Across Lifecycle Repetitions")
        plt.legend()
        plt.tight_layout()
        plt.savefig(CHART_DIR / "07_tc07_recovery_exposure_by_run.png", dpi=220)
        plt.close()

    if {"recovery_completeness_percent", "replica_convergence_percent"}.issubset(g.columns):
        plt.figure(figsize=(9.5, 5.6))
        plt.plot(g["run_id"], g["recovery_completeness_percent"], marker="o", label="Recovery completeness")
        plt.plot(g["run_id"], g["replica_convergence_percent"], marker="o", label="Replica convergence")
        plt.xticks(g["run_id"].astype(int))
        plt.ylim(0, 105)
        plt.xlabel("TC07 lifecycle repetition")
        plt.ylabel("Percent")
        plt.title("TC07: Recovery Verification Across Lifecycle Repetitions")
        plt.legend()
        plt.tight_layout()
        plt.savefig(CHART_DIR / "08_tc07_recovery_quality_by_run.png", dpi=220)
        plt.close()


def chart_resource_observation(runs: pd.DataFrame):
    use = runs[runs["test_case_id"].isin(PRIMARY + ["TC07"])].copy()
    if use.empty: return
    rows=[]
    for tc, g in use.groupby("test_case_id"):
        x=pd.to_numeric(g["edge_process_rss_mb"], errors="coerce").dropna()
        if not x.empty: rows.append((tc,float(x.mean())))
    if not rows: return
    p=pd.DataFrame(rows, columns=["test_case_id","mean_edge_rss_mb"])
    order=[tc for tc in PRIMARY+["TC07"] if tc in set(p["test_case_id"])]
    p=p.set_index("test_case_id").loc[order].reset_index()
    plt.figure(figsize=(9.5,5.6))
    plt.bar(p["test_case_id"], p["mean_edge_rss_mb"])
    plt.xlabel("Scenario")
    plt.ylabel("Observed edge process RSS (MB)")
    plt.title("Descriptive Edge Memory Observation")
    plt.tight_layout()
    plt.savefig(CHART_DIR / "09_edge_memory_observation.png", dpi=220)
    plt.close()


def main():
    runs = load_runs()
    events = load_requests()

    primary = primary_summary(runs)
    tc02runs = tc02_failover_summary(runs)
    tc02stats = tc02_descriptive(runs)
    recovery = recovery_table(runs)
    tc07 = tc07_table(runs)
    phase = request_phase_summary(events)
    checks = protocol_checks(runs, events)

    for name, df in [
        ("01_primary_comparative_summary.csv", primary),
        ("02_tc02_failover_runs.csv", tc02runs),
        ("03_tc02_descriptive_statistics.csv", tc02stats),
        ("04_recovery_progression.csv", recovery),
        ("05_tc07_lifecycle_validation.csv", tc07),
        ("06_request_phase_latency_summary.csv", phase),
        ("07_protocol_and_data_quality_checks.csv", checks),
    ]:
        if not df.empty:
            df.to_csv(TABLE_DIR / name, index=False)

    chart_primary_availability(primary)
    chart_primary_rate(runs)
    chart_primary_latency(runs)
    chart_tc02_failover(runs)
    chart_recovery_exposure(recovery)
    chart_recovery_quality(recovery)
    chart_tc07_validation(tc07)
    chart_resource_observation(runs)

    meta = {
        "analysis_version": "V4.1 research evaluation supplement 1.0",
        "primary_comparison": PRIMARY,
        "tc07_treatment": "analysed separately as lifecycle validation",
        "confidence_interval": "95% Student t interval for run-level continuous metrics where n >= 2",
        "statistical_unit": "experimental run; requests are transaction-level observations",
        "notes": [
            "No inferential significance test is forced onto the localhost experiment.",
            "CPU snapshot values remain descriptive and are not interpreted as workload-average CPU utilisation.",
            "Configured request rate is treated as nominal interval-derived rate; achieved rate uses actual workload duration.",
        ],
    }
    (OUT_DIR / "research_analysis_metadata.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("Research evaluation analysis complete.")
    print(f"Tables: {TABLE_DIR}")
    print(f"Charts: {CHART_DIR}")
    print(f"Checks: {TABLE_DIR / '07_protocol_and_data_quality_checks.csv'}")


if __name__ == "__main__":
    main()
