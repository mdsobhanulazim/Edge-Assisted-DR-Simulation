# Lightweight Edge-Assisted Disaster Recovery Model — V4.1 Research Version

This package is the **V4.1 research prototype** for a controlled MSc dissertation study of lightweight edge-assisted disaster recovery for a cloud-dependent SME-oriented ordering service.

It is not intended to be a production disaster-recovery product. It is an experimental instrument for testing whether a small local edge service can preserve selected transactions during temporary cloud unavailability and recover them to the primary cloud after restoration.

## Requirements

- Windows 10 or Windows 11
- Python 3
- `pip`
- Modern browser

Install the Python dependencies by running:

```powershell
.\01_install_requirements.bat
```

The package installs:

- Flask
- Requests
- Pandas
- Matplotlib
- psutil

---

## Start the dashboard

Run:

```powershell
.\02_start_v4_1_dashboard.bat
```

The dashboard opens at:

```text
http://127.0.0.1:5050
```

Keep the dashboard command window open while using the prototype.

The local services are:

| Component | Address | Role |
|---|---|---|
| Dashboard | `127.0.0.1:5050` | Experiment control and evidence views |
| Cloud service | `127.0.0.1:5000` | Simulated primary ordering service |
| Edge service | `127.0.0.1:5001` | Simulated local continuity service |

---

## Test cases

| Test case | Purpose | Formal initial state / behaviour |
|---|---|---|
| **TC01** | Healthy baseline | Cloud online, edge online; transactions completed by cloud |
| **TC02** | Edge-assisted outage | Cloud offline, edge online; first failed cloud attempt triggers `NORMAL → EDGE_DEGRADED` |
| **TC03** | Cloud-only outage baseline | Cloud offline; failover disabled |
| **TC04** | Complete service failure | Cloud offline, edge offline |
| **TC05** | Scoped recovery-state check | Measures latest edge-assisted batch before/after recovery without global-history contamination |
| **TC06** | Scoped recovery synchronization | Synchronizes pending transactions from the latest edge-assisted batch to restored cloud |
| **TC07** | End-to-end recovery lifecycle | Automated healthy operation, cloud failure, edge operation, restoration, recovery sync and healthy verification |

### TC07 note

For TC07, the dashboard field **Requests/phase** is used for each of three request phases:

1. normal baseline;
2. cloud-outage / edge-degraded operation;
3. post-recovery verification.

For example, `10 requests/phase` creates `30 request attempts per TC07 lifecycle run`.

---

## V4.1 measures

### Transaction availability

```text
successful requests / total requests × 100
```

A request is successful when the intended business transaction is accepted by either the primary cloud service or the permitted edge continuity path.

### Observed failover recovery time

For the first successful transition from normal cloud operation to edge-degraded operation:

```text
first successful edge completion time − start of failed cloud attempt
```

This is a **technical observed failover indicator**, not an organizational Recovery Time Objective (RTO).

### Cloud failure detection time

The elapsed duration of the cloud request attempt that identifies the cloud as unavailable.

### RPO-related exposure

```text
accepted outage records not yet represented in the primary cloud
```

This is a record-count indicator related to recoverability. It is **not a conventional time-based organizational RPO**.

### Recovery completeness

```text
accepted outage transactions present in restored cloud
------------------------------------------------------- × 100
all accepted outage transactions in the selected scope
```

### Replica convergence

```text
selected recovery-scope records present in both cloud and edge
and no longer marked pending
--------------------------------------------------------------- × 100
all records in the selected recovery scope
```

Recovery completeness answers: **Did the cloud recover every transaction accepted during the outage?**

Replica convergence answers: **Has the cloud-edge recovery scope returned to the intended synchronized state?**

### Configured and achieved request rate

The configured rate is `1 / request interval`. The achieved rate is calculated from the actual workload duration so failover and processing overhead are visible rather than hidden.

### Lightweight resource observations

V4.1 records:

- cloud process RSS memory;
- edge process RSS memory;
- CPU percentage snapshot;
- cloud database size;
- edge database size.

These values describe this local prototype environment only and should not be interpreted as production capacity measurements.

---

## Recommended formal experiment

Read `V4_1_RESEARCH_PROTOCOL.md` before collecting final dissertation evidence.

A suggested controlled batch matching the current methodology is:

- TC01–TC04: **30 requests per run**, **0.2 s configured interval**, **10 repetitions**;
- TC05: run after the completed TC02 batch to record its scoped pre-recovery state;
- TC06: restore cloud and recover the same scoped TC02 batch;
- TC07: run separately as an end-to-end lifecycle validation, using a clearly reported requests-per-phase value and repetition count.

Do **not** manually start or stop a service during a formal TC01–TC04 run. Manual service controls are retained for demonstration and troubleshooting, but the final controlled dataset should use automatic preparation.

---

## Evidence files

V4.1 writes evidence under:

```text
v4_1_results/
```

Raw evidence:

```text
v4_1_results/raw/v4_1_raw_experiment_results.csv
v4_1_results/raw/v4_1_request_event_log.csv
v4_1_results/raw/v4_1_database_snapshots.csv
v4_1_results/raw/v4_1_sync_event_log.csv
v4_1_results/raw/v4_1_lifecycle_event_log.csv
```

After selecting **Generate V4.1 results and charts**, the analysis script creates:

```text
v4_1_results/clean/
v4_1_results/summaries/
v4_1_results/charts/
v4_1_results/database_exports/
```

The global record inventory in `database_exports` is a **diagnostic inventory only**. Formal recovery completeness and convergence results are calculated from a selected batch/experiment scope.

---

## Clean experiment and archiving

Use the dashboard **Archive current evidence** button before resetting a valuable batch.

For a fresh final experiment:

1. archive any existing evidence;
2. select **Reset databases and all result files**;
3. allow cloud and edge to restart;
4. verify both services are online;
5. begin the documented test protocol.

You can also run:

```powershell
python reset_v4_1_databases.py
```

Then restart the services or the dashboard.


## Research interpretation

The prototype can support evidence about:

- whether local edge capability preserves selected transactions during temporary cloud unavailability;
- the client-visible cost of the first failover transition;
- the number of accepted transactions temporarily existing outside the cloud;
- whether those records are recoverable after cloud restoration;
- whether the selected cloud-edge scope converges after recovery;
- the observed resource footprint of the lightweight local service.

It does **not** establish production readiness, universal SME suitability, commercial cost effectiveness, enterprise-scale capacity, cybersecurity adequacy, or organizational RTO/RPO targets.
