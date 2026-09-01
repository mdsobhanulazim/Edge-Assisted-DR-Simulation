# V4.1 Final Research Protocol

This protocol is intended to keep the dissertation experiment reproducible and separate formal evidence from informal dashboard demonstrations.

## A. Before the final batch

1. Confirm `01_install_requirements.bat` has completed successfully.
2. Start the V4.1 dashboard `02_start_v4_1_dashboard`.
3. Archive any previous evidence that must be retained.
4. Reset the databases and `v4_1_results`.
5. Keep **Automatically prepare the required cloud/edge state** enabled.
6. Do not manually change cloud or edge state while TC01–TC04 are running.

## B. Controlled comparison: TC01–TC04

Use the same workload configuration for all four scenarios unless the dissertation explicitly documents an additional experiment:

Example:
- requests per run: 30
- configured request interval: 0.2 seconds
- repetitions: 10

Run:

1. TC01 Normal operation;
2. TC02 Cloud failure with stateful edge failover;
3. TC03 Cloud failure without failover;
4. TC04 Complete service failure.

The dashboard automatically prepares the service topology before each test case.

## C. Scoped recovery evidence

After TC02 has completed, the entire TC02 repetition set shares a batch identifier.

1. Run **TC05** to capture the latest edge-assisted batch's pre-recovery state.
2. Run **TC06**. The dashboard starts the restored cloud and synchronizes only the selected recovery scope.
3. Confirm that the resulting row reports recovery completeness, replica convergence, post-recovery RPO-related exposure and the number of synchronized records.

Do not interpret the global cloud/edge database totals as the recovery consistency percentage. They may legitimately contain records from other test cases.

## D. TC07 lifecycle validation

Run TC07 separately from the TC01–TC04 comparison. Choose and report a fixed:

Example:
- requests-per-phase value: **10**
- interval: **0.2 seconds**
- repetition count: **10**

TC07 automatically performs:

1. NORMAL baseline;
2. cloud failure injection;
3. first-request failure detection and edge failover;
4. EDGE_DEGRADED transaction processing;
5. pre-recovery exposure measurement;
6. cloud restoration;
7. edge-to-cloud synchronization;
8. recovery completeness and convergence verification;
9. post-recovery NORMAL cloud requests.

No manual service intervention should occur inside a formal TC07 run.

## E. Generate analysis evidence

Select **Generate V4.1 results and charts** after the final experiment.

Preserve:

- raw CSV logs;
- cleaned datasets;
- scenario summary;
- recovery summary;
- generated charts;
- database exports;
- data-quality report.

Archive the final evidence package before further experimentation.

## F. Interpretation boundaries

- **observed failover recovery time**, not measured organizational RTO;
- **RPO-related exposure in records**, not time-based organizational RPO;
- **recovery completeness** for whether accepted outage records reached the restored cloud;
- **replica convergence** for whether the selected recovery scope is represented in both nodes and no longer pending;
- **observed resource footprint** for lightweight evidence, not enterprise capacity benchmarking.
