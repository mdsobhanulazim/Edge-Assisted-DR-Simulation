# A Lightweight Edge-Assisted Disaster Recovery Simulation Tool

A lightweight experimental prototype developed to evaluate edge-assisted disaster recovery for a cloud-based ordering service.

The prototype simulates a primary cloud service and a secondary edge service. It supports controlled service failures, edge failover, data consistency checking, recovery synchronization, availability measurement, recovery lifecycle, Recovery Time Objective (RTO) measurement and record-based Recovery Point Objective (RPO) assessment.

This project was developed as part of the **UFCF9Y-60-M - Csct masters project 25sep_1 ft** at **UWE Bristol**.

Evidence test run used in research is under:
v4_1_results/
dashboard_logs/
**archives/v4_1_evidence_20260822_184846.zip**

---

## Main Features

- Local cloud ordering service
- Local edge ordering service
- Manual cloud and edge start/stop controls
- Automatic preparation of the server state required for each test case
- Cloud-to-edge normal data synchronization
- Edge-to-cloud recovery synchronization
- Controlled disaster recovery test scenarios
- A full recovery lifecycle testing
- Request-level and run-level experiment logging
- Availability, RTO, RPO and consistency measurement
- Automatic generation of clean datasets, summaries and charts
- Experiment evidence archiving
- Clean database and results reset

---

## System Requirements

Before running the prototype, ensure that the following are installed:

- Windows 10 or Windows 11
- Python 3
- `pip`
- A web browser such as Google Chrome, Microsoft Edge or Firefox

Python must be added to the Windows system `PATH`.

To confirm that Python is available, open PowerShell and run:

```powershell
python --version
```

You can also check `pip` using:

```powershell
python -m pip --version
```

---

## Downloading the Project

Open PowerShell and clone the repository:

```powershell
git clone https://github.com/mdsobhanulazim/Edge-Assisted-DR-Simulation.git
```

Move into the project directory:

```powershell
cd Edge-Assisted-DR-Simulation
```

Alternatively, download the repository as a ZIP file from GitHub and extract it to a local folder.

---

# Running the Prototype on Windows

## Step 1: Install the Required Python Packages

This step is normally required only when running the project for the first time.

From the project folder, run:

```powershell
.\01_install_requirements.bat
```

The script installs the Python packages listed in `requirements.txt`, including:

- Flask
- Requests
- Pandas
- Matplotlib

Wait until the following message is displayed:

```text
Requirements installed successfully.
```

Press any key to close the installation window.

---

## Step 2: Start the Dashboard

Run:

```powershell
.\02_start_v4_1_dashboard.bat
```

The dashboard should open automatically in the default web browser at:

```text
http://127.0.0.1:5050
```

Keep the PowerShell or Command Prompt window open while using the dashboard.

If the browser does not open automatically, manually enter the dashboard address in the browser.

---

## Alternative PowerShell Commands

The prototype can also be started without using the batch files.

Install the requirements:

```powershell
python -m pip install -r requirements.txt
```

Start the dashboard:

```powershell
python dashboard_app.py
```

Then open:

```text
http://127.0.0.1:5050
```

---

# Dashboard Services

The dashboard manages the following local services:

| Component | Address | Purpose |
|---|---|---|
| Dashboard | `http://127.0.0.1:5050` | Experiment control and monitoring |
| Cloud server | `http://127.0.0.1:5000` | Primary ordering service |
| Edge server | `http://127.0.0.1:5001` | Secondary failover service |

The cloud and edge services do not need to be started in separate PowerShell windows when they are managed through the dashboard.

---

# Using the Dashboard

## 1. Start or Stop the Servers

The top section of the dashboard contains separate controls for:

- **Start Cloud**
- **Stop Cloud**
- **Start Edge**
- **Stop Edge**

The cloud and edge servers can be controlled independently, including while a test workload is running.

The dashboard displays the current status of each server as online or offline.

---

## 2. Select a Test Scenario

Select a test case from the **Run a scenario** section.

The following scenarios are available:

| Test case | Scenario | Required service state |
|---|---|---|
| TC01 | Normal operation | Cloud online and edge online |
| TC02 | Cloud failure with edge failover | Cloud offline and edge online |
| TC03 | Cloud failure without failover | Cloud offline; edge not used for failover |
| TC04 | Complete service failure | Cloud offline and edge offline |
| TC05 | Data consistency and RPO check | Compares cloud and edge records |
| TC06 | Cloud recovery synchronization | Synchronizes pending edge records back to the restored cloud |
| TC07 | A complete recovery lifecycle | Normal - Edge_Degraded - Recovery - Normal |

---

## 3. Configure the Workload

Before running TC01–TC04, configure the following values:

### Repetitions

The number of times the selected test case will be repeated.

Example:

```text
10
```

A single repetition is recommended for an initial test. Multiple repetitions, such as 5 or 10, can be used for the final experiment.

### Requests per Run

The number of ordering requests generated during each repetition.

Example:

```text
30
```

### Seconds Between Requests

The delay between consecutive requests.

Example values:

| Interval | Approximate workload |
|---:|---|
| `1.0` seconds | Low workload |
| `0.2` seconds | Medium workload |
| `0.1` seconds | High workload |

An interval of `0.2` seconds represents approximately five attempted requests per second, although the achieved rate may be lower because of timeout or failover processing.

---

## 4. Choose Automatic or Manual Server Control

### Automatic preparation

Leave the following option selected:

```text
Automatically prepare the required cloud/edge state
```

The dashboard will automatically start or stop the cloud and edge servers according to the selected test case before the workload begins.

### Manual preparation

Clear the automatic preparation option to control the servers manually.

Before starting the test, use the individual Start and Stop buttons to create the required server state.

For example, for TC02:

1. Stop the cloud server.
2. Start or keep the edge server running.
3. Select TC02.
4. Run the selected scenario.

Manual server controls remain available during the test run.

---

## 5. Run the Scenario

Click:

```text
Run selected scenario
```

Wait until the dashboard job status changes from **Running** to **Completed**.

It is recommended not to start another operation while the current operation is running.

The dashboard will display:

- Successful and failed requests
- Cloud and edge request outcomes
- Availability percentage
- Mean RTO
- Cloud and edge record counts
- Record-based RPO
- Data consistency percentage
- Pending recovery records
- Request-level events
- Activity logs

---

# Suggested Experimental Sequence

A complete experiment can be conducted in the following order:

## Step 1: Archive Existing Evidence

Select:

```text
Archive current evidence
```

This creates a ZIP archive of the current databases and experiment evidence.

## Step 2: Reset the Experiment

Select:

```text
Reset databases and all result files
```

This permanently removes the existing cloud database, edge database and generated result files.

Use this operation only when beginning a new clean experiment.

## Step 3: Run TC01

Run the normal-operation scenario with both the cloud and edge services available.

## Step 4: Run TC02

Simulate a cloud outage while the edge service remains available and failover is enabled.

## Step 5: Run TC03

Simulate a cloud outage without allowing edge failover.

## Step 6: Run TC04

Simulate complete service failure by making both cloud and edge unavailable.

## Step 7: Run TC05

Select:

```text
TC05: Check RPO and consistency
```

This compares records in the cloud and edge databases using their client order identifiers.

## Step 8: Restore the Cloud and Run TC06

Start both servers and select:

```text
TC06: Restore: sync edge → cloud
```

This transfers eligible pending orders from the edge database to the restored cloud database.

## Step 9: Run a full recovery lifecycle TC07

Simulate Normal -> Edge_Degraded -> Recovery -> Normal

```text
TC07: Run TC07 Full Lifecycle
```

## Step 10: Generate Results and Charts

Select:

```text
Generate clean results and charts
```

This processes the collected experiment evidence and generates clean datasets, summaries and charts.

## Step 10: Archive the Final Evidence

After completing the final experiment, select:

```text
Archive current evidence
```

Keep the archive as a reproducible evidence package for the dissertation evaluation.

---

# Result Locations

Generated evidence is stored in the following folders:

```text
v4_1_results/
dashboard_logs/
archives/
```

The `v4_1_results` folder contains experiment datasets, request-level evidence, processed results, summaries and generated charts.

The `dashboard_logs` folder contains cloud and edge service logs.

The `archives` folder contains timestamped ZIP copies of the experiment evidence.

---

# Running the Command-Line Scenario Runner

The original command-line scenario runner is also available.

First, start the cloud server in one PowerShell window:

```powershell
python cloud_server.py
```

Start the edge server in a second PowerShell window:

```powershell
python edge_server.py
```

Open a third PowerShell window and run:

```powershell
python scenario_runner_v4_standard.py
```

Follow the displayed menu and prepare the cloud and edge server states when instructed.

The dashboard method is recommended because it provides service controls, live monitoring, results, logs and charts in one interface.

---

# Running the Analysis Separately

The result analysis script can be executed directly using:

```powershell
python analyse_v4_1_results.py
```

The analysis should normally be run after experiment results have been generated.

---

# Performing a Manual Clean Reset

Close or stop the cloud and edge servers before running:

```powershell
python reset_v4_1_databases.py
```

This deletes:

- `cloud.db`
- `edge.db`
- `metrics_log.csv`, when present
- The complete `v4_1_results` folder

After the reset, restart the cloud and edge services so that new databases can be created.

**Warning:** This operation permanently deletes the existing experiment data. Archive the evidence before resetting it.

---

# Stopping the Prototype

To close the prototype safely:

1. Use the dashboard to stop the cloud server.
2. Use the dashboard to stop the edge server.
3. Return to the dashboard PowerShell or Command Prompt window.
4. Press `Ctrl + C`.
5. Press any key when prompted to close the window.
6. Close the dashboard browser tab.

Closing only the browser tab does not stop the Python dashboard process.
Use Windows PowerShell to look for dashboard process
1. netstat -ano | findstr ":5050"
2. taskkill /PID "LISTENING port" /T /F

---

# Troubleshooting

## Python Is Not Recognised

If the following message appears:

```text
python is not recognized as an internal or external command
```

Install Python and select the option:

```text
Add Python to PATH
```

Restart PowerShell after installation.

## Required Module Is Missing

Run:

```powershell
python -m pip install -r requirements.txt
```

Then restart the dashboard.

## Dashboard Does Not Open

Run:

```powershell
python dashboard_app.py
```

Check the PowerShell output for an error. When no error is displayed, manually open:

```text
http://127.0.0.1:5050
```

## Cloud or Edge Does Not Start

Check:

```text
dashboard_logs/cloud_server.log
dashboard_logs/edge_server.log
```

Also confirm that ports `5000` and `5001` are not already being used by another application.

## Dashboard Port Is Already in Use

Check whether another dashboard window is already running. Close the previous dashboard process before starting a new one.

---

## Disclaimer

This prototype is an academic simulation developed for research and evaluation purposes. It is not tested yet as a production disaster recovery system and does not process real customer information.

<img width="1366" height="768" alt="Dashboard" src="https://github.com/user-attachments/assets/9853b5bc-7e9a-455f-92fe-2a06729de494" />

