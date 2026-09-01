"""Reset V4.1 databases and experiment evidence for a clean controlled batch."""
from pathlib import Path
import shutil

BASE = Path(__file__).resolve().parent
for filename in ["cloud.db", "edge.db", "metrics_log.csv", ".v4_1_dashboard_processes.json"]:
    path = BASE / filename
    if path.exists():
        path.unlink()
        print(f"Deleted {filename}")

results = BASE / "v4_1_results"
if results.exists():
    shutil.rmtree(results)
    print("Deleted v4_1_results folder")

print("Clean V4.1 reset complete.")
print("Restart cloud_server.py and edge_server.py, or restart them from the dashboard.")
