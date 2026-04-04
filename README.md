# SQL Server Stress Test Tool

A GUI tool to stress-test SQL Server 2022 by driving concurrent queries via
Microsoft Entra ID service-principal authentication. Supports multiple SQL
Servers with automatic round-robin distribution.

## Prerequisites

| Requirement | Details |
|---|---|
| **Python** | 3.10+ |
| **ODBC Driver** | [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) |
| **Entra ID** | A service principal (App Registration) with the **SQL Server Contributor** or appropriate role on each target server, and granted access as an external user in each database |

### Entra ID / SQL Server Setup

1. Register an App in Microsoft Entra ID and note the **Tenant ID**, **Client
   (Application) ID**, and create a **Client Secret**.
2. On each Azure SQL Server, set the Entra admin and create a contained user
   for the service principal:
   ```sql
   CREATE USER [<app-name>] FROM EXTERNAL PROVIDER;
   ALTER ROLE db_datareader ADD MEMBER [<app-name>];
   -- grant additional permissions as needed
   ```

## Installation

```powershell
cd C:\sql-stress-test
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Usage

```powershell
python main.py
```

### Workflow

1. **Add Servers** — enter each server address and database, click *Add Server*.
   Queries are distributed round-robin across all servers.
2. **Configure Auth** — enter the service principal's Tenant ID, Client ID, and
   Client Secret. Click *Test Auth* to verify before starting.
3. **Set Query** — the default `SELECT 1` is a lightweight connectivity test.
   Replace with a heavier query to test throughput.
4. **Start Test** — set the initial worker count and optional inter-query delay
   (milliseconds), then click *Start Test*.
5. **Scale Up** — while running, use the *+ Workers* button to add more
   concurrent connections and observe when errors begin.
6. **Monitor** — the hero metric **Connections / Min** and the live chart show
   throughput in real time. Error lines are highlighted in red in the log.
7. **Stop** — click *Stop* to gracefully shut down all workers.

### Key Metrics

| Metric | Meaning |
|---|---|
| **Connections / Min** | Total connection+query cycles completed in the last 60 s |
| **Active Workers** | Threads currently executing queries |
| **Queries / Min** | Successful queries in the last 60 s |
| **Errors / Min** | Failed attempts in the last 60 s |
| **Success Rate** | Lifetime success percentage |

### Logs

All activity is logged to timestamped files under the `logs/` directory.

## Project Structure

```
sql-stress-test/
├── main.py            # GUI application (tkinter + matplotlib)
├── engine.py          # Stress-test engine (threading, round-robin, metrics)
├── auth.py            # Entra ID token acquisition (MSAL)
├── requirements.txt   # Python dependencies
└── README.md
```
