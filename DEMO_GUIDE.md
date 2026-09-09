# 🚀 Live Demonstration Step-by-Step Guide

This guide walks you through demonstrating the entire Kafka + Avro order pipeline, real-time aggregation, retry mechanisms, Dead Letter Queue (DLQ), and Kafka Web UI dashboard.

---

## 📑 Contents
1. [Prerequisites & Setup](#1-prerequisites--setup)
2. [Demo Mode 1: Real Kafka Cluster & Dashboard (Full Interactive Demo)](#2-demo-mode-1-real-kafka-cluster--dashboard-full-interactive-demo)
   - [Step 1: Start Kafka & UI](#step-1-start-kafka--ui)
   - [Step 2: Open the Kafka Web Dashboard](#step-2-open-the-kafka-web-dashboard)
   - [Step 3: Launch 3 Terminals](#step-3-launch-3-terminals)
   - [Step 4: What to Point Out During Presentation](#step-4-what-to-point-out-during-presentation)
3. [Demo Mode 2: Instant Simulator Demo (No Docker Required)](#3-demo-mode-2-instant-simulator-demo-no-docker-required)
4. [Automated Test Suite Verification](#4-automated-test-suite-verification)
5. [Troubleshooting](#5-troubleshooting)

---

## 1. Prerequisites & Setup

Open PowerShell in the project directory (`D:\Big Data`):

```powershell
# Activate Python Virtual Environment
.\.venv\Scripts\Activate.ps1

# (Optional) If you get a Script Execution policy warning in PowerShell, run:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
# .\.venv\Scripts\Activate.ps1
```

---

## 2. Demo Mode 1: Real Kafka Cluster & Dashboard (Full Interactive Demo)

### Step 1: Start Kafka & UI

Make sure Docker Desktop is open, then run:

```powershell
docker compose up -d
```

Verify containers are running:
```powershell
docker compose ps
```
You will see 3 services:
- `kafka-broker` (running Kafka KRaft on port 9092)
- `kafka-init-topics` (initialized topics `orders` and `orders-dlq`)
- `kafka-ui` (running web UI on port 8080)

---

### Step 2: Open the Kafka Web Dashboard

1. Open your browser and go to: **[http://localhost:8080](http://localhost:8080)**
2. Check the cluster status:
   - Status: **`Online` (1 cluster)** with a **green dot**
   - Click **Topics** on the left navigation menu:
     - `orders` (3 partitions)
     - `orders-dlq` (1 partition)

---

### Step 3: Launch 3 Terminals

Open 3 terminal windows/tabs in `D:\Big Data` (ensure virtual environment is activated or use `.\.venv\Scripts\python.exe`):

#### 🖥️ **Terminal 1 — DLQ Inspector** (Start this first)
```powershell
.\.venv\Scripts\python.exe dlq_inspector.py
```
> *Watches `orders-dlq` for any failed or poisoned records.*

---

#### 🖥️ **Terminal 2 — Consumer & Real-time Aggregator** (Start this second)
```powershell
.\.venv\Scripts\python.exe consumer.py
```
> *Consumes messages from `orders`, deserializes Avro binary, updates running average, performs exponential backoff retries on transient errors, and routes poison pills to DLQ.*

---

#### 🖥️ **Terminal 3 — Producer (Live Stream with Failure Injection)**
```powershell
.\.venv\Scripts\python.exe producer.py --count 20 --rate 1.5 --inject-failures
```
> *Publishes 20 Avro-serialized orders (1 order every 1.5s), injecting:*
> - *Normal valid transactions*
> - *Transient network/service glitches (triggers retries)*
> - *Poison pills: Negative price (triggers DLQ)*
> - *Poison pills: Corrupted raw bytes (triggers DLQ)*

---

### Step 4: What to Point Out During Presentation

1. **Terminal 2 (Consumer Terminal)**:
   - Point out **`[SUCCESS]`** logs showing the dynamic calculation:
     - **Running Average Price** updating after every single order without loading old data into memory.
     - **Total Revenue** and **Order Count** tracking.
   - Point out **`[RETRY 1/3] ... Retrying in 1.0s`** and **`[RETRY 2/3] ... Retrying in 2.0s`** showing **exponential backoff**.
   - Point out **`[PERMANENT FAILURE]`** routing unrecoverable messages to DLQ.

2. **Terminal 1 (DLQ Inspector Terminal)**:
   - Point out the structured JSON diagnostic envelopes containing:
     - `failure_reason`: Exact error description (e.g. negative price, corrupted Avro header).
     - `attempts_made`: How many retry attempts took place before giving up.
     - `original_topic` & `original_offset`: Full audit traceability.
     - `raw_payload_preview`: Inspection payload.

3. **Browser Dashboard (`http://localhost:8080`)**:
   - Go to **Topics ➔ `orders` ➔ `Messages` tab**: Show the live binary Avro payload stream distributed across partitions 0, 1, and 2.
   - Go to **Topics ➔ `orders-dlq` ➔ `Messages` tab**: Show the dead-lettered payloads and diagnostic JSON.
   - Go to **Consumers ➔ `order-processing-group`**: Show partition assignments, current offset vs end offset, and zero consumer lag.

---

## 3. Demo Mode 2: Instant Simulator Demo (No Docker Required)

If you need to show an instant end-to-end presentation in a single terminal without starting Docker:

```powershell
.\.venv\Scripts\python.exe simulate_demo.py
```

**Output Phases:**
- **Phase 1 (Producer)**: Generates 15 Avro-encoded transactions.
- **Phase 2 (Consumer)**: Performs real-time deserialization, running average updates, retry with backoff, and DLQ dispatch.
- **Phase 3 (DLQ Inspection)**: Dumps all dead-lettered envelopes with full diagnostic audit logs.
- **Summary**: Displays final order count, final running average, and total dead letters handled.

---

## 4. Automated Test Suite Verification

To prove code correctness and edge-case handling, run `pytest`:

```powershell
.\.venv\Scripts\pytest.exe -v test_pipeline.py
```

**All 12 Tests will pass:**
- Schema validation & field definitions (`orderId`, `product`, `price`).
- Avro binary roundtrip encoding & decoding.
- Schema violation rejection on missing fields.
- Corrupted byte payload handling.
- Real-time mathematical running average calculation.
- Negative price & zero price validation.
- Transient error retry & recovery mechanism.
- Failure injection modes.

---

## 5. Troubleshooting

- **`ModuleNotFoundError: No module named 'fastavro'`**:
  Make sure you run using `.\.venv\Scripts\python.exe` or activate the environment with `.\.venv\Scripts\Activate.ps1`.

- **Kafka UI shows "Offline (1 cluster)"**:
  Restart the containers with the latest configuration:
  ```powershell
  docker compose down
  docker compose up -d
  ```

- **Stop all background containers after demo**:
  ```powershell
  docker compose down
  ```
