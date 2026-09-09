# Kafka Avro Order Processing System

A distributed, event-driven order processing system built with **Apache Kafka** and **Apache Avro**. The system implements real-time price aggregation (running average), configurable retry logic with exponential backoff for transient failures, and a Dead Letter Queue (DLQ) for poisoned or unrecoverable messages.

---

## 📋 Table of Contents
- [Assignment Requirements Coverage](#-assignment-requirements-coverage)
- [System Architecture](#-system-architecture)
- [Message Schema (`order.avsc`)](#-message-schema-orderavsc)
- [Project Structure](#-project-structure)
- [Prerequisites & Installation](#-prerequisites--installation)
- [🚀 Comprehensive Demo Guide (DEMO_GUIDE.md)](./DEMO_GUIDE.md)
- [Live Demonstration Guide](#-live-demonstration-guide)
  - [Option A: Standalone Simulator (Instant In-Memory Demo)](#option-a-standalone-simulator-instant-demo)
  - [Option B: Multi-Terminal Live Demo with Real Kafka Cluster](#option-b-multi-terminal-live-demo-with-kafka-broker)
- [Core Implementation Details](#-core-implementation-details)
  - [1. Real-Time Price Aggregation](#1-real-time-price-aggregation)
  - [2. Retry Logic with Exponential Backoff](#2-retry-logic-with-exponential-backoff)
  - [3. Dead Letter Queue (DLQ)](#3-dead-letter-queue-dlq)
- [Automated Testing](#-automated-testing)

---

## 🎯 Assignment Requirements Coverage

| Requirement | Implementation | Status |
| :--- | :--- | :---: |
| **Kafka-based Messaging** | Topics: `orders` (3 partitions) and `orders-dlq` (1 partition). | ✅ Done |
| **Avro Serialization** | Schemaless binary serialization using `fastavro` adhering to `order.avsc`. | ✅ Done |
| **Real-time Aggregation** | Live running average price calculated and logged on every successful transaction. | ✅ Done |
| **Retry Logic** | Configurable retry mechanism (default 3 attempts) with exponential backoff for transient issues. | ✅ Done |
| **Dead Letter Queue (DLQ)** | Poison pills (negative prices, corrupt bytes, exhausted retries) routed to `orders-dlq` with failure envelope. | ✅ Done |
| **Live Demonstration** | Supported via multi-terminal Kafka execution and instant simulator script `simulate_demo.py`. | ✅ Done |
| **Git Repository** | Fully initialized Git repository with clear, descriptive commit history. | ✅ Done |

---

## 🏗️ System Architecture

```
                       +-------------------------------+
                       |   Order Producer (producer)   |
                       |  - Generates orders           |
                       |  - Encodes using order.avsc   |
                       +---------------+---------------+
                                       |
                                       v
                    +------------------------------------+
                    |        Kafka Topic: "orders"       |
                    +------------------+-----------------+
                                       |
                                       v
                     +----------------------------------+
                     |    Order Consumer (consumer)     |
                     |  - Deserializes Avro payload     |
                     +-----------------+----------------+
                                       |
                     +-----------------+-----------------+
                     |                                   |
         [Valid Transaction]                [Transient Error]
                     |                                   |
                     v                                   v
       +----------------------------+       +-------------------------+
       |   Real-Time Aggregator     |       |   Retry Handler         |
       |  - Running Average Price   |       |  - Max 3 attempts       |
       |  - Total Revenue & Count   |       |  - Exponential Backoff  |
       +----------------------------+       +------------+------------+
                                                         |
                                             [Retries Exhausted /
                                             Permanent Poison Pill]
                                                         |
                                                         v
                                        +---------------------------------+
                                        |      Kafka Topic: "orders-dlq"  |
                                        +----------------+----------------+
                                                         |
                                                         v
                                        +---------------------------------+
                                        |    DLQ Inspector (inspector)    |
                                        |   - Displays diagnostic errors  |
                                        +---------------------------------+
```

---

## 📜 Message Schema (`order.avsc`)

The messages conform to the following Apache Avro schema:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.assignment.orders",
  "doc": "Schema for purchase transactions",
  "fields": [
    {
      "name": "orderId",
      "type": "string",
      "doc": "Unique identifier for the order (e.g., 1001, 1002)"
    },
    {
      "name": "product",
      "type": "string",
      "doc": "Name of the purchased item (e.g., Item1, Item2)"
    },
    {
      "name": "price",
      "type": "float",
      "doc": "Price of the product"
    }
  ]
}
```

---

## 📂 Project Structure

```
├── order.avsc             # Apache Avro schema definition
├── avro_utils.py          # FastAvro serialization & deserialization utilities
├── producer.py            # Kafka order producer with failure injection
├── consumer.py            # Kafka consumer with aggregator, retries & DLQ routing
├── dlq_inspector.py       # Terminal viewer for inspecting DLQ messages
├── simulate_demo.py       # Standalone in-memory end-to-end demo simulator
├── test_pipeline.py       # Automated unit & integration test suite (pytest)
├── docker-compose.yml     # Apache Kafka (KRaft mode) + Kafka UI
├── requirements.txt       # Python dependencies
├── .gitignore             # Git ignore configuration
└── README.md              # Documentation & presentation guide
```

---

## ⚙️ Prerequisites & Installation

### 1. Python Environment Setup
Activate a virtual environment and install the required dependencies:

```powershell
# Create virtual environment
python -m venv .venv

# Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Install required dependencies
pip install -r requirements.txt
```

---

## 🎬 Live Demonstration Guide

You can demonstrate the system in two ways:

### Option A: Standalone Simulator (Instant Demo)
If you want an immediate, self-contained demonstration without starting external Docker containers:

```powershell
python simulate_demo.py
```

**What this showcases in real time:**
1. **Producer Phase**: Emits 15 Avro-encoded order messages with randomized prices, including transient failure triggers and poison pills.
2. **Consumer Phase**:
   - Decodes Avro binary data.
   - Calculates and prints the **Running Average** after every valid order.
   - Triggers **Retry logic with exponential backoff** on temporary failures (Order `#1005`).
   - Catches **permanent poison pills** (negative price `#1008` and corrupted Avro bytes `#1012`) and routes them directly to the DLQ.
3. **DLQ Phase**: Dumps the dead-lettered envelopes with full diagnostic context (reason, offset, timestamps, payload preview).

---

### Option B: Multi-Terminal Live Demo with Kafka Broker

#### Step 1: Start Kafka Cluster
If Docker is installed:
```bash
docker compose up -d
```
*(Kafka broker will run on `localhost:9092`, and Kafka UI web dashboard at `http://localhost:8080`)*

#### Step 2: Open 3 Separate Terminal Windows

**Terminal 1 — Start the DLQ Inspector:**
```powershell
python dlq_inspector.py
```

**Terminal 2 — Start the Order Consumer:**
```powershell
python consumer.py
```

**Terminal 3 — Start the Producer (with failure injection enabled):**
```powershell
python producer.py --count 20 --rate 1.5 --inject-failures
```

---

## 🔍 Core Implementation Details

### 1. Real-Time Price Aggregation
The aggregator (`RealTimeAggregator` in `consumer.py`) computes running averages dynamically without unbounded memory usage:
$$\text{Running Average} = \frac{\sum \text{Prices}}{\text{Total Valid Orders}}$$

Each incoming transaction updates:
- Order Count
- Total Revenue
- Running Average Price
- Min & Max observed prices

### 2. Retry Logic with Exponential Backoff
Transient failures (e.g. downstream service timeouts) are retried up to `MAX_RETRIES` (default: 3) using an exponential backoff formula:
$$\text{delay} = \text{INITIAL\_BACKOFF} \times 2^{(\text{attempt} - 1)}$$

### 3. Dead Letter Queue (DLQ)
Messages that fail unrecoverably (invalid business data, corrupted Avro bytes, or exhausted retries) are wrapped in a JSON audit envelope and published to `orders-dlq`:
```json
{
  "dead_letter_timestamp": "2026-09-09T18:21:27.264483+00:00",
  "failure_reason": "Invalid order price: -99.99. Price must be strictly positive.",
  "attempts_made": 1,
  "original_topic": "orders",
  "original_offset": 7,
  "raw_payload_preview": "{'orderId': '1008', 'product': 'POISON_PILL_NEGATIVE_PRICE', 'price': -99.99}"
}
```

---

## 🧪 Automated Testing

Run the test suite using `pytest`:

```powershell
pytest -v test_pipeline.py
```

### Test Coverage (12 passed tests):
- ✅ `test_avro_schema_loaded`: Verifies schema loads and contains `orderId`, `product`, `price`.
- ✅ `test_avro_serialization_roundtrip`: Verifies binary encode/decode fidelity.
- ✅ `test_avro_serialization_missing_field`: Verifies rejection of schema-violating records.
- ✅ `test_avro_deserialization_corrupt_data`: Verifies handling of corrupt payloads.
- ✅ `test_aggregator_initial_state`: Verifies zero-state initialization.
- ✅ `test_aggregator_running_average_calculation`: Verifies arithmetic correctness of running average.
- ✅ `test_process_valid_order`: Verifies valid business processing.
- ✅ `test_permanent_failure_negative_price`: Verifies negative price triggers DLQ.
- ✅ `test_permanent_failure_zero_price`: Verifies zero price rejection.
- ✅ `test_transient_error_and_recovery`: Verifies 3-step retry and recovery.
- ✅ `test_generate_order_normal`: Verifies producer normal mode.
- ✅ `test_generate_order_failure_modes`: Verifies producer failure injection.
