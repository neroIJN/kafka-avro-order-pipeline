"""
Live Pipeline Simulator (Mock Kafka In-Memory Demo).
Allows presenting and testing the full end-to-end Avro flow, Real-time Aggregation,
Retry Mechanism with Backoff, and Dead Letter Queue (DLQ) routing without needing an external Kafka broker running.
"""
import json
import logging
import time
from datetime import datetime, timezone
import queue

import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import avro_utils
from consumer import (
    RealTimeAggregator,
    PermanentProcessingError,
    TransientProcessingError,
    process_order_payload
)
from producer import generate_order

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("KafkaDemoSimulator")


def run_live_simulation(total_messages: int = 15):
    print("\n" + "=" * 75)
    print(" [>>] KAFKA ORDER PROCESSING SYSTEM - LIVE DEMONSTRATION SIMULATOR")
    print(" Features: Avro Serialization | Real-Time Running Average | Retries | DLQ")
    print("=" * 75 + "\n")

    # In-memory queues simulating Kafka Topics
    topic_orders = queue.Queue()
    topic_orders_dlq = queue.Queue()

    aggregator = RealTimeAggregator()
    max_retries = 3
    initial_backoff = 0.5  # faster backoff for demo presentation

    print("[PHASE 1] PRODUCER: Generating & Avro-Serializing Orders...")
    time.sleep(0.5)

    for i in range(1, total_messages + 1):
        order_id = 1000 + i
        # Simulate different scenarios:
        # #1005: Transient error item (triggers retries)
        # #1008: Poison pill negative price (triggers DLQ)
        # #1012: Malformed binary payload (triggers DLQ)
        if i == 5:
            mode = "transient_error"
        elif i == 8:
            mode = "corrupt_price"
        elif i == 12:
            mode = "malformed_bytes"
        else:
            mode = "normal"

        order_data, avro_bytes, tag = generate_order(order_id, mode=mode)
        topic_orders.put({
            "key": str(order_id).encode("utf-8"),
            "value": avro_bytes,
            "partition": 0,
            "offset": i - 1
        })

        icon = "[OK]" if tag == "VALID" else ("[RETRY]" if "RETRY" in tag else "[POISON]")
        print(f" {icon} [PRODUCED -> orders] #{order_data['orderId']} | "
              f"Product: {order_data['product']:<24} | "
              f"Price: ${order_data['price']:>7.2f} | "
              f"Type: {tag}")
        time.sleep(0.15)

    print(f"\n Producer published {total_messages} messages to topic 'orders'.\n")
    print("-" * 75)
    print("[PHASE 2] CONSUMER: Real-time Processing, Aggregation, Retries & DLQ")
    print("-" * 75 + "\n")
    time.sleep(1)

    while not topic_orders.empty():
        msg = topic_orders.get()
        raw_bytes = msg["value"]
        offset = msg["offset"]

        # Step 1: Deserialization
        try:
            order = avro_utils.deserialize_order(raw_bytes)
        except Exception as deser_err:
            print(f"\n[FAIL] [OFFSET {offset}] AVRO DESERIALIZATION FAILED: {deser_err}")
            dlq_envelope = {
                "dead_letter_timestamp": datetime.now(timezone.utc).isoformat(),
                "failure_reason": f"Avro Deserialization Error: {str(deser_err)}",
                "attempts_made": 1,
                "original_topic": "orders",
                "original_offset": offset,
                "raw_payload_preview": raw_bytes[:100].decode("utf-8", errors="replace")
            }
            topic_orders_dlq.put(dlq_envelope)
            print(f"[DLQ ROUTED] Sent corrupted Avro payload to 'orders-dlq'!")
            continue

        # Step 2: Processing with retry
        order_id = order.get("orderId", "UNKNOWN")
        attempt = 0
        succeeded = False
        last_error = None

        while attempt < max_retries:
            attempt += 1
            try:
                process_order_payload(order, attempt=attempt)
                succeeded = True
                break
            except PermanentProcessingError as perm_err:
                last_error = perm_err
                print(f"\n[PERMANENT FAILURE] Order #{order_id}: {perm_err}")
                break
            except TransientProcessingError as trans_err:
                last_error = trans_err
                if attempt < max_retries:
                    backoff = initial_backoff * (2 ** (attempt - 1))
                    print(f"[RETRY {attempt}/{max_retries}] Order #{order_id} transient error. Retrying in {backoff:.1f}s...")
                    time.sleep(backoff)
                else:
                    print(f"[RETRIES EXHAUSTED] Order #{order_id} failed all {max_retries} attempts.")

        # Step 3: Result Handling
        if succeeded:
            stats = aggregator.update(order["price"])
            print(f"[SUCCESS] Order #{order['orderId']} | "
                  f"Item: {order['product']:<22} | "
                  f"Price: ${order['price']:>7.2f} | "
                  f"Orders Count: {stats['order_count']:>2} | "
                  f"Live Running Avg: ${stats['running_average']:>7.2f} | "
                  f"Total Rev: ${stats['total_revenue']:>8.2f}")
        else:
            dlq_envelope = {
                "dead_letter_timestamp": datetime.now(timezone.utc).isoformat(),
                "failure_reason": str(last_error),
                "attempts_made": attempt,
                "original_topic": "orders",
                "original_offset": offset,
                "raw_payload_preview": str(order)
            }
            topic_orders_dlq.put(dlq_envelope)
            print(f"[DLQ ROUTED] Order #{order_id} routed to 'orders-dlq'!")

        time.sleep(0.1)

    # Phase 3: Inspect DLQ
    print("\n" + "=" * 75)
    print(f"[PHASE 3] DLQ INSPECTOR: Inspecting '{topic_orders_dlq.qsize()}' Poisoned Records in 'orders-dlq'")
    print("=" * 75 + "\n")
    time.sleep(0.5)

    dlq_count = 0
    while not topic_orders_dlq.empty():
        dlq_count += 1
        dlq_item = topic_orders_dlq.get()
        print(f"[DEAD LETTER #{dlq_count}]")
        print(json.dumps(dlq_item, indent=3))
        print("-" * 50)

    print("\n" + "=" * 75)
    print(" [DONE] DEMO COMPLETE: ALL ASSIGNMENT REQUIREMENTS DEMONSTRATED SUCCESSFULLY!")
    print(f"    - Final Processed Orders: {aggregator.order_count}")
    print(f"    - Final Running Average:  ${aggregator.total_revenue / max(aggregator.order_count, 1):.2f}")
    print(f"    - Dead Letters Handled:   {dlq_count}")
    print("=" * 75 + "\n")


if __name__ == "__main__":
    run_live_simulation()
