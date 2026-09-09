"""
Kafka Order Producer with Avro Serialization.
Generates simulated purchase transactions and publishes them to the Kafka 'orders' topic.
Supports failure injection to test retry handling and Dead Letter Queue (DLQ) routing.
"""
import argparse
import json
import logging
import os
import random
import sys
import time
from typing import Optional

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

import avro_utils

load_dotenv()

# Configuration defaults
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders")

# Sample product catalog for realistic simulations
PRODUCTS = [
    "MacBook Pro 16",
    "Dell UltraSharp 27 Monitor",
    "Sony WH-1000XM5 Headphones",
    "Keychron Q1 Mechanical Keyboard",
    "Logitech MX Master 3S Mouse",
    "CalDigit TS4 Thunderbolt Dock",
    "iPad Pro 11-inch",
    "Ergonomic Standing Desk",
    "Bose QuietComfort Earbuds",
    "USB-C Fast Charging Hub"
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("OrderProducer")


def create_kafka_producer(bootstrap_servers: str):
    """Initializes and returns a KafkaProducer instance."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            acks="all",
            retries=3,
            max_in_flight_requests_per_connection=1,
            client_id="order-producer"
        )
        logger.info(f"Connected to Kafka broker(s) at {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to connect to Kafka at {bootstrap_servers}: {e}")
        raise


def generate_order(order_id: int, mode: str = "normal") -> tuple[dict, bytes, str]:
    """
    Generates an order dictionary and its Avro binary payload.
    
    Modes:
      - 'normal': Valid order with normal product and randomized positive price.
      - 'transient_error': Order with product 'TRANSIENT_ERROR_ITEM' to trigger consumer retry logic.
      - 'corrupt_price': Order with negative price to trigger permanent validation failure -> DLQ.
      - 'malformed_bytes': Completely corrupt bytes that cannot be deserialized by Avro -> DLQ.
    """
    if mode == "transient_error":
        order_data = {
            "orderId": str(order_id),
            "product": "TRANSIENT_ERROR_ITEM",
            "price": round(random.uniform(25.0, 150.0), 2)
        }
        payload = avro_utils.serialize_order(order_data)
        return order_data, payload, "TRANSIENT_RETRY_SIMULATION"

    elif mode == "corrupt_price":
        # Price is negative (business rule violation)
        order_data = {
            "orderId": str(order_id),
            "product": "POISON_PILL_NEGATIVE_PRICE",
            "price": -99.99
        }
        payload = avro_utils.serialize_order(order_data)
        return order_data, payload, "PERMANENT_FAIL_DLQ_NEGATIVE_PRICE"

    elif mode == "malformed_bytes":
        # Raw corrupt bytes that fail Avro decoding
        order_data = {"orderId": str(order_id), "product": "CORRUPT_BYTES", "price": 0.0}
        payload = b"NOT_VALID_AVRO_BINARY_DATA_\x00\xFF\xAA\xBB"
        return order_data, payload, "PERMANENT_FAIL_DLQ_MALFORMED_BYTES"

    else:
        # Normal, valid transaction
        product = random.choice(PRODUCTS)
        # Price randomized between 15.00 and 1500.00
        price = round(random.uniform(15.0, 1500.0), 2)
        order_data = {
            "orderId": str(order_id),
            "product": product,
            "price": price
        }
        payload = avro_utils.serialize_order(order_data)
        return order_data, payload, "VALID"


def run_producer(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    rate_per_sec: float = 1.0,
    total_count: int = 20,
    inject_failures: bool = False,
    start_order_id: int = 1001
):
    """Runs the producer loop, publishing Avro messages to Kafka."""
    logger.info("=" * 60)
    logger.info(" Starting Kafka Avro Order Producer")
    logger.info(f" Broker:          {bootstrap_servers}")
    logger.info(f" Target Topic:    {topic}")
    logger.info(f" Rate:            {rate_per_sec} msg/sec")
    logger.info(f" Total Messages:  {total_count if total_count > 0 else 'Continuous'}")
    logger.info(f" Failure Injection: {'ENABLED' if inject_failures else 'DISABLED'}")
    logger.info("=" * 60)

    producer = create_kafka_producer(bootstrap_servers)

    current_id = start_order_id
    sent_count = 0
    delay = 1.0 / rate_per_sec if rate_per_sec > 0 else 0

    try:
        while total_count <= 0 or sent_count < total_count:
            # Determine generation mode
            mode = "normal"
            if inject_failures:
                # Every 5th order triggers transient retry
                if sent_count > 0 and sent_count % 8 == 0:
                    mode = "corrupt_price"
                elif sent_count > 0 and sent_count % 12 == 0:
                    mode = "malformed_bytes"
                elif sent_count > 0 and sent_count % 5 == 0:
                    mode = "transient_error"

            order_data, avro_bytes, tag = generate_order(current_id, mode=mode)

            # Publish to Kafka with orderId as key for partition ordering
            key_bytes = str(order_data["orderId"]).encode("utf-8")
            future = producer.send(
                topic,
                key=key_bytes,
                value=avro_bytes
            )
            record_metadata = future.get(timeout=10)

            status_icon = "[OK]" if tag == "VALID" else ("[RETRY]" if "RETRY" in tag else "[POISON]")
            logger.info(
                f"{status_icon} [PUBLISHED] #{order_data['orderId']} | "
                f"Item: {order_data['product'][:20]:<20} | "
                f"Price: ${order_data['price']:>8.2f} | "
                f"Tag: {tag:<22} | "
                f"Partition: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}"
            )

            current_id += 1
            sent_count += 1

            if delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user (Ctrl+C).")
    finally:
        logger.info(f"Flushing and closing producer. Total orders published: {sent_count}")
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="Kafka Avro Order Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})"
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka orders topic (default: {DEFAULT_TOPIC})"
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Messages published per second (default: 1.0)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=25,
        help="Total messages to publish (0 for continuous loop, default: 25)"
    )
    parser.add_argument(
        "--inject-failures",
        action="store_true",
        help="Inject transient and permanent failure scenarios to test retries and DLQ"
    )
    parser.add_argument(
        "--start-id",
        type=int,
        default=1001,
        help="Starting Order ID (default: 1001)"
    )

    args = parser.parse_args()
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate_per_sec=args.rate,
        total_count=args.count,
        inject_failures=args.inject_failures,
        start_order_id=args.start_id
    )


if __name__ == "__main__":
    main()
