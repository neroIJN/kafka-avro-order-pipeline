"""
Kafka Order Consumer with Avro Deserialization, Real-time Price Aggregation,
Retry Logic for Transient Errors, and Dead Letter Queue (DLQ) Dispatch.
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

import avro_utils

load_dotenv()

# Configuration defaults
DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_ORDER_TOPIC = os.getenv("KAFKA_ORDER_TOPIC", "orders")
DEFAULT_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders-dlq")
DEFAULT_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "order-processing-group")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_BACKOFF_SEC = float(os.getenv("INITIAL_BACKOFF_SEC", "1.0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("OrderConsumer")


class RealTimeAggregator:
    """
    Maintains real-time running statistics across all successfully processed orders.
    Calculates running average price dynamically without storing full history in memory.
    """
    def __init__(self):
        self.order_count: int = 0
        self.total_revenue: float = 0.0
        self.min_price: float = float("inf")
        self.max_price: float = 0.0

    def update(self, price: float) -> Dict[str, Any]:
        """Updates the aggregator with a new order price and returns current snapshot."""
        self.order_count += 1
        self.total_revenue += price
        if price < self.min_price:
            self.min_price = price
        if price > self.max_price:
            self.max_price = price

        running_average = self.total_revenue / self.order_count
        return {
            "order_count": self.order_count,
            "total_revenue": round(self.total_revenue, 2),
            "running_average": round(running_average, 2),
            "min_price": round(self.min_price, 2),
            "max_price": round(self.max_price, 2),
        }


class TransientProcessingError(Exception):
    """Raised when a temporary processing failure occurs that should be retried."""
    pass


class PermanentProcessingError(Exception):
    """Raised when an unrecoverable validation/data failure occurs that belongs in DLQ."""
    pass


def process_order_payload(order: Dict[str, Any], attempt: int = 1) -> None:
    """
    Simulates business processing of an order record.
    Enforces business validation and simulates transient failure for testing.
    """
    price = order.get("price")
    order_id = order.get("orderId")
    product = order.get("product", "")

    # Business Validation Rule 1: Order price must be positive
    if price is None or price <= 0:
        raise PermanentProcessingError(
            f"Invalid order price: {price}. Price must be strictly positive."
        )

    # Business Validation Rule 2: Order must have an ID and Product
    if not order_id or not product:
        raise PermanentProcessingError(
            f"Missing required fields: orderId='{order_id}', product='{product}'."
        )

    # Simulated transient network/downstream glitch
    # If the product is TRANSIENT_ERROR_ITEM, fail the first 2 attempts, then succeed on 3rd!
    if product == "TRANSIENT_ERROR_ITEM":
        if attempt < 3:
            raise TransientProcessingError(
                f"Transient inventory/payment timeout for order #{order_id} (Attempt {attempt})."
            )
        else:
            logger.info(f"[RECOVERED] Order #{order_id} recovered successfully after {attempt} attempts!")


def route_to_dlq(
    dlq_producer,
    dlq_topic: str,
    raw_key: Optional[bytes],
    raw_value: bytes,
    reason: str,
    attempts: int,
    original_topic: str,
    original_partition: int,
    original_offset: int
) -> None:
    """
    Packages poisoned or exhausted messages with error metadata and sends them to the DLQ topic.
    """
    dlq_envelope = {
        "dead_letter_timestamp": datetime.now(timezone.utc).isoformat(),
        "failure_reason": reason,
        "attempts_made": attempts,
        "original_topic": original_topic,
        "original_partition": original_partition,
        "original_offset": original_offset,
        # Store decoded string if possible, or hex representation
        "raw_payload_preview": raw_value[:100].decode("utf-8", errors="replace")
    }

    try:
        envelope_bytes = json.dumps(dlq_envelope, indent=2).encode("utf-8")
        future = dlq_producer.send(
            dlq_topic,
            key=raw_key,
            value=envelope_bytes
        )
        metadata = future.get(timeout=10)
        logger.warning(
            f"[DLQ ROUTED] Sent poisoned record to topic='{dlq_topic}' "
            f"[Partition: {metadata.partition}, Offset: {metadata.offset}] | Reason: {reason}"
        )
    except Exception as e:
        logger.critical(f"CRITICAL: Failed to publish message to DLQ topic {dlq_topic}: {e}")


def run_consumer(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    order_topic: str = DEFAULT_ORDER_TOPIC,
    dlq_topic: str = DEFAULT_DLQ_TOPIC,
    group_id: str = DEFAULT_GROUP_ID,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF_SEC
):
    """
    Main consumer loop. Connects to Kafka, reads Avro orders, applies retry logic,
    computes running average aggregation, and sends poison messages to DLQ.
    """
    from kafka import KafkaConsumer, KafkaProducer

    logger.info("=" * 65)
    logger.info(" Starting Kafka Avro Order Consumer & Real-time Aggregator")
    logger.info(f" Broker:             {bootstrap_servers}")
    logger.info(f" Subscribed Topic:   {order_topic}")
    logger.info(f" DLQ Topic:          {dlq_topic}")
    logger.info(f" Consumer Group:     {group_id}")
    logger.info(f" Max Retry Attempts: {max_retries}")
    logger.info("=" * 65)

    try:
        consumer = KafkaConsumer(
            order_topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            client_id="order-consumer"
        )
        dlq_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            acks="all",
            client_id="dlq-producer"
        )
    except Exception as e:
        logger.error(f"Failed to connect consumer/DLQ producer to Kafka: {e}")
        sys.exit(1)

    aggregator = RealTimeAggregator()

    logger.info("Consumer ready. Waiting for incoming orders...")

    try:
        for msg in consumer:
            raw_key = msg.key
            raw_bytes = msg.value
            order = None

            # --- STEP 1: Avro Deserialization ---
            try:
                order = avro_utils.deserialize_order(raw_bytes)
            except Exception as deser_err:
                logger.error(f"[ERROR] Avro Deserialization failed: {deser_err}")
                route_to_dlq(
                    dlq_producer=dlq_producer,
                    dlq_topic=dlq_topic,
                    raw_key=raw_key,
                    raw_value=raw_bytes,
                    reason=f"Avro Deserialization Error: {str(deser_err)}",
                    attempts=1,
                    original_topic=msg.topic,
                    original_partition=msg.partition,
                    original_offset=msg.offset
                )
                continue

            # --- STEP 2: Processing with Retry Logic ---
            order_id = order.get("orderId", "UNKNOWN")
            attempt = 0
            processing_succeeded = False
            last_error = None

            while attempt < max_retries:
                attempt += 1
                try:
                    process_order_payload(order, attempt=attempt)
                    processing_succeeded = True
                    break

                except PermanentProcessingError as perm_err:
                    # Permanent business rule failure: Immediately route to DLQ
                    last_error = perm_err
                    logger.error(f"[PERMANENT FAILURE] Order #{order_id}: {perm_err}")
                    break

                except TransientProcessingError as trans_err:
                    last_error = trans_err
                    if attempt < max_retries:
                        backoff = initial_backoff * (2 ** (attempt - 1))
                        logger.warning(
                            f"[RETRY {attempt}/{max_retries}] Order #{order_id} failed transiently: "
                            f"{trans_err}. Retrying in {backoff:.1f}s..."
                        )
                        time.sleep(backoff)
                    else:
                        logger.error(
                            f"[RETRIES EXHAUSTED] Order #{order_id} failed after {max_retries} attempts."
                        )

            # --- STEP 3: Handle Outcome (Aggregation or DLQ) ---
            if processing_succeeded:
                stats = aggregator.update(order["price"])
                logger.info(
                    f"[PROCESSED] Order #{order['orderId']:<6} | "
                    f"Product: {order['product'][:20]:<20} | "
                    f"Price: ${order['price']:>8.2f} | "
                    f"Total Count: {stats['order_count']:>4} | "
                    f"Live Running Avg: ${stats['running_average']:>8.2f} | "
                    f"Total Revenue: ${stats['total_revenue']:>10.2f}"
                )
            else:
                route_to_dlq(
                    dlq_producer=dlq_producer,
                    dlq_topic=dlq_topic,
                    raw_key=raw_key,
                    raw_value=raw_bytes,
                    reason=str(last_error),
                    attempts=attempt,
                    original_topic=msg.topic,
                    original_partition=msg.partition,
                    original_offset=msg.offset
                )

    except KeyboardInterrupt:
        logger.info("\nConsumer shutting down gracefully...")
    finally:
        consumer.close()
        dlq_producer.flush()
        dlq_producer.close()
        logger.info("Kafka consumer and DLQ producer closed.")


def main():
    parser = argparse.ArgumentParser(description="Kafka Order Consumer & Live Aggregator")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})"
    )
    parser.add_argument(
        "--order-topic",
        default=DEFAULT_ORDER_TOPIC,
        help=f"Kafka orders topic (default: {DEFAULT_ORDER_TOPIC})"
    )
    parser.add_argument(
        "--dlq-topic",
        default=DEFAULT_DLQ_TOPIC,
        help=f"Kafka DLQ topic (default: {DEFAULT_DLQ_TOPIC})"
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help=f"Consumer group ID (default: {DEFAULT_GROUP_ID})"
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=MAX_RETRIES,
        help=f"Maximum retry attempts for transient errors (default: {MAX_RETRIES})"
    )
    parser.add_argument(
        "--backoff",
        type=float,
        default=INITIAL_BACKOFF_SEC,
        help=f"Initial exponential backoff delay in seconds (default: {INITIAL_BACKOFF_SEC})"
    )

    args = parser.parse_args()
    run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        order_topic=args.order_topic,
        dlq_topic=args.dlq_topic,
        group_id=args.group_id,
        max_retries=args.max_retries,
        initial_backoff=args.backoff
    )


if __name__ == "__main__":
    main()
