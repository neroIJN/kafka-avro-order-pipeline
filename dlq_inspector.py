"""
Dead Letter Queue (DLQ) Inspector.
Monitors the 'orders-dlq' topic and prints poisoned messages with diagnostic context.
Used for live presentation and debugging exhausted/failed records.
"""
import argparse
import json
import logging
import os
import sys

# Ensure UTF-8 output on Windows consoles
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv

load_dotenv()

DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEFAULT_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "orders-dlq")
DEFAULT_GROUP_ID = "dlq-inspector-group"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("DLQInspector")


def run_dlq_inspector(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    dlq_topic: str = DEFAULT_DLQ_TOPIC,
    group_id: str = DEFAULT_GROUP_ID
):
    from kafka import KafkaConsumer

    logger.info("=" * 65)
    logger.info(" Starting Dead Letter Queue (DLQ) Inspector")
    logger.info(f" Broker:            {bootstrap_servers}")
    logger.info(f" DLQ Topic:         {dlq_topic}")
    logger.info(f" Consumer Group:    {group_id}")
    logger.info("=" * 65)

    try:
        consumer = KafkaConsumer(
            dlq_topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            client_id="dlq-inspector"
        )
    except Exception as e:
        logger.error(f"Failed to connect DLQ Inspector to Kafka: {e}")
        sys.exit(1)

    logger.info(f"Listening on DLQ topic '{dlq_topic}' for dead-lettered events...\n")
    dlq_counter = 0

    try:
        for msg in consumer:
            dlq_counter += 1
            raw_key = msg.key.decode("utf-8", errors="replace") if msg.key else "None"
            
            try:
                envelope = json.loads(msg.value.decode("utf-8"))
            except Exception:
                envelope = {"raw": msg.value.decode("utf-8", errors="replace")}

            print("\n" + "!" * 65)
            print(f"[DEAD LETTER RECORD #{dlq_counter}]")
            print(f"   Record Key:          {raw_key}")
            print(f"   DLQ Partition:       {msg.partition} | Offset: {msg.offset}")
            print(f"   Original Topic:      {envelope.get('original_topic', 'N/A')}")
            print(f"   Original Partition:  {envelope.get('original_partition', 'N/A')}")
            print(f"   Original Offset:     {envelope.get('original_offset', 'N/A')}")
            print(f"   Failure Reason:      {envelope.get('failure_reason', 'N/A')}")
            print(f"   Attempts Made:       {envelope.get('attempts_made', 'N/A')}")
            print(f"   Failed Timestamp:    {envelope.get('dead_letter_timestamp', 'N/A')}")
            print(f"   Payload Preview:     {envelope.get('raw_payload_preview', 'N/A')}")
            print("!" * 65 + "\n")

    except KeyboardInterrupt:
        logger.info("\nDLQ Inspector stopped by user.")
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Dead Letter Queue Inspector")
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})"
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

    args = parser.parse_args()
    run_dlq_inspector(
        bootstrap_servers=args.bootstrap_servers,
        dlq_topic=args.dlq_topic,
        group_id=args.group_id
    )


if __name__ == "__main__":
    main()
