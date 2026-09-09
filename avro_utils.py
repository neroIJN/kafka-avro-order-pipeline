"""
Avro utility functions for loading schema and serializing/deserializing Order records.
"""
import io
import os
from typing import Any, Dict
import fastavro

# Default path to schema file
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "order.avsc")


def load_schema(schema_path: str = SCHEMA_PATH) -> Dict[str, Any]:
    """Load and parse the Avro schema from the specified JSON/AVSC file."""
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Avro schema file not found at: {schema_path}")
    return fastavro.schema.load_schema(schema_path)


# Pre-load parsed schema for high-performance reuse
ORDER_SCHEMA = load_schema()


def serialize_order(order: Dict[str, Any], schema: Dict[str, Any] = ORDER_SCHEMA) -> bytes:
    """
    Serialize an order dictionary into Avro binary format (schemaless).
    
    Expected fields:
      - orderId (str)
      - product (str)
      - price (float)
    """
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, schema, order)
    return bytes_writer.getvalue()


def deserialize_order(data: bytes, schema: Dict[str, Any] = ORDER_SCHEMA) -> Dict[str, Any]:
    """
    Deserialize an Avro binary payload into an order dictionary.
    """
    bytes_reader = io.BytesIO(data)
    return fastavro.schemaless_reader(bytes_reader, schema)
