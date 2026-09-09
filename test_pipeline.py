"""
Comprehensive unit and pipeline tests for the Kafka Order Processing System.
Tests Avro serialization, schema compliance, real-time aggregation math,
retry mechanisms, and Dead Letter Queue (DLQ) routing.
"""
import io
import json
import pytest
import fastavro

import avro_utils
from consumer import (
    RealTimeAggregator,
    PermanentProcessingError,
    TransientProcessingError,
    process_order_payload
)
from producer import generate_order


# ==============================================================================
# 1. Avro Serialization & Schema Conformance Tests
# ==============================================================================

def test_avro_schema_loaded():
    """Verify that order.avsc exists, loads, and contains the required assignment fields."""
    schema = avro_utils.ORDER_SCHEMA
    assert schema["type"] == "record"
    assert schema["name"] in ("Order", "com.assignment.orders.Order")

    field_names = [f["name"] for f in schema["fields"]]
    assert "orderId" in field_names
    assert "product" in field_names
    assert "price" in field_names


def test_avro_serialization_roundtrip():
    """Verify that an order dict serializes to binary and accurately deserializes."""
    sample_order = {
        "orderId": "1001",
        "product": "Sony WH-1000XM5 Headphones",
        "price": 349.99
    }

    serialized_bytes = avro_utils.serialize_order(sample_order)
    assert isinstance(serialized_bytes, bytes)
    assert len(serialized_bytes) > 0

    deserialized = avro_utils.deserialize_order(serialized_bytes)
    assert deserialized["orderId"] == "1001"
    assert deserialized["product"] == "Sony WH-1000XM5 Headphones"
    assert pytest.approx(deserialized["price"], 0.01) == 349.99


def test_avro_serialization_missing_field():
    """Verify that attempting to serialize without required fields raises an exception."""
    incomplete_order = {
        "orderId": "1002"
        # Missing product and price
    }
    with pytest.raises(Exception):
        avro_utils.serialize_order(incomplete_order)


def test_avro_deserialization_corrupt_data():
    """Verify that deserializing arbitrary corrupt bytes raises an error."""
    corrupt_bytes = b"CORRUPTED_NON_AVRO_BINARY_DATA"
    with pytest.raises(Exception):
        avro_utils.deserialize_order(corrupt_bytes)


# ==============================================================================
# 2. Real-Time Aggregator (Running Average) Tests
# ==============================================================================

def test_aggregator_initial_state():
    """Verify initial zero state of aggregator."""
    aggregator = RealTimeAggregator()
    assert aggregator.order_count == 0
    assert aggregator.total_revenue == 0.0


def test_aggregator_running_average_calculation():
    """
    Verify that running average is updated correctly after each transaction.
    Example sequence:
      1. Order $100.00 -> Count: 1, Total: $100.00, Avg: $100.00
      2. Order $200.00 -> Count: 2, Total: $300.00, Avg: $150.00
      3. Order $150.00 -> Count: 3, Total: $450.00, Avg: $150.00
      4. Order $50.00  -> Count: 4, Total: $500.00, Avg: $125.00
    """
    agg = RealTimeAggregator()

    s1 = agg.update(100.00)
    assert s1["order_count"] == 1
    assert s1["total_revenue"] == 100.00
    assert s1["running_average"] == 100.00

    s2 = agg.update(200.00)
    assert s2["order_count"] == 2
    assert s2["total_revenue"] == 300.00
    assert s2["running_average"] == 150.00

    s3 = agg.update(150.00)
    assert s3["order_count"] == 3
    assert s3["total_revenue"] == 450.00
    assert s3["running_average"] == 150.00

    s4 = agg.update(50.00)
    assert s4["order_count"] == 4
    assert s4["total_revenue"] == 500.00
    assert s4["running_average"] == 125.00
    assert s4["min_price"] == 50.00
    assert s4["max_price"] == 200.00


# ==============================================================================
# 3. Business Validation & Retry Logic Tests
# ==============================================================================

def test_process_valid_order():
    """Verify valid order passes business processing without exception."""
    order = {"orderId": "1001", "product": "Laptop", "price": 999.00}
    # Should complete without error
    process_order_payload(order, attempt=1)


def test_permanent_failure_negative_price():
    """Verify negative price raises PermanentProcessingError (direct to DLQ)."""
    order = {"orderId": "1002", "product": "Faulty Item", "price": -50.00}
    with pytest.raises(PermanentProcessingError) as excinfo:
        process_order_payload(order, attempt=1)
    assert "Invalid order price" in str(excinfo.value)


def test_permanent_failure_zero_price():
    """Verify zero price raises PermanentProcessingError."""
    order = {"orderId": "1003", "product": "Free Item", "price": 0.00}
    with pytest.raises(PermanentProcessingError):
        process_order_payload(order, attempt=1)


def test_transient_error_and_recovery():
    """
    Verify retry simulation:
      - Attempt 1 fails with TransientProcessingError
      - Attempt 2 fails with TransientProcessingError
      - Attempt 3 succeeds
    """
    order = {"orderId": "1004", "product": "TRANSIENT_ERROR_ITEM", "price": 49.99}

    # Attempt 1: Should raise transient error
    with pytest.raises(TransientProcessingError):
        process_order_payload(order, attempt=1)

    # Attempt 2: Should raise transient error
    with pytest.raises(TransientProcessingError):
        process_order_payload(order, attempt=2)

    # Attempt 3: Succeeds without raising exception!
    process_order_payload(order, attempt=3)


# ==============================================================================
# 4. Producer Generation Modes Tests
# ==============================================================================

def test_generate_order_normal():
    """Verify normal mode generates valid Avro payload."""
    order_data, payload, tag = generate_order(1010, mode="normal")
    assert order_data["orderId"] == "1010"
    assert order_data["price"] > 0
    assert tag == "VALID"
    decoded = avro_utils.deserialize_order(payload)
    assert decoded["orderId"] == "1010"


def test_generate_order_failure_modes():
    """Verify producer correctly crafts fault-injection payloads for retries & DLQ."""
    # Transient error mode
    data1, payload1, tag1 = generate_order(1011, mode="transient_error")
    assert data1["product"] == "TRANSIENT_ERROR_ITEM"
    assert "RETRY" in tag1

    # Corrupt negative price mode
    data2, payload2, tag2 = generate_order(1012, mode="corrupt_price")
    assert data2["price"] < 0
    assert "DLQ" in tag2

    # Malformed raw bytes
    data3, payload3, tag3 = generate_order(1013, mode="malformed_bytes")
    assert "NOT_VALID" in payload3.decode("latin1")
    assert "DLQ" in tag3
