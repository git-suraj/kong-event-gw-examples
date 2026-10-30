from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer, Producer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind


DEFAULT_OTLP_TRACES_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "http://127.0.0.1:4318/v1/traces",
)


SCENARIOS: dict[str, dict[str, str]] = {
    "1": {
        "bootstrap_servers": "127.0.0.1:19092",
        "topic": "nw.crm.advisory.prospect-locations.v1",
        "payload": json.dumps(
            {
                "event": "prospect-location-check",
                "source": "core-proxy",
            }
        ),
        "security_protocol": "PLAINTEXT",
    },
    "5": {
        "bootstrap_servers": "127.0.0.1:19292",
        "topic": "infosec.security.fraud.risk-scores.v3",
        "payload": json.dumps(
            {
                "event": "fraud-score-review",
                "source": "wealth-advisors",
                "score": 87,
            }
        ),
        "security_protocol": "SASL_PLAINTEXT",
        "sasl_username": "wealth-advisors",
        "sasl_password": "secret",
        "sasl_mechanism": "PLAIN",
    },
    "6": {
        "bootstrap_servers": "127.0.0.1:19092",
        "topic": "nw.ledger.transactions.high-value-wire-transfers.v1",
        "payload": json.dumps(
            {
                "transaction_id": "wt-1001",
                "amount": 250000,
                "currency": "USD",
                "beneficiary": "Acme Treasury",
            }
        ),
        "security_protocol": "PLAINTEXT",
    },
}


@dataclass
class ScenarioConfig:
    bootstrap_servers: str
    topic: str
    payload: str
    security_protocol: str = "PLAINTEXT"
    sasl_username: str | None = None
    sasl_password: str | None = None
    sasl_mechanism: str | None = None


def load_scenario(name: str) -> ScenarioConfig:
    scenario = SCENARIOS.get(name)
    if scenario is None:
        raise ValueError(f"Unsupported scenario: {name}")
    return ScenarioConfig(**scenario)


def build_client_config(config: ScenarioConfig) -> dict[str, str]:
    client_config: dict[str, str] = {
        "bootstrap.servers": config.bootstrap_servers,
        "security.protocol": config.security_protocol,
    }
    if config.sasl_username and config.sasl_password and config.sasl_mechanism:
        client_config["sasl.username"] = config.sasl_username
        client_config["sasl.password"] = config.sasl_password
        client_config["sasl.mechanism"] = config.sasl_mechanism
    return client_config


def configure_tracing(service_name: str) -> TracerProvider:
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=DEFAULT_OTLP_TRACES_ENDPOINT)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    return provider


def format_trace_id(trace_id: int) -> str:
    return f"{trace_id:032x}"


def encode_headers(headers: dict[str, str]) -> dict[str, bytes]:
    return {key: value.encode("utf-8") for key, value in headers.items()}


def decode_headers(headers: list[tuple[str, bytes]] | None) -> dict[str, str]:
    if not headers:
        return {}
    decoded: dict[str, str] = {}
    for key, value in headers:
        if value is None:
            continue
        decoded[key] = value.decode("utf-8") if isinstance(value, bytes) else str(value)
    return decoded


def build_marker() -> str:
    return f"otel-{int(time.time())}"


def create_payload(base_payload: str, marker: str) -> str:
    payload = json.loads(base_payload)
    payload["trace_marker"] = marker
    return json.dumps(payload)


def produce_message(config: ScenarioConfig, marker: str) -> dict[str, Any]:
    tracer = trace.get_tracer("trace.producer")
    producer = Producer(build_client_config(config))
    payload = create_payload(config.payload, marker)

    with tracer.start_as_current_span("kafka.produce", kind=SpanKind.PRODUCER) as span:
        span.set_attribute("messaging.system", "kafka")
        span.set_attribute("messaging.destination.name", config.topic)
        span.set_attribute("messaging.operation.name", "produce")
        span.set_attribute("app.trace_marker", marker)

        carrier: dict[str, str] = {}
        inject(carrier)
        producer.produce(
            config.topic,
            value=payload.encode("utf-8"),
            headers=encode_headers(carrier),
        )
        producer.flush()

        return {
            "trace_id": format_trace_id(span.get_span_context().trace_id),
            "topic": config.topic,
            "payload": payload,
            "marker": marker,
            "headers": carrier,
        }


def consume_message(config: ScenarioConfig, marker: str, group_id: str) -> dict[str, Any]:
    tracer = trace.get_tracer("trace.consumer")
    consumer_config = build_client_config(config)
    consumer_config.update(
        {
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer = Consumer(consumer_config)
    consumer.subscribe([config.topic])

    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is None or message.error():
                continue

            payload = message.value().decode("utf-8")
            if marker not in payload:
                continue

            carrier = decode_headers(message.headers())
            parent_context = extract(carrier=carrier)
            with tracer.start_as_current_span(
                "kafka.consume",
                context=parent_context,
                kind=SpanKind.CONSUMER,
            ) as span:
                span.set_attribute("messaging.system", "kafka")
                span.set_attribute("messaging.destination.name", config.topic)
                span.set_attribute("messaging.operation.name", "consume")
                span.set_attribute("app.trace_marker", marker)
                span.set_attribute("messaging.kafka.partition", message.partition())
                span.set_attribute("messaging.kafka.offset", message.offset())

                return {
                    "trace_id": format_trace_id(span.get_span_context().trace_id),
                    "topic": config.topic,
                    "payload": payload,
                    "marker": marker,
                    "headers": carrier,
                    "partition": message.partition(),
                    "offset": message.offset(),
                }
    finally:
        consumer.close()

    raise TimeoutError(f"Timed out waiting for marker {marker} on topic {config.topic}")
