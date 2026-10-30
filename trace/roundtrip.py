#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from otel_kafka_common import build_marker, load_scenario


TRACE_DIR = Path(__file__).resolve().parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a real OTEL Kafka round trip through Kong Event Gateway."
    )
    parser.add_argument(
        "scenario",
        choices=["1", "5", "6"],
        help="Scenario shortcut: 1=core-proxy, 5=ACL/auth, 6=encryption topic",
    )
    parser.add_argument(
        "--group-id",
        default="trace-roundtrip",
        help="Kafka consumer group id to use for the consume side",
    )
    return parser.parse_args()


def run_role(script_name: str, *args: str) -> dict[str, object]:
    script_path = TRACE_DIR / script_name
    result = subprocess.run(
        [sys.executable, str(script_path), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout.strip())


def main() -> int:
    args = parse_args()
    config = load_scenario(args.scenario)
    marker = build_marker()
    group_id = f"{args.group_id}-{marker}"

    print(f"Scenario: {args.scenario}")
    print(f"Bootstrap servers: {config.bootstrap_servers}")
    print(f"Topic: {config.topic}")
    print(f"Trace marker: {marker}")

    producer_result = run_role("producer.py", args.scenario, "--marker", marker)
    producer_trace_id = str(producer_result["trace_id"])
    print(f"Producer OTEL trace_id: {producer_trace_id}")

    consumer_result = run_role(
        "consumer.py",
        args.scenario,
        "--marker",
        marker,
        "--group-id",
        group_id,
    )
    consumer_trace_id = str(consumer_result["trace_id"])
    print(f"Consumer OTEL trace_id: {consumer_trace_id}")

    if producer_trace_id != consumer_trace_id:
        print("Trace ID mismatch. Context propagation failed.", file=sys.stderr)
        return 1

    print()
    print("Trace context propagated successfully.")
    print("Grafana Tempo TraceQL:")
    print(f'  {{ trace:id = "{producer_trace_id}" }}')
    print('  { resource.service.name = "trace-producer" }')
    print('  { resource.service.name = "trace-consumer" }')
    print('  { resource.service.name = "eventgw" && span:name = "request" }')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
