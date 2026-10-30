#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json

from otel_kafka_common import configure_tracing, consume_message, load_scenario


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume one OTEL-instrumented Kafka message.")
    parser.add_argument("scenario", choices=["1", "5", "6"])
    parser.add_argument("--marker", required=True, help="Unique marker to locate in payload")
    parser.add_argument("--group-id", required=True, help="Kafka consumer group id")
    parser.add_argument(
        "--service-name",
        default="trace-consumer",
        help="OTEL service.name to export for the consumer process",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_scenario(args.scenario)
    provider = configure_tracing(args.service_name)
    try:
        result = consume_message(config, args.marker, args.group_id)
        print(json.dumps(result))
        return 0
    finally:
        provider.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
