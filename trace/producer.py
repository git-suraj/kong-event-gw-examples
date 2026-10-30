#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json

from otel_kafka_common import configure_tracing, load_scenario, produce_message


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce one OTEL-instrumented Kafka message.")
    parser.add_argument("scenario", choices=["1", "5", "6"])
    parser.add_argument("--marker", required=True, help="Unique marker added to payload")
    parser.add_argument(
        "--service-name",
        default="trace-producer",
        help="OTEL service.name to export for the producer process",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_scenario(args.scenario)
    provider = configure_tracing(args.service_name)
    try:
        result = produce_message(config, args.marker)
        print(json.dumps(result))
        return 0
    finally:
        provider.shutdown()


if __name__ == "__main__":
    raise SystemExit(main())
