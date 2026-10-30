# OTEL End-to-End Trace Demo

This folder contains a real OpenTelemetry trace propagation demo for Kafka
traffic going through Kong Event Gateway.

It uses:

- an OTEL-instrumented producer process
- W3C trace context injected into Kafka headers
- an OTEL-instrumented consumer process
- context extraction from Kafka headers on consume

That gives you a real OpenTelemetry trace ID shared across producer and
consumer spans using standard header propagation, instead of a payload-only
marker.

## Install

```bash
python3 -m venv .venv-trace
. .venv-trace/bin/activate
pip install -r trace/requirements.txt
```

## Run

Scenario shortcuts:

- `1`: core proxy topic over `127.0.0.1:19092`
- `5`: authenticated wealth-advisors flow over `127.0.0.1:19292`
- `6`: encryption topic over `127.0.0.1:19092`

```bash
python3 trace/roundtrip.py 5
```

Example output:

```text
Scenario: 5
Bootstrap servers: 127.0.0.1:19292
Topic: infosec.security.fraud.risk-scores.v3
Trace marker: otel-1783949202
Producer OTEL trace_id: 7b1d...
Consumer OTEL trace_id: 7b1d...
```

## What runs

- `producer.py`: sends one message and injects W3C trace headers
- `consumer.py`: reads that message back and extracts W3C trace headers
- `roundtrip.py`: orchestrates both as separate processes so each has its own
  OTEL `service.name`

## Tempo

Once the script prints the OTEL trace ID, search for it in Grafana Tempo:

```traceql
{ trace:id = "YOUR_TRACE_ID" }
```

You can also search by service:

```traceql
{ resource.service.name = "trace-producer" }
{ resource.service.name = "trace-consumer" }
{ resource.service.name = "eventgw" && span:name = "request" }
```

## Important note about Kong spans

This demo guarantees standards-based trace propagation between the producer and
consumer by using OTEL context in Kafka headers.

Whether Kong Event Gateway emits spans into the exact same distributed trace
depends on whether the gateway extracts and continues that incoming trace
context for Kafka traffic. If it does, the `eventgw` spans will appear under
the same trace ID in Tempo. If it does not, you will still have a valid
producer trace and consumer trace joined across Kafka clients using OTEL
standard propagation.
