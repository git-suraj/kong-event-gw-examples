#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./trace-demo.sh <scenario>

Examples:
  ./trace-demo.sh 1
  ./trace-demo.sh 3
  ./trace-demo.sh 5
  ./trace-demo.sh 6

This script generates a producer -> Kong Event Gateway -> Kafka -> Kong Event Gateway -> consumer flow
so Grafana has fresh logs, metrics, and traces to inspect.

Important:
  - The JSON field added by this script is a payload marker only.
  - It is not an OpenTelemetry trace ID.
  - True end-to-end OTEL tracing requires producer and consumer applications
    that propagate W3C trace context in Kafka headers.
EOF
}

normalize_scenario() {
  case "${1:-}" in
    1|01) echo "01" ;;
    2|02) echo "02" ;;
    3|03) echo "03" ;;
    4|04) echo "04" ;;
    5|05) echo "05" ;;
    6|06) echo "06" ;;
    7|07) echo "07" ;;
    8|08) echo "08" ;;
    *) return 1 ;;
  esac
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

produce_and_consume() {
  local context="$1"
  local topic="$2"
  local payload="$3"

  echo "Payload marker: $payload"
  echo "Producing through context $context on topic $topic"
  kafkactl --context "$context" produce "$topic" --value="$payload"

  echo "Consuming messages through context $context on topic $topic until the payload marker is found"
  kafkactl --context "$context" consume "$topic" --from-beginning | rg -m 1 '"trace_marker":"'
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -ne 1 ]]; then
    usage
    exit $([[ $# -eq 1 ]] && echo 0 || echo 1)
  fi

  require_cmd kafkactl

  local scenario
  scenario="$(normalize_scenario "$1")" || {
    usage >&2
    exit 1
  }

  local context=""
  local topic=""
  local payload=""
  local trace_marker
  trace_marker="demo-$(date +%s)"

  case "$scenario" in
    01|02)
      context="core-proxy"
      topic="nw.crm.advisory.prospect-locations.v1"
      payload="{\"trace_marker\":\"$trace_marker\",\"scenario\":\"$scenario\",\"source\":\"core-proxy\",\"event\":\"prospect-location-check\"}"
      ;;
    03)
      context="wealth-management-la"
      topic="portfolios.esg-allocation-adjustments.v1"
      payload="{\"trace_marker\":\"$trace_marker\",\"scenario\":\"$scenario\",\"source\":\"wealth-management-la\",\"event\":\"allocation-adjustment\"}"
      ;;
    04|05)
      context="wealth-advisors"
      topic="infosec.security.fraud.risk-scores.v3"
      payload="{\"trace_marker\":\"$trace_marker\",\"scenario\":\"$scenario\",\"source\":\"wealth-advisors\",\"event\":\"fraud-score-review\",\"score\":87}"
      ;;
    06)
      context="core-proxy"
      topic="nw.ledger.transactions.high-value-wire-transfers.v1"
      payload="{\"trace_marker\":\"$trace_marker\",\"scenario\":\"$scenario\",\"transaction_id\":\"wt-1001\",\"amount\":250000,\"currency\":\"USD\",\"beneficiary\":\"Acme Treasury\"}"
      ;;
    07)
      context="wealth-advisors-schema"
      topic="infosec.security.fraud.risk-scores.v3"
      payload="{\"trace_marker\":\"$trace_marker\",\"riskScore\":91,\"subjectId\":\"client-1001\",\"sourceSystem\":\"northwind-risk\"}"
      ;;
    08)
      context="core-proxy"
      topic="nw.crm.customers.kyc-profiles.v1"
      payload="{\"trace_marker\":\"$trace_marker\",\"customer_id\":\"cust-1001\",\"ssn\":\"123-45-6789\",\"tax_id\":\"99-1234567\",\"risk_tier\":\"high\"}"
      ;;
  esac

  echo "Using payload marker: $trace_marker"
  echo "Context: $context"
  echo "Topic: $topic"
  echo

  produce_and_consume "$context" "$topic" "$payload"

  cat <<EOF

Trace traffic generated.

Grafana:
  http://localhost:3000

Search commands:
  kafkactl --context $context consume $topic --from-beginning | rg '$trace_marker'

Grafana Explore:
  Logs base query   -> {service_name="eventgw"}
  Logs note         -> payload markers are not expected in Event Gateway logs
  Traces filter     -> { resource.service.name = "eventgw" && name = "request" }

Payload marker field:
  trace_marker = $trace_marker

OTEL note:
  The gateway emits OpenTelemetry trace IDs for its own spans.
  This script does not create a distributed traceparent across producer -> gateway -> consumer.
  For that, use instrumented producer and consumer apps that propagate W3C trace context in Kafka headers.
EOF
}

main "$@"
