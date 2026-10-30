#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_COMPOSE_FILE="$ROOT_DIR/kafka/docker-compose.yaml"
GATEWAY_COMPOSE_FILE="$ROOT_DIR/docker-compose.yaml"
KONNECT_ENV_FILE="$ROOT_DIR/konnect.env"
KONGCTL_CERT_CONFIG_FILE="$ROOT_DIR/kongctl/data_plane_certificate.yaml"
DOT_ENV_FILE="$ROOT_DIR/.env"
STATE_FILE="$ROOT_DIR/.demo-state"

usage() {
  cat <<'EOF'
Usage:
  ./start-demo.sh <example>

Examples:
  ./start-demo.sh 1
  ./start-demo.sh 03
  ./start-demo.sh 8

Supported examples:
  1  2  3  4  5  6  7  8

What this script does:
  1. Validates local prerequisites.
  2. Applies the Konnect data plane certificate bootstrap config.
  3. Resolves the Konnect gateway cluster ID and persists it to konnect.env.
  4. Starts Kafka and supporting services.
  5. Waits for Kafka readiness.
  6. Seeds topics and schemas idempotently.
  7. Starts the local Grafana LGTM observability stack and gateway.
  8. Generates any required example keys and persists them to .env.
  9. Applies the selected example config with kongctl.

Notes:
  - This script requires kongctl access to Konnect.
  - The gateway cluster ID in konnect.env is treated as derived state and will be
    updated to match the current Konnect resource.
  - Observability is enabled by default via OTLP -> http://lgtm:4317.
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

write_state() {
  local example_dir="$1"
  local config_file="$2"
  cat >"$STATE_FILE" <<EOF
LAST_EXAMPLE_DIR=$example_dir
LAST_CONFIG_FILE=$config_file
EOF
}

load_dot_env() {
  if [[ -f "$DOT_ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$DOT_ENV_FILE"
    set +a
  fi
}

load_konnect_env() {
  set -a
  # shellcheck disable=SC1090
  source "$KONNECT_ENV_FILE"
  set +a
}

persist_env_var() {
  local file="$1"
  local key="$2"
  local value="$3"
  touch "$file"
  if grep -q "^${key}=" "$file"; then
    perl -0pi -e "s/^${key}=.*/${key}=${value}/m" "$file"
  else
    printf '\n%s=%s\n' "$key" "$value" >>"$file"
  fi
}

persist_gateway_cluster_id() {
  local cluster_id="$1"
  persist_env_var "$KONNECT_ENV_FILE" "KONG_KONNECT_GATEWAY_CLUSTER_ID" "$cluster_id"
}

resolve_gateway_cluster_id() {
  kongctl get event-gateway keg-examples-gateway --output json --jq '.id' --jq-raw-output 2>/dev/null || true
}

ensure_konnect_bootstrap() {
  load_konnect_env

  if ! openssl x509 -in "$ROOT_DIR/kongctl/certs/tls.crt" -noout -checkend 0 >/dev/null 2>&1; then
    echo "Data plane certificate is missing, invalid, or expired: kongctl/certs/tls.crt" >&2
    echo "Renew the certificate and update KONG_KONNECT_CLIENT_CERT in konnect.env before retrying." >&2
    exit 1
  fi

  if [[ -z "${KONGCTL_DEFAULT_KONNECT_PAT:-}" ]]; then
    cat >&2 <<EOF
KONGCTL_DEFAULT_KONNECT_PAT is not set.

start-demo.sh now bootstraps Konnect automatically, which requires kongctl
credentials with permission to apply and read Event Gateway resources.
EOF
    exit 1
  fi

  echo "Applying Konnect bootstrap certificate config..."
  printf 'yes\n' | kongctl apply -f "$KONGCTL_CERT_CONFIG_FILE"

  echo "Resolving gateway cluster ID from Konnect..."
  local cluster_id
  cluster_id="$(resolve_gateway_cluster_id)"

  if [[ -z "$cluster_id" ]]; then
    cat >&2 <<EOF
Unable to determine KONG_KONNECT_GATEWAY_CLUSTER_ID.

Expected flow:
  1. kongctl applies kongctl/data_plane_certificate.yaml
  2. Konnect creates or exposes the gateway cluster named keg-examples-gateway
  3. kongctl resolves its ID

Verify that your PAT can access the target Konnect org and gateway resources.
EOF
    exit 1
  fi

  persist_gateway_cluster_id "$cluster_id"
  export KONG_KONNECT_GATEWAY_CLUSTER_ID="$cluster_id"
  echo "Resolved and persisted gateway cluster ID: $cluster_id"
}

normalize_example() {
  local input="${1:-}"
  case "$input" in
    1|01) echo "01-basic-proxy" ;;
    2|02) echo "02-topic-alias" ;;
    3|03) echo "03-topic-filter" ;;
    4|04) echo "04-auth-mediation" ;;
    5|05) echo "05-acl-enforcement" ;;
    6|06) echo "06-encryption" ;;
    7|07) echo "07-schema-validation" ;;
    8|08) echo "08-field-encryption" ;;
    *) return 1 ;;
  esac
}

ensure_example_env() {
  local example_dir="$1"
  load_dot_env

  case "$example_dir" in
    06-encryption|07-schema-validation|08-field-encryption)
      if [[ -z "${TRANSACTION_ENCRYPTION_KEY:-}" ]]; then
        export TRANSACTION_ENCRYPTION_KEY
        TRANSACTION_ENCRYPTION_KEY="$(openssl rand -base64 32)"
        persist_env_var "$DOT_ENV_FILE" "TRANSACTION_ENCRYPTION_KEY" "$TRANSACTION_ENCRYPTION_KEY"
        echo "Generated TRANSACTION_ENCRYPTION_KEY and persisted it to .env"
      fi
      ;;
  esac

  case "$example_dir" in
    08-field-encryption)
      if [[ -z "${FIELD_ENCRYPTION_KEY:-}" ]]; then
        export FIELD_ENCRYPTION_KEY
        FIELD_ENCRYPTION_KEY="$(openssl rand -base64 32)"
        persist_env_var "$DOT_ENV_FILE" "FIELD_ENCRYPTION_KEY" "$FIELD_ENCRYPTION_KEY"
        echo "Generated FIELD_ENCRYPTION_KEY and persisted it to .env"
      fi
      ;;
  esac
}

wait_for_kafka() {
  echo "Waiting for Kafka readiness..."
  local attempts=30
  local delay=5
  local i
  for ((i = 1; i <= attempts; i++)); do
    if docker compose -f "$KAFKA_COMPOSE_FILE" exec -T kafka1 \
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list >/dev/null 2>&1; then
      echo "Kafka is ready."
      return 0
    fi
    echo "  attempt $i/$attempts: Kafka not ready yet"
    sleep "$delay"
  done
  echo "Kafka did not become ready in time." >&2
  exit 1
}

wait_for_gateway() {
  echo "Waiting for gateway readiness..."
  local attempts=24
  local delay=5
  local i
  for ((i = 1; i <= attempts; i++)); do
    if [[ "$(docker inspect -f '{{.State.Health.Status}}' keg-data-plane 2>/dev/null || true)" == "healthy" ]]; then
      echo "Gateway is healthy."
      return 0
    fi
    echo "  attempt $i/$attempts: gateway not healthy yet"
    sleep "$delay"
  done
  echo "Gateway did not become healthy in time." >&2
  exit 1
}

wait_for_lgtm() {
  echo "Waiting for LGTM readiness..."
  local attempts=24
  local delay=5
  local i
  for ((i = 1; i <= attempts; i++)); do
    if [[ "$(docker inspect -f '{{.State.Running}}' lgtm 2>/dev/null || true)" == "true" ]]; then
      echo "LGTM is running."
      return 0
    fi
    echo "  attempt $i/$attempts: LGTM not ready yet"
    sleep "$delay"
  done
  echo "LGTM did not become ready in time." >&2
  exit 1
}

apply_example() {
  local example_dir="$1"
  local config_file="$2"
  local acl_detach_config_file="$ROOT_DIR/kongctl/acl-detach-reset.yaml"

  sync_config_with_retries "Syncing Kong config" "$config_file" 4 5 && return 0

  case "$example_dir" in
    # Scenario 7's cluster is configured through the API to work around a
    # kongctl SDK bug. Resetting it here would erase its namespace mapping.
    05-acl-enforcement|06-encryption|08-field-encryption)
      echo "Target sync did not converge. Detaching ACL policies through intermediate reset..."
      sync_config_with_retries "Syncing ACL-detach reset config" "$acl_detach_config_file" 4 5
      echo "Retrying target scenario after ACL detach..."
      sync_config_with_retries "Syncing Kong config" "$config_file" 4 5
      ;;
    *)
      return 1
      ;;
  esac
}

sync_config_with_retries() {
  local label="$1"
  local config_file="$2"
  local attempts="$3"
  local delay="$4"
  local attempt

  echo "${label}: $config_file"
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if printf 'yes\n' | kongctl sync -f "$config_file"; then
      return 0
    fi

    if (( attempt == attempts )); then
      break
    fi

    echo "Sync attempt $attempt/$attempts failed. Waiting ${delay}s before retry..."
    sleep "$delay"
  done

  return 1
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" || $# -ne 1 ]]; then
    usage
    exit $([[ $# -eq 1 ]] && echo 0 || echo 1)
  fi

  require_cmd docker
  require_cmd kongctl
  require_cmd perl
  require_cmd openssl

  if [[ ! -f "$KONNECT_ENV_FILE" ]]; then
    echo "Missing $KONNECT_ENV_FILE" >&2
    echo "Create it from konnect.env.example and populate the Konnect values first." >&2
    exit 1
  fi

  local example_dir
  if ! example_dir="$(normalize_example "$1")"; then
    usage >&2
    exit 1
  fi

  local config_file="$ROOT_DIR/examples/$example_dir/kongctl/config.yaml"
  if [[ ! -f "$config_file" ]]; then
    echo "Example config not found: $config_file" >&2
    exit 1
  fi

  ensure_konnect_bootstrap
  ensure_example_env "$example_dir"

  echo "Starting Kafka services..."
  docker compose -f "$KAFKA_COMPOSE_FILE" up -d
  wait_for_kafka

  echo "Seeding topics and schemas..."
  docker compose --profile init -f "$KAFKA_COMPOSE_FILE" up \
    --abort-on-container-exit \
    --exit-code-from create-topics \
    create-topics register-schema

  echo "Starting gateway..."
  docker compose -f "$GATEWAY_COMPOSE_FILE" up -d
  wait_for_lgtm
  wait_for_gateway

  apply_example "$example_dir" "$config_file"
  write_state "$example_dir" "$config_file"

  echo
  echo "Demo ready."
  echo "Applied example: $example_dir"
  echo "Grafana: http://localhost:3000"
  echo "Dashboard: http://localhost:3000/d/keg-observability/kong-event-gateway-kafka-metrics"
  echo "Metrics are sourced from Event Gateway Kafka metrics in Prometheus."
  echo "Logs and traces remain available in Grafana Explore."
  echo "Generate trace traffic: ./trace-demo.sh $1"
  echo "OTLP gRPC endpoint: http://localhost:4317"
}

main "$@"
