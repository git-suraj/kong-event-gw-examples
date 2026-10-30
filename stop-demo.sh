#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_COMPOSE_FILE="$ROOT_DIR/kafka/docker-compose.yaml"
GATEWAY_COMPOSE_FILE="$ROOT_DIR/docker-compose.yaml"
KONGCTL_CERT_CONFIG_FILE="$ROOT_DIR/kongctl/data_plane_certificate.yaml"
STATE_FILE="$ROOT_DIR/.demo-state"
EXAMPLE_CONFIG_GLOB="$ROOT_DIR/examples"/*/kongctl/config.yaml

usage() {
  cat <<'EOF'
Usage:
  ./stop-demo.sh [example]

Without an argument:
  Stops local services and deletes all demo Kong configs from Konnect.

With an example argument:
  Deletes the corresponding Kong config first, then deletes the remaining demo
  Kong configs and stops local containers.

This script also deletes the bootstrap data plane certificate config from Konnect
when present.

Examples:
  ./stop-demo.sh
  ./stop-demo.sh 3
EOF
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
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

delete_kong_config() {
  local config_file="$1"
  if [[ -f "$config_file" ]]; then
    echo "Deleting Kong config: $config_file"
    printf 'yes\n' | kongctl delete -f "$config_file" || true
  fi
}

cleanup_konnect_resources() {
  local selected_config="${1:-}"
  local config_file

  if [[ -n "$selected_config" ]]; then
    delete_kong_config "$selected_config"
  fi

  for config_file in $EXAMPLE_CONFIG_GLOB; do
    if [[ -n "$selected_config" && "$config_file" == "$selected_config" ]]; then
      continue
    fi
    delete_kong_config "$config_file"
  done

  if [[ -f "$KONGCTL_CERT_CONFIG_FILE" ]]; then
    echo "Deleting Konnect data plane certificate bootstrap config..."
    printf 'yes\n' | kongctl delete -f "$KONGCTL_CERT_CONFIG_FILE" || true
  fi
}

main() {
  if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
    exit 0
  fi

  require_cmd docker

  if [[ $# -gt 1 ]]; then
    usage >&2
    exit 1
  fi

  if [[ $# -eq 1 ]]; then
    require_cmd kongctl
    local example_dir
    if ! example_dir="$(normalize_example "$1")"; then
      usage >&2
      exit 1
    fi

    local config_file="$ROOT_DIR/examples/$example_dir/kongctl/config.yaml"
    cleanup_konnect_resources "$config_file"
  else
    if command -v kongctl >/dev/null 2>&1; then
      cleanup_konnect_resources
    fi
  fi

  echo "Stopping gateway..."
  docker compose -f "$GATEWAY_COMPOSE_FILE" down

  echo "Stopping Kafka and support services..."
  docker compose -f "$KAFKA_COMPOSE_FILE" --profile init down

  rm -f "$STATE_FILE"

  echo "Cleanup complete."
}

main "$@"
