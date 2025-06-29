#!/bin/bash
# Wrapper script to run commands with integration environment loaded
# Usage: ./scripts/with_integration_env.sh <command>
# Example: ./scripts/with_integration_env.sh python -m pytest tests/integration/

set -e

# Load the environment loader functions
source "$(dirname "$0")/load_env.sh"

# Load the integration environment
if ! load_and_export_env .env.integration; then
    echo "❌ Failed to load environment" >&2
    exit 1
fi

# Execute the command passed as arguments
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <command> [args...]"
    echo "Example: $0 python -m pytest tests/integration/"
    exit 1
fi

exec "$@"