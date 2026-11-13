#!/bin/sh

set -eu

port="${1:-8085}"
host="${2:-127.0.0.1}"

case "$port" in
  ''|*[!0-9]*)
    echo "Port must be a number between 1 and 65535." >&2
    exit 2
    ;;
esac

if [ "$port" -lt 1 ] || [ "$port" -gt 65535 ]; then
  echo "Port must be a number between 1 and 65535." >&2
  exit 2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to serve the dashboard." >&2
  exit 1
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
dist_dir="$script_dir/dist"

if [ ! -f "$dist_dir/index.html" ]; then
  echo "Dashboard entry point not found: $dist_dir/index.html" >&2
  exit 1
fi

echo "Serving the grocery dashboard at http://$host:$port"
echo "Press Ctrl-C to stop."

exec python3 -m http.server "$port" --bind "$host" --directory "$dist_dir"
