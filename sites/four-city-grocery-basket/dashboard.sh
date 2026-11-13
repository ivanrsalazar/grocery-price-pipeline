#!/bin/sh

set -eu

action="${1:-}"
port="${2:-8085}"
host="${3:-127.0.0.1}"

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

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
runtime_dir="$script_dir/.runtime"
pid_file="$runtime_dir/dashboard-$port.pid"
log_file="$runtime_dir/dashboard-$port.log"

is_running() {
  [ -f "$pid_file" ] || return 1

  pid=$(sed -n '1p' "$pid_file")
  case "$pid" in
    ''|*[!0-9]*) return 1 ;;
  esac

  kill -0 "$pid" 2>/dev/null
}

case "$action" in
  start)
    mkdir -p "$runtime_dir"

    if is_running; then
      echo "Dashboard is already running on port $port (PID $pid)."
      exit 0
    fi

    rm -f "$pid_file"
    nohup "$script_dir/serve.sh" "$port" "$host" \
      >"$log_file" 2>&1 </dev/null &
    pid=$!
    printf '%s\n' "$pid" >"$pid_file"

    sleep 1
    if is_running; then
      echo "Dashboard started at http://$host:$port (PID $pid)."
      echo "Log: $log_file"
    else
      echo "Dashboard failed to start. Check $log_file" >&2
      rm -f "$pid_file"
      exit 1
    fi
    ;;
  status)
    if is_running; then
      echo "Dashboard is running on port $port (PID $pid)."
    else
      rm -f "$pid_file"
      echo "Dashboard is not running on port $port."
      exit 1
    fi
    ;;
  stop|kill)
    if ! is_running; then
      rm -f "$pid_file"
      echo "Dashboard is not running on port $port."
      exit 0
    fi

    kill "$pid"

    attempts=0
    while kill -0 "$pid" 2>/dev/null && [ "$attempts" -lt 5 ]; do
      sleep 1
      attempts=$((attempts + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
      echo "Dashboard did not stop; process $pid is still running." >&2
      exit 1
    fi

    rm -f "$pid_file"
    echo "Dashboard on port $port stopped."
    ;;
  *)
    echo "Usage: $0 {start|status|stop} [port] [host]" >&2
    exit 2
    ;;
esac
