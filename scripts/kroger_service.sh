#!/bin/zsh

set -eu
unsetopt BG_NICE

PROJECT_DIR="${0:A:h:h}"
VENV_DIR="$PROJECT_DIR/.venv"
RUNTIME_DIR="$PROJECT_DIR/.dagster"
PID_FILE="$RUNTIME_DIR/kroger.pid"
LOG_FILE="$RUNTIME_DIR/kroger.log"
DEFINITIONS_MODULE="grocery_pipeline.definitions_kroger"

load_environment() {
  # launchd/nohup shells do not automatically read ~/.zshrc, where the Kroger
  # credentials are currently configured.
  if [[ -f "$HOME/.zshrc" ]]; then
    set +u
    source "$HOME/.zshrc"
    set -u
  fi

  export DAGSTER_HOME="$RUNTIME_DIR"
  export GROCERY_BQ_PROJECT="${GROCERY_BQ_PROJECT:-grocery-pipe-line}"
}

require_runtime() {
  if [[ ! -x "$VENV_DIR/bin/dagster" ]]; then
    print -u2 "Missing $VENV_DIR. Run: uv venv --python python3.13 .venv && uv pip install --python .venv/bin/python -e ."
    exit 1
  fi
  if [[ -z "${KROGER_CLIENT_ID:-}" || -z "${KROGER_CLIENT_SECRET:-}" ]]; then
    print -u2 "KROGER_CLIENT_ID and KROGER_CLIENT_SECRET are not available after loading ~/.zshrc"
    exit 1
  fi
}

find_service_roots() {
  pgrep -f "$VENV_DIR/bin/dagster dev -m $DEFINITIONS_MODULE --host 127.0.0.1 --port 3000" 2>/dev/null || true
}

is_running() {
  local -a roots
  roots=(${(f)"$(find_service_roots)"})
  (( ${#roots[@]} > 0 )) || return 1

  # Recover from a missing/stale PID file after the CLI re-parents itself.
  print "${roots[1]}" >"$PID_FILE"
  return 0
}

collect_descendants() {
  local root_pid="$1"
  local child_pid
  for child_pid in $(pgrep -P "$root_pid" 2>/dev/null); do
    collect_descendants "$child_pid"
    print "$child_pid"
  done
}

start_service() {
  mkdir -p "$RUNTIME_DIR"
  load_environment
  require_runtime

  if is_running; then
    local -a existing_roots
    existing_roots=(${(f)"$(find_service_roots)"})
    if (( ${#existing_roots[@]} == 1 )) && curl -fsS http://127.0.0.1:3000/ >/dev/null 2>&1; then
      print "Kroger pipeline is already running (PID ${existing_roots[1]})."
      return 0
    fi
    print -u2 "Found ${#existing_roots[@]} Kroger Dagster processes. Run: $0 restart"
    return 1
  fi

  rm -f "$PID_FILE"
  cd "$PROJECT_DIR"
  nohup "$VENV_DIR/bin/dagster" dev \
    -m "$DEFINITIONS_MODULE" \
    --host 127.0.0.1 \
    --port 3000 \
    >>"$LOG_FILE" 2>&1 &
  local pid=$!
  print "$pid" >"$PID_FILE"
  disown "$pid" 2>/dev/null || true

  for _ in {1..20}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      print -u2 "Dagster exited during startup. Check $LOG_FILE"
      rm -f "$PID_FILE"
      exit 1
    fi
    if curl -fsS http://127.0.0.1:3000/ >/dev/null 2>&1; then
      local -a started_roots
      started_roots=(${(f)"$(find_service_roots)"})
      if (( ${#started_roots[@]} != 1 )); then
        print -u2 "Expected one Dagster process, found ${#started_roots[@]}."
        stop_service
        exit 1
      fi
      pid="${started_roots[1]}"
      print "$pid" >"$PID_FILE"
      print "Started Kroger pipeline (PID $pid)."
      print "UI:  http://127.0.0.1:3000"
      print "Log: $LOG_FILE"
      return 0
    fi
    sleep 1
  done

  print -u2 "Dagster did not become ready within 20 seconds. Check $LOG_FILE"
  stop_service
  exit 1
}

status_service() {
  if is_running; then
    local -a roots
    roots=(${(f)"$(find_service_roots)"})
    if (( ${#roots[@]} != 1 )); then
      print -u2 "Kroger pipeline has ${#roots[@]} parent processes; expected one."
      ps -p "${(j:,:)roots}" -o pid=,etime=,command=
      return 1
    fi
    local pid="${roots[1]}"
    print "Kroger pipeline is running (PID $pid)."
    ps -p "$pid" -o pid=,etime=,command=
    print "UI:  http://127.0.0.1:3000"
    print "Log: $LOG_FILE"
  else
    print "Kroger pipeline is not running."
    [[ -f "$PID_FILE" ]] && rm -f "$PID_FILE"
    return 1
  fi
}

stop_service() {
  if ! is_running; then
    print "Kroger pipeline is not running."
    [[ -f "$PID_FILE" ]] && rm -f "$PID_FILE"
    return 0
  fi

  local -a roots
  roots=(${(f)"$(find_service_roots)"})
  local -a service_pids
  service_pids=()
  local pid
  for pid in ${roots[@]}; do
    service_pids+=(${(f)"$(collect_descendants "$pid")"} "$pid")
  done
  service_pids=(${(u)service_pids})
  kill -TERM ${service_pids[@]} 2>/dev/null || true

  for _ in {1..15}; do
    if ! curl -fsS http://127.0.0.1:3000/ >/dev/null 2>&1; then
      # Reap any remaining non-listening workers; dead/zombie PIDs are ignored.
      kill -KILL ${service_pids[@]} 2>/dev/null || true
      rm -f "$PID_FILE"
      print "Stopped Kroger pipeline."
      return 0
    fi
    sleep 1
  done

  print "Process did not stop gracefully; sending SIGKILL."
  kill -KILL ${service_pids[@]} 2>/dev/null || true
  rm -f "$PID_FILE"
}

run_once() {
  mkdir -p "$RUNTIME_DIR"
  load_environment
  require_runtime
  cd "$PROJECT_DIR"
  exec "$VENV_DIR/bin/dagster" job execute \
    -m "$DEFINITIONS_MODULE" \
    -j bronze_kroger_products_daily_job
}

run_foreground() {
  mkdir -p "$RUNTIME_DIR"
  load_environment
  require_runtime
  cd "$PROJECT_DIR"
  exec "$VENV_DIR/bin/dagster" dev \
    -m "$DEFINITIONS_MODULE" \
    --host 127.0.0.1 \
    --port 3000
}

case "${1:-}" in
  start) start_service ;;
  status) status_service ;;
  stop) stop_service ;;
  restart) stop_service; start_service ;;
  foreground) run_foreground ;;
  run-once) run_once ;;
  logs) mkdir -p "$RUNTIME_DIR"; touch "$LOG_FILE"; exec tail -f "$LOG_FILE" ;;
  *)
    print "Usage: $0 {start|status|stop|restart|foreground|run-once|logs}"
    exit 2
    ;;
esac
