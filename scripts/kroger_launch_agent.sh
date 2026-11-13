#!/bin/zsh

set -eu

PROJECT_DIR="${0:A:h:h}"
LABEL="com.ivanrsalazar.kroger-pipeline"
SOURCE_PLIST="$PROJECT_DIR/scripts/$LABEL.plist"
AGENT_DIR="$HOME/Library/LaunchAgents"
INSTALLED_PLIST="$AGENT_DIR/$LABEL.plist"
DOMAIN="gui/$(id -u)"
SERVICE="$DOMAIN/$LABEL"

install_agent() {
  mkdir -p "$AGENT_DIR" "$PROJECT_DIR/.dagster"
  plutil -lint "$SOURCE_PLIST" >/dev/null
  cp "$SOURCE_PLIST" "$INSTALLED_PLIST"
  launchctl bootout "$SERVICE" >/dev/null 2>&1 || true
  launchctl bootstrap "$DOMAIN" "$INSTALLED_PLIST"
  launchctl kickstart -k "$SERVICE"
  print "Installed and started $LABEL."
  print "UI:  http://127.0.0.1:3000"
  print "Log: $PROJECT_DIR/.dagster/kroger.log"
}

status_agent() {
  if launchctl print "$SERVICE" >/dev/null 2>&1; then
    launchctl print "$SERVICE" | grep -E 'state =|pid =|last exit code ='
    print "UI:  http://127.0.0.1:3000"
    return 0
  fi
  print "Kroger LaunchAgent is not loaded."
  return 1
}

stop_agent() {
  if launchctl print "$SERVICE" >/dev/null 2>&1; then
    launchctl bootout "$SERVICE"
    print "Stopped $LABEL."
  else
    print "Kroger LaunchAgent is not loaded."
  fi
}

uninstall_agent() {
  stop_agent
  if [[ -f "$INSTALLED_PLIST" ]]; then
    mv "$INSTALLED_PLIST" "$HOME/.Trash/$LABEL.plist"
    print "Moved the LaunchAgent plist to Trash."
  fi
}

case "${1:-}" in
  install|start) install_agent ;;
  status) status_agent ;;
  stop) stop_agent ;;
  uninstall) uninstall_agent ;;
  logs) mkdir -p "$PROJECT_DIR/.dagster"; touch "$PROJECT_DIR/.dagster/kroger.log"; exec tail -f "$PROJECT_DIR/.dagster/kroger.log" ;;
  *)
    print "Usage: $0 {install|start|status|stop|uninstall|logs}"
    exit 2
    ;;
esac
