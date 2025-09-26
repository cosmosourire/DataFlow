#!/usr/bin/env bash

CMD='kubectl -n kafka port-forward svc/my-cluster-dual-role-external-0 32000:9093 --address 127.0.0.1'
PID=/tmp/port-poward.pid
LOG=/tmp/port-poward.log

case "$1" in
  start)
    if [ -f "$PID" ] && kill -0 "$(cat "$PID")" 2>/dev/null; then
      echo "이미 실행 중 (pid=$(cat "$PID")). 로그: $LOG"
      exit 0
    fi
    nohup $CMD > "$LOG" 2>&1 &
    echo $! > "$PID"
    echo "시작됨 (pid=$(cat "$PID")). 로그: $LOG"
    ;;
  stop)
    if [ -f "$PID" ]; then
      kill "$(cat "$PID")" 2>/dev/null || true
      rm -f "$PID"
      echo "중지됨."
    else
      echo "실행 중 아님."
    fi
    ;;
  *)
    echo "사용법: $0 {start|stop}"
    exit 1
    ;;
esac

