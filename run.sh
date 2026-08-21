#!/usr/bin/env bash
# ============================================================================
# AmazingData HTTP 网关 - 启动 + 存活检测
# ----------------------------------------------------------------------------
# 设计目标: 单脚本同时完成"启动服务"和"存活检测", 由 crontab 每分钟调用,
# 实现进程守护(进程挂掉 / 无响应时自动拉起)。
#
# 用法:
#   ./run.sh           # 检测存活, 未运行或异常则启动(供 crontab 调用)
#   ./run.sh stop      # 停止服务
#   ./run.sh status    # 查看运行状态
#   ./run.sh restart   # 重启
#
# 可选环境变量:
#   PYTHON_BIN         python 解释器路径(默认 python3)
#   AMAZINGDATA_CONFIG config.ini 路径(默认脚本同目录)
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON_BIN:-python3}"
CONFIG="${AMAZINGDATA_CONFIG:-$SCRIPT_DIR/config.ini}"
RUN_DIR="$SCRIPT_DIR/run"
PIDFILE="$RUN_DIR/http_service.pid"
LOGFILE="$RUN_DIR/server.log"
mkdir -p "$RUN_DIR"

# 从 config.ini 读出 HTTP 监听端口(用于探活); 失败则回退 8000
PORT="$("$PYTHON" - "$CONFIG" <<'PY' 2>/dev/null
import configparser, sys
try:
    c = configparser.ConfigParser()
    c.read(sys.argv[1])
    print(c.get("server", "port", fallback="8000"))
except Exception:
    print("8000")
PY
)"
HEALTH_URL="http://127.0.0.1:${PORT}/health"

log() { echo "[$(date '+%F %T')] $*" >> "$LOGFILE"; }

is_running() {
    # 返回 0 表示: 进程在且 /health 探活通过
    [ -f "$PIDFILE" ] || return 1
    local pid
    pid="$(cat "$PIDFILE" 2>/dev/null)"
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null || return 1
    if command -v curl >/dev/null 2>&1; then
        curl -fsS --max-time 3 "$HEALTH_URL" >/dev/null 2>&1
    else
        # 无 curl 时退化为只判断进程存活
        return 0
    fi
}

start() {
    log "starting http_service (port=$PORT, python=$PYTHON)"
    nohup "$PYTHON" "$SCRIPT_DIR/http_service.py" >> "$LOGFILE" 2>&1 &
    echo $! > "$PIDFILE"
    sleep 2
    if is_running; then
        log "started ok, pid=$(cat "$PIDFILE")"
    else
        log "WARN: started but health check failed, see $LOGFILE"
    fi
}

stop() {
    if [ -f "$PIDFILE" ]; then
        local pid
        pid="$(cat "$PIDFILE")"
        if [ -n "$pid" ]; then
            log "stopping pid=$pid"
            kill "$pid" 2>/dev/null
            # 最多等 10s 优雅退出
            for _ in $(seq 1 10); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
        fi
        rm -f "$PIDFILE"
    fi
    log "stopped"
}

status() {
    if is_running; then
        echo "RUNNING (pid=$(cat "$PIDFILE"), port=$PORT)"
        exit 0
    else
        echo "NOT RUNNING (port=$PORT)"
        exit 1
    fi
}

case "${1:-}" in
    stop)    stop ;;
    status)  status ;;
    restart) stop; sleep 1; start ;;
    "")      # 默认: 存活检测 + 必要时拉起(给 crontab 用)
        if is_running; then
            log "alive, skip"
        else
            log "not alive, restarting"
            rm -f "$PIDFILE"
            start
        fi
        ;;
    *) echo "usage: $0 [stop|status|restart]"; exit 2 ;;
esac
