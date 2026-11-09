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
#   PYTHON_BIN         python 解释器路径(默认 venv/bin/python)
#   AMAZINGDATA_CONFIG config.ini 路径(默认脚本同目录)
#   STARTUP_GRACE      宽限期秒数(默认 120): "进程在、但端口还没监听"持续超过这个时长
#                      才判定为真卡死并重启; 期间只等待, 绝不重复拉起
#   HEALTH_TIMEOUT     单次 /health 探活超时秒数(默认 5)
#
# --- 2026-09-21 修复记录(服务反复重启) -------------------------------------
#   1) 加 flock 单实例锁: crontab 每分钟调用, 上一轮还在 start 的 sleep 中就被并发
#      再执行一次, 于是 nohup 出第二个进程 -> uvicorn 'Errno 98 地址已在使用'
#      (server.log 累计 6 次)。flock 保证同一时刻只有一份 run.sh 在跑。
#   2) 区分"进程不存在"与"进程在、端口还没起来"。旧逻辑用一次 sleep 2 + 单次
#      curl --max-time 3 就下结论, 而服务启动时"登录+验证"要 20~60s, 这段时间
#      端口必然不通 -> 每分钟被误判死亡并重复拉起(run.sh 侧累计误判 304 次)。
#      现在: 端口不通时先记下"开始不健康的时刻", 只有持续超过 STARTUP_GRACE 才重启。
#      这样既能覆盖冷启动(登录耗时), 也能覆盖进程内 execv 自愈重绑端口的那几秒
#      (那条路径 PID 不变、也来不及写任何启动标记)。
#   3) start 前先确认无存活进程, 从根上杜绝双实例。
# ============================================================================
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON="${PYTHON_BIN:-$SCRIPT_DIR/venv/bin/python}"
CONFIG="${AMAZINGDATA_CONFIG:-$SCRIPT_DIR/config.ini}"
RUN_DIR="$SCRIPT_DIR/run"
PIDFILE="$RUN_DIR/http_service.pid"
LOGFILE="$RUN_DIR/server.log"
LOCKFILE="$RUN_DIR/run.lock"
UNHEALTHY="$RUN_DIR/http_service.unhealthy_since"
GRACE="${STARTUP_GRACE:-120}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-5}"
mkdir -p "$RUN_DIR"

log() { echo "[$(date '+%F %T')] $*" >> "$LOGFILE"; }

# ---------------------------------------------------------------------------
# 单实例串行锁(2026-09-21 新增): 防止 crontab 并发调用导致重复拉起双实例
# ---------------------------------------------------------------------------
exec 9>"$LOCKFILE"
if ! flock -n 9; then
    # 拿不到锁: 另一份 run.sh 正在执行(crontab 重入), 直接退出。
    # 这里必须留一行日志 —— 万一以后又出现"锁 fd 被子进程继承"这类问题, 日志会每分钟
    # 出现一次本行, 一眼就能看出守护已被卡住(2026-09-21 正是这样发现的)。
    log "another run.sh holds the lock, skip"
    exit 0
fi

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

# 进程是否存在(PIDFILE 里的 pid 还活着)
proc_alive() {
    [ -f "$PIDFILE" ] || return 1
    local pid
    pid="$(cat "$PIDFILE" 2>/dev/null)"
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null
}

# 端口是否在监听(/health 恒返回 200; TGW 会话可用性由进程内 watchdog 负责)
health_ok() {
    if ! command -v curl >/dev/null 2>&1; then
        return 0          # 无 curl 时退化为只判断进程存活
    fi
    curl -fsS --max-time "$HEALTH_TIMEOUT" "$HEALTH_URL" >/dev/null 2>&1
}

# 记录"端口开始不通"的时刻(只在首次失败时写, 后续保留最早时刻)
mark_unhealthy() {
    [ -f "$UNHEALTHY" ] || date +%s > "$UNHEALTHY"
}

clear_unhealthy() { rm -f "$UNHEALTHY"; }

# 已持续不健康多少秒; 无标记返回 -1
unhealthy_age() {
    [ -f "$UNHEALTHY" ] || { echo -1; return; }
    local t0 now
    t0="$(cat "$UNHEALTHY" 2>/dev/null)"
    case "$t0" in ''|*[!0-9]*) echo -1; return ;; esac
    now="$(date +%s)"
    echo $(( now - t0 ))
}

is_running() {
    # 返回 0 表示: 进程在且端口在监听
    proc_alive || return 1
    health_ok
}

start() {
    if proc_alive; then
        # 已有存活进程: 绝不重复拉起(双实例 = 端口冲突的根源)
        log "start skipped: pid=$(cat "$PIDFILE") already alive"
        return 0
    fi
    log "starting http_service (port=$PORT, python=$PYTHON)"
    date +%s > "$UNHEALTHY"
    # 注意 "9>&-": 必须把锁 fd 关掉再交给服务进程。bash 的 `exec 9>file` 不设 CLOEXEC,
    # 子进程会继承 fd 9 并一直持有那把 flock —— 服务只要活着, run.sh 后续每次跑都会
    # `flock -n` 失败而静默退出, 守护彻底失效(2026-09-21 沙箱实测复现)。
    nohup "$PYTHON" "$SCRIPT_DIR/http_service.py" >> "$LOGFILE" 2>&1 9>&- &
    echo $! > "$PIDFILE"
    sleep 2
    if is_running; then
        log "started ok, pid=$(cat "$PIDFILE"), port ready"
        clear_unhealthy
    else
        log "started pid=$(cat "$PIDFILE"), port not ready yet (startup in progress, grace ${GRACE}s)"
    fi
}

stop() {
    if [ -f "$PIDFILE" ]; then
        local pid
        pid="$(cat "$PIDFILE" 2>/dev/null)"
        if [ -n "$pid" ]; then
            log "stopping pid=$pid"
            kill "$pid" 2>/dev/null
            # 最多等 15s 优雅退出
            for _ in $(seq 1 15); do
                kill -0 "$pid" 2>/dev/null || break
                sleep 1
            done
            kill -9 "$pid" 2>/dev/null
        fi
        rm -f "$PIDFILE"
    fi
    clear_unhealthy
    log "stopped"
}

status() {
    if is_running; then
        echo "RUNNING (pid=$(cat "$PIDFILE"), port=$PORT)"
        exit 0
    elif proc_alive; then
        echo "STARTING (pid=$(cat "$PIDFILE"), port=$PORT not listening yet)"
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
        if proc_alive && health_ok; then
            clear_unhealthy         # 端口已就绪, 清掉"不健康"计时
            log "alive, skip"
        elif proc_alive; then
            # 进程在、端口还没监听: 先只计时, 不插手
            mark_unhealthy
            _age="$(unhealthy_age)"
            if [ "$_age" -lt "$GRACE" ]; then
                log "port not ready yet (pid=$(cat "$PIDFILE"), unhealthy ${_age}s/${GRACE}s), wait"
            else
                # 持续不通超过宽限期 -> 判定真卡死, 才重启
                _old="$(cat "$PIDFILE" 2>/dev/null)"
                log "hung: pid=$_old alive but $HEALTH_URL unreachable for ${_age}s (>${GRACE}s), restarting"
                stop
                if [ -n "$_old" ] && kill -0 "$_old" 2>/dev/null; then
                    # 老进程杀不掉时宁可不启, 也不制造双实例撞端口
                    log "ERROR: pid=$_old survived stop; skip start to avoid port conflict"
                else
                    start
                fi
            fi
        else
            clear_unhealthy
            log "not alive, restarting"
            rm -f "$PIDFILE"
            start
        fi
        ;;
    *) echo "usage: $0 [stop|status|restart]"; exit 2 ;;
esac
