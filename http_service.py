"""
AmazingData HTTP 中间层 + 看门狗 + 内置 UI
------------------------------------------------
- REST 查询:  POST /api/query   body={"cls","method","params"}
- 行情推送:    WS   /ws/market  首帧 {"code_list":[...], "period":"snapshot"},
              控制帧 {"type":"add"|"remove","code_list":[...]} 动态增删订阅
- 健康检查:    GET  /health
- 运行指标:    GET  /api/stats  (JSON, 供 UI 拉取)
- 状态面板:    GET  /           (HTML UI, 展示运行时间/数据量/日志)
- 看门狗:      周期保活 + 会话失效时"自重启进程"(不在脏会话内重连, 避免段错误崩进程)

启动方式(建议用 run.sh 配合 crontab 守护, 详见 README.md):
    1) 复制 config.example.ini 为 config.ini, 填入你的 TGW 账号/密码/数据源地址;
    2) 直接运行:
        python3 http_service.py
    3) 或用 uvicorn:
        python3 -m uvicorn http_service:app --host 0.0.0.0 --port 8000
配置来源优先级: 环境变量(TGW_*/HTTP_ 前缀) > config.ini(与脚本同目录) > 默认值。
"""
import os
import sys
import time
import json
import re
import asyncio
import threading
import inspect
import logging
import logging.handlers
import collections
from contextlib import asynccontextmanager

import AmazingData as ad
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from pydantic import BaseModel

# ----------------------------------------------------------------------------
# 配置
# ----------------------------------------------------------------------------
import configparser

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

# 优先读 config.ini(与脚本同目录, 可用环境变量 AMAZINGDATA_CONFIG 指定路径),
# 环境变量可覆盖配置文件中的同名项。请勿把真实凭据写进会提交的 config 文件,
# 只提交 config.example.ini 模板。
_CONFIG_PATH = os.environ.get("AMAZINGDATA_CONFIG") or os.path.join(_APP_DIR, "config.ini")
_CFG = configparser.ConfigParser()
if os.path.isfile(_CONFIG_PATH):
    _CFG.read(_CONFIG_PATH)


def _cfg(section, key, default):
    try:
        return _CFG.get(section, key)
    except (configparser.NoSectionError, configparser.NoOptionError):
        return default


# TGW 数据源登录(即"数据库"入口): 账号/密码/服务器地址/端口
# 注意: 环境变量使用 TGW_ / HTTP_ 前缀, 避免与系统自带的 USER/HOST/PORT 等冲突。
USER = os.environ.get("TGW_USER") or _cfg("tgw", "user", "")
PASSWORD = os.environ.get("TGW_PASSWORD") or _cfg("tgw", "password", "")
HOST = os.environ.get("TGW_HOST") or _cfg("tgw", "host", "127.0.0.1")
PORT = int(os.environ.get("TGW_PORT") or _cfg("tgw", "port", "8600"))

# HTTP 服务监听地址/端口(即运行本脚本的机器)
SERVER_HOST = os.environ.get("HTTP_HOST") or _cfg("server", "host", "0.0.0.0")
SERVER_PORT = int(os.environ.get("HTTP_PORT") or _cfg("server", "port", "8000"))

# ----------------------------------------------------------------------------
# 本地缓存目录
# SDK 内置默认 local_path 是 Windows 路径 "D://AmazingData_local_data//",
# 在 Linux 上会被当作相对路径, 落盘到进程 CWD 下的 D:/AmazingData_local_data(残留脏目录)。
# 这里统一注入规范默认路径(脚本目录/local_data), 仅当方法接受 local_path 且调用方未显式指定时生效;
# 调用方显式传 local_path / is_local=false 的请求不受影响。
# ----------------------------------------------------------------------------
_LOCAL_DATA = _cfg("paths", "local_data", os.path.join(_APP_DIR, "local_data"))
DEFAULT_LOCAL_PATH = os.path.join(_LOCAL_DATA, "") 
if not DEFAULT_LOCAL_PATH.endswith(os.sep):
    DEFAULT_LOCAL_PATH += os.sep
os.makedirs(DEFAULT_LOCAL_PATH, exist_ok=True)

# 缓存目录统计(供 UI 展示, 节流 10s 刷新一次避免每次轮询遍历目录)
_CACHE_STAT = {"size": 0, "files": 0, "ts": 0.0}


def _refresh_cache_stat():
    global _CACHE_STAT
    now = time.time()
    if now - _CACHE_STAT["ts"] < 10:
        return
    size = files = 0
    try:
        for _root, _dirs, _fnames in os.walk(DEFAULT_LOCAL_PATH):
            for _fn in _fnames:
                try:
                    size += os.path.getsize(os.path.join(_root, _fn))
                    files += 1
                except OSError:
                    pass
    except OSError:
        pass
    _CACHE_STAT = {"size": size, "files": files, "ts": now}

# ----------------------------------------------------------------------------
# 日志 -> 内存环形缓冲(供 UI 展示) + 文件
# ----------------------------------------------------------------------------
LOG_BUF = collections.deque(maxlen=500)
REQ_BUF = collections.deque(maxlen=500)   # 请求日志专用缓冲


class _BufHandler(logging.Handler):
    def emit(self, record):
        try:
            LOG_BUF.append({"cat": getattr(record, "cat", "backend"), "text": self.format(record)})
        except Exception:
            pass


class _ReqHandler(logging.Handler):
    """捕获 uvicorn access 请求日志, 过滤掉 /api/stats 轮询."""
    def emit(self, record):
        try:
            text = self.format(record)
            if "/api/stats" in text:
                return
            REQ_BUF.append(text)
        except Exception:
            pass


_root = logging.getLogger()
_root.setLevel(logging.INFO)


class _LogFmt(logging.Formatter):
    """在每条日志里带出 cat (backend/watchdog), 便于进程 execv 自愈重启后
    从持久化日志文件尾部回填 LOG_BUF 时, 仍能区分来源、保持看门狗日志连续不归零."""

    def format(self, record):
        if not hasattr(record, "cat"):
            record.cat = "backend"
        return super().format(record)


_fmt = _LogFmt("%(asctime)s %(levelname)s [%(cat)s] %(message)s",
               datefmt="%Y-%m-%d %H:%M:%S")
_bufh = _BufHandler()
_bufh.setFormatter(_fmt)
_root.addHandler(_bufh)
try:
    # 持久化日志目录: 放到 /var/log/amazingdata 而非 /tmp, 服务器重启后仍能查到原因
    os.makedirs("/var/log/amazingdata", exist_ok=True)
    _LOG_DIR = "/var/log/amazingdata"
except Exception:
    _LOG_DIR = "/tmp"
try:
    # 按体积轮转: 单文件 5MB, 保留最近 3 份 (http_service_app.log + .1/.2/.3)
    _fh = logging.handlers.RotatingFileHandler(
        os.path.join(_LOG_DIR, "http_service_app.log"), maxBytes=5 * 1024 * 1024,
        backupCount=3, encoding="utf-8",
    )
    _fh.setFormatter(_fmt)
    _root.addHandler(_fh)
except Exception:
    pass


def _seed_log_buf():
    """进程启动(execv 自愈重启 / TGW 重登换新进程)时, 从持久化日志文件尾部回填 LOG_BUF,
    使状态面板的看门狗/后端日志在重启后不重新归零, 保持历史连续.
    LOG_BUF 为进程内 deque, 每次 execve 都会清空; 而日志文件是追加+轮转、不会丢,
    故从这里取最近 500 行作为重启后的初始历史."""
    try:
        _path = os.path.join(_LOG_DIR, "http_service_app.log")
        if not os.path.exists(_path):
            return
        with open(_path, "r", encoding="utf-8", errors="replace") as _f:
            _lines = _f.readlines()
        for _ln in _lines[-500:]:
            _ln = _ln.rstrip("\n")
            if not _ln:
                continue
            _m = re.match(
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+ \[(\w+)\] (.*)$", _ln)
            if _m:
                LOG_BUF.append({"cat": _m.group(1), "text": _ln})
            else:
                # 旧格式(部署前无 [cat] 标签): 按消息前缀判断来源,
                # 避免历史看门狗日志被误归为 backend
                _m2 = re.match(
                    r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \w+ (.*)$", _ln)
                _msg = _m2.group(1) if _m2 else _ln
                _cat = "watchdog" if _msg.startswith("watchdog") else "backend"
                LOG_BUF.append({"cat": _cat, "text": _ln})
    except Exception:
        pass


_seed_log_buf()


def log(msg, level=logging.INFO, cat="backend"):
    _root.log(level, msg, extra={"cat": cat})


# ----------------------------------------------------------------------------
# 全局状态 / 指标
# ----------------------------------------------------------------------------
START_TIME = time.time()
STATS = {
    "total_requests": 0,
    "total_data_rows": 0,
    "total_bytes": 0,          # 累计 HTTP 查询响应字节(/api/query)
    "ws_bytes": 0,             # 仅 WS 行情推送累计字节(UI "WS 流量"卡片)
    "error_count": 0,
    "reconnect_count": 0,
    "ws_connections": 0,
    "last_success_ts": None,   # None = 本进程从未成功验证过会话(判活依据)
    "last_error": "",
}
# 当前 TGW 会话建立时间(成功登录并验证后更新):
# 会话重新拉起(execv / systemctl / 进程内重连登录)后重置, 用于 UI "TGW 登录时间"
TGW_LOGIN_TS = None
# 跨重启持久化的累计统计: 写 stats_persist.json, 进程 execv/systemctl 重启后不归零
_STATS_FILE = os.path.join(_APP_DIR, "stats_persist.json")
# 看门狗发起的恢复重启标记(与 systemd 冷启动/真崩溃区分):
# 由 _restart_process 在重启前写入, 新进程启动时读到即沿用 service_start_ts(服务运行时间不归零), 随后删除.
# 进程若真崩溃(看门狗死亡)则来不及写此标记 -> 新进程判为冷启动 -> 归零.
_RECOVERY_MARKER = os.path.join(_APP_DIR, "watchdog_recovery.marker")
_PERSIST_KEYS = ["total_requests", "total_data_rows", "total_bytes", "ws_bytes", "error_count"]
_PERSIST_TS = 0.0
_PERSIST_LOCK = threading.Lock()

# 今日统计(自然日 0 点重置, 跨重启保留; 随 stats_persist.json 持久化)
_TODAY = {"date": "", "ws_bytes": 0, "http_bytes": 0, "requests": 0, "rows": 0}


def _today_date():
    return time.strftime("%Y-%m-%d")


def _bump_today(key, n):
    """今日统计累加; 日期变化时自动重置为新的自然日."""
    today = _today_date()
    if _TODAY["date"] != today:
        _TODAY["date"] = today
        for k in ("ws_bytes", "http_bytes", "requests", "rows"):
            _TODAY[k] = 0
    _TODAY[key] += n


_svc_start = None
try:  # 启动时恢复上次累计值与今日统计(仅当日有效, 跨天自动归零)
    with open(_STATS_FILE) as _f:
        _saved = json.load(_f)
    _stats_part = _saved.get("stats", _saved)   # 兼容旧版平铺格式
    for _k in _PERSIST_KEYS:
        if isinstance(_stats_part.get(_k), int):
            STATS[_k] = _stats_part[_k]
    _t = _saved.get("today")
    if _t and _t.get("date") == _today_date():
        for _k in ("ws_bytes", "http_bytes", "requests", "rows"):
            if isinstance(_t.get(_k), int):
                _TODAY[_k] = _t[_k]
        _TODAY["date"] = _t["date"]
    _svc_start = _saved.get("service_start_ts")
except Exception:
    pass

# 服务进程存活起始时间(UI "服务运行时间"):
# 看门狗发起的恢复重启(login fail / 会话死 -> execv, 或完整退出路径)均沿用持久化 service_start_ts, 不归零;
# 仅看门狗自身崩溃(进程意外死亡, 来不及写恢复标记) / systemd 冷拉起 / 手动重启时才重置.
_RECOVERY = os.path.exists(_RECOVERY_MARKER)
if os.environ.get("_AD_SELF_RESTART") == "1" and _svc_start:
    SERVICE_START_TS = float(_svc_start)
    # 自愈成功标记: 该进程由 watchdog execv 替换而来(PID 不变但 Python 已重新初始化),
    # 与 systemd 冷启动区分开, 便于从日志确认"自愈真的发生过"(2026-08-17 起)。
    # 此时 TGW_LOGIN_TS 会在下方 _startup_login() 中重置为新登录时间。
    log("SELF-RESTART OK: process replaced via execve (new TGW login in _startup_login)",
        cat="watchdog")
elif _RECOVERY and _svc_start:
    # 看门狗发起的恢复重启(含 execv 前先退出的完整退出路径): 服务运行时间延续, 不归零
    SERVICE_START_TS = float(_svc_start)
    log("CONTINUE after watchdog recovery restart (service uptime preserved)", cat="watchdog")
else:
    SERVICE_START_TS = time.time()
# 消费恢复标记: 无论是否沿用, 启动成功后即清除, 避免后续真崩溃被误判为看门狗恢复
try:
    os.remove(_RECOVERY_MARKER)
except FileNotFoundError:
    pass

_RESTARTING = False          # 防止看门狗/请求并发触发多次重启
_KEEPALIVE_INTERVAL = 180    # 每 3 分钟做一次真实查询保活, 更快发现上游会话掉线
# 探针最长执行时间: 会话死时 SDK get_calendar 可阻塞 ~150s(2026-08-17 实测), 
# 超时即判定会话死, 交由 watchdog 判死重启, 不再干等阻塞拖死服务
_KEEPALIVE_TIMEOUT = 20
_LAST_KEEPALIVE = START_TIME

# SDK 并发策略(2026-08-16 压测结论):
#   - MarketData/InfoData 全接口 8 线程并发安全(实测 0 错误 0 串扰);
#   - BaseData 下载型接口(get_*_code_info / get_future_code_list / get_option_code_list /
#     get_etf_pcf 等"每日最新代码表")并发时 SDK 内部下载机制竞争, 返回 None,
#     需全局锁串行化。
_SERIAL_METHODS = {
    "get_code_info", "get_future_code_list", "get_future_code_info",
    "get_option_code_list", "get_etf_pcf",
}
_SDK_LOCK = threading.Lock()
# watchdog 保活探测线程(单飞): 避免 get_calendar 在会话死时阻塞事件循环 ~120s
_KEEPALIVE_THREAD = None
_KEEPALIVE_FAILED = False


def fmt_bytes(n):
    """字节数 -> 人类可读 KB/MB/GB."""
    try:
        n = float(n)
    except Exception:
        return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{int(n)} B"
        n /= 1024.0
    return f"{n:.1f} TB"


def _proc_mem():
    """读取本进程 RSS 与系统内存(总/可用), 字节; 非 Linux 或无 /proc 返回 None."""
    rss = total = avail = None
    try:
        with open("/proc/self/status") as f:
            for _line in f:
                if _line.startswith("VmRSS:"):
                    rss = int(_line.split()[1]) * 1024  # kB -> bytes
                    break
    except OSError:
        pass
    try:
        with open("/proc/meminfo") as f:
            for _line in f:
                if _line.startswith("MemTotal:"):
                    total = int(_line.split()[1]) * 1024
                elif _line.startswith("MemAvailable:"):
                    avail = int(_line.split()[1]) * 1024
    except OSError:
        pass
    return rss, total, avail


# 内存统计(供 UI 展示, 节流 5s 刷新一次, 读 /proc 开销极小)
_MEM_STAT = {"rss": None, "total": None, "avail": None, "used": None, "used_pct": None, "ts": 0.0}


def _refresh_mem_stat():
    global _MEM_STAT
    now = time.time()
    if now - _MEM_STAT["ts"] < 5:
        return
    rss, total, avail = _proc_mem()
    used = (total - avail) if total and avail else None
    pct = round(used / total * 100, 1) if total and avail else None
    _MEM_STAT = {"rss": rss, "total": total, "avail": avail, "used": used,
                 "used_pct": pct, "ts": now}


def _startup_login():
    global TGW_LOGIN_TS
    try:
        # SDK 的 ad.login 打印 login success 但返回 False/None,
        # 不能依赖其返回值判断登录状态, 用一次真实查询验证会话是否可用。
        ad.login(username=USER, password=PASSWORD, host=HOST, port=PORT)
        ad.BaseData().get_calendar()
        TGW_LOGIN_TS = time.time()
        STATS["last_success_ts"] = time.time()
        log("startup login ok=True (verified by get_calendar)")
        return True
    except Exception as e:
        log(f"startup login error: {e!r}", logging.ERROR)
        return False


_startup_login()


# MarketData 构造需要交易日历, 懒加载一次并缓存(K线/快照刚需)
_CAL = None
_CAL_LOCK = threading.Lock()


def _get_calendar():
    global _CAL
    if _CAL is None:
        with _CAL_LOCK:
            if _CAL is None:
                _CAL = ad.BaseData().get_calendar()
    return _CAL


# period 支持传字符串名(如 "day"/"min5"), 自动映射为 SDK 枚举值(如 10008)
_PERIOD_VALUES = {}
try:
    for _n in dir(ad.constant.Period):
        if not _n.startswith("_"):
            _PERIOD_VALUES[_n] = getattr(ad.constant.Period, _n).value
except Exception:
    pass
_PERIOD_NAMES = {v: n for n, v in _PERIOD_VALUES.items()}   # 枚举值 -> 字符串名


def _normalize_period(params):
    p = params.get("period")
    if isinstance(p, str) and p in _PERIOD_VALUES:
        params = dict(params)
        params["period"] = _PERIOD_VALUES[p]
    return params


_RESTART_FILE = "/tmp/http_service_restarts.txt"
_RESTART_COOLDOWN = 120  # 两次 execv 重启最小间隔(秒): 熔断, 防上游故障时无限重启风暴


def _read_restart_meta():
    """重启计数文件格式: '<count> <last_restart_ts>' (二者皆可选)."""
    try:
        with open(_RESTART_FILE) as f:
            parts = f.read().split()
        n = int(parts[0]) if parts else 0
        ts = float(parts[1]) if len(parts) > 1 else 0.0
        return n, ts
    except Exception:
        return 0, 0.0


def _read_restart_count():
    return _read_restart_meta()[0]


def _bump_restart_count():
    n = _read_restart_meta()[0] + 1
    try:
        with open(_RESTART_FILE, "w") as f:
            f.write(f"{n} {time.time()}")
    except Exception:
        pass
    return n


def _last_restart_ts():
    return _read_restart_meta()[1]


def _persist_stats(force=False):
    """节流把累计统计落盘(默认 5s 一次), 进程 execv/systemctl 重启后不归零.
    force=True 用于 execv 自杀前强制写最新值(execv 不经过退出清理)."""
    global _PERSIST_TS
    now = time.time()
    with _PERSIST_LOCK:
        if not force and now - _PERSIST_TS < 5:
            return
        _PERSIST_TS = now
        try:
            with open(_STATS_FILE, "w") as f:
                json.dump({"stats": {k: STATS[k] for k in _PERSIST_KEYS},
                           "today": _TODAY,
                           "service_start_ts": SERVICE_START_TS}, f)
        except Exception:
            pass


def _inprocess_login_retry():
    """上游故障致启动/重登录失败时, 在'全新进程'内按退避重试登录(不 execv).
    此时会话尚未建立任何成功查询, 非脏会话, 重连安全; 上游恢复即翻回 last_success_ts.
    直接避免 8/12 那种 execv 重启风暴."""
    global _LAST_KEEPALIVE, TGW_LOGIN_TS
    delay = 10
    while STATS["last_success_ts"] is None:
        time.sleep(delay)
        try:
            ad.login(username=USER, password=PASSWORD, host=HOST, port=PORT)
            ad.BaseData().get_calendar()
            now = time.time()
            TGW_LOGIN_TS = now
            _LAST_KEEPALIVE = now
            STATS["last_success_ts"] = now
            log("in-process login retry ok (upstream recovered)", cat="watchdog")
            return
        except Exception as e:
            log(f"in-process login retry failed: {e!r} (will retry)", logging.WARNING, cat="watchdog")
            delay = min(delay * 2, 120)


_RETRY_THREAD = None


def _ensure_retry_thread():
    global _RETRY_THREAD
    if _RETRY_THREAD is None or not _RETRY_THREAD.is_alive():
        _RETRY_THREAD = threading.Thread(target=_inprocess_login_retry, daemon=True)
        _RETRY_THREAD.start()


def _restart_process(force=False):
    """干净地用相同环境变量重新拉起自己(全新登录), 避免脏会话内 ad.login 段错误。
    仅当进程健康(_session_alive())时受 60s 年轻保护, 防止 watchdog 启动后立刻连续误触发;
    force=True(订阅换集合的主动重启)不受年轻保护。
    熔断: 冷却期内(_RESTART_COOLDOWN)不再 execv, 改由进程内退避重试, 杜绝重启风暴."""
    global _RESTARTING
    if _RESTARTING:
        return
    if not force and _session_alive() and time.time() - START_TIME < 60:
        log("restart skipped (process too young)", logging.WARNING, cat="watchdog")
        return
    # 熔断: 上游故障时若频繁 execv 会陷入 8/12 式重启风暴; 冷却期内改由进程内重试自愈
    if not force and time.time() - _last_restart_ts() < _RESTART_COOLDOWN:
        if STATS["last_success_ts"] is None:
            _ensure_retry_thread()
        log("restart throttled (cooldown active), in-process retry handling recovery",
            logging.WARNING, cat="watchdog")
        return
    STATS["reconnect_count"] = _bump_restart_count()
    _persist_stats(force=True)   # execv 前强制落盘, 保证重启后统计不丢
    # 写看门狗恢复标记: 标识本次重启由看门狗发起(非崩溃), 供新进程沿用 service_start_ts
    try:
        with open(_RECOVERY_MARKER, "w") as _mf:
            _mf.write(str(SERVICE_START_TS))
    except Exception:
        pass
    _RESTARTING = True
    log("session dead -> restarting process for a fresh TGW login ...", cat="watchdog")
    try:
        # 带 _AD_SELF_RESTART 标记 execv: 新进程据此沿用 SERVICE_START_TS, 服务运行时间不归零
        # 注意: 带环境变量须用 os.execve(3 参), os.execv 只接受 (path, argv) 两个参数,
        # 否则 TypeError('execv expected 2 arguments, got 3') 导致自愈永远失败(2026-08-17 实测).
        _env = os.environ.copy()
        _env["_AD_SELF_RESTART"] = "1"
        os.execve(
            sys.executable,
            [sys.executable, "-m", "uvicorn", "http_service:app",
             "--host", "0.0.0.0", "--port", "8000"],
            _env,
        )
    except Exception as e:
        _RESTARTING = False
        log(f"restart failed: {e!r}", logging.ERROR, cat="watchdog")


STATS["reconnect_count"] = _read_restart_count()


# ----------------------------------------------------------------------------
# 看门狗: 周期保活 + 会话失效自重启
# ----------------------------------------------------------------------------
def _session_alive():
    """以'最近成功请求'判断会话是否可用; last_success_ts 为 None 表示从未成功过, 直接判死."""
    ts = STATS["last_success_ts"]
    if ts is None:
        return False
    return (time.time() - ts) < 900


def _keepalive_probe():
    """独立线程里做保活探测, 避免 get_calendar 在会话死时阻塞事件循环 ~120s.
    单飞由 watchdog_loop 的线程存活判断保证; 失败/超时置 _KEEPALIVE_FAILED,
    由 watchdog 下个 tick(≤30s) 判死重启.
    get_calendar 在会话死时可能阻塞 ~150s(2026-08-17 实测), 这里用子线程 + join
    限时 _KEEPALIVE_TIMEOUT, 超时不等 SDK 返回直接判死, 交给 execv 重启换新进程."""
    global _LAST_KEEPALIVE, _KEEPALIVE_FAILED
    _t0 = time.time()
    _holder = {}

    def _do_probe():
        try:
            with _SDK_LOCK:   # 与下载型串行接口互斥, 避免探针与查询竞争
                ad.BaseData().get_calendar()
            _holder["ok"] = True
        except Exception as e:
            _holder["err"] = e

    _t = threading.Thread(target=_do_probe, daemon=True)
    _t.start()
    _t.join(_KEEPALIVE_TIMEOUT)
    if _t.is_alive():
        # SDK 仍卡在 get_calendar: 不再等, 立即判死交由 execv 重启(进程替换后卡住线程自然消失)
        log(f"watchdog tgw keepalive TIMEOUT after {_KEEPALIVE_TIMEOUT}s (session dead)",
            logging.ERROR, cat="watchdog")
        _KEEPALIVE_FAILED = True
        return
    if _holder.get("err") is not None:
        log(f"watchdog tgw keepalive failed: {_holder['err']!r} (took {time.time()-_t0:.1f}s)",
            logging.ERROR, cat="watchdog")
        _KEEPALIVE_FAILED = True
        return
    _LAST_KEEPALIVE = time.time()
    STATS["last_success_ts"] = time.time()
    log(f"watchdog: tgw keepalive ok (took {time.time()-_t0:.1f}s)", cat="watchdog")


async def watchdog_loop():
    global _KEEPALIVE_THREAD, _KEEPALIVE_FAILED
    log("watchdog started", cat="watchdog")
    while True:
        await asyncio.sleep(30)
        try:
            alive = _session_alive()
            now = time.time()
            # 保活: 会话还活着且距上次保活已超过间隔, 丢独立线程做真实查询(不阻塞事件循环)
            if alive and now - _LAST_KEEPALIVE > _KEEPALIVE_INTERVAL:
                if _KEEPALIVE_THREAD is None or not _KEEPALIVE_THREAD.is_alive():
                    _KEEPALIVE_THREAD = threading.Thread(target=_keepalive_probe, daemon=True)
                    _KEEPALIVE_THREAD.start()
            # 探针失败 -> 判死; 或 keepalive 成功刷新了 last_success_ts, 重新评估
            if _KEEPALIVE_FAILED:
                _KEEPALIVE_FAILED = False
                alive = False
            if not alive:
                log("watchdog: session dead -> restarting process", cat="watchdog")
                _restart_process()
            else:
                log("watchdog: session alive", cat="watchdog")
        except Exception as e:
            log(f"watchdog error: {e!r}", logging.ERROR, cat="watchdog")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 让 uvicorn 的 access 日志(GET/POST 请求)进独立请求日志缓冲(已过滤 /api/stats 轮询)
    # uvicorn 启动时会用默认 dictConfig 重置 logger, 故此处直接挂 handler 而非依赖 propagate
    try:
        _acc = logging.getLogger("uvicorn.access")
        _reqh = _ReqHandler()
        _reqh.setFormatter(_fmt)
        _acc.handlers = [_reqh]
        _acc.propagate = False
        log("request logging attached to req buffer")
    except Exception as e:
        log(f"attach access logger failed: {e!r}", logging.ERROR)
    task = asyncio.create_task(watchdog_loop())
    _persist_stats(force=True)   # 启动即落盘 SERVICE_START_TS, 保证后续 execv 自愈能沿用
    yield
    task.cancel()
    _persist_stats(force=True)   # 优雅关闭(systemctl restart/stop)前强制落盘最新统计


# ----------------------------------------------------------------------------
# 工具: 序列化 / 计数
# ----------------------------------------------------------------------------
def to_jsonable(x):
    if x is None:
        return None
    if isinstance(x, list):
        return [to_jsonable(i) for i in x]
    if hasattr(x, "model_dump"):  # pydantic
        try:
            return to_jsonable(x.model_dump())
        except Exception:
            return str(x)
    if hasattr(x, "to_dict"):      # pandas DataFrame / Series
        try:
            # DataFrame 若带语义化 index(如 get_code_info 的 code_market 股票代码),
            # orient="records" 会丢弃 index, 这里先 reset_index 保留为普通列;
            # 默认 RangeIndex(无意义行号) 与 Series 保持原行为。
            if hasattr(x, "columns") and getattr(x.index, "name", None) is not None \
                    and type(x.index).__name__ != "RangeIndex":
                return to_jsonable(x.reset_index().to_dict(orient="records"))
            return to_jsonable(x.to_dict(orient="records"))
        except Exception:
            return str(x)
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    # 递归清洗: 递归进到叶子, 处理 DataFrame 行内 NaN/Inf(如 get_stock_basic 的
    # DELISTDATE), 避免 FastAPI JSONResponse(allow_nan=False) 渲染时裸 500。
    if isinstance(x, float) and (x != x or x == float("inf") or x == float("-inf")):
        return None
    if isinstance(x, (str, int, float, bool)):
        return x
    return str(x)


def count_rows(x):
    if hasattr(x, "model_dump"):
        return 1
    if isinstance(x, list):
        return len(x)
    if hasattr(x, "to_dict") and hasattr(x, "__len__"):  # DataFrame
        try:
            return len(x)
        except Exception:
            return 1
    if isinstance(x, dict):
        return 1
    return 1


# ----------------------------------------------------------------------------
# FastAPI 应用
# ----------------------------------------------------------------------------
app = FastAPI(title="AmazingData HTTP", lifespan=lifespan)


@app.get("/health")
def health():
    alive = _session_alive()
    return {"status": "ok" if alive else "error", "login": alive}


@app.get("/api/stats")
def api_stats():
    _refresh_cache_stat()
    _refresh_mem_stat()
    uptime = int(time.time() - START_TIME)
    return {
        "status": "ok" if _session_alive() else "degraded",
        "login": STATS["last_success_ts"] is not None,
        "uptime_seconds": uptime,
        "start_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(START_TIME)),
        "service_uptime_seconds": int(time.time() - SERVICE_START_TS),
        "service_start": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(SERVICE_START_TS)),
        "tgw_login_seconds": None if TGW_LOGIN_TS is None
                             else int(time.time() - TGW_LOGIN_TS),
        "total_requests": STATS["total_requests"],
        "total_data_rows": STATS["total_data_rows"],
        "total_bytes": STATS["total_bytes"],
        "total_bytes_human": fmt_bytes(STATS["total_bytes"]),
        "ws_bytes": STATS["ws_bytes"],
        "ws_bytes_human": fmt_bytes(STATS["ws_bytes"]),
        "today": {
            "date": _TODAY["date"],
            "ws_bytes": _TODAY["ws_bytes"],
            "ws_bytes_human": fmt_bytes(_TODAY["ws_bytes"]),
            "http_bytes": _TODAY["http_bytes"],
            "http_bytes_human": fmt_bytes(_TODAY["http_bytes"]),
            "requests": _TODAY["requests"],
            "rows": _TODAY["rows"],
        },
        "error_count": STATS["error_count"],
        "reconnect_count": STATS["reconnect_count"],
        "ws_connections": STATS["ws_connections"],
        "last_success": "never" if STATS["last_success_ts"] is None
                        else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(STATS["last_success_ts"])),
        "last_error": STATS["last_error"],
        "cache_bytes": _CACHE_STAT["size"],
        "cache_bytes_human": fmt_bytes(_CACHE_STAT["size"]),
        "cache_files": _CACHE_STAT["files"],
        "cache_path": DEFAULT_LOCAL_PATH,
        "mem_rss": _MEM_STAT["rss"],
        "mem_rss_human": fmt_bytes(_MEM_STAT["rss"]) if _MEM_STAT["rss"] else "-",
        "mem_total": _MEM_STAT["total"],
        "mem_total_human": fmt_bytes(_MEM_STAT["total"]) if _MEM_STAT["total"] else "-",
        "mem_available": _MEM_STAT["avail"],
        "mem_available_human": fmt_bytes(_MEM_STAT["avail"]) if _MEM_STAT["avail"] else "-",
        "mem_used": _MEM_STAT["used"],
        "mem_used_human": fmt_bytes(_MEM_STAT["used"]) if _MEM_STAT["used"] else "-",
        "mem_used_pct": _MEM_STAT["used_pct"],
        "logs": [x["text"] for x in list(LOG_BUF)[-80:]],
        "logs_watchdog": [x["text"] for x in list(LOG_BUF) if x["cat"] == "watchdog"][-80:],
        "logs_backend": [x["text"] for x in list(LOG_BUF) if x["cat"] == "backend"][-80:],
        "logs_request": list(REQ_BUF)[-80:],
    }


class QueryReq(BaseModel):
    cls: str
    method: str
    params: dict = {}


@app.post("/api/query")
def api_query(q: QueryReq):
    # 同步 def: FastAPI 丢线程池执行, 避免同步 SDK 调用阻塞 uvicorn 事件循环
    # (慢查询不再冻结 /api/stats、/health、WS 推送; 查询之间在线程池并发)
    STATS["total_requests"] += 1
    _bump_today("requests", 1)
    try:
        if q.cls == "MarketData":
            inst = ad.MarketData(_get_calendar())   # 构造需交易日历
        else:
            inst = getattr(ad, q.cls)()
        func = getattr(inst, q.method)
        params = _normalize_period(q.params)
        # 统一注入默认参数(仅当方法签名接受且调用方未显式指定时):
        #  - local_path: 让缓存落到 config.ini 中 paths.local_data 指定的规范目录
        #  - is_local=false: 每次从 TGW 拉最新数据(同时更新本地缓存), 避免读到旧缓存
        if "local_path" not in params or "is_local" not in params:
            try:
                _sig = inspect.signature(func).parameters
                if "local_path" not in params and "local_path" in _sig:
                    params["local_path"] = DEFAULT_LOCAL_PATH
                if "is_local" not in params and "is_local" in _sig:
                    params["is_local"] = False
            except (ValueError, TypeError):
                pass
        # BaseData 下载型接口并发不安全(实测 SDK 内部下载机制竞争返回 None), 串行化;
        # 其余接口(MarketData/InfoData)实测并发安全, 无锁直接并发
        if q.cls == "BaseData" and q.method in _SERIAL_METHODS:
            with _SDK_LOCK:
                result = func(**params)
        else:
            result = func(**params)
        rows = count_rows(result)
        data = to_jsonable(result)
        _http_bytes = len(json.dumps(data, ensure_ascii=False, default=str).encode("utf-8"))
        STATS["total_bytes"] += _http_bytes
        STATS["total_data_rows"] += rows
        _bump_today("http_bytes", _http_bytes)
        _bump_today("rows", rows)
        STATS["last_success_ts"] = time.time()
        _persist_stats()
        return {"ok": True, "rows": rows, "data": data}
    except Exception as e:
        STATS["error_count"] += 1
        STATS["last_error"] = str(e)
        _persist_stats()
        log(f"query {q.cls}.{q.method} error: {e!r}", logging.ERROR)
        # 若会话已失效, 触发进程自重启(全新登录), 不在脏会话内重连
        if not _session_alive():
            _restart_process()
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    except BaseException as e:
        # SDK 部分方法内部会抛 SystemExit 等 BaseException, 穿透 except Exception 会变成裸 500
        STATS["error_count"] += 1
        STATS["last_error"] = f"{type(e).__name__}: {e}"
        _persist_stats()
        log(f"query {q.cls}.{q.method} base-exception: {e!r}", logging.ERROR)
        if not _session_alive():
            _restart_process()
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": f"{type(e).__name__}: {e}"})


# ----------------------------------------------------------------------------
# 实时订阅管理: 全局单订阅(多客户端共享) + 动态增删
# ----------------------------------------------------------------------------
# TGW 单账号同一时间只支持一个订阅会话: SDK 的 sub.run() 会阻塞一直占用,
# 且无干净的停止/退订接口(实测 tgw.UnSubscribe 也不会把数据流释放给新的
# Subscribe)。历史实现每次 WS 连接都 new 一个 SubscribeData + daemon 线程跑
# sub.run(), 客户端断开后线程不回收, 第一个订阅线程永久霸占会话, 后续任何
# 新订阅都收不到 tick。
# 这里改为: 全局唯一 SubscribeData + 唯一 run() 线程; 各客户端把自身 asyncio
# 队列注册到标的名下, on_tick 按 data.code 扇出到所有注册队列。
#
# 动态增删(方案A, 2026-08-13; 2026-08-14 去掉持久化):
#   - remove: 纯客户端过滤, 立即生效(把本连接队列从该标的注册表摘除, 不再转发)。
#   - add(集合内): 立即生效(标的已在全局订阅流里, 只是开启转发)。
#   - add(集合外): 全局订阅流无法动态扩, 需重启进程(os.execv 全新登录)换集合;
#     重启后全局集合 = 重启后首连客户端请求的 code_list。
#     代价: 约 10s 中断, 所有客户端需重连。
_SUB_LOCK = threading.Lock()
_SUB = {
    "instance": None,   # ad.SubscribeData 单例
    "thread": None,     # 唯一 sub.run() 线程
    "codes": set(),     # 已订阅的 code 集合(重启后 = 首连请求决定, 不持久化)
    "period": None,     # 已订阅的 period 枚举值(首帧锁定)
    "period_name": None,  # period 字符串名
    "queues": {},       # code -> set(asyncio.Queue)  各客户端订阅队列
}


def _period_name(value):
    return _PERIOD_NAMES.get(value, str(value))


def _start_subscription(code_list, period_value, loop):
    """启动全局唯一订阅。须持 _SUB_LOCK 调用; 重复调用返回 (False, 原因)。
    实际订阅集合 = 请求集合(首连客户端 code_list, 不合并任何历史持久化)。"""
    if _SUB["instance"] is not None:
        return False, "subscription already started"
    merged = sorted(set(code_list))
    sub = ad.SubscribeData()

    @sub.register(code_list=merged, period=period_value)
    def on_tick(data, period):
        try:
            code = getattr(data, "code", None)
            if not code:
                return
            with _SUB_LOCK:
                qs = list(_SUB["queues"].get(code, ()))
            for q in qs:
                try:
                    loop.call_soon_threadsafe(q.put_nowait, data.model_dump())
                except Exception:
                    pass
        except Exception:
            pass

    _SUB["instance"] = sub
    _SUB["codes"] = set(merged)
    _SUB["period"] = period_value
    _SUB["period_name"] = _period_name(period_value)
    _SUB["thread"] = threading.Thread(target=sub.run, daemon=True,
                                      name="subscribe-run")
    _SUB["thread"].start()
    log(f"global subscription started: codes={merged} period={_SUB['period_name']}")
    return True, ""


@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    """WS 协议:
    首帧: {"code_list":[...], "period":"snapshot"}  -> 订阅(定义/加入全局集合)
    控制帧(首帧之后任意帧):
        {"type":"add", "code_list":[...]}           -> 增订(集合外则重启扩容)
        {"type":"remove", "code_list":[...]}        -> 退订(本连接立即生效)
    服务端推送:
        {"type":"tick", "data":{...}}               行情
        {"type":"ack", "action":"subscribe|add|remove", ...} 操作确认
        {"type":"error", "message":...}
        {"type":"restarting", "message":...}        集合外增订触发服务重启
    """
    await ws.accept()
    STATS["ws_connections"] += 1
    loop = asyncio.get_event_loop()
    q: "asyncio.Queue" = asyncio.Queue()
    my_codes = set()          # 本连接当前订阅的标的
    reader_task = None

    async def send(obj):
        await ws.send_text(json.dumps(obj, ensure_ascii=False, default=str))

    def attach(codes):
        """把本连接队列挂到标的名下(立即开始接收该标的 tick)。"""
        nonlocal my_codes
        for c in codes:
            _SUB["queues"].setdefault(c, set()).add(q)
        my_codes |= set(codes)

    def detach(codes):
        """把本连接队列从标的名下摘除(停止接收该标的 tick), 不影响全局订阅流。"""
        nonlocal my_codes
        for c in codes:
            qs = _SUB["queues"].get(c)
            if qs:
                qs.discard(q)
                if not qs:
                    _SUB["queues"].pop(c, None)
            my_codes.discard(c)

    async def restart_for_new_codes(new_codes, missing):
        """集合外增订: 重启进程(os.execv 替换本进程, 不返回), 重启后集合由首连决定。
        若因进程过新被跳过, 回 error 帧。"""
        await send({"type": "restarting",
                    "message": f"新增标的 {missing} 不在订阅集合, 服务即将重启换集合, "
                               f"约 10 秒后请重新连接; 新集合={sorted(new_codes)}"})
        log(f"subscribe restart triggered: new codes={sorted(new_codes)}",
            logging.WARNING)
        await asyncio.sleep(0.3)   # 让 restarting 帧先发出去
        _restart_process(force=True)   # 用户主动换集合, 不受年轻保护
        if not _RESTARTING:
            await send({"type": "error",
                        "message": "重启被跳过(已有重启在进行), 请稍后重试"})

    async def reader():
        """读取控制帧: add / remove。"""
        try:
            while True:
                text = await ws.receive_text()
                try:
                    cmd = json.loads(text)
                except Exception:
                    continue
                action = cmd.get("type")
                codes = [c for c in cmd.get("code_list", [])
                         if isinstance(c, str) and c]
                if action == "add" and codes:
                    with _SUB_LOCK:
                        missing = sorted({c for c in codes if c not in _SUB["codes"]})
                        if missing:
                            new_codes = sorted(set(_SUB["codes"]) | set(missing))
                        else:
                            attach(codes)
                    if missing:
                        await restart_for_new_codes(new_codes, missing)
                    else:
                        await send({"type": "ack", "action": "add",
                                    "added": sorted(codes)})
                        log(f"ws add codes={sorted(codes)}")
                elif action == "remove" and codes:
                    with _SUB_LOCK:
                        removed = sorted(c for c in codes if c in my_codes)
                        detach(removed)
                    await send({"type": "ack", "action": "remove",
                                "removed": removed})
                    log(f"ws remove codes={removed}")
                elif action:
                    await send({"type": "error",
                                "message": f"unknown command: {action}"})
        except Exception:
            pass   # 连接关闭

    try:
        cfg = await ws.receive_json()
        if not isinstance(cfg, dict) or "code_list" not in cfg:
            await send({"type": "error", "message": "首帧必须包含 code_list"})
            return
        code_list = list(dict.fromkeys(cfg.get("code_list", [])))
        period = cfg.get("period", "snapshot")
        try:
            period_value = getattr(ad.constant.Period, period).value
        except AttributeError:
            await send({"type": "error", "message": f"unknown period: {period}"})
            return
        err = None
        missing = []
        with _SUB_LOCK:
            if _SUB["instance"] is None:
                ok, err = _start_subscription(code_list, period_value, loop)
                if ok:
                    attach(code_list)
            else:
                if period_value != _SUB["period"]:
                    err = (f"当前订阅 period={_SUB['period_name']}, "
                           f"请求 {period} 不一致")
                else:
                    missing = sorted({c for c in code_list if c not in _SUB["codes"]})
                    if missing:
                        new_codes = sorted(set(_SUB["codes"]) | set(missing))
                    else:
                        attach(code_list)
        if err:
            await send({"type": "error", "message": err})
            log(f"ws subscribe rejected: {err}", logging.WARNING)
            return
        if missing:
            await restart_for_new_codes(new_codes, missing)
            return   # 进程将被 execv 替换, 不再继续
        log(f"ws client attached: code_list={code_list} period={period} "
            f"(global codes={sorted(_SUB['codes'])})")
        await send({"type": "ack", "action": "subscribe",
                    "codes": sorted(my_codes),
                    "global_codes": sorted(_SUB["codes"])})
        reader_task = asyncio.create_task(reader())
        # 并发等待「行情 tick」与「reader 任务结束」: 客户端一断开, reader 任务
        # 立即结束, 主循环随之退出, 不再卡在 q.get() 等到下一个 tick 才撞墙
        # (那种场景会触发 send-after-close 误报)。
        while True:
            get_task = asyncio.ensure_future(q.get())
            done, pending = await asyncio.wait(
                {get_task, reader_task}, return_when=asyncio.FIRST_COMPLETED)
            if reader_task in done:
                # 客户端已断开(reader 退出)
                for t in pending:
                    t.cancel()
                log("ws client disconnected")
                break
            data = get_task.result()
            if data.get("code") not in my_codes:
                continue    # remove 后队列里残留的旧 tick, 直接丢弃
            # tick 数据含 datetime 等非 JSON 原生类型, send_json 会抛
            # TypeError('Object of type datetime is not JSON serializable'),
            # 必须经 to_jsonable() 递归清洗后再发文本帧。
            payload = {"type": "tick", "data": to_jsonable(data)}
            text = json.dumps(payload, ensure_ascii=False, default=str)
            # total_bytes 只计 HTTP 查询响应; WS 推送流量单独计 ws_bytes
            STATS["ws_bytes"] += len(text.encode("utf-8"))
            _bump_today("ws_bytes", len(text.encode("utf-8")))
            STATS["last_success_ts"] = time.time()   # tick 能到 = 会话活, 计入判活
            _persist_stats()
            try:
                await ws.send_text(text)
            except Exception as e:
                # 客户端已断开: Starlette 在 send 时抛 WebSocketDisconnect,
                # 或连接已 CLOSED 后尝试 send 抛
                # "Unexpected ASGI message 'websocket.send', after sending 'websocket.close'"。
                # 这是断连的善后噪音, 降级为 INFO, 不再误报成 ERROR。
                name = type(e).__name__
                s = str(e)
                if name == "WebSocketDisconnect" or "websocket" in s:
                    log(f"ws send skipped (client gone): {name}")
                else:
                    log(f"ws send error: {e!r}", logging.ERROR)
                break
    except Exception as e:
        # WebSocketDisconnect 是客户端正常断开, 属常规事件, 记 INFO 即可
        if type(e).__name__ == "WebSocketDisconnect":
            log(f"ws client disconnected: {e!r}")
        else:
            log(f"ws handler error: {e!r}", logging.ERROR)
    finally:
        if reader_task:
            reader_task.cancel()
        with _SUB_LOCK:
            detach(sorted(my_codes))
        STATS["ws_connections"] = max(0, STATS["ws_connections"] - 1)


# ----------------------------------------------------------------------------
# UI 面板
# 前后端分离: 页面为独立静态文件 index.html(与 http_service.py 同目录),
# 修改 UI 只需覆盖该文件并刷新浏览器, 无需重启 server。
# 仅当静态文件缺失时回退到内嵌的极简占位页, 避免整个服务因缺文件而挂。
# ----------------------------------------------------------------------------
_UI_FILE = os.path.join(_APP_DIR, "index.html")
_FALLBACK_HTML = ("<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
                  "<title>AmazingData</title></head><body><h1>AmazingData 服务面板</h1>"
                  "<p style='color:#f85149'>index.html 缺失, 请检查部署。</p></body></html>")


@app.get("/")
def ui():
    """返回面板页面(独立静态文件 index.html, 改 UI 无需重启 server)."""
    if os.path.isfile(_UI_FILE):
        return FileResponse(_UI_FILE, headers={"Cache-Control": "no-cache"})
    return HTMLResponse(_FALLBACK_HTML)


# ----------------------------------------------------------------------------
# 直跑入口
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)
