"""
AmazingData HTTP 中间层 + 看门狗 + 内置 UI
------------------------------------------------
- REST 查询:  POST /api/query   body={"cls","method","params"}
- 行情推送:    WS   /ws/market  首帧 {"code_list":[...], "period":"snapshot"},
              控制帧 {"type":"add"|"remove","code_list":[...]} 动态增删订阅
              code_list 传 "all"(或 {"all":true}) = 全市场全推
- 健康检查:    GET  /health
- 运行指标:    GET  /api/stats  (JSON, 供 UI 拉取)
- 状态面板:    GET  /           (HTML UI, 展示运行时间/数据量/日志)
- 看门狗:      周期保活 + 会话失效时"自重启进程"(不在脏会话内重连, 避免段错误崩进程)

启动方式(建议用 run.sh 配合 crontab 守护, 详见 README.md):
    1) 编辑与脚本同目录的 config.ini, 填入你的 TGW 账号/密码/数据源地址(无需复制模板);
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
import datetime
import glob
import fcntl
import shutil
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
# 环境变量可覆盖配置文件中的同名项。config.ini 是唯一配置来源(直接改内容, 不再有
# config.example.ini), 填了真实凭据后不要提交到公开仓库。
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

# ---------------------------------------------------------------------------
# 判活策略 v5 (2026-09-21 晚, 三次修正): 「Push 心跳 = 会话存活; 判死必须有物证」
# ---------------------------------------------------------------------------
# v4 的错(本轮修掉, 代价是 18:44 / 18:57 / 19:27 三次白重启):
#   把 `Query Heartbeat Check | Receive <Heartbeat> from DGW!` 当成"会话心跳"。
#   实测证明: **这条心跳只证明"查询通道当前已建立", 不证明会话存活** —— SDK 在查询通道
#   空闲约 10 分钟后会主动把它回收, 日志里留下:
#       19:24:10  MDGA Info | Time check | QueryImpl is free, will release
#       19:24:10  MDGA Info | Release status | Release QueryImpl succesful
#   此后 "Query Heartbeat Check" 彻底消失, 直到下一次真实查询才重建通道。
#   全天这类回收 9 次(间隔 11~17 分钟), 而三次判死重启与其中三次**一一对应**:
#       17:50:35 回收 -> 探针 35s 内跟上, 探针成功            -> 未判死
#       18:01:54 回收 -> 探针 42s 内跟上, 成功(16.8s)          -> 未判死
#       18:12:53 回收 -> 探针 43s 内跟上, 成功(4.1s)           -> 未判死
#       18:41:08 回收 -> strike 1/3 -> 2/3 -> 3/3 @18:44:08    -> 判死重启 18:44:10  [白]
#       18:54:41 回收 -> strike 1/3 -> 2/3 -> 3/3 @18:57:41    -> 判死重启 18:57:45  [白]
#       19:08:47 回收 -> 真实查询 12s 前刚成功 => recent_ok    -> 未判死
#       19:24:10 回收 -> strike 1/3 -> 2/3 -> 3/3 @19:27:36    -> 判死重启 19:27:39  [白]
#       19:38:10 回收 -> strike 1/3 @19:40:09 -> 真实查询到达, 心跳恢复 -> 链条断(差 2 格)
#       19:55:17 回收 -> v5 已上线: 全程 "session alive (push 9s / query ...)" -> 0 strike
#   注意 4 次触发链的事件(18:41 / 18:54 / 19:24 / 19:38)与其前后两次的差别**不在会话健康度**,
#   而在于那几次恰好"确实空闲"—— 上游 10 分钟没被用过 => 回收 => 心跳停, 同时"近期无成功查询"
#   也成立。判死的两个条件被"空闲"这一**正常状态**耦合在一起了, 这才是根因。
#   同一时刻 **Push Heartbeat 从未中断**(整点小时恰好 360 条 = 每 10s 一条; 全天 12 个小时
#   零缺失; 唯一一次间断是 08:25:13~08:32:26 的真故障, 当时 Push 与 Query 同时变成
#   `Session isn't logon!`)。即会话一直是活的, 我们在拿一把"空闲即释放"的尺子量生死。
#   最终判定(19:55:17 那次回收, v5 线上实测): 31 轮轮询 dead=0 / strike=0 / RESTART=0,
#   Query 年龄一路涨到 149s 而日志始终只打印 session alive —— 同一时段(v3/v4)本该触发
#   第 4 次白重启。这就是"换对了心跳口径"的直接对照。
# v6 (2026-09-22): **判活判死回归第一代口径 —— 真实查询说了算, 心跳只做观测。**
#
# 为什么要撤掉 v5 那条"A. Push 心跳新鲜 => 判活"(v5 的第一判据):
#   v5 让"会话级健康"给"功能级可用"背书。2026-09-22 凌晨的故障证明这个背书是错的 ——
#     09-21 23:34 起查询通道被上游拒令牌(`Authentication for this token fails`),
#     QueryImpl 进入 0.2s 一轮的无限重连(8 小时 8.5 万次), **所有数据查询 100% 失败**;
#     而 Push 心跳每小时恰好 360 条、一条不缺, `/health` 全程 status=ok、login=true。
#     看门狗 8 小时里 9 次探针 0 成功, 每一次都被 v5 那句
#     "probe timeout/error BUT upstream heartbeat is fresh -> treat as alive" 放行,
#     一次都没能判死。**心跳新鲜只证明连接在, 不证明数据取得到。**
#
# v6 口径:
#   判活 = 真实查询成功(探针 / 业务查询)     [L3 功能级证据]
#   判死 = 探针两重都失败(连续 3 轮)         [L3 功能级证据]
#   心跳 = 纯观测(仍读、仍打印、仍暴露在 /health 与 /api/stats, **但不参与任何判定**)
#
# 探针改成两重, 第一重成功即止(不再查第二重):
#   第一重 InfoData.get_treasury_yield(y10 单码, 20250101~20250102, is_local=False)
#     - 载荷极小(get_calendar 要拉 8730 行 / 78KB), 且 is_local=False 强制走上游查询
#       通道 => 它验证的正是"查询通道能不能用"(实测: 通道坏时它挂住不返回,
#       SDK 侧同时在刷 `Authentication for this token fails`);
#     - 实测字节码顶层无 lock/acquire/release, 不取 SDK 库内锁。
#   第二重 BaseData.get_calendar() —— 第一重没拿到成功证据时的兜底(同样不取库内锁)。
#   两重超时相加(20 + 60 = 80s) < _KEEPALIVE_TIMEOUT(90s)。
#
# 唯一一处"豁免"(与 v5 有本质区别, 见 _QUERY_OK_GRACE 的注释):
#   探针两重都失败时, 若**业务查询**在 _QUERY_OK_GRACE 秒内刚成功过, 本轮不计 strike。
#   这是两条**真实查询证据互相矛盾**时的仲裁; v5 那种是拿**心跳**(根本不是查询证据)
#   去豁免探针失败。空闲时段永远不会满足这条(空闲时没有成功的业务查询) =>
#   不会重蹈 v3/v4"空闲 => 凑出死亡证据"的覆辙。
#
# 仍然保留的两条(v5 里依然正确的部分):
#   * Query 心跳被 SDK 回收(空闲满 ~10 分钟)是正常现象, 永远不能当死亡证据;
#   * 探针线程存活 >_PROBE_WEDGE_MAX => wedge, 即使心跳新鲜也强制重启(清线程与库内锁)。
_HB_FRESH_MAX = 45           # Push 心跳新鲜阈值(秒) —— v6 起**仅用于展示与日志**, 不判活
_HB_DEAD_MAX = 120           # v6 起不再参与判定(保留供人工读日志时对照)
_HB_SUSPECT_AGE = 300        # 诊断用: Push 心跳陈旧这么久而真实查询仍持续成功 => 标记可疑
_HB_CACHE_TTL = 10           # 心跳日志读取缓存(秒), 看门狗每轮都用也不读盘
_HB_TAIL_BYTES = 262144      # 只读 SDK 日志尾部(全天约 3.5MB, 读尾部够用)
_SDK_LOG_DIR = os.environ.get("AMAZINGDATA_SDK_LOG_DIR", "/root/mdga_file/log")
_HB_PUSH = "Push"            # 心跳类型: 会话级(推送通道, 持续收发)
_HB_QUERY = "Query"          # 心跳类型: 查询通道级(空闲 ~10min 被 SDK 回收)

# ---------------------------------------------------------------------------
# v2 (已废弃) 的错: 探针 = `with _SDK_LOCK: get_calendar()`, 拿"探针耗时"当判死依据。
#   * get_calendar 会拉 1990 至今全量日历 —— 2 个上游请求 / 8730 行 / 78KB,
#     全天 5877 次探针里 1440 次 >20s、317 次 >45s, 且**呈双峰**(<3s 或 >20s, 中间
#     3~20s 一次都没有)。探针耗时只反映上游负载, 与会话是否存活无关。
#   * 16:43~16:46 因此被连续误判三次"会话死" -> execv 白重启一次。而同一时刻
#     SDK 日志里上游心跳每 10s 一次零缺失, 全天 OnClose 只在 08:28 出现过。
# v3 的错(本轮修掉): 为了解决"探针太慢", 探针换成了 MarketData.query_kline(中位 0.13s)。
#   但**换探针时没有重新数锁**:
#     * query_kline 在 SDK 内部会 acquire 一把**模块级全局锁 QueryLock**
#       (实测字节码: offset 24 LOAD_ATTR acquire -> offset 3066 LOAD_ATTR release,
#        持锁横跨嵌套 get_kline_data 里一个**无超时**的 EnvKline.wait_event.wait());
#     * 探针一旦卡住, 这条线程就**永久持有 QueryLock** -> 用户所有 kline/snapshot
#       请求全部被堵死(v3 里 v2 那句"这种调用绝不能当探针"被违背了);
#     * 更糟: 看门狗遇到"探针线程还活着"只 continue, 又因为"心跳新鲜=>判活"短路,
#       永不重启去清理 -> 永久卡死、无自愈。
#     * 线上实测 `keepalive probe still running, wait` 出现 26 次(v2 探针时代),
#       最长盲区约 12 分钟(15:52:59 -> 16:04:59 / 16:05:59 -> 16:17:59)。
#       典型现场: TIMEOUT after 45s -> strike 1/3 -> still running, wait -> TIMEOUT
#                 -> strike 2/3 -> still running, wait -> keepalive ok (took 37.6s)
#       —— 会话全程活着, 代码在数 strike。
# v4 保留的改动(仍然有效):
#   1) 探针 = **不取 SDK 库内锁**的 BaseData.get_calendar(实测其字节码顶层完全没有
#      lock/acquire/release)。因为主判据是心跳, "探针快不快"不决定判死结论 ——
#      所以选探针的标准是"哪个不抢库内锁"; 超时 90s(实测 p100=33.3s, 留 2.7 倍余量)。
#   2) wedge 检测: 探针线程存活超过 _PROBE_WEDGE_MAX => 判定线程**卡死**(而非"慢")。
#      Python 杀不掉线程, 重启是唯一清理手段 —— 因此**即使心跳新鲜也强制重启**,
#      否则"卡住的线程持锁 + 看门狗永不重启"会让服务永久卡死。
#   3) 可观测字段 probe_running_s(探针线程已存活秒数), 便于事后确认是否发生过 wedge。

_KEEPALIVE_TIMEOUT = 90      # 探针外层总超时(两重相加 20+60=80s, 90s 留余量)
                             # 注意: 这个超时**只决定"本轮拿不到存活证据"**, 不直接判死;
                             # 而且它拦不住线程本身(见 _PROBE_WEDGE_MAX)
# v6: 探针周期 —— **不论心跳如何, 每 60s 发一发真实查询**。
# 判活判死都由真实查询说话, 所以它必须周期性发, 而不能像 v5 那样"心跳不新鲜才发"
# (v5 那套的必然后果: 心跳健康 => 永不发探针 => 功能坏了也发现不了)。
_PROBE_INTERVAL = 60
_PROBE1_TIMEOUT = 20         # 第一重(国债收益率)超时: 单码两日区间, 正常应 <1s
_PROBE2_TIMEOUT = 60         # 第二重(get_calendar)超时: 实测 p100=33.3s, 60s 留 1.8 倍余量
_PROBE1_NAME = "InfoData.get_treasury_yield"
_PROBE2_NAME = "BaseData.get_calendar"
_TREASURY_CODE = "y10"       # 第一重探针标的: 10 年期国债收益率
_TREASURY_BEGIN = 20250101   # 固定历史区间: 结果稳定, 不会因为"今天没数据"而失败
_TREASURY_END = 20250102
_QUERY_OK_GRACE = 30         # 探针两重都失败时的**仲裁**窗口: 业务查询在这么久内成功过
                             # => 上游是通的, 本次失败算偶发/探针自身问题, 本轮不计 strike。
                             # (这是真实查询证据之间的仲裁, **不是**拿心跳豁免探针)
_SESSION_ALIVE_WINDOW = 300  # _session_alive(): 这么久内有真实查询成功 => 会话可用
# 探针线程"卡死"判定: 超过这么久还没结束 => 不是慢, 是 wedged。
# 正常探针 p100=33.3s, 300s 不会误伤; 卡死的线程会永久持有 SDK 库内锁, 必须重启清理。
_PROBE_WEDGE_MAX = 300
_KEEPALIVE_START_TS = 0.0      # 当前探针线程的启动时刻(0=没有在跑的探针)
# 连续判死次数阈值: 需连续 N 轮都"拿不到任何存活证据"才 execv 换进程
_KEEPALIVE_STRIKES_REQUIRED = 3
_KEEPALIVE_STRIKES = 0
_LAST_KEEPALIVE = START_TIME   # 最近一次确认存活(心跳或探针成功)
_LAST_VERIFY = 0.0             # 最近一次探针的**发起**时间(周期控制用); 0=还没做过,
                               # 于是每次进程启动(含 execv 自愈)都会立即补一次端到端探针
_PROBE_OK_TS = 0.0             # 最近一次真实查询探针**成功**的时间(判活的核心证据)
_PROBE_NAME = _PROBE1_NAME + " -> " + _PROBE2_NAME   # 探针链(用于日志与 /api/stats 展示)
# 最近一次探针的结论(供 /health 与 /api/stats 展示, 便于事后判断"到底是哪一重失败"):
#   {"ok": bool, "via": 用了哪一重, "took": 秒, "err": 失败原因(仅失败时), "ts": 时刻}
_PROBE_LAST = {}
# 最近一次"真实业务查询成功"的时间(只由 /api/query 成功路径刷新, 探针自己不刷)。
# 用途: 心跳不可用时, 若业务查询刚刚成功过, 会话显然活着, 这一轮探针可以省掉,
# 既减少无谓查询, 也避免探针与业务查询抢同一条上游查询通道而互相拖慢。
_LAST_QUERY_OK = START_TIME

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
# watchdog 保活探测线程(单飞): 探针放子线程 + join 限时, 不阻塞事件循环也不拖住看门狗
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


# ---------------------------------------------------------------------------
# v9e (2026-11-06): local_data 缓存清理搬进进程内, 与 TGW 重连绑定
# ---------------------------------------------------------------------------
# 背景: SDK 的 is_local 只控制「读」。is_local=False 查询 fund_share/nav/iopv 等接口时,
#   仍会把全量历史落盘到 local_data/infodata/(*.h5, 单文件约 4.3MB), 长期能涨到 GB 级。
#   缓存随时可再生(重新查询即重写), 清理没有副作用。
# 原先靠 crontab `17 3 */3 * * /bin/bash clean_local_data.sh`(每 3 天 03:17 一次)清理。
# 现在改为 **每次 TGW 重连成功后**清一次 —— 三条登录路径(bootstrap/重试线程/定时重登)
#   都收敛到 _startup_login(), 所以只在那里挂一个钩子就能全覆盖, 不依赖外部 cron。
# 时机: 登录成功后延后 _CACHE_CLEAN_DELAY 秒再动手。刚登录时 /api/query 会有一波冷启动
#   查询, 等它们放开句柄再删目录更干净(Linux 下 unlink 已打开文件不会报错, 但 SDK 对
#   "目录突然消失"没有保证)。清理线程 daemon 化, 不影响登录与订阅建立。
_CACHE_CLEAN_DELAY = float(os.environ.get("AD_CACHE_CLEAN_DELAY", "30"))
_CACHE_CLEAN_LOCK = threading.Lock()
_CACHE_CLEAN = {"last_ts": None, "count": 0, "files": 0, "bytes": 0,
                "last_reason": "", "last_err": "", "running": False}


def _dir_stat(path):
    """返回 (文件数, 字节数); 目录不存在时返回 (0, 0)。"""
    n = size = 0
    try:
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    size += os.path.getsize(os.path.join(root, f))
                    n += 1
                except OSError:
                    pass
    except Exception:
        pass
    return n, size


def _clean_local_cache(reason=""):
    """删除 local_data/infodata。返回 (ok, 摘要)。只删 infodata 子目录, local_data 本身保留。"""
    d = os.path.join(_APP_DIR, "local_data", "infodata")
    n, size = _dir_stat(d)
    cnt = _CACHE_CLEAN["count"] + 1
    if not os.path.isdir(d):
        _CACHE_CLEAN.update(last_ts=time.time(), count=cnt, files=0, bytes=0,
                            last_reason=reason, last_err="", running=False)
        return True, "infodata 不存在, 无需清理"
    err = ""
    try:
        shutil.rmtree(d, ignore_errors=True)
    except Exception as e:          # ignore_errors 已吞掉大部分, 这里只兜底
        err = repr(e)
    left_n, _ = _dir_stat(d)
    ok = (left_n == 0 and not err)
    _CACHE_CLEAN.update(last_ts=time.time(), count=cnt, files=n, bytes=size,
                        last_reason=reason, last_err=err, running=False)
    if not ok:
        return False, "清理失败: %s" % (err or "残留 %d 个文件" % left_n)
    return True, "删除 %d 个文件 %.1fMB" % (n, size / 1048576.0)


def _spawn_cache_clean(reason="login"):
    """登录成功后异步清理缓存(延后 _CACHE_CLEAN_DELAY 秒), 不阻塞登录与订阅。"""
    def _run():
        try:
            if _CACHE_CLEAN_DELAY > 0:
                time.sleep(_CACHE_CLEAN_DELAY)
            with _CACHE_CLEAN_LOCK:
                ok, msg = _clean_local_cache(reason)
            log("local cache cleaned (%s): %s" % (reason, msg),
                logging.INFO if ok else logging.WARNING)
        except Exception as e:
            log("local cache clean thread error: %r" % (e,), logging.ERROR)

    try:
        threading.Thread(target=_run, daemon=True, name="cache-clean").start()
    except Exception as e:
        log("local cache clean dispatch failed: %r" % (e,), logging.WARNING)


def _startup_login():
    global TGW_LOGIN_TS, _CAL
    try:
        # SDK 的 ad.login 打印 login success 但返回 False/None,
        # 不能依赖其返回值判断登录状态, 用一次真实查询验证会话是否可用。
        ad.login(username=USER, password=PASSWORD, host=HOST, port=PORT)
        _cal = ad.BaseData().get_calendar()
        if _cal is None:
            # 会话未真正建立时 get_calendar 可能返回 None; 若当作成功, 后面 MarketData
            # 会带着 None 日历一路 500(历史上表现为 startup login error:
            # TypeError("'NoneType' object is not subscriptable"))。这里直接判失败走重试。
            raise RuntimeError("get_calendar returned None (session not established)")
        # 顺手把交易日历缓存下来: MarketData 构造需要它, 缓存后 /api/query 与保活探针
        # 都不再重复拉这 8730 行(旧探针每次都拉一遍全量日历, 白白拖慢自己)。
        _CAL = _cal
        TGW_LOGIN_TS = time.time()
        STATS["last_success_ts"] = time.time()
        log("startup login ok=True (verified by get_calendar, calendar cached)")
        # 每次重连成功都清一次 local_data 缓存(见 v9e 注释), 替代原来的 crontab
        _spawn_cache_clean("login")
        return True
    except Exception as e:
        log(f"startup login error: {e!r}", logging.ERROR)
        return False


# ---------------------------------------------------------------------------
# 首次登录 -> 改为「后台线程」(2026-09-21 修复: 端口延迟 bind 引发的重启循环)
# ---------------------------------------------------------------------------
# 原实现是在模块顶层同步调用 _startup_login(): 登录 + 真实查询验证要 20~60s, 而
# uvicorn 是先 import 完整个模块、再 create_server 绑定端口 —— 这段窗口里 8000 端口
# 根本没有监听, crontab 每分钟一次的 run.sh 探活必然失败 -> 被反复判死并重复拉起
# 第二个实例 -> uvicorn 'Errno 98 地址已在使用'(server.log 里累计 6 次); run.sh 侧
# 由此产生的误判重启累计 304 次。
# 现在: 启动阶段只「启动线程」, 立即返回 -> uvicorn 秒级 bind 端口 -> /health 立刻
# 可应答(端口存活判定成立), 登录过程完全异步完成, 期间数据接口按需短暂等待。
_LOGIN_STATE = {"ready": False, "done_ts": None, "error": None}
_LOGIN_LOCK = threading.Lock()
_LOGIN_READY_TIMEOUT = 30.0   # 数据接口等待首次登录的上限(秒): 超时返回 503, 不返回裸 500

# ---------------------------------------------------------------------------
# v8 (2026-09-28): 登录超时治理 —— 单次登录 30s 超时 -> 登出还槽位 -> 等 10s 重试;
# 连续 3 次未成功 -> 重启进程(清掉挂死在 ad.login 里的线程和泄漏的 TGW fd)。
# 背景: 2026-09-28 -98 事故 —— ad.login 被上游拒(status[-98] 连接数超限)后 SDK 的
# PushImpl 重连线程退出, 但 ad.login 调用本身永不返回, 登录线程僵死; watchdog 因
# "initial login in progress" 持续跳过判断, 服务空转 6.5 小时。超时包装保证每轮
# 登录必有结论, 失败必有后续动作(登出还槽位/重启), 不再有无超时的悬空等待。
_LOGIN_TIMEOUT_S = 30.0     # 单次登录判定超时(秒)
_LOGIN_FAIL_MAX = 3         # 连续未成功 N 次 -> 重启进程
_LOGIN_RETRY_GAP = 10.0     # 登出后等待(秒)再发起下一次登录
_LOGIN_STRIKES = 0          # 连续登录未成功计数(任一次成功即清零)
_LOGIN_BLOCKED_UNTIL = 0.0  # 此时间戳前禁止发起登录(定时登出/登录窗口, 见 _scheduled_relogin_loop)


def _run_login_threaded(timeout=_LOGIN_TIMEOUT_S):
    """在独立线程执行 _startup_login, join(timeout)。返回三态:
      "ok"     登录成功
      "fail"   线程已返回但登录未成功(如 -98 秒拒) —— SDK 状态仍可用, 可进程内重试
      "wedged" 超时, 线程仍挂在 ad.login/get_calendar 里 —— SDK 原生状态已不可信,
               进程内后续任何 SDK 调用(包括 logout)都会被堵死, 只能重启进程"""
    box = {"ok": False}

    def _do():
        try:
            box["ok"] = bool(_startup_login())
        except BaseException:   # noqa: BLE001 - 任何异常都算本次登录失败
            box["ok"] = False

    th = threading.Thread(target=_do, name="login-attempt", daemon=True)
    th.start()
    th.join(timeout)
    if not th.is_alive():
        return "ok" if box["ok"] else "fail"
    return "wedged"


def _bootstrap_login():
    """后台完成首次登录; 单次 30s 超时。fail 交给进程内重试线程(登出还槽位 ->
    等 10s -> 重登; 连续 3 次未成功 -> 重启进程); wedged 交重试线程走重启路径。"""
    global _LOGIN_STRIKES
    with _LOGIN_LOCK:
        res = _run_login_threaded()
        _LOGIN_STATE["done_ts"] = time.time()
        if res == "ok":
            _LOGIN_STRIKES = 0
            _LOGIN_STATE["ready"] = True
            return
        _LOGIN_STATE["error"] = f"startup login {res}"
    # 上游暂不可用: 进程内重试(会话尚未建立任何成功查询, 重连安全, 不动用 execv)
    try:
        _ensure_retry_thread()
    except NameError:      # 极端时序: 线程先于模块剩余定义执行完毕
        pass


def _wait_login_ready(timeout=None):
    """等首次登录就绪(只有进程刚起来的一小段会有等待); 已就绪立即返回 True."""
    if _LOGIN_STATE["ready"]:
        return True
    deadline = time.time() + (_LOGIN_READY_TIMEOUT if timeout is None else timeout)
    while time.time() < deadline:
        if _LOGIN_STATE["ready"]:
            return True
        time.sleep(0.5)
    return _LOGIN_STATE["ready"]


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
    """登录失败/超时后的进程内重试(单飞线程, 不主动 execv —— 重启由失败计数触发):
    - "fail"(快速被拒, 如 -98): 登出还槽位 -> 等 10s -> 重登; 连续 3 次 -> 重启进程
    - "wedged"(30s 超时, 线程挂在 ad.login/get_calendar 里): **不做任何进程内补救**
      (与挂死线程并发调 SDK 只会全部堵死, v8 首版在 15:32-15:42 踩过), 直接重启进程;
      若处于重启冷却期则等冷却走完再重启, 期间不再发起 SDK 调用。
    v8 (2026-09-28) 修复: 旧实现直接调 ad.login 无超时, -98 被拒后调用永不返回,
    本线程随之僵死且无人能救 —— 这是 6.5 小时故障的核心一环。"""
    global _LAST_KEEPALIVE, TGW_LOGIN_TS, _CAL, _LOGIN_STRIKES
    # v7: 上一次登录可能失败但已在服务端占了槽位(尤其 -98 那种"半成功"), 重登前先还一次
    _graceful_logout("pre-relogin", timeout=3.0)
    while STATS["last_success_ts"] is None:
        # 定时登出/登录窗口内(03:58-04:01 / 08:58-09:00)只等待, 不抢跑
        if time.time() < _LOGIN_BLOCKED_UNTIL:
            time.sleep(2)
            continue
        res = _run_login_threaded()
        if res == "ok":
            _LOGIN_STRIKES = 0
            now = time.time()
            TGW_LOGIN_TS = now
            _LAST_KEEPALIVE = now
            _LOGIN_STATE["ready"] = True
            log("in-process login retry ok (upstream recovered)", cat="watchdog")
            return
        if res == "wedged":
            # 登录线程挂在原生库里, 本进程的 SDK 状态已不可信: 唯一出路是重启进程
            _LOGIN_STRIKES = 0
            log(f"login attempt WEDGED (>{_LOGIN_TIMEOUT_S:.0f}s, thread stuck in "
                f"ad.login/get_calendar) -> restarting process", logging.ERROR, cat="watchdog")
            _restart_process()   # 内部含登出/落盘; 冷却期被挡回则等冷却走完再重启
            while STATS["last_success_ts"] is None:
                _n, _ts = _read_restart_meta()
                remaining = _RESTART_COOLDOWN - (time.time() - _ts)
                if remaining <= 0:
                    _restart_process()
                    continue
                time.sleep(min(remaining, 15))
            continue
        _LOGIN_STRIKES += 1
        log(f"login attempt failed (rejected/error), strike {_LOGIN_STRIKES}/{_LOGIN_FAIL_MAX}",
            logging.WARNING, cat="watchdog")
        if _LOGIN_STRIKES >= _LOGIN_FAIL_MAX:
            _LOGIN_STRIKES = 0
            log(f"login failed {_LOGIN_FAIL_MAX} consecutive times -> restarting process",
                logging.ERROR, cat="watchdog")
            _restart_process()   # 内部含登出/落盘/冷却; 若被冷却挡回则继续循环重试
            continue
        _graceful_logout("post-login-fail", timeout=3.0)   # 登出还槽位再战
        time.sleep(_LOGIN_RETRY_GAP)   # 登出后等 10s 再登录


_RETRY_THREAD = None


def _ensure_retry_thread():
    global _RETRY_THREAD
    if _RETRY_THREAD is None or not _RETRY_THREAD.is_alive():
        _RETRY_THREAD = threading.Thread(target=_inprocess_login_retry, daemon=True)
        _RETRY_THREAD.start()


# ---------------------------------------------------------------------------
# v8 (2026-09-28): 定时登出/登录 —— 每天强制刷新 TGW 会话
# v9c (2026-11-06): 改成配置驱动, 默认三段: 04:10 / 09:00 / 21:00。
#   每段 = 先登出(_lead 分钟), 到点再登录; **全程进程内完成, 不 execv、不重启网关**,
#   因此前端"服务运行时间"(SERVICE_START_TS) 不会归零。
# 目的: 会话定期重建, 防止长会话老化与僵尸连接累积(2026-09-28 -98 事故的预防措施)。
# 独立 daemon 线程; 登出后到配对登录时刻之间 _LOGIN_BLOCKED_UNTIL 生效 —— 重试线程
# 与 watchdog 在窗口内都不动作, 窗口内查询按"未就绪"拒绝。
_SCHED_DEFAULT_TIMES = "04:10,09:00,21:00"


def _parse_sched_slots(raw, lead_min=2):
    """把 "04:10,09:00,21:00" 解析成 (("04:08","04:10"), ...)。
    也接受显式区间 "03:58-04:01"; 非法项跳过。"""
    slots = []
    for item in str(raw or "").replace("，", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            if "-" in item:
                out_hm, in_hm = [x.strip() for x in item.split("-", 1)]
                datetime.datetime.strptime(out_hm, "%H:%M")
                datetime.datetime.strptime(in_hm, "%H:%M")
            else:
                in_hm = item
                t = datetime.datetime.strptime(in_hm, "%H:%M") \
                    - datetime.timedelta(minutes=lead_min)
                out_hm = t.strftime("%H:%M")
            slots.append((out_hm, in_hm))
        except ValueError:
            log("relogin: bad slot %r ignored (expect HH:MM or HH:MM-HH:MM)" % (item,),
                logging.WARNING, cat="watchdog")
    return tuple(slots)


_SCHED_SLOTS = (_parse_sched_slots(_cfg("relogin", "times", _SCHED_DEFAULT_TIMES),
                                   int(_cfg("relogin", "logout_lead_min", "2") or 2))
                or _parse_sched_slots(_SCHED_DEFAULT_TIMES))


def _next_scheduled_action(now):
    """返回下一个 (执行时刻, 'logout'/'login', 配对登录时刻或 None); 无则 None。"""
    base = now.date()
    for day in (base, base + datetime.timedelta(days=1)):
        items = []
        for out_hm, in_hm in _SCHED_SLOTS:
            oh, om = int(out_hm[:2]), int(out_hm[3:])
            ih, im = int(in_hm[:2]), int(in_hm[3:])
            items.append((datetime.datetime.combine(day, datetime.time(oh, om)),
                          "logout", datetime.datetime.combine(day, datetime.time(ih, im))))
            items.append((datetime.datetime.combine(day, datetime.time(ih, im)),
                          "login", None))
        for it in sorted(items, key=lambda x: x[0]):
            if it[0] > now:
                return it
    return None


def _scheduled_relogin_loop():
    global _LOGIN_BLOCKED_UNTIL, _LOGIN_STRIKES
    log("scheduled relogin thread started: %s (in-process, uptime preserved)"
        % ", ".join("%s->%s" % (o, i) for o, i in _SCHED_SLOTS), cat="watchdog")
    while True:
        try:
            nxt = _next_scheduled_action(datetime.datetime.now())
            if nxt is None:
                time.sleep(30)
                continue
            t, act, login_t = nxt
            log("schedule: next %s at %s%s" % (
                act, t.strftime("%m-%d %H:%M"),
                "" if login_t is None else " (paired login %s)" % login_t.strftime("%H:%M")),
                cat="watchdog")
            while datetime.datetime.now() < t:
                time.sleep(min(20, max(0.5, (t - datetime.datetime.now()).total_seconds())))
            if act == "logout":
                # 先封登录窗口再登出, 防止重试线程/探针在窗口内抢跑拉起会话
                _LOGIN_BLOCKED_UNTIL = login_t.timestamp() if login_t else time.time() + 60
                _graceful_logout("scheduled-%s" % t.strftime("%H:%M"), timeout=5.0)
                STATS["last_success_ts"] = None
                TGW_LOGIN_TS = None
                _LOGIN_STATE["ready"] = False   # 查询接口按"未就绪"拒绝, 而不是裸报错
                log("scheduled logout done (%s); login suppressed until %s" % (
                    t.strftime("%H:%M"),
                    datetime.datetime.fromtimestamp(_LOGIN_BLOCKED_UNTIL).strftime("%H:%M")),
                    cat="watchdog")
            else:  # login
                _LOGIN_BLOCKED_UNTIL = 0.0
                if STATS["last_success_ts"] is not None:
                    continue   # 窗口内已被其他途径登录成功(如人工干预), 跳过
                with _LOGIN_LOCK:
                    res = _run_login_threaded()
                    if res == "ok":
                        _LOGIN_STRIKES = 0
                        _LOGIN_STATE["ready"] = True
                        log("scheduled login ok (%s)" % t.strftime("%H:%M"), cat="watchdog")
                        continue
                if res == "wedged":
                    # SDK 原生状态挂死: 不做进程内补救, 直接走重启路径(交重试线程处理)
                    log("scheduled login WEDGED (%s) -> restart via retry loop"
                        % t.strftime("%H:%M"), logging.ERROR, cat="watchdog")
                    STATS["last_success_ts"] = None
                    _ensure_retry_thread()
                    continue
                log("scheduled login failed (%s) -> handing to retry loop" % t.strftime("%H:%M"),
                    logging.WARNING, cat="watchdog")
                _graceful_logout("scheduled-login-fail", timeout=3.0)
                _ensure_retry_thread()
        except Exception as e:
            log(f"scheduled relogin loop error: {e!r}", logging.ERROR, cat="watchdog")
            time.sleep(30)


# ---------------------------------------------------------------------------
# v7 (2026-09-22): 优雅登出 —— 重连/重启/退出前主动释放账号并发连接槽位
# ---------------------------------------------------------------------------
# 背景(P0): 账号有**并发连接数上限**, 超限登录会被上游拒(status[-98])。两个放大器:
#   1) 本服务每次重启都重新 ad.login, 但**从不登出** —— 旧会话要等上游自己超时才释放;
#   2) TGW native 库建的 socket **不带 O_CLOEXEC**(实测 /proc/<pid>/fdinfo:
#      TGW 的 fd flags=04002, 而 Python 自建的 fd 是 02004002 = 带 FD_CLOEXEC)。
#      execve 只会自动关闭带 CLOEXEC 的 fd, 于是**每次 execv 自愈都会把旧的 TGW 连接
#      原样留给新进程** —— 自愈 N 次就攒下 N 条"没有人再读、但服务端认为还活着"的连接,
#      槽位只增不减, 最终必然撞上限。
# 对策(两条都做, 单独一条都不够):
#   a) 重启/重登/退出前 best-effort 调 ad.logout(USER)(SDK 内部即 tgw.Close()), 主动还槽;
#   b) execv 前把 fd>=3 全部置 FD_CLOEXEC, 由内核在 execve 时关闭, 新进程从零开始。
# 实现要点: 登出放在 **fork 出来的子进程** 里做 —— 脏会话里 native 库可能挂死甚至段错误,
#   子进程炸了不影响父进程继续 execv; 父进程最多等 _LOGOUT_TIMEOUT 秒, 超时 SIGKILL 并继续。
_LOGOUT_TIMEOUT = float(os.environ.get("AD_LOGOUT_TIMEOUT", "5"))
_LOGOUT_STATE = {"count": 0, "ok": 0, "fail": 0, "timeout": 0,
                 "last_ts": None, "last_ms": None, "last_ok": None,
                 "last_reason": "", "last_err": "", "fd_marked_cloexec": 0}


def _socket_fd_count():
    """本进程持有的 socket fd 数(诊断用: 观察 execv 前后有没有残留连接)。"""
    try:
        n = 0
        for name in os.listdir("/proc/self/fd"):
            try:
                if os.readlink("/proc/self/fd/" + name).startswith("socket:"):
                    n += 1
            except OSError:
                continue
        return n
    except Exception:
        return -1


def _mark_fd_cloexec():
    """把 fd>=3 全部置 FD_CLOEXEC(stdio 与 uvicorn 监听 socket 不需要继承:
    execve 后的新进程会自己重新 bind)。返回实际新标记的 fd 数量。"""
    n = 0
    try:
        _fd_dir = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"
        for name in os.listdir(_fd_dir):
            try:
                fd = int(name)
            except ValueError:
                continue
            if fd < 3:
                continue
            try:
                fl = fcntl.fcntl(fd, fcntl.F_GETFD)
                if not (fl & fcntl.FD_CLOEXEC):
                    fcntl.fcntl(fd, fcntl.F_SETFD, fl | fcntl.FD_CLOEXEC)
                    n += 1
            except OSError:
                continue
    except Exception:
        pass
    _LOGOUT_STATE["fd_marked_cloexec"] = n
    return n


def _graceful_logout(reason="", timeout=None):
    """best-effort 登出当前 TGW 会话, 立即释放账号并发连接槽位。

    返回 "ok" / "fail" / "timeout"。**绝不因登出失败而阻塞重启**: 无论结果如何,
    调用方都继续走自己的重启/退出路径。

    为什么是**本进程内的线程**而不是 fork 子进程(2026-09-22 实测改回来的):
      先做的 fork 版本在线上 09:09:06 稳定超时 3041ms —— fork 只会留下调用线程,
      TGW native 库自己的**收发线程在子进程里不存在**, tgw.Close() 等一个永远不会有
      人处理的回应 => 必然挂死。本进程内跑, 收发线程都在, Close 才有机会真正完成。
    代价与兜底: 若 Close 真的挂死/段错误, 线程是 daemon, join(timeout) 到点就走;
      重启路径下一句就是 execv(挂住的线程随进程映像一起消失), 退出路径进程本来就要死。
    """
    timeout = _LOGOUT_TIMEOUT if timeout is None else timeout
    t0 = time.time()
    box = {}

    def _do():
        try:
            ad.logout(username=USER)
            box["done"] = True
        except BaseException as e:      # noqa: BLE001 - 登出失败只记录, 不阻断重启
            box["err"] = repr(e)

    th = threading.Thread(target=_do, name="graceful-logout", daemon=True)
    try:
        th.start()
    except Exception as e:
        _LOGOUT_STATE.update(count=_LOGOUT_STATE["count"] + 1, last_ts=t0, last_ok=False,
                             last_reason=reason, last_err=f"thread start failed: {e!r}")
        log(f"graceful logout ({reason}) thread start failed: {e!r}",
            logging.WARNING, cat="watchdog")
        return "fail"
    th.join(timeout)
    ms = int((time.time() - t0) * 1000)
    if box.get("done"):
        result, ok = "ok", True
        _LOGOUT_STATE["ok"] += 1
    elif th.is_alive():
        result, ok = "timeout", False
        _LOGOUT_STATE["timeout"] += 1
    else:
        result, ok = "fail", False
        _LOGOUT_STATE["fail"] += 1
    _LOGOUT_STATE.update(count=_LOGOUT_STATE["count"] + 1, last_ts=t0, last_ms=ms,
                         last_ok=ok, last_reason=reason,
                         last_err="" if ok else box.get("err", "logout thread hung"))
    log(f"graceful logout ({reason}): {result} in {ms}ms (releases account connection slot)",
        logging.INFO if ok else logging.WARNING, cat="watchdog")
    if ok:
        time.sleep(0.2)     # 给上游一点时间把登出落库, 再发起新登录
    return result


def _pre_restart_cleanup(reason="pre-restart"):
    """重启前的标准动作: 登出 + 切断 fd 继承(v7)。返回 (logout 结果, 新标记 CLOEXEC 的 fd 数)。"""
    socks = _socket_fd_count()
    res = _graceful_logout(reason)
    n = _mark_fd_cloexec()
    log(f"pre-restart cleanup ({reason}): logout={res}, fds_marked_cloexec={n}, "
        f"socket_fds_before={socks}", cat="watchdog")
    return res, n


def _restart_process(force=False):
    """干净地用相同环境变量重新拉起自己(全新登录), 避免脏会话内 ad.login 段错误。
    仅当进程健康(_session_alive())时受 60s 年轻保护, 防止 watchdog 启动后立刻连续误触发;
    force=True(订阅换集合的主动重启)不受年轻保护。
    熔断: 冷却期内(_RESTART_COOLDOWN)不再 execv, 改由进程内退避重试, 杜绝重启风暴."""
    global _RESTARTING
    if _RESTARTING:
        return
    # 首次登录仍在进行中(进程刚起): 此时 last_success_ts 必然为 None, 上面的"年轻保护"
    # 会失效, 若不拦住就会把"正在登录"当成"会话已死"反复 execv。必须显式跳过。
    if not force and not _LOGIN_STATE["ready"] and _LOGIN_STATE["done_ts"] is None:
        log("restart skipped (initial login still in progress)", logging.WARNING, cat="watchdog")
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
        # v7: 重启前先登出 + 切断 fd 继承 —— 否则旧 TGW 连接会被 execve 后的新进程
        # 原样继承(那些 fd 不带 FD_CLOEXEC), 自愈一次就多占一条账号连接, 攒满即 -98。
        _pre_restart_cleanup("pre-restart")
        # 2026-09-21: 端口/地址改为跟随配置(config.ini 的 server.*), 不再硬编码 8000,
        # 否则改了监听端口后 execv 自愈会跑到 8000 上, 与配置不一致。
        # 带 _AD_SELF_RESTART 标记 execv: 新进程据此沿用 SERVICE_START_TS, 服务运行时间不归零。
        # 注意: 带环境变量必须用 os.execve(3 参), os.execv 只接受 (path, argv) 两个参数,
        # 否则 TypeError('execv expected 2 arguments, got 3') 导致自愈永远失败(2026-08-17 实测)。
        _env = os.environ.copy()
        _env["_AD_SELF_RESTART"] = "1"
        os.execve(
            sys.executable,
            [sys.executable, "-m", "uvicorn", "http_service:app",
             "--host", str(SERVER_HOST), "--port", str(SERVER_PORT)],
            _env,
        )
    except Exception as e:
        _RESTARTING = False
        log(f"restart failed: {e!r}", logging.ERROR, cat="watchdog")


STATS["reconnect_count"] = _read_restart_count()


# ----------------------------------------------------------------------------
# 看门狗: 周期保活 + 会话失效自重启
# ----------------------------------------------------------------------------
_HB_STAT = {_HB_PUSH: {"age": None, "ts": 0.0, "file": ""},
            _HB_QUERY: {"age": None, "ts": 0.0, "file": ""}}
# 心跳源"可疑"标记: 仅供诊断与 /api/stats 展示 —— **不再影响判活决策**。
# (老逻辑用它把心跳判据永久关掉, 现在判死必须有"真实查询失败"这个物证, 不会因为
#  心跳本身有问题而误重启, 所以不需要再关。)
_HB_SUSPECT = False


def _mark_hb_suspect(hb_age):
    """标记"心跳源可疑"(只做一次, 留一条 WARNING 说明文件路径)。仅诊断。"""
    global _HB_SUSPECT
    if _HB_SUSPECT:
        return
    _HB_SUSPECT = True
    log(f"heartbeat source SUSPECT: newest upstream push heartbeat is {hb_age:.0f}s old "
        f"while real queries keep succeeding (file={_HB_STAT[_HB_PUSH].get('file')!r})",
        logging.WARNING, cat="watchdog")


def _sdk_hb_age(kind):
    """距最近一次指定类型心跳的秒数; SDK 日志读不到时返回 None。

    kind="Push"  : "Push Heartbeat Check | Receive <Heartbeat> from DGW!"
        —— 推送通道每 10s 一次的收发回执, **会话存活的判据**。实测全天 7022 条、
           每小时恰好 360 条(每 10s 一条)无间断; 唯一一次中断是 08:25:13~08:32:26
           的真故障(当时它与 Query 一起变成 `Session isn't logon!`, 并伴随 wss
           连接失败循环 7 分钟)。
    kind="Query" : "Query Heartbeat Check | Receive <Heartbeat> from DGW!"
        —— **只证明"查询通道当前已建立"**。SDK 会在查询通道空闲约 10 分钟后主动回收
           (`Time check | QueryImpl is free, will release`), 此后本心跳消失直到下一次
           真实查询重建通道。全天发生 8 次 => 它**不能**当存活判据(见文件头 v5 说明)。

    结果按 _HB_CACHE_TTL 缓存(看门狗每轮调用也只有每 10s 一次读尾部文件的开销);
    读不到(文件缺失/轮转/格式变了)一律返回 None —— 由调用方发真实查询取证,
    绝不因为"读不到日志"就判会话死。"""
    st = _HB_STAT[kind]
    now = time.time()
    if now - st["ts"] < _HB_CACHE_TTL:
        return st["age"]
    st["ts"] = now
    try:
        cands = glob.glob(os.path.join(_SDK_LOG_DIR, "log_MDGA_*.log"))
        if not cands:
            st["age"] = None
            return None
        path = max(cands, key=os.path.getmtime)     # 跨零点自动跟到当天的新文件
        size = os.path.getsize(path)
        with open(path, "rb") as f:
            if size > _HB_TAIL_BYTES:
                f.seek(size - _HB_TAIL_BYTES)
            tail = f.read().decode("utf-8", "replace")
        stamp = None
        needle = kind + " Heartbeat Check"
        for line in reversed(tail.splitlines()):
            if needle in line and "Receive <Heartbeat> from DGW" in line:
                stamp = line[:19]                       # "YYYY-MM-DD HH:MM:SS"
                break
        if not stamp:
            st["age"] = None                            # 尾部没找到(刚轮转新文件): 交给探针兜底
            st["file"] = path
            return None
        age = now - time.mktime(time.strptime(stamp, "%Y-%m-%d %H:%M:%S"))
        st["file"] = path
        st["age"] = age if age >= -5 else None     # 时钟回拨等异常一律当"未知"
        return st["age"]
    except Exception:
        st["age"] = None
        return None


def _sdk_heartbeat_age():
    """会话级(推送通道)心跳年龄 —— 判活第一依据。"""
    return _sdk_hb_age(_HB_PUSH)


def _sdk_query_hb_age():
    """查询通道心跳年龄 —— **仅供观测**: 空闲会被 SDK 回收, 不参与判死。"""
    return _sdk_hb_age(_HB_QUERY)


def _start_keepalive_probe(reason):
    """起一个单飞的探针线程(调用方需先确认没有在跑的探针)。
    这里记"发起时刻"到 _LAST_VERIFY: 探针周期(_PROBE_INTERVAL)与 /api/stats 的
    last_verify 都以发起时刻为准 —— 失败也照常按周期重试, 不会每 30s 反复发起。"""
    global _KEEPALIVE_THREAD, _KEEPALIVE_START_TS, _KEEPALIVE_FAILED, _LAST_VERIFY
    _KEEPALIVE_FAILED = False   # 新一轮开始: 上一轮的结论已被 watchdog 消费掉
    _LAST_VERIFY = time.time()
    _KEEPALIVE_START_TS = _LAST_VERIFY
    _KEEPALIVE_THREAD = threading.Thread(target=_keepalive_probe, daemon=True)
    _KEEPALIVE_THREAD.start()
    log(f"watchdog: keepalive probe started ({reason})", cat="watchdog")


def _nudge_probe(reason):
    """业务查询失败时"催一次探针取证", 而不是在请求路径上直接重启。

    做法: 把 _LAST_VERIFY 清零 => 看门狗下一轮(<=30s)必然满足
    `now - _LAST_VERIFY >= _PROBE_INTERVAL`, 立刻发一次两重探针。

    为什么不直接在请求里 `_restart_process()`(v6 改动):
      * 走到这里的失败**不都是会话死** —— 参数写错、单只标的超时、上游偶发抖动都会
        落到同一个 except。在请求路径上 execv 会打断并发中的其它请求。
      * 每次重启都要重新登录, 而账号有连接数上限(status[-98]), 今天 6/6 全是自己
        重启撞的 —— 重启必须稀缺。
      * 判死交给探针的两重真实查询, 连续 3 次才算数, 口径与看门狗完全一致。"""
    global _LAST_VERIFY
    if _LAST_VERIFY > 0:
        _LAST_VERIFY = 0.0
        log(f"query-failure nudged an immediate probe ({reason})", cat="watchdog")


def _session_alive():
    """会话是否**真的可用** —— v6 起由**真实查询证据**判定(第一代口径), 心跳不参与。

    判据: 近 _SESSION_ALIVE_WINDOW 内有过一次"真实查询成功" ——
          探针成功(_PROBE_OK_TS) 或 业务查询成功(STATS["last_success_ts"])。

    为什么不再看心跳(2026-09-22): 心跳是**连接级**信号, 它证不了"数据能不能取到"。
    09-21 23:34 起查询通道被上游拒令牌、所有数据查询 100% 失败 8 小时, 而 Push 心跳
    每小时 360 条一条不缺 —— 靠心跳判活, /health 会一直报 status=ok、login=true,
    运维看到的是一台"完全健康"的机器。last_success_ts 为 None 表示本进程从未成功过,
    返回 False(交由登录重试/看门狗处理)。"""
    now = time.time()
    if _PROBE_OK_TS and (now - _PROBE_OK_TS) < _SESSION_ALIVE_WINDOW:
        return True
    ts = STATS["last_success_ts"]
    if ts is None:
        return False
    return (now - ts) < _SESSION_ALIVE_WINDOW


def _probe_run(fn, timeout):
    """在子线程里跑 fn 并 join 限时。返回 ("ok", 值) / ("error", 异常) / ("timeout", None)。

    用子线程是因为这些 SDK 调用**内部都没有超时**; join 到点即返回。
    注意 join 超时**不会杀掉线程**(Python 杀不掉线程), 它会继续跑 —— 由
    _PROBE_WEDGE_MAX 兜底判定是否算卡死。
    这里连 BaseException 一起捕获: SDK 部分方法内部会抛 SystemExit, 只捕 Exception
    会穿透成裸 500 / 打挂探针线程。"""
    holder = {}

    def _worker():
        try:
            holder["v"] = fn()
        except BaseException as e:      # noqa: BLE001 - 必须连 SystemExit 一起兜住
            holder["e"] = e

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return "timeout", None
    if "e" in holder:
        return "error", holder["e"]
    return "ok", holder.get("v")


def _probe1_treasury():
    """第一重探针: 10 年期国债收益率(y10), 单码 + 两日区间。

    为什么拿它当第一重(2026-09-22 指定):
      * **载荷极小** —— get_calendar 要拉 1990 至今全量日历(8730 行 / 78KB / 2 个上游请求),
        本调用只有 1 个标的、2 天区间;
      * `is_local=False` **强制走上游查询通道**(实测: 通道被拒令牌时它挂住不返回, 同时
        SDK 侧在刷 `Authentication for this token fails`) => 它验证的正是"查询通道能不能用",
        而不是"本地缓存里有没有";
      * 实测字节码顶层无 lock/acquire/release —— 不取 SDK 库内锁, 卡住只占自己那条线程。
    返回 None 判失败(与 get_calendar 同一判据: 会话未真正建立时 SDK 会返回 None)。

    ⚠️ **必须显式传 local_path**(2026-09-22 实测, 否则第一重会白等 20s):
      探针直调 SDK, 不走 /api/query, 因而**不会**享受 /api/query 的"默认参数注入"。
      不带 local_path 时 SDK 落到自己的默认路径, 实测会**挂住不返回**(线上探针连续
      TIMEOUT 20s, 而同一时刻经 /api/query 带 local_path 的调用只要 0.2s 就成功)。
      所以这里显式带上 DEFAULT_LOCAL_PATH, 与 /api/query 的注入行为保持一致。"""
    df = ad.InfoData().get_treasury_yield(
        code_list=[_TREASURY_CODE],
        begin_date=_TREASURY_BEGIN,
        end_date=_TREASURY_END,
        is_local=False,
        local_path=DEFAULT_LOCAL_PATH,
    )
    if df is None:
        raise RuntimeError("get_treasury_yield returned None (session not established)")
    return df


def _probe2_calendar():
    """第二重探针: BaseData.get_calendar() —— 第一重没拿到成功证据时的兜底。

    选它当第二重的理由仍是"**不抢 SDK 库内锁**": 实测字节码顶层完全没有
    lock/acquire/release。
    对比 MarketData.query_kline: offset 24 LOAD_ATTR acquire -> offset 3066 LOAD_ATTR
    release, 持锁横跨嵌套 get_kline_data 里一个**无超时**的 `EnvKline.wait_event.wait()`;
    而 QueryLock 是 AmazingData.environment 的**模块级**对象(全进程一把) =>
    探针一旦卡住就永久持锁, 所有用户 kline/snapshot 一起堵死。

    也不持有 _SDK_LOCK: 那把锁只用于 BaseData **下载型**接口串行化(见 _SERIAL_METHODS),
    get_calendar 不在其中 —— 实测 8 路并发 get_calendar 全部返回 8730 行且不拖慢并发 kline。
    旧写法 `with _SDK_LOCK: get_calendar()` 一旦卡住就**永远不释放锁**, 之后所有下载型业务
    查询连同后续每一次探针全部被锁死, 只能重启恢复。"""
    cal = ad.BaseData().get_calendar()
    if cal is None:
        raise RuntimeError("get_calendar returned None (session not established)")
    return cal


def _keepalive_probe():
    """真实查询探针 —— **两重, 第一重成功即止(不再查第二重)**:

      第一重  InfoData.get_treasury_yield   (轻量, is_local=False 强制走上游查询通道)
      第一重失败/超时 -> 第二重  BaseData.get_calendar

    每重各自 join 限时(20s / 60s, 合计 80s < 外层预算 90s), 两重都不取 SDK 库内锁。
    结论只写两个地方: 失败 -> _KEEPALIVE_FAILED; 成功 -> _PROBE_OK_TS 与
    STATS["last_success_ts"]。**探针自己不做任何判死动作**, 判定统一由 watchdog_loop
    按"连续 3 轮死亡证据"来做。

    (2026-09-21 的教训: 探针线程若卡死, 它可能永久持有库内锁, 所以仍然必须有
     _PROBE_WEDGE_MAX 的 wedge 兜底 —— Python 杀不掉线程, 只能强制重启清掉。)"""
    global _LAST_KEEPALIVE, _KEEPALIVE_FAILED, _PROBE_OK_TS, _PROBE_LAST
    global _KEEPALIVE_STRIKES
    _t0 = time.time()

    st, err = _probe_run(_probe1_treasury, _PROBE1_TIMEOUT)
    via, took = "probe#1 " + _PROBE1_NAME, time.time() - _t0

    if st != "ok":
        why = "TIMEOUT" if st == "timeout" else repr(err)
        log(f"watchdog: {via} failed ({why}, {took:.1f}s) "
            f"-> trying probe#2 {_PROBE2_NAME}", logging.WARNING, cat="watchdog")
        _t1 = time.time()
        st, err = _probe_run(_probe2_calendar, _PROBE2_TIMEOUT)
        via, took = "probe#2 " + _PROBE2_NAME, time.time() - _t1

    total = time.time() - _t0
    if st == "ok":
        _LAST_KEEPALIVE = time.time()
        _PROBE_OK_TS = _LAST_KEEPALIVE
        STATS["last_success_ts"] = _LAST_KEEPALIVE
        # 拿到存活证据 => 立刻清零 strike。
        # 清零只在这里(和探针失败时的仲裁)发生, **绝不**在"没有新结论的轮次"里发生:
        # 那样会让"连续 3 次死亡证据"永远凑不齐, 真断线时反而不重启(2026-09-21 踩过)。
        _KEEPALIVE_STRIKES = 0
        _PROBE_LAST = {"ok": True, "via": via, "took": round(took, 2),
                       "total": round(total, 2), "ts": _LAST_KEEPALIVE}
        log(f"watchdog: probe ok via {via} (took {took:.2f}s, total {total:.2f}s)",
            cat="watchdog")
        return

    why = "TIMEOUT" if st == "timeout" else repr(err)
    _KEEPALIVE_FAILED = True
    _PROBE_LAST = {"ok": False, "via": via, "took": round(took, 2),
                   "total": round(total, 2), "err": why, "ts": time.time()}
    log(f"watchdog: BOTH probes failed - {via}: {why} (total {total:.2f}s)",
        logging.ERROR, cat="watchdog")


async def watchdog_loop():
    global _KEEPALIVE_THREAD, _KEEPALIVE_FAILED, _KEEPALIVE_STRIKES
    global _LAST_KEEPALIVE, _LAST_VERIFY
    log("watchdog started", cat="watchdog")
    while True:
        await asyncio.sleep(30)
        try:
            # 首次登录还在进行中(进程刚起): 一律不判死, 安静等登录结果
            if not _LOGIN_STATE["ready"] and _LOGIN_STATE["done_ts"] is None:
                log("watchdog: initial login in progress, skip judgement", cat="watchdog")
                continue

            # 定时登出/登录窗口(03:58-04:01 / 08:58-09:00): 会话是**计划内登出**,
            # 探针失败属预期行为, 不计 strike、不重启、不触发重试(窗口到点由
            # _scheduled_relogin_loop 自己登录)
            if time.time() < _LOGIN_BLOCKED_UNTIL:
                log("watchdog: scheduled relogin window, skip judgement", cat="watchdog")
                continue

            now = time.time()

            # ---- 探针线程还在跑: 先分清是"慢"还是"卡死" ----
            # join 超时**不会杀掉线程**(Python 也杀不掉线程), 它只是在后台继续跑。
            # 若它卡在 SDK 的无超时 wait() 上, 就会一直活着:
            #   * 看门狗每轮走到这里就 continue => 既不判活也不判死, **等于瞎掉**;
            #   * 卡住的线程若还持有 SDK 库内锁(query_kline 那条路径), 会把用户查询一起堵死;
            #   * 而"心跳新鲜 => 判活"的短路又让看门狗永不重启去清理 => 永久卡死、无自愈。
            # 所以超过 _PROBE_WEDGE_MAX 就按"卡死"处理: **即使心跳新鲜也强制重启**,
            # 因为重启是唯一能把卡住的线程(及其持有的锁)清掉的手段。
            if _KEEPALIVE_THREAD is not None and _KEEPALIVE_THREAD.is_alive():
                _running = now - _KEEPALIVE_START_TS if _KEEPALIVE_START_TS else 0.0
                if _running > _PROBE_WEDGE_MAX:
                    # 这里用 force=True 绕开"年轻保护/重启冷却": 卡死的线程在进程内**无法**
                    # 被清理(既杀不掉, 也等不到它自己返回), 被冷却挡回去就等于永久卡死。
                    # 不会造成重启风暴 —— wedge 条件本身自带 300s 下限: 每次 execv 后探针线程
                    # 归零, 必须再活过 300s 才可能触发下一次, 300s 就是天然的最小重启间隔。
                    log(f"watchdog: keepalive probe thread WEDGED for {_running:.0f}s "
                        f"(> {_PROBE_WEDGE_MAX}s); it cannot be killed and may still hold "
                        f"the SDK query lock, and in-process retry cannot clear it "
                        f"-> force-restarting process", logging.ERROR, cat="watchdog")
                    _restart_process(force=True)
                    continue
                log(f"watchdog: keepalive probe still running ({_running:.0f}s), wait",
                    cat="watchdog")
                continue

            probe_failed = _KEEPALIVE_FAILED
            _KEEPALIVE_FAILED = False
            # 心跳: v6 起**只读、只打印**, 不参与任何判定。
            # 它只证明"连接在", 证明不了"数据取得到"(2026-09-22 凌晨 8 小时的教训)。
            hb_p = _sdk_heartbeat_age()      # 推送通道(会话级)
            hb_q = _sdk_query_hb_age()       # 查询通道级(空闲被 SDK 回收 = 正常现象)
            hb_desc = ("push " + ("unreadable" if hb_p is None else f"{hb_p:.0f}s")
                       + " / query " + ("unreadable" if hb_q is None else f"{hb_q:.0f}s"))

            # ---- 诊断(不影响判定): 心跳长期陈旧而真实查询一直成功 => 记一次"心跳源可疑" ----
            if (hb_p is not None and hb_p > _HB_SUSPECT_AGE and _PROBE_OK_TS
                    and (now - _PROBE_OK_TS) < _HB_SUSPECT_AGE):
                _mark_hb_suspect(hb_p)

            # ================== 判定: 真实查询说了算 (v6) ==================
            if probe_failed:
                # 探针两重都失败 —— 这是**唯一的死亡证据**。
                # 唯一豁免: **业务查询**(同属真实 L3 证据)在仲裁窗口内刚成功过 => 上游是通的,
                # 本次失败算偶发/探针自身问题, 本轮不计 strike。
                # 与 v5 的本质区别: v5 是拿**心跳**(连查询证据都不是)豁免探针失败, 那条路会让
                # "功能全挂但心跳健康"的故障永远判不出来(2026-09-22 凌晨 8 小时就是);
                # 这里豁免依据本身就是一次成功的真实查询, 空闲时段不成立 =>
                # 不会重蹈 v3/v4"空闲 => 凑出死亡证据"的覆辙。
                biz_ok_age = now - _LAST_QUERY_OK
                if biz_ok_age < _QUERY_OK_GRACE:
                    _KEEPALIVE_STRIKES = 0
                    log(f"watchdog: BOTH probes failed BUT a business query succeeded "
                        f"{biz_ok_age:.0f}s ago (<= {_QUERY_OK_GRACE}s grace) -> upstream is "
                        f"alive, not counting as dead evidence ({hb_desc})",
                        logging.WARNING, cat="watchdog")
                else:
                    _KEEPALIVE_STRIKES += 1
                    log(f"watchdog: BOTH probes failed and no business query succeeded within "
                        f"{_QUERY_OK_GRACE}s (last biz ok {biz_ok_age:.0f}s ago) -> dead "
                        f"evidence {_KEEPALIVE_STRIKES}/{_KEEPALIVE_STRIKES_REQUIRED} "
                        f"({hb_desc})", logging.WARNING, cat="watchdog")
                    if STATS["last_success_ts"] is None:
                        _ensure_retry_thread()   # 从未登录成功: 进程内退避重连即可, 不必 execv
                    if _KEEPALIVE_STRIKES >= _KEEPALIVE_STRIKES_REQUIRED:
                        _KEEPALIVE_STRIKES = 0
                        log(f"watchdog: session dead for {_KEEPALIVE_STRIKES_REQUIRED} "
                            f"consecutive dead evidences (probe={_PROBE_NAME}; {hb_desc}) "
                            f"-> restarting process", logging.ERROR, cat="watchdog")
                        _restart_process()
                        continue
            else:
                # 中性轮: 本轮**没有新的探针结论**(结论还没回来 / 还没到下一次发起时间)。
                # 这里**绝不清零 strike** —— 它既不是存活证据也不是死亡证据。清零只发生在
                # 探针真的拿到存活证据时(见 _keepalive_probe)。若在这里清零, "连续 3 次
                # 死亡证据"就永远凑不齐, 真断线时反而不会重启(2026-09-21 踩过这个坑)。
                if _PROBE_OK_TS:
                    log(f"watchdog: session alive (last probe ok "
                        f"{int(now - _PROBE_OK_TS)}s ago via {_PROBE_LAST.get('via') or '-'}; "
                        f"strike {_KEEPALIVE_STRIKES}/{_KEEPALIVE_STRIKES_REQUIRED}; "
                        f"{hb_desc})", cat="watchdog")
                else:
                    log(f"watchdog: no probe verdict yet -> keeping judgement open "
                        f"(strike {_KEEPALIVE_STRIKES}/{_KEEPALIVE_STRIKES_REQUIRED}; "
                        f"{hb_desc})", cat="watchdog")

            # ---- 到点就发下一发探针(**不论心跳如何**) ----
            # v5 是"心跳不新鲜才发探针", 那套的必然后果是: 心跳健康 => 永不发查询 =>
            # 功能坏了也发现不了。判活判死都由真实查询说话, 它就必须周期性发。
            if now - _LAST_VERIFY >= _PROBE_INTERVAL:
                _start_keepalive_probe(
                    "first probe since start" if _LAST_VERIFY <= 0
                    else f"periodic probe, last started {int(now - _LAST_VERIFY)}s ago")
            continue
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
    # 首次登录: 丢后台线程执行(2026-09-21 修复)。此处若同步等待登录, 端口会晚 20~60s
    # 才监听, 会被 run.sh 误判死亡并重复拉起。线程化后事件循环立即返回, 端口秒级可用。
    if not _LOGIN_STATE["ready"] and _LOGIN_STATE["done_ts"] is None:
        threading.Thread(target=_bootstrap_login, name="bootstrap-login",
                         daemon=True).start()
        log("initial TGW login dispatched to background thread (port binds immediately)")
    # v8: 定时登出/登录线程(每天 03:58/04:01, 08:58/09:00), 无论首登状态如何都要跑
    threading.Thread(target=_scheduled_relogin_loop, name="scheduled-relogin",
                     daemon=True).start()
    log("scheduled relogin thread dispatched (%s, in-process)"
        % ", ".join("%s->%s" % (o, i) for o, i in _SCHED_SLOTS))
    # v9: 全市场预加载线程(09:10 拉表 / 09:00-09:15 定版窗口不订 / 其余时间有表即订)
    try:
        _PRELOAD["loop"] = asyncio.get_running_loop()
        threading.Thread(target=_preload_loop, name="preload-universe",
                         daemon=True).start()
    except Exception as e:
        log(f"preload thread dispatch failed: {e!r}", logging.ERROR)
    task = asyncio.create_task(watchdog_loop())
    _persist_stats(force=True)   # 启动即落盘 SERVICE_START_TS, 保证后续 execv 自愈能沿用
    yield
    task.cancel()
    # v7: 优雅退出(SIGTERM / systemctl stop / run.sh restart)前主动登出, 还回账号连接槽位。
    # run.sh 给了 15s 优雅退出预算, 所以这里最多占 8s; 登出失败也不影响退出流程。
    _graceful_logout("shutdown", timeout=min(_LOGOUT_TIMEOUT, 8.0))
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
    """存活探针: 始终返回 200 —— 端口在监听即代表"进程存活", 供 run.sh 守护判定;
    内容里的 status/login 表示 **TGW 到底能不能取到数据** —— v6 起由**真实查询证据**
    判定(探针/业务查询成功过), 不再看心跳。由进程内 watchdog 负责自愈。
    (不要把会话不可用做成非 200: 那会让外部守护把"上游抖动"当"进程死了"而重复拉起。)"""
    alive = _session_alive()
    starting = (not _LOGIN_STATE["ready"]) and _LOGIN_STATE["done_ts"] is None
    hb = _sdk_heartbeat_age()
    hb_q = _sdk_query_hb_age()
    return {"status": "ok" if alive else ("starting" if starting else "error"),
            "login": alive,
            "ready": _LOGIN_STATE["ready"],
            "starting": starting,
            # --- 心跳字段: v6 起**仅供参考**, 不参与判活判死 ---
            # 它们只证明"连接在", 证明不了"数据取得到": 2026-09-22 凌晨两路心跳全程健康,
            # 数据查询却挂了 8 小时、/health 一直 status=ok。判活请看 probe_ok_age。
            "hb_age": None if hb is None else int(hb),
            "hb_max": _HB_FRESH_MAX,
            "hb_qry_age": None if hb_q is None else int(hb_q),
            "hb_suspect": _HB_SUSPECT,
            # --- v9 全市场订阅状态: 客户端只要挂得上就说明预加载正常 ---
            "sub": _sub_status(),
            # --- v9e local_data 缓存清理: 每次重连成功后触发一次 ---
            "cache_clean": _CACHE_CLEAN,
            # --- 真实查询证据: v6 判活判死的依据 ---
            "probe": _PROBE_NAME,
            # 距最近一次探针成功多少秒(None = 本进程还没成功过); 探针周期 = probe_interval
            "probe_ok_age": None if not _PROBE_OK_TS else int(time.time() - _PROBE_OK_TS),
            "probe_interval": _PROBE_INTERVAL,
            # 最近一次探针结论: {"ok","via","took","total",["err"],"ts"} —— 便于定位是哪一重失败
            "probe_last": _PROBE_LAST}


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
        "sub": _sub_status(),
        "last_success": "never" if STATS["last_success_ts"] is None
                        else time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(STATS["last_success_ts"])),
        # 心跳字段: v6 起**仅供参考**(不参与判活判死)。hb_age = 推送通道, hb_qry_age =
        # 查询通道(空闲约 10min 被 SDK 主动回收, 正常现象)。判活看 probe_ok_age / probe_last。
        "hb_age": None if _sdk_heartbeat_age() is None else int(_sdk_heartbeat_age()),
        "hb_qry_age": None if _sdk_query_hb_age() is None else int(_sdk_query_hb_age()),
        "hb_file": _HB_STAT[_HB_PUSH]["file"],
        "hb_suspect": _HB_SUSPECT,
        "last_keepalive": int(time.time() - _LAST_KEEPALIVE),
        # 距上次探针**发起**多久(秒); "never" = 本进程启动后还没发过(启动后第一轮就会发)
        "last_verify": ("never" if _LAST_VERIFY <= 0
                        else int(time.time() - _LAST_VERIFY)),
        # ---- v6: 探针链(第一重成功即止) + 真实查询证据 —— 判活判死只看这些 ----
        "probe": _PROBE_NAME,
        "probe1": _PROBE1_NAME,
        "probe2": _PROBE2_NAME,
        "probe_interval": _PROBE_INTERVAL,
        "probe_timeout1": _PROBE1_TIMEOUT,
        "probe_timeout2": _PROBE2_TIMEOUT,
        # 距最近一次探针成功多少秒(None = 本进程还没成功过)
        "probe_ok_age": None if not _PROBE_OK_TS else int(time.time() - _PROBE_OK_TS),
        # 最近一次探针结论: {"ok","via","took","total",["err"],"ts"}
        "probe_last": _PROBE_LAST,
        # 距最近一次**业务查询**成功多少秒(None = 本进程还没成功过)。
        # 它的作用是探针失败时的**仲裁**: <_QUERY_OK_GRACE 就不计 strike。
        "last_biz_query_ok_age": int(time.time() - _LAST_QUERY_OK),
        # 探针线程已存活秒数(0=当前没有在跑的探针)。持续增长且不回落 = 探针卡死,
        # 超过 _PROBE_WEDGE_MAX 看门狗会强制重启; 该字段用于事后确认是否发生过 wedge。
        "probe_wedge_max": _PROBE_WEDGE_MAX,
        # ---- v7: 优雅登出(释放账号并发连接槽位, 防 status[-98]) ----
        # count=累计尝试, ok/fail/timeout 分类计数, last_ms=最近一次耗时,
        # fd_marked_cloexec=最近一次 execv 前被置 CLOEXEC 的 fd 数(TGW socket 泄漏的证据)
        "logout": dict(_LOGOUT_STATE),
        "probe_running_s": (int(time.time() - _KEEPALIVE_START_TS)
                            if (_KEEPALIVE_THREAD is not None and _KEEPALIVE_THREAD.is_alive()
                                and _KEEPALIVE_START_TS) else 0),
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
    global _LAST_QUERY_OK
    # 同步 def: FastAPI 丢线程池执行, 避免同步 SDK 调用阻塞 uvicorn 事件循环
    # (慢查询不再冻结 /api/stats、/health、WS 推送; 查询之间在线程池并发)
    STATS["total_requests"] += 1
    _bump_today("requests", 1)
    # 首次登录尚未就绪时短暂等待, 避免在未登录会话上直接调 SDK 变成裸 500(503 更准确,
    # 调用方/网关也能据此重试)。注意: 本函数是同步 def, FastAPI 丢线程池执行, 不阻塞事件循环。
    if not _wait_login_ready():
        STATS["error_count"] += 1
        STATS["last_error"] = "service starting: initial TGW login not ready"
        log(f"query {q.cls}.{q.method} rejected: initial login not ready", logging.WARNING)
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error": "service starting: initial TGW login not ready"})
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
        # 业务查询成功 = 会话真的活着, 记下来供看门狗跳过本轮的保活探针(见 _LAST_QUERY_OK)
        _LAST_QUERY_OK = time.time()
        _persist_stats()
        return {"ok": True, "rows": rows, "data": data}
    except Exception as e:
        STATS["error_count"] += 1
        STATS["last_error"] = str(e)
        _persist_stats()
        log(f"query {q.cls}.{q.method} error: {e!r}", logging.ERROR)
        # v6: 请求路径上**不再直接重启** —— 参数错误、偶发超时都会走到这里, 在那儿 execv
        # 会打断并发中的其它请求, 而且每次重启都要重新登录(会撞账号连接数上限 status[-98])。
        # 改为"催一次探针取证": 清零 _LAST_VERIFY 让看门狗下一轮(<=30s)立刻发探针,
        # 由探针的两重真实查询来判断到底是不是会话死了, 连续 3 次才算数。
        _nudge_probe(f"{q.cls}.{q.method} failed")
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})
    except BaseException as e:
        # SDK 部分方法内部会抛 SystemExit 等 BaseException, 穿透 except Exception 会变成裸 500
        STATS["error_count"] += 1
        STATS["last_error"] = f"{type(e).__name__}: {e}"
        _persist_stats()
        log(f"query {q.cls}.{q.method} base-exception: {e!r}", logging.ERROR)
        _nudge_probe(f"{q.cls}.{q.method} base-exception")
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
# 这里改为: 全局唯一 SubscribeData + 唯一 run() 线程; 各客户端把自身缓冲
# 注册到标的名下, on_tick 按 data.code 扇出到所有注册缓冲(缓冲是覆盖式的
# 最新值槽位, 见下方 _ClientBuf, 慢客户端只丢中间状态、永不堆积)。
#
# 动态增删(方案A, 2026-08-13; 2026-08-14 去掉持久化):
#   - remove: 纯客户端过滤, 立即生效(把本连接槽位从该标的注册表摘除, 不再转发)。
#   - add(集合内): 立即生效(标的已在全局订阅流里, 只是开启转发)。
#   - add(集合外): 全局订阅流无法动态扩, 需重启进程(os.execv 全新登录)换集合;
#     重启后全局集合 = 重启后首连客户端请求的 code_list。
#     代价: 约 10s 中断, 所有客户端需重连。
_SUB_LOCK = threading.Lock()
_SUB = {
    "instance": None,   # ad.SubscribeData 单例
    "thread": None,     # 唯一 sub.run() 线程
    "codes": set(),     # 已订阅的 code 集合(重启后 = 首连请求决定, 不持久化)
    "period": None,     # 单市场时的 period 枚举值; 多市场(mixed)时为 None
    "period_name": None,  # period 字符串名, 多市场固定为 "mixed"
    "periods": [],      # v10: 本次订阅用到的 period 枚举值(升序去重)
    "markets": [],      # v10: 本次订阅包含的市场键(stock/etf/index/...)
    "queues": {},       # code -> set(_ClientBuf)  各客户端订阅(挂载式)
    "all_queues": set(),  # 传 code_list="all" 的连接缓冲(整市场全推, 不逐 code 挂载)
    "coalesced": 0,     # 旧帧被同标的新帧覆盖的次数(覆盖式缓冲, 不是丢弃)
    "gen": 0,           # 订阅代号: 每次重建 +1, 旧 on_tick 代号不符则静默
    "session_ts": None,  # 建立本订阅时的 TGW 登录时间戳(会话变了要重订)
    "ticks": 0,         # 本订阅累计收到 tick 数
    "last_tick_ts": 0.0,
}


def _period_name(value):
    return _PERIOD_NAMES.get(value, str(value))


def _not_in_universe_msg(codes):
    """表外代码的统一回复文案。全市场表 = 09:10 拉取的快照, 表外即未上市/已退市/写错。"""
    codes = list(codes)
    head = ", ".join(codes[:50])
    tail = f" 等 {len(codes)} 只" if len(codes) > 50 else ""
    return (f"全推数据中无此代码: {head}{tail}"
            f"(不在今日全市场表内: 未上市/已退市/代码写错)")


# ----------------------------------------------------------------------------
# WS 客户端缓冲：覆盖式「最新值槽位」+ 零延迟唤醒 / 全推("all")支持
# ----------------------------------------------------------------------------
# 老实现是每个连接一个 FIFO asyncio.Queue: 慢客户端会把历史帧堆到 5000 条才丢,
# 语义是"一串历史"而不是"最新状态"。现在改成覆盖式槽位:
#   内存里每只代码只保留**最新一份**快照, 新的一条直接覆盖旧的一条;
#   内存上限 = 本连接订阅的代码数(全推 = 7873 份 ≈ 27MB), 与客户端读得多慢无关;
#   发送协程挂在 asyncio.Event 上等唤醒 —— 没数据就不醒、不发帧、不占 CPU,
#   有数据立刻醒(微秒级), 与"来一条推一条"的实时性一致。
# 代价: 同一只代码在发送前又被更新过, 中间那几次会被合并(计在 /health 的
#   sub.coalesced)。快照本身 3 秒一档, 正常客户端几乎碰不到。
class _ClientBuf:
    """一个 WS 连接的待发缓冲(覆盖式, 非队列)。"""

    __slots__ = ("pending", "event", "coalesced")

    def __init__(self):
        self.pending = {}          # code -> payload(该只最新一份)
        self.event = asyncio.Event()
        self.coalesced = 0         # 本连接被合并掉的旧帧数

    def size(self):
        return len(self.pending)


def _push_tick(buf, item):
    """把一条 tick 写进连接的覆盖式槽位(同 code 覆盖旧值), 并唤醒发送协程。
    只在事件循环线程调用(由 call_soon_threadsafe 派发)。"""
    code = item.get("code")
    if code is None:
        return
    if code in buf.pending:
        buf.coalesced += 1
        _SUB["coalesced"] += 1
    buf.pending[code] = item
    if not buf.event.is_set():
        buf.event.set()


# ----------------------------------------------------------------------------
# v10 (2026-11-09): 订阅 flag 补齐
# ----------------------------------------------------------------------------
# SDK 的 SubscribeData.run() 只映射了一部分 period -> SubscribeDataType:
#   snapshot(10014) -> kSnapshot(10) / 指数 -> kIndexSnapshot(13)
#   snapshotfuture(20007) -> kFutureSnapshot(20)
# 但 **snapshotHKT(20005) 与 snapshotoption(20008) 两个分支缺失**, 逆回购也落在默认
# kNone(0)。服务端收到 flag=0 的订阅项会"接受订阅但不推数据"(静默空订阅),
# ack 正常、日志写 success, 却永远没有 tick —— 最坑的一种故障。
# 所以 run() 之前给 tgw.Subscribe 包一层: 只补 flag 仍为 0 的项,
# 已有原生映射的(10/13/20)一律不动。
_FLAG_MAP = {}          # 裸代码(不带 .SH/.SZ) -> 期望的 SubscribeDataType 值
_FLAG_PATCHED = False


def _install_flag_patch():
    """把 tgw.Subscribe 包一层, 给 flag==0 的订阅项补类型。幂等, 可重复调用。"""
    global _FLAG_PATCHED
    if _FLAG_PATCHED:
        return True
    try:
        import AmazingData.subscribe_api.on_data as _od
        _orig = _od.tgw.Subscribe
        _dt = _od.tgw.SubscribeDataType

        def _patched_subscribe(items, spi):
            n_fix = 0
            for it in items:
                if getattr(it, "flag", 0) != 0:
                    continue
                code = str(getattr(it, "security_code", "") or "")
                f = _FLAG_MAP.get(code)
                if f is None:                       # 兜底: 按代码长度判市场
                    ln = len(code)
                    if ln == 5:
                        f = _dt.kHKTSnapshot        # 港股通
                    elif ln == 8:
                        f = _dt.kOptionSnapshot     # ETF 期权
                    elif ln == 6:
                        f = _dt.kSnapshot           # A股/ETF/指数/逆回购
                if f:
                    it.flag = f
                    n_fix += 1
            if n_fix:
                log("subscribe: patched flag on %d items (hkt/option/glra)"
                    % n_fix, cat="watchdog")
            return _orig(items, spi)

        _od.tgw.Subscribe = _patched_subscribe
        _FLAG_PATCHED = True
        log("subscribe: tgw.Subscribe flag patch installed", cat="watchdog")
        return True
    except Exception as e:
        log("subscribe: flag patch install failed: %r" % (e,), logging.ERROR)
        return False


def _start_subscription(code_list, period_value, loop, groups=None):
    """启动全局唯一订阅。须持 _SUB_LOCK 调用; 重复调用返回 (False, 原因)。

    v10(2026-11-09): 支持多市场分组订阅。
      groups = [(market, codes, period_value), ...], 每个市场用自己的 period;
      这些分组会注册进同一个 SubscribeData, run() 时放进**同一次** tgw.Subscribe,
      每个 item 各带自己的 flag, 服务端多路并行推送, 内存/会话零增量。
      groups 为空时退化为单组(period_value), 保持 lazy 模式与旧调用兼容。
    """
    if _SUB["instance"] is not None:
        return False, "subscription already started"
    if groups:
        grp = [(str(m), sorted({c for c in cs if c}), int(p))
               for (m, cs, p) in groups if cs]
    else:
        grp = [("legacy", sorted(set(code_list)), int(period_value))]
    merged = sorted({c for _m, cs, _p in grp for c in cs})
    if not merged:
        return False, "empty code list"
    sub = ad.SubscribeData()
    _SUB["gen"] += 1
    _gen = _SUB["gen"]

    def on_tick(data, period):
        try:
            # 代号不符 = 这是一条已被丢弃的旧订阅, 静默掉, 避免重建后重复推送
            if _SUB["gen"] != _gen:
                return
            code = getattr(data, "code", None)
            if not code:
                return
            _SUB["ticks"] += 1
            _SUB["last_tick_ts"] = time.time()
            with _SUB_LOCK:
                qs = list(_SUB["queues"].get(code, ()))
                qs_all = list(_SUB["all_queues"])
            # 没人要这只代码 -> 连 model_dump() 都不做(全市场 1900 帧/秒时这一步很值钱)
            if not qs and not qs_all:
                return
            payload = data.model_dump()
            for q in qs:
                try:
                    loop.call_soon_threadsafe(_push_tick, q, payload)
                except Exception:
                    pass
            for q in qs_all:
                try:
                    loop.call_soon_threadsafe(_push_tick, q, payload)
                except Exception:
                    pass
        except Exception:
            pass

    # v10: 逐市场注册 —— 每个市场用自己的 period; 同一 SubscribeData 里混装多组,
    # run() 会把它们合并进一次 tgw.Subscribe(每项各带自己的 flag)。
    reg_ok = 0
    for _mkt, _codes, _p in grp:
        try:
            sub.register(code_list=_codes, period=_p)(on_tick)
            reg_ok += 1
        except Exception as e:
            log("subscribe: register(%s) failed: %r" % (_mkt, e), logging.ERROR)

    # v10: run() 的分支表缺 snapshotHKT/snapshotoption 两个映射(flag 会停在默认 0),
    # 逆回购的 flag 也停在 0。这里按"代码 -> 期望 flag"精确补齐, 长度兜底。
    _FLAG_MAP.clear()
    for _mkt, _codes, _p in grp:
        _f = _MARKET_FLAG.get(_mkt)
        if _f is None:
            continue
        for _c in _codes:
            _FLAG_MAP[_c] = _f
    _install_flag_patch()

    periods = sorted({p for _m, _c, p in grp})
    _SUB["instance"] = sub
    _SUB["codes"] = set(merged)
    _SUB["periods"] = periods
    _SUB["period"] = periods[0] if len(periods) == 1 else None
    _SUB["period_name"] = (_period_name(periods[0]) if len(periods) == 1
                           else "mixed")
    _SUB["markets"] = [m for m, _c, _p in grp]
    _SUB["session_ts"] = TGW_LOGIN_TS
    _SUB["last_tick_ts"] = time.time()   # 起订阅先给 180s 宽限, 别让自愈误判
    _SUB["thread"] = threading.Thread(target=sub.run, daemon=True,
                                      name="subscribe-run")
    _SUB["thread"].start()
    log(f"global subscription started: n_codes={len(merged)}"
        f" markets={','.join(_SUB['markets'])} period={_SUB['period_name']}"
        f" groups={reg_ok}/{len(grp)} gen={_gen} session_ts={TGW_LOGIN_TS}")
    return True, ""


# ----------------------------------------------------------------------------
# v9 (2026-11-06): 全市场预加载订阅 —— 网关自己持有一条"全市场"订阅流
# ----------------------------------------------------------------------------
# 与 v8 及以前的区别:
#   旧: 全局集合 = 首个客户端的 code_list; 后来客户端要集合外的代码 -> execv 重启换集合
#       (约 10s 中断, 所有客户端掉线)。"服务多客户端"实际上做不到。
#   新: 09:10 拉全市场代码表, 09:15 用全表起一条订阅(其余时间有表就订); 客户端连上来只做"挂槽位/摘槽位",
#       要哪几只就转哪几只, 永不重启进程, 多客户端天然共享同一条流。
#
# 时序(2026-11-07 定稿, 按"跨日 09:15 分界"而不是"当天 09:15 起"来理解):
#   * 09:15 ~ 次日 09:00: 只要有表(盘上缓存也算)就**立刻建订阅**。半夜/凌晨/盘中重启或崩溃,
#     开机即用, 不再"死等 09:15"。用的是现有表(昨天的表), 因为不是每天都有新股/退市股。
#   * 09:00 ~ 09:15: **表定版窗口**, 不建也不重建订阅。理由: 09:00 的进程内定时重登会把旧会话
#     (连同旧订阅)废掉, 09:10 才拉当天的新表; 这段先建订阅会绑在旧表上, 而 SDK 的
#     SubscribeData 只有 register/run、**没有 unsubscribe** —— 订阅只能靠"重启进程"或
#     "换会话"作废, 在同一会话上重订会变成两份订阅。所以等 09:15 用当天表一次性建。
#   * 拉表失败(或表没变化)就沿用现有表, 不卡订阅: 09:10 拉不到, 09:15 照样用旧表起订阅。
#   * 表中途被换掉的风险也由此消除: 当天订阅建好后把"表已定版"落到缓存, 当天不再拉表,
#     避免"表换了、订阅还是旧的"这种静默收不到。
#   盘中若再发生重登/自愈, 本线程会用新会话自动重订。
#
# 内存实测(2026-11-06, 本机 1874MB): ad.login() 自身就占 ~1.14GB(SDK 固定缓冲池),
#   7873 只(A股+ETF+指数)全市场订阅的增量近似为 0。所以"全市场常驻"不额外吃内存,
#   但一个进程只能有一份 —— 不要再起第二个 SDK 进程, 那会直接 OOM。
#
# 配置(config.ini 可选, 缺省即下面的默认值):
#   [preload]
#   enabled = 1
#   fetch_markets     = all                 # 每天 09:10 拉哪些市场的代码表, all=全部 8 类
#   subscribe_markets = stock,etf,index     # 开盘订阅哪些市场(必须是 fetch_markets 的子集)
#   list_time = 09:10
#   hold_start = 09:00          # hold_start~subscribe_time 之间不建订阅(表定版窗口)
#   subscribe_time = 09:15
#
# v10(2026-11-09) 市场注册表: 键 -> (中文名, security_type, 取表方法, period, flag)
#   stock  A股股票  EXTRA_STOCK_A       get_code_list        snapshot(10014)       kSnapshot(10)
#   etf    ETF      EXTRA_ETF           get_code_list        snapshot              kSnapshot(10)
#   index  指数     EXTRA_INDEX_A       get_code_list        snapshot              kIndexSnapshot(13)
#   kzz    可转债   EXTRA_KZZ           get_code_list        snapshot              kSnapshot(10)
#   glra   逆回购   EXTRA_GLRA          get_code_list        snapshot              kSnapshot(10)
#   hkt    港股通   EXTRA_HKT           get_code_list        snapshotHKT(20005)    kHKTSnapshot(12)
#   future 期货     EXTRA_FUTURE        get_future_code_list snapshotfuture(20007) kFutureSnapshot(20)
#   option ETF期权  EXTRA_ETF_OP        get_option_code_list snapshotoption(20008) kOptionSnapshot(11)
#
# 一个 SubscribeData 里可按市场分别 register(各自的 period), run() 时合并成
# **一次** tgw.Subscribe(每项带自己的 flag), 所以多市场并行推送不额外占内存/会话。
# 旧的 security_types / period 两个键已废弃(仍可留在配置里, 读取时忽略)。
# 环境变量 AD_PRELOAD_NOW=1 可跳过时间判断立即拉表+订阅(仅用于盘中/盘后验证)。
_PRELOAD_ENABLED = (_cfg("preload", "enabled", "1") or "1").strip().lower() in (
    "1", "true", "yes", "on")

_MARKETS = (
    # key,   中文名,     security_type,  取表方法,               period,           flag 属性名
    ("stock",  "A股股票", "EXTRA_STOCK_A",  "get_code_list",        "snapshot",       "kSnapshot"),
    ("etf",    "ETF",     "EXTRA_ETF",      "get_code_list",        "snapshot",       "kSnapshot"),
    ("index",  "指数",    "EXTRA_INDEX_A",  "get_code_list",        "snapshot",       "kIndexSnapshot"),
    ("kzz",    "可转债",  "EXTRA_KZZ",      "get_code_list",        "snapshot",       "kSnapshot"),
    ("glra",   "逆回购",  "EXTRA_GLRA",     "get_code_list",        "snapshot",       "kSnapshot"),
    ("hkt",    "港股通",  "EXTRA_HKT",      "get_code_list",        "snapshotHKT",    "kHKTSnapshot"),
    ("future", "期货",    "EXTRA_FUTURE",   "get_future_code_list", "snapshotfuture", "kFutureSnapshot"),
    ("option", "ETF期权", "EXTRA_ETF_OP",   "get_option_code_list", "snapshotoption", "kOptionSnapshot"),
)
_MARKET_KEYS = tuple(m[0] for m in _MARKETS)
_MARKET_BY_KEY = {m[0]: m for m in _MARKETS}
_MARKET_LABEL = {m[0]: m[1] for m in _MARKETS}
_MARKET_FLAG = {}          # market -> SubscribeDataType 值(补丁用), 取不到就靠长度兜底
try:
    import tgw as _tgw_mod
    for _m in _MARKETS:
        _MARKET_FLAG[_m[0]] = getattr(_tgw_mod.SubscribeDataType, _m[5])
except Exception as _e:     # 装不上 tgw 也要能启动, 只是港股通/期权会拿不到数据
    log("preload: SubscribeDataType unavailable (%r)" % (_e,), logging.WARNING)


def _parse_markets(raw, default):
    """把 "stock,etf,index" / "all" 解析成市场键列表(保序去重, 未知键忽略并告警)。"""
    s = (raw or "").strip()
    if not s:
        return list(default)
    items = [x.strip().lower() for x in s.replace(";", ",").replace(" ", ",").split(",")
             if x.strip()]
    if any(x in ("all", "*") for x in items):
        return list(_MARKET_KEYS)
    out, bad = [], []
    for x in items:
        if x in _MARKET_BY_KEY:
            if x not in out:
                out.append(x)
        else:
            bad.append(x)
    if bad:
        log("preload: 未知市场键 %r 已忽略(可用: %s)"
            % (bad, ",".join(_MARKET_KEYS)), logging.WARNING)
    return out or list(default)


# 每天拉取的代码表: 默认全部 8 类(拉表成本很低, 表全了以后改订阅不用重拉)
_PRELOAD_FETCH_MARKETS = _parse_markets(
    _cfg("preload", "fetch_markets", "all"), _MARKET_KEYS)
# 开盘订阅哪些市场: 默认与改造前一致(A股+ETF+指数), 其余按需在 config.ini 打开
_PRELOAD_SUB_MARKETS = _parse_markets(
    _cfg("preload", "subscribe_markets", "stock,etf,index"),
    ("stock", "etf", "index"))
_PRELOAD_TYPES = [m[2] for m in _MARKETS if m[0] in _PRELOAD_FETCH_MARKETS]  # 兼容旧日志
_PRELOAD_LIST_HM = _cfg("preload", "list_time", "09:10")
_PRELOAD_HOLD_HM = _cfg("preload", "hold_start", "09:00")
_PRELOAD_SUB_HM = _cfg("preload", "subscribe_time", "09:15")
_PRELOAD_PERIOD = _cfg("preload", "period", "")   # 已废弃: 周期改为按市场自动选择
_PRELOAD_NOW = os.environ.get("AD_PRELOAD_NOW", "").strip() not in ("", "0", "false", "no")
_PRELOAD_CACHE = os.path.join(_APP_DIR, "run", "preload_codes.json")

_PRELOAD = {
    "codes": [],
    "by_market": {},        # v10: 市场键 -> 代码列表(每天拉的 8 类全存这里)
    "market_counts": {},    # v10: 市场键 -> 数量(便于 /health 一眼看全)
    "list_date": None,      # 代码表对应日期 YYYY-MM-DD
    "list_ts": None,
    "source": None,         # disk / remote
    "error": None,
    "subscribed_date": None,
    "resub_count": 0,
    "last_error_ts": 0.0,
    "loop": None,           # 主事件循环(lifespan 注入), on_tick 跨线程投递要用
    "zero_streak": 0,       # 交易时段内连续"无 tick"轮数
    "heal_date": None,      # 自愈重启的计数归属日期
    "heal_count": 0,        # 当天自愈重启次数(上限 3, 防重启风暴)
    "table_skip_date": None,  # 今天已因"订阅还挂在旧表上"跳过一次拉表(只记一次日志)
}


def _in_trading_window(t):
    """是否处在"应该有 tick"的时段: A股 09:30-11:30 / 13:00-15:00;
    订了期货时再加上夜盘 21:00-02:30(期货夜盘有行情, A股没有)。"""
    a = datetime.time(9, 30)
    b = datetime.time(11, 30)
    c = datetime.time(13, 0)
    d = datetime.time(15, 0)
    if (a <= t <= b) or (c <= t <= d):
        return True
    if "future" in _PRELOAD_SUB_MARKETS:
        return t >= datetime.time(21, 0) or t <= datetime.time(2, 30)
    return False


def _hm_to_time(hm, default):
    try:
        h, m = str(hm).split(":")
        return datetime.time(int(h), int(m))
    except Exception:
        return default


_PRELOAD_LIST_T = _hm_to_time(_PRELOAD_LIST_HM, datetime.time(9, 10))
_PRELOAD_SUB_T = _hm_to_time(_PRELOAD_SUB_HM, datetime.time(9, 15))
_PRELOAD_HOLD_T = _hm_to_time(_PRELOAD_HOLD_HM, datetime.time(9, 0))


def _in_preopen_hold(t=None):
    """是否处在"表定版窗口"(默认 09:00-09:15): 这段时间不建/不重建订阅。

    窗口边界由 hold_start 与 subscribe_time 决定; 配反了(hold_start >= subscribe_time)
    就当作没有窗口, 行为退化为"有表就订"。"""
    if t is None:
        t = datetime.datetime.now().time()
    if _PRELOAD_HOLD_T >= _PRELOAD_SUB_T:
        return False
    return _PRELOAD_HOLD_T <= t < _PRELOAD_SUB_T


def _load_preload_cache():
    """进程重启时从磁盘恢复代码表, 省掉重新拉表的 35s。

    v10: 缓存按市场分开存(by_market)。旧格式缓存只有扁平的 codes(A股+ETF+指数),
    读到后 by_market 为空, 主循环会因此强制重拉一次多市场表(见 _preload_loop)。"""
    try:
        with open(_PRELOAD_CACHE, "r", encoding="utf-8") as f:
            d = json.load(f)
        codes = [str(c) for c in (d.get("codes") or [])]
        by_market = {str(k): [str(c) for c in v]
                     for k, v in (d.get("by_market") or {}).items() if v}
        if codes:
            _PRELOAD["codes"] = codes
            _PRELOAD["by_market"] = by_market
            _PRELOAD["market_counts"] = {k: len(v) for k, v in by_market.items()}
            _PRELOAD["list_date"] = d.get("date")
            _PRELOAD["list_ts"] = d.get("ts")
            _PRELOAD["source"] = "disk"
            log("preload: restored %d codes from cache (date=%s, markets=%s)%s"
                % (len(codes), d.get("date"), ",".join(sorted(by_market)) or "-",
                   "" if by_market else " [旧格式缓存, 将重拉多市场表]"))
    except FileNotFoundError:
        pass
    except Exception as e:
        log("preload: cache load failed: %r" % (e,), logging.WARNING)


def _fetch_market_codes(bd, key, tries=3):
    """拉单个市场的代码表, 独立重试。返回 list(失败/为空返回 [])。

    该接口今天多次出现瞬时 NoneType(整批失败、几十秒后自愈), 所以每类各自重试,
    一类抖动不牵连其它类。"""
    m = _MARKET_BY_KEY.get(key)
    if m is None:
        return []
    st, meth = m[2], m[3]
    last = None
    for _i in range(tries):
        try:
            lst = getattr(bd, meth)(security_type=st)
            if lst:
                return [str(x) for x in lst]
            last = "returned empty"
        except Exception as e:
            last = repr(e)
        time.sleep(1.0)
    log("preload: %s(%s) 重试 %d 次仍失败: %s" % (meth, st, tries, last),
        logging.ERROR)
    return []


def _fetch_preload_codes():
    """每天拉一次全市场代码表(默认 8 类全拉), 按市场分开存。成功返回 by_market, 失败 None。

    只拉 fetch_markets 里的市场; **订阅哪些市场由 subscribe_markets 决定**(见 _build_sub_groups)。
    表全拉的好处: 之后想加市场只改 config.ini 重启即可, 不用重新拉表。"""
    if _PRELOAD["loop"] is None:
        return None
    t0 = time.time()
    try:
        bd = ad.BaseData()
    except Exception as e:
        _PRELOAD["error"] = "BaseData: %s" % e
        _PRELOAD["last_error_ts"] = time.time()
        log("preload: BaseData() failed: %r" % (e,), logging.ERROR)
        return None
    by_market, failed = {}, []
    for key in _PRELOAD_FETCH_MARKETS:
        got = _fetch_market_codes(bd, key)
        if got:
            by_market[key] = sorted(set(got))
            log("preload: %-6s %-18s -> %5d codes (%.1fs)"
                % (key, _MARKET_BY_KEY[key][2], len(by_market[key]), time.time() - t0))
        else:
            failed.append(key)
    if not by_market:
        _PRELOAD["error"] = "all markets failed"
        _PRELOAD["last_error_ts"] = time.time()
        log("preload: 全市场表拉取失败, 沿用现有表", logging.ERROR)
        return None
    # 个别市场失败: 沿用缓存里同市场的旧表, 别把整份表作废
    for key in failed:
        old = _PRELOAD["by_market"].get(key) or []
        if old:
            by_market[key] = old
            log("preload: %s 拉取失败, 沿用缓存 %d 只" % (key, len(old)),
                logging.WARNING)
    codes = sorted({c for v in by_market.values() for c in v})
    _PRELOAD["codes"] = codes
    _PRELOAD["by_market"] = by_market
    _PRELOAD["market_counts"] = {k: len(v) for k, v in by_market.items()}
    _PRELOAD["list_date"] = datetime.date.today().isoformat()
    _PRELOAD["list_ts"] = time.time()
    _PRELOAD["source"] = "remote"
    _PRELOAD["error"] = ("partial: " + ",".join(failed)) if failed else None
    try:
        os.makedirs(os.path.dirname(_PRELOAD_CACHE), exist_ok=True)
        with open(_PRELOAD_CACHE, "w", encoding="utf-8") as f:
            json.dump({"date": _PRELOAD["list_date"], "ts": _PRELOAD["list_ts"],
                       "by_market": by_market,
                       "counts": _PRELOAD["market_counts"],
                       "codes": codes}, f)
    except Exception as e:
        log("preload: cache save failed: %r" % (e,), logging.WARNING)
    log("preload: universe ready, %d codes / %d markets in %.1fs (%s)"
        % (len(codes), len(by_market), time.time() - t0,
           ", ".join("%s=%d" % (k, len(v)) for k, v in sorted(by_market.items()))))
    return by_market


def _settle_universe_for_today(today):
    """当天定版: 订阅已按当前这份表建好, 当天不再换表(把 date 改写成今天并落盘)。

    为什么需要:
      1) 拉表失败时也要定版 —— 沿用现有表。否则盘中每轮(10s)都会重拉一次(单次约 35s),
         而且万一 10:30 才拉成功, 表就跟已经建好的订阅不一致了(客户端能挂上、却永远收不到),
         属于最坏的静默故障;
      2) 定版标记随缓存落盘, 进程重启后读到 date=今天 就不再拉表, 订阅集合与表恒一致。
    只在 list_time 之后定版: 半夜/凌晨用昨天的表建的订阅不算定版, 09:10 该拉还得拉。"""
    if _PRELOAD["list_date"] == today:
        return
    if not _PRELOAD_NOW and datetime.datetime.now().time() < _PRELOAD_LIST_T:
        return
    old_src = _PRELOAD["source"] or "disk"
    _PRELOAD["list_date"] = today
    try:
        os.makedirs(os.path.dirname(_PRELOAD_CACHE), exist_ok=True)
        with open(_PRELOAD_CACHE, "w", encoding="utf-8") as f:
            json.dump({"date": today, "ts": _PRELOAD["list_ts"] or time.time(),
                       "settled": True,
                       "by_market": _PRELOAD["by_market"],
                       "counts": _PRELOAD["market_counts"],
                       "codes": _PRELOAD["codes"]}, f)
    except Exception as e:
        log("preload: settle cache save failed: %r" % (e,), logging.WARNING)
    log("preload: universe settled for %s, %d codes (表未更新则沿用 %s 的表, 当天不再换表)"
        % (today, len(_PRELOAD["codes"]), old_src), cat="watchdog")


def _build_sub_groups():
    """按 config.ini 的 subscribe_markets, 从已拉到的表里组织出订阅分组。

    返回 [(market, codes, period_value), ...], 每个市场用自己的 period。
    v10 的关键: 这些分组进同一个 SubscribeData, 一次 tgw.Subscribe 全部提交,
    所以"A股 + ETF + 指数 + 港股通 + 期货 + 期权 + 逆回购"是多路并行、单会话、单进程。"""
    groups = []
    for key in _PRELOAD_SUB_MARKETS:
        codes = _PRELOAD["by_market"].get(key) or []
        if not codes:
            log("preload: 市场 %s 已选但表里没有代码(本轮拉取失败?), 跳过" % key,
                logging.WARNING)
            continue
        pname = _MARKET_BY_KEY[key][4]
        pval = _PERIOD_VALUES.get(pname)
        if pval is None:
            log("preload: 市场 %s 的 period %r 不在 SDK 枚举里, 跳过" % (key, pname),
                logging.ERROR)
            continue
        groups.append((key, codes, pval))
    return groups


def _ensure_preload_subscription():
    """确保全市场订阅在跑; 会话变了(定时重登/自愈)就用新会话重订。返回是否新建。"""
    if _PRELOAD["loop"] is None or not _PRELOAD["codes"]:
        return False
    groups = _build_sub_groups()
    if not groups:
        _PRELOAD["error"] = ("no market selected (subscribe_markets=%s)"
                             % ",".join(_PRELOAD_SUB_MARKETS))
        log("preload: subscribe_markets=%r 没有可用代码, 跳过订阅"
            % (",".join(_PRELOAD_SUB_MARKETS),), logging.ERROR)
        return False
    with _SUB_LOCK:
        cur = _SUB["instance"]
        if cur is not None:
            th = _SUB["thread"]
            stale = _SUB["session_ts"] != TGW_LOGIN_TS
            dead = (th is None) or (not th.is_alive())
            if not stale and not dead:
                return False
            log("preload: rebuilding subscription (stale_session=%s thread_dead=%s)"
                % (stale, dead), logging.WARNING, cat="watchdog")
            _SUB["gen"] += 1        # 让旧线程的 on_tick 立即失效
            _SUB["instance"] = None
            _SUB["thread"] = None
            _PRELOAD["zero_streak"] = 0
            if _PRELOAD["subscribed_date"]:
                _PRELOAD["resub_count"] += 1
        ok, err = _start_subscription(_PRELOAD["codes"], None, _PRELOAD["loop"],
                                      groups=groups)
        if not ok:
            _PRELOAD["error"] = err
        return ok


def _preload_loop():
    """代码表刷新 + 订阅拉起 + 订阅复活。10s 一轮, 极轻。"""
    log("preload thread started (enabled=%s list=%s sub=%s fetch=%s subscribe=%s now=%s)"
        % (_PRELOAD_ENABLED, _PRELOAD_LIST_HM, _PRELOAD_SUB_HM,
           ",".join(_PRELOAD_FETCH_MARKETS) or "-",
           ",".join(_PRELOAD_SUB_MARKETS) or "-", _PRELOAD_NOW), cat="watchdog")
    if not _PRELOAD_ENABLED:
        return
    _load_preload_cache()
    while True:
        _sleep = 10.0
        try:
            if not _LOGIN_STATE.get("ready"):
                time.sleep(5)
                continue
            now = datetime.datetime.now()
            today = now.date().isoformat()

            # 1) 代码表: 每天一份, 到点才拉(重启后用磁盘缓存, 不重复拉)。
            #    拉表前先确认"换表后能重建订阅": 订阅还活着且没换会话时**不能换表** ——
            #    没有 unsubscribe 接口, 同一会话上重订会变成两份订阅; 而只改表不重订会出现
            #    "表里有这只、订阅里没有"(客户端能挂上却永远收不到)的静默故障。
            #    正常情况下 09:00 的定时重登已经把会话换掉, 所以这条守卫只在
            #    "relogin 没配 09:00 / 09:00 那次没成功"时才生效, 代价是当天继续用旧表。
            _sub_usable = (_SUB["instance"] is not None
                           and _SUB["session_ts"] == TGW_LOGIN_TS)
            # v10: by_market 为空 = 盘上是旧格式扁平缓存(没有分市场), 必须重拉一次,
            #      这种"补表"不看 list_time(半夜重启也要先把表补齐, 否则订不了多市场)
            _need_table = (_PRELOAD["list_date"] != today
                           or not _PRELOAD["by_market"])
            _time_ok = (_PRELOAD_NOW or now.time() >= _PRELOAD_LIST_T
                        or not _PRELOAD["by_market"])
            if _need_table and _time_ok:
                if _sub_usable and not _PRELOAD_NOW:
                    if _PRELOAD["table_skip_date"] != today:
                        _PRELOAD["table_skip_date"] = today
                        log("preload: skip table refresh today (订阅仍挂在旧表上且会话没换; "
                            "换表会与订阅不一致, 等下次换会话再拉)",
                            logging.WARNING, cat="watchdog")
                else:
                    _fetch_preload_codes()

            # 2) 订阅: 09:15 ~ 次日 09:00 只要有表就建/维持(半夜/凌晨重启也能开机即用);
            #    09:00-09:15 是表定版窗口, 不建也不重建(旧会话已被 09:00 重登废掉, 等 09:15
            #    用当天表一次性建)。当天建好后定版, 之后重启不再换表。
            if _PRELOAD["codes"]:
                if _in_preopen_hold(now.time()) and not _PRELOAD_NOW:
                    # 窗口内别傻睡一整轮: 睡到 09:15 那一刻, 集合竞价的第一帧别漏
                    _now2 = datetime.datetime.now()
                    _sub_at = datetime.datetime.combine(_now2.date(), _PRELOAD_SUB_T)
                    _sleep = max(1.0, min(10.0,
                                          (_sub_at - _now2).total_seconds() + 0.3))
                elif _ensure_preload_subscription():
                    _PRELOAD["subscribed_date"] = today
                    _settle_universe_for_today(today)
                    log("preload: subscription up, %d codes, markets=%s period=%s"
                        % (len(_SUB["codes"]), ",".join(_SUB["markets"]),
                           _SUB["period_name"]), cat="watchdog")

            # 3) 零推送自愈: 交易时段内订阅在跑、却持续 3 轮(30s) 没有任何 tick,
            #    说明流可能"连上了但收不到"(2026-09-22 那类静默故障) -> 重启进程重登重订。
            #    注意只在有行情的时段判, 午休/收盘后本来就没推送。
            if _in_trading_window(now.time()) and _SUB["instance"] is not None:
                last = _SUB["last_tick_ts"]
                age = time.time() - last if last else 0
                if last and age > 180:
                    _PRELOAD["zero_streak"] += 1
                else:
                    _PRELOAD["zero_streak"] = 0
                if _PRELOAD["zero_streak"] >= 3:
                    if _PRELOAD["heal_date"] != today:
                        _PRELOAD["heal_date"] = today
                        _PRELOAD["heal_count"] = 0
                    if _PRELOAD["heal_count"] < 3:
                        _PRELOAD["heal_count"] += 1
                        _PRELOAD["zero_streak"] = 0
                        log("preload: no tick for %ds in trading window -> restart "
                            "(heal %d/3 today)" % (int(age), _PRELOAD["heal_count"]),
                            logging.ERROR, cat="watchdog")
                        _restart_process(force=True)
                    else:
                        log("preload: still no tick after heals, quota exhausted today",
                            logging.ERROR, cat="watchdog")
                        _PRELOAD["zero_streak"] = 0
        except Exception as e:
            log("preload loop error: %r" % (e,), logging.ERROR, cat="watchdog")
        time.sleep(_sleep)


def _sub_status():
    """订阅状态快照(供 /health 与 /api/stats 展示)。"""
    with _SUB_LOCK:
        th = _SUB["thread"]
        alive = bool(th and th.is_alive())
        stale = _SUB["instance"] is not None and _SUB["session_ts"] != TGW_LOGIN_TS
        # ready = 这条订阅真的能收数据: 实例在, 且没有被重登换掉的旧会话
        ready = _SUB["instance"] is not None and not stale
        n_codes = len(_SUB["codes"])
        period = _SUB["period_name"]
        periods = list(_SUB["periods"])
        markets = list(_SUB["markets"])
        ticks = _SUB["ticks"]
        last = _SUB["last_tick_ts"]
        n_client_codes = len(_SUB["queues"])
        n_all_clients = len(_SUB["all_queues"])
        coalesced = _SUB["coalesced"]
    hold = _in_preopen_hold()
    return {
        "mode": "preload" if _PRELOAD_ENABLED else "lazy",
        "ready": ready,
        "thread_alive": alive,
        "session_stale": stale,
        "universe": n_codes,
        "period": period,
        "periods": periods,                       # v10: 多市场时为 [10014, 20005, ...]
        "markets": markets,                       # v10: 实际订阅的市场键
        "market_labels": {m: _MARKET_LABEL.get(m, m) for m in markets},
        "market_counts": {m: len(_PRELOAD["by_market"].get(m) or ())
                          for m in _PRELOAD_SUB_MARKETS},
        "subscribe_markets": list(_PRELOAD_SUB_MARKETS),   # config.ini 选择
        "fetch_markets": list(_PRELOAD_FETCH_MARKETS),     # config.ini 选择
        "ticks": ticks,
        "last_tick_age": None if not last else int(time.time() - last),
        "client_codes": n_client_codes,
        "all_clients": n_all_clients,
        "coalesced": coalesced,
        "universe_list_date": _PRELOAD["list_date"],
        "universe_source": _PRELOAD["source"],
        "universe_error": _PRELOAD["error"],
        "resub_count": _PRELOAD["resub_count"],
        "preopen_hold": hold,                       # 09:00-09:15 表定版窗口, 期间不建订阅
        "hold_until": _PRELOAD_SUB_HM if hold else None,
    }


@app.websocket("/ws/market")
async def ws_market(ws: WebSocket):
    """WS 协议:
    首帧: {"code_list":[...], "period":"snapshot"}  -> 订阅(定义/加入全局集合)
    控制帧(首帧之后任意帧):
        {"type":"add", "code_list":[...]}           -> 增订(预加载模式下不可用代码只报错)
        {"type":"remove", "code_list":[...]}        -> 退订(本连接立即生效)
        code_list 传 "all" (或 {"all":true})        -> 全市场全推: 网关把订阅流收到的
                                                       每一条 tick 都转发给本连接

    服务端推送:
        {"type":"tick", "data":{...}}               行情
        {"type":"ack", "action":"subscribe|add|remove", ...} 操作确认
        {"type":"error", "message":...}
        {"type":"restarting", "message":...}        仅非预加载模式(手工建订阅)才会出现

    v9 预加载模式铁律: 客户端的任何请求都不允许触发进程重启。
    请求的代码不在全市场表里 -> 只回 {"type":"error","message":"全推数据中无此代码: ..."},
    连接不断、可用代码照常推送。

    "all" 语义: all_mode=True 时不逐 code 挂队列(否则一次要往 7873 个 code 下塞 set),
    而是把本连接缓冲登记到 _SUB["all_queues"], on_tick 对每条 tick 额外投递一份。
    all 模式下 remove 某只代码 = 加入本连接的 excluded 屏蔽名单, 其余照常推。
    """
    await ws.accept()
    STATS["ws_connections"] += 1
    loop = asyncio.get_event_loop()
    # 覆盖式缓冲(不是队列): 每只代码只留最新一份, 内存上限 = 订阅的代码数
    q = _ClientBuf()
    _cli = getattr(ws, "client", None)
    _addr = f"{_cli.host}:{_cli.port}" if _cli else "unknown"
    my_codes = set()          # 本连接当前订阅的标的(逐 code 挂载的那些)
    all_mode = False          # True = 全市场全推
    excluded = set()          # 全推模式下被 remove 屏蔽掉的代码
    reader_task = None

    async def send(obj):
        await ws.send_text(json.dumps(obj, ensure_ascii=False, default=str))

    def attach(codes):
        """把本连接缓冲挂到标的名下(立即开始接收该标的 tick)。"""
        nonlocal my_codes
        for c in codes:
            _SUB["queues"].setdefault(c, set()).add(q)
        my_codes |= set(codes)

    def detach(codes):
        """把本连接缓冲从标的名下摘除(停止接收该标的 tick), 不影响全局订阅流。"""
        nonlocal my_codes
        for c in codes:
            qs = _SUB["queues"].get(c)
            if qs:
                qs.discard(q)
                if not qs:
                    _SUB["queues"].pop(c, None)
            my_codes.discard(c)

    def attach_all():
        """登记为全市场全推连接(不逐 code 挂载, 只进 all_queues 一个集合)。"""
        nonlocal all_mode
        all_mode = True
        _SUB["all_queues"].add(q)

    def detach_all():
        """取消全推, 本连接回到"什么都不推"的状态。"""
        nonlocal all_mode
        all_mode = False
        _SUB["all_queues"].discard(q)

    def want(code):
        """这条 tick 要不要发给本连接。"""
        if not code:
            return False
        if all_mode:
            return code not in excluded
        return code in my_codes

    async def next_frame():
        """取下一份待发数据。没有新数据就挂在 event 上等唤醒(不轮询、不发帧)。
        同一只代码在发送前被更新过, 这里只会拿到最新那份(旧的已被覆盖)。"""
        while not q.pending:
            q.event.clear()
            await q.event.wait()
        code = next(iter(q.pending))     # 按写入顺序取; 跨标的先后不保证
        return q.pending.pop(code)

    def parse_codes(payload):
        """解析 code_list: 兼容 "600000.SH" 单串与 ["..."] 数组; 返回 (codes, want_all)。
        "all"/"*" 是通配符, 不当作代码; 非字符串元素直接忽略(不再触发 unhashable 崩溃)。"""
        if isinstance(payload, str):
            payload = [payload]        # 单串不再被按字符拆开
        if not isinstance(payload, (list, tuple)):
            return [], False
        want_all = False
        seen = {}
        for c in payload:
            if not isinstance(c, str) or not c:
                continue
            if c.strip().lower() in ("all", "*"):
                want_all = True
                continue
            seen[c] = None
        return list(seen), want_all

    async def restart_for_new_codes(new_codes, missing):
        """集合外增订: 仅非预加载模式(手工建订阅)才重启进程扩容。
        预加载模式下全局流固定为全市场表, 表外代码不可能订阅成功, 重启毫无意义
        且会造成"重启-重连-再重启"的风暴, 因此一律只回错误帧。"""
        if _PRELOAD_ENABLED:
            await send({"type": "error",
                        "message": _not_in_universe_msg(missing)})
            log(f"ws add rejected (not in universe): {missing[:10]}", logging.WARNING)
            return
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
                codes, want_all = parse_codes(cmd.get("code_list", []))
                if action == "add":
                    if want_all:
                        with _SUB_LOCK:
                            attach_all()
                            excluded.clear()
                        await send({"type": "ack", "action": "add", "added": "all",
                                    "universe_size": len(_SUB["codes"])})
                        log(f"ws add all -> 全市场全推 (universe={len(_SUB['codes'])})")
                    if codes:
                        missing, added, new_codes = [], [], []
                        with _SUB_LOCK:
                            missing = sorted({c for c in codes if c not in _SUB["codes"]})
                            added = sorted(c for c in codes if c in _SUB["codes"])
                            if all_mode:
                                # 全推模式下 add 已覆盖全市场, 这里只做"解除屏蔽"
                                excluded.difference_update(codes)
                                added = sorted(c for c in codes if c in _SUB["codes"])
                            elif _PRELOAD_ENABLED:
                                # 表内代码立即挂上; 表外代码只回错误, 绝不重启
                                attach(added)
                            elif missing:
                                new_codes = sorted(set(_SUB["codes"]) | set(missing))
                            else:
                                attach(codes)
                                added = sorted(codes)
                        if all_mode:
                            if added:
                                await send({"type": "ack", "action": "add",
                                            "added": added, "mode": "all"})
                                log(f"ws add (all mode) un-excluded={added}")
                        elif _PRELOAD_ENABLED:
                            if added:
                                await send({"type": "ack", "action": "add",
                                            "added": added})
                                log(f"ws add codes={added}")
                        elif missing:
                            await restart_for_new_codes(new_codes, missing)
                            return
                        else:
                            await send({"type": "ack", "action": "add",
                                        "added": added})
                            log(f"ws add codes={added}")
                        if missing and _PRELOAD_ENABLED:
                            await send({"type": "error",
                                        "message": _not_in_universe_msg(missing)})
                            log(f"ws add rejected (not in universe): {missing[:10]}",
                                logging.WARNING)
                elif action == "remove":
                    if want_all:
                        with _SUB_LOCK:
                            detach_all()
                            detach(sorted(my_codes))
                            excluded.clear()
                        await send({"type": "ack", "action": "remove",
                                    "removed": "all"})
                        log("ws remove all -> 停止全推")
                    elif codes:
                        with _SUB_LOCK:
                            if all_mode:
                                # 全推下 remove = 只屏蔽这几只, 其余继续推
                                excluded.update(codes)
                                detach(codes)   # 同时清掉逐 code 挂载, 免得退出全推后复活
                                removed = sorted(codes)
                            else:
                                removed = sorted(c for c in codes if c in my_codes)
                                detach(removed)
                        await send({"type": "ack", "action": "remove",
                                    "removed": removed,
                                    **({"mode": "all_exclude"} if all_mode else {})})
                        log(f"ws remove codes={removed}"
                            f"{' (all mode, excluded)' if all_mode else ''}")
                elif action:
                    await send({"type": "error",
                                "message": f"unknown command: {action}"})
        except Exception:
            pass   # 连接关闭

    # 首次登录未就绪时先异步等待(不阻塞事件循环), 避免在未登录会话上起订阅
    _waited = 0.0
    while not _LOGIN_STATE["ready"] and _waited < _LOGIN_READY_TIMEOUT:
        await asyncio.sleep(0.5)
        _waited += 0.5
    if not _LOGIN_STATE["ready"]:
        await send({"type": "error",
                    "message": "service starting: initial TGW login not ready, retry later"})
        return

    try:
        cfg = await ws.receive_json()
        if not isinstance(cfg, dict) or ("code_list" not in cfg and "all" not in cfg):
            await send({"type": "error",
                        "message": '首帧必须包含 code_list (要全市场请传 "all")'})
            return
        code_list, want_all = parse_codes(cfg.get("code_list", []))
        if cfg.get("all"):
            want_all = True
        period = cfg.get("period", "snapshot")
        # v10: 多市场订阅后周期不再唯一, "mixed"/"auto"/"all" 表示"不挑周期, 要整条流"
        _mixed_names = ("mixed", "auto", "all", "")
        if period is None or (isinstance(period, str) and period.strip().lower() in _mixed_names):
            period_value = None          # 不校验周期, 由网关按市场自动选择
        else:
            try:
                period_value = getattr(ad.constant.Period, period).value
            except AttributeError:
                await send({"type": "error", "message": f"unknown period: {period}"})
                return
        err = None
        missing = []
        new_codes = []
        with _SUB_LOCK:
            if _PRELOAD_ENABLED:
                # ---- v9 预加载模式: 全局流 = 全市场, 客户端只挂/摘队列, 永不重启 ----
                # 会话变了(stale)也算未就绪: 旧订阅已被重登废掉, 让客户端重连比挂在死订阅上强
                _stale = _SUB["instance"] is not None and _SUB["session_ts"] != TGW_LOGIN_TS
                if _SUB["instance"] is None or _stale:
                    if _in_preopen_hold():
                        err = ("表定版窗口(09:00-09:15): 09:00 的会话重建会作废旧订阅, "
                               "09:15 才用当天全市场表起新订阅, 请 09:15 后重连")
                    else:
                        err = "全市场行情尚未就绪(订阅建立中或会话重建中), 请稍后重连"
                elif period_value is not None and period_value not in _SUB["periods"]:
                    _pnames = ",".join(_period_name(p) for p in _SUB["periods"])
                    err = (f"网关全局订阅 period={_SUB['period_name']}({_pnames}), "
                           f"请求 {period} 不一致(单账号只允许一条订阅流; "
                           f"可传 period=\"mixed\" 表示不挑周期)")
                elif want_all:
                    # 全市场全推: 只登记到 all_queues, 不逐 code 挂 7873 个队列
                    attach_all()
                else:
                    avail = [c for c in code_list if c in _SUB["codes"]]
                    missing = sorted({c for c in code_list if c not in _SUB["codes"]})
                    attach(avail)
            elif want_all:
                # lazy 模式下没有"全市场表"概念, 全推无从谈起
                err = ('code_list="all" 需要预加载模式([preload] enabled=1); '
                       '当前为 lazy 模式, 请显式列出代码')
            elif _SUB["instance"] is None:
                # lazy 模式(无预加载表)下没有"按市场分配周期"可言, 默认按快照订
                _pv = (period_value if period_value is not None
                       else _PERIOD_VALUES.get("snapshot", 10014))
                ok, err = _start_subscription(code_list, _pv, loop)
                if ok:
                    attach(code_list)
            else:
                if period_value is not None and period_value not in _SUB["periods"]:
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
        _warn_missing = None
        if missing:
            if _PRELOAD_ENABLED:
                # 预加载模式下缺的代码 = 不在今日全市场表里(未上市/退市/代码写错)
                # 只报错, 不重启; 可用代码照常推送
                _warn_missing = _not_in_universe_msg(missing)
                log(f"ws subscribe: {len(missing)} codes not in universe: "
                    f"{missing[:10]}", logging.WARNING)
            else:
                await restart_for_new_codes(new_codes, missing)
                return   # 进程将被 execv 替换, 不再继续
        # v9d: 首帧订阅把代码明细打出来, 最多 30 只(一次订几千只时避免日志行被撑爆)
        if all_mode:
            log(f"ws client attached: {_addr} n=all period={period} "
                f"(universe={len(_SUB['codes'])}) codes=ALL")
            _ack = {"type": "ack", "action": "subscribe",
                    "codes": "all", "all": True,
                    "universe_size": len(_SUB["codes"]),
                    "period": _SUB["period_name"]}
        else:
            _codes_sorted = sorted(my_codes)
            if len(_codes_sorted) <= 30:
                _codes_desc = str(_codes_sorted)
            else:
                _codes_desc = "%s ... 共 %d 只" % (_codes_sorted[:30],
                                                 len(_codes_sorted))
            log(f"ws client attached: {_addr} n={len(my_codes)} period={period} "
                f"(universe={len(_SUB['codes'])}) codes={_codes_desc}")
            _ack = {"type": "ack", "action": "subscribe",
                    "codes": sorted(my_codes),
                    "universe_size": len(_SUB["codes"]),
                    "period": _SUB["period_name"]}
        if _PRELOAD_ENABLED:
            # 预加载模式下全局池 7000+ 只, 不能整包回传; 只回本连接实际的 + 不可用清单
            # (全推模式没有 unavailable 概念, 整个池子都在推)
            if not all_mode:
                _ack["unavailable"] = missing[:200]
        else:
            _ack["global_codes"] = sorted(_SUB["codes"])
        await send(_ack)
        if _warn_missing:
            # ack 之后紧跟一条 error, 让客户端明确知道哪几只是"全推数据中无此代码"
            await send({"type": "error", "message": _warn_missing})
        reader_task = asyncio.create_task(reader())
        # 并发等待「有待发行情」与「reader 任务结束」: 客户端一断开, reader 任务
        # 立即结束, 主循环随之退出, 不再卡在等数据等到下一个 tick 才撞墙
        # (那种场景会触发 send-after-close 误报)。
        while True:
            get_task = asyncio.ensure_future(next_frame())
            done, pending = await asyncio.wait(
                {get_task, reader_task}, return_when=asyncio.FIRST_COMPLETED)
            if reader_task in done:
                # 客户端已断开(reader 退出)
                for t in pending:
                    t.cancel()
                log(f"ws client disconnected: {_addr}")
                break
            data = get_task.result()
            if not want(data.get("code")):
                continue    # remove/屏蔽后槽位里残留的旧 tick, 直接丢弃
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
            log(f"ws client disconnected: {_addr}")
        else:
            log(f"ws handler error: {e!r}", logging.ERROR)
    finally:
        if reader_task:
            reader_task.cancel()
        with _SUB_LOCK:
            detach_all()          # 全推连接: 从 all_queues 摘除
            detach(sorted(my_codes))
        q.pending.clear()         # 立刻释放本连接的待发快照
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
