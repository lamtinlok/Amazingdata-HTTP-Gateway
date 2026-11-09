# AmazingData HTTP REST 调用规范文档

> 文档日期：**2026-11-09**（REST 查询部分沿用 2026-08-12 实测；行情订阅部分按 v10 多市场改造更新，2026-11-09 盘中复测）
> 测试环境：本机 <客户端机器>（纯 HTTP 客户端，**不装 AmazingData SDK**）→ 远端 `<网关机器IP>:8000`（HTTP 中间层，持有 TGW 登录会话）→ TGW 数据源 `<TGW数据源IP_电信入口>:8600`（电信入口；备选联通入口 `<TGW数据源IP_联通入口>:8600`）
> 后端：`http_service.py`（FastAPI + uvicorn，启动时 `ad.login()` 一次；v9 起由网关进程自己持有全市场订阅）
> SDK 版本：AmazingData **1.1.9**
> REST 示例于 2026-08-12 用 curl 实测；WS 订阅示例于 2026-11-06 盘中实测（网关 `<网关机器IP>:8000`）。

## 0. 版本变更（v9 / v10，2026-11）—— 与 2026-08 版的最大区别

行情订阅的架构变了，**旧版客户端说明书里关于「订阅集合」的规则全部作废**：

| 项 | 2026-08（旧） | 现在（v9d / v10） |
|---|---|---|
| 谁来订阅 | 第一个连上来的客户端发 `code_list` 决定全局集合 | 网关自持一条全市场流：**09:05 拉表，09:00~09:15 不订，09:15 用当天表订全部**；其余时间重启就立刻订（v10 默认 A股 + ETF + 指数 + 逆回购，约 7891 只） |
| 客户端 `code_list` 含义 | 定义全局订阅集合 | **从已有全市场流里选出自己要转发哪些**，只影响本连接 |
| 增订表外代码 | 触发进程重启（`restarting` 帧，约 10s 中断） | **只回 `error 全推数据中无此代码`，连接不断、不重启** |
| 多客户端 | 共享一个集合，后连的会触发重启 | 各自独立缓冲，互不影响，任意并发 |
| ack 字段 | `codes` + `global_codes`（全量列表） | `codes` + `universe_size`（数量）+ `period`（v10 多市场时 `mixed`）+ `unavailable` |
| 重启影响 | 客户端必须重连 | 定时重登在进程内完成，**WS 不断**；只有进程级自愈才断 |
| 品种覆盖 | 只看股票 / ETF | **股票 + ETF + 指数**（2026-11-06 起）；v10 再扩到 8 类市场（默认订 stock/etf/index/glra）。指数 tick 字段更少，见 §3.6.5 |
| 全市场全推 | 无（只能把 7000+ 代码写进 `code_list`，ack 会回显整包） | **`code_list:"all"`** 一条就够，ack 只回 `codes:"all"` + `universe_size`，见 §3.6.3 ④ |

> 如果你是照 2026-08 版写的客户端：只要不依赖 `global_codes` 字段、不指望「发 add 就能扩集合」，代码不用改。
>
> **2026-11-06 23:10 增量：指数并入全推流。** 池子 7237 → 7873，`/health.universe` 与 `ack.universe_size` 都会变；
> 老客户端如果只订股票/ETF，行为完全不变；**订了指数则必须处理缺字段**（§3.6.5 ③）。
>
> **2026-11-06 23:50 增量：支持 `code_list:"all"`。** 另外顺手修了两个坑：`code_list` 传裸字符串不再被按字符拆开，
> 混入非字符串元素不再导致 `TypeError` 断连。
>
> **2026-11-07 00:20 增量：推送缓冲改为「覆盖式最新值」。** 每个连接不再是一条 FIFO 队列，而是
> 「每只代码只留最新一份」的槽位：有数据立刻推、没数据不发帧、客户端读得慢只会合并中间状态而不是堆积。
> 帧格式、`ack`、`add`/`remove`、`all` 语法**全部不变**，客户端无需改动；`/health.sub` 的 `queue_drops`
> 换成了 `coalesced`（被合并掉的旧帧数），`[ws] queue_max` 配置项废弃。
>
> **2026-11-09 增量：v10 多市场改造（线上 `<网关机器IP>:8000`）。** 订阅池从「A股+ETF+指数」扩成
> **8 类市场**（stock / etf / index / kzz / glra / hkt / future / option），代码表全拉、
> `subscribe_markets` 决定实际订阅哪些。当前线上配置 `stock,etf,index,glra` → **7891 只**
> （A股 5579 + ETF 1658 + 指数 636 + 逆回购 18），`/health.sub.universe` 从 7873 变 7891。
> 周期不再全局唯一：各市场自带周期（日/指数快照 `10014`、港股通 `snapshotHKT`、期货 `snapshotfuture`、
> 期权 `snapshotoption`），多个市场混订时 `/health.sub.period` 会变成 **`mixed`**。
> **协议对客户端零破坏**：首帧/`ack`/`tick` 帧格式、`add`/`remove`、`all` 全部未变；
> 新增 `period:"mixed"/"auto"/"all"` 表示「不挑周期」，原有 `period:"snapshot"` 在
> stock/etf/index/glra 混订时依然合法（见 §3.6.3 ①）。

---

## 1. 通用约定

### 1.1 服务地址
- 基础地址：`http://<网关机器IP>:8000`（同网段直连；若不在同一网络，用 `ssh -L 8000:127.0.0.1:8000 root@<网关机器IP>` 做端口转发后访问 `http://127.0.0.1:8000`）

### 1.2 唯一数据接口：`POST /api/query`
所有数据查询统一走这一个接口，请求体为：

```json
{
  "cls": "类名",              // BaseData / InfoData / MarketData / SubscribeData ...
  "method": "方法名",          // SDK 真实方法名（见 §3）
  "params": { ... }            // 方法的全部关键字参数（kwargs）
}
```

`params` 会以 `**kwargs` 方式传给 SDK 方法，所以**参数名必须与 SDK 签名完全一致**（大小写敏感）。

### 1.3 响应格式
成功：

```json
{
  "ok": true,
  "rows": 1619,          // 数据量：列表=list 长度；dict=code 个数
  "data": [ ... ]        // 见下方"数据形态"
}
```

失败（被后端捕获的异常）：

```json
{
  "ok": false,
  "error": "异常消息"
}
```

> ⚠️ 少部分方法抛 `BaseException`，会穿透后端异常处理，返回**裸 HTTP 500**（响应体为纯文本 `Internal Server Error`），`ok:false` 的 JSON 都拿不到。见 §6.2。

### 1.4 数据形态（重要）
`data` 字段有两种形态，解析代码必须兼容：

| 形态 | 说明 | 典型方法 |
|------|------|----------|
| **标量列表** | `["516190.SH", "513530.SH", ...]` | `get_code_list` / `get_calendar` / `get_hist_code_list` |
| **记录列表** | `[{...}, {...}]`，每条是字段字典 | `get_margin_summary` / `get_share_holder` / `get_balance_sheet`(若可用) |
| **按代码分组的 dict** | `{"510300.SH": [{...}, ...]}`，key 为证券代码 | `get_fund_share` / `get_fund_iopv` / `get_industry_daily` / `query_kline` |

### 1.5 日期格式
日期参数一律用 **8 位整型**：`20240101`（不是字符串，不是时间戳）。

### 1.6 其他端点
| 端点 | 用途 |
|------|------|
| `GET /health` | 进程存活 + TGW 会话状态 + 订阅状态（**永远返回 200**，见下） |
| `GET /api/stats` | 运行指标：运行时间/请求数/数据行/流量/内存/订阅/探针/登出/最近日志 |
| `GET /` | 状态面板（独立静态文件 `index.html`，与 http_service.py 同目录，改 UI 覆盖文件即可，**无需重启 server**） |
| `WS /ws/market` | 实时行情推送（见 §3.6，**v9 起预加载架构、v10 起多市场**） |

**`/health` 关键字段**（2026-11 版）：

```json
{
  "status": "ok",              // ok / starting / error —— 由真实查询证据判定，不看心跳
  "login": true,
  "ready": true,               // 首次登录完成
  "starting": false,
  "hb_age": 12, "hb_max": 60, "hb_qry_age": 340, "hb_suspect": false,   // 仅参考，不参与判活
  "sub": {
    "mode": "preload",         // preload=全市场自订阅；lazy=旧模式（配置关闭预加载时）
    "ready": true,             // 能收数据: 订阅实例在, 且没挂在被重登作废的旧会话上
    "thread_alive": true,
    "session_stale": false,    // 订阅线程还挂在旧 TGW 会话上（重登瞬间为 true）
    "universe": 7891,          // 已订阅代码数（由 config.ini 的 subscribe_markets 决定）
    "markets": ["stock", "etf", "index", "glra"],   // 实际订阅的市场（v10）
    "market_labels": {"stock": "A股股票", "etf": "ETF", "index": "指数", "glra": "逆回购"},
    "market_counts": {"stock": 5579, "etf": 1658, "index": 636, "glra": 18},   // 各市场只数
    "subscribe_markets": ["stock", "etf", "index", "glra"],   // config.ini 选择订阅哪些
    "fetch_markets": ["stock", "etf", "index", "kzz", "glra", "hkt", "future", "option"],  // 拉表 8 类
    "periods": [10014],        // 本次订阅流用到的 period（多市场时可能多个）
    "period": "snapshot",      // 多周期混订时为 "mixed"（v10）
    "ticks": 75845,            // 本进程累计收到 tick 数
    "last_tick_age": 2,        // 距上一条 tick 秒数；收盘后一直增长属正常
    "client_codes": 4,         // 当前有客户端订阅的代码数
    "all_clients": 0,          // 当前处于「全推(all)」模式的连接数
    "coalesced": 0,            // 被同标的更新覆盖掉的旧帧数(慢客户端才会增长)
    "universe_list_date": "2026-11-06",   // 代码表对应日期
    "universe_source": "disk", // disk=读本地缓存 / remote=当天新拉取
    "universe_error": null,
    "resub_count": 2,          // 会话更换后重建订阅的次数
    "preopen_hold": false,     // true=正处 09:00~09:15 表定版窗口（这段不建订阅）
    "hold_until": null         // 窗口内为 "09:15"，其余为 null
  },
  "cache_clean": {"last_ts": 1793970000, "count": 3, "files": 0, "bytes": 0,
                  "last_reason": "login", "last_err": "", "running": false},
  "probe": "get_calendar", "probe_ok_age": 8, "probe_interval": 60,
  "probe_last": {"ok": true, "via": "query", "took": 0.4, "total": 0.4}
}
```

> ⚠️ **`/health` 恒为 200 是刻意设计**：外部守护（`run.sh`）用它判断「进程还活着」，会话抖动由进程内 watchdog 处理。把会话不可用改成非 200 会导致重登窗口被守护误判为卡死而重启进程，运行计时归零。
>
> 客户端要看「行情通不通」，请读 `sub.ready` 与 `sub.last_tick_age`，而不是 HTTP 状态码。

**`/api/stats` 里对客户端有用的字段**：`service_uptime_seconds` / `service_start`（服务计时，进程内重登不归零）、`sub`（同 `/health`）、`logout`、`probe_ok_age`、`today`（当日流量）、`mem_*`。

### 1.7 耗时经验（实测）
- `get_code_list` / `get_code_info` / `get_hist_code_list` / `get_adj_factor` / `get_option_code_list`：**15–21 秒**（TGW 全量拉取，正常现象，客户端超时要设长）。
- 其余 InfoData 明细类：0.2–4 秒。
- 登录在**进程启动时**执行一次；之后每天还会在进程内定时换 3 次会话（`[relogin]`，见 §3.6.7），都由网关自己完成。**请求端每次调用都复用当前会话，不会触发登录**。

### 1.8 通用请求方式
本机不装 SDK，任何语言直接发 HTTP：

```bash
# curl
curl -X POST http://<网关机器IP>:8000/api/query \
  -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_code_list","params":{"security_type":"EXTRA_ETF"}}'
```

```python
# Python requests
import requests
r = requests.post("http://<网关机器IP>:8000/api/query",
                  json={"cls": "BaseData", "method": "get_calendar", "params": {"market": "SH"}},
                  timeout=60)
d = r.json()
print(d["ok"], d["rows"])
```

### 1.9 本地缓存路径（local_path，2026-08-12 起后端统一注入）

> SDK 内置默认 `local_path` 是 Windows 路径 `D://AmazingData_local_data//`，在 Linux 上会被当作**相对路径**落盘到进程 CWD 下的 `D:/AmazingData_local_data`（脏目录，已清理）。后端现在统一注入规范默认路径：

- **默认值**：`<网关部署目录>/local_data/`（脚本目录 `local_data`，服务启动时自动创建）
- **注入条件**：仅当方法签名接受对应参数 **且调用方未显式传**时注入；显式传 `local_path` / `is_local` 的请求完全不受影响
- **默认 `is_local=false`（2026-08-12 21:45 起）**：调用方不传 `is_local` 时，后端统一注入 `false` → **每次从 TGW 拉最新数据**，同时更新本地缓存；只有显式传 `is_local:true` 才读本地缓存
- ⚠️ **TGW 上游故障时**：`is_local=false` 会直接报错（如 `'NoneType' object is not subscriptable`），此时显式 `is_local:true` 可读本地缓存兜底（数据为上次落盘内容）
- **缓存写入后形态**：`basedata/backward_factor/backward_factor.h5`、`basedata/hist_code_list/YYYYMMDD.pkl`、`infodata/<type>/<code>.h5` 等，与 `config/local_data_folder.pyc` 枚举一一对应
- **全量回归验证（2026-08-12 22:00）**：注入逻辑对签名甄别准确——`get_hist_code_list` 仅接受 `local_path`（不注入 `is_local`，显式传 `is_local` 会报 `unexpected keyword argument`）；`get_adj_factor`/`get_backward_factor` 等同时接受两者。缓存实际落盘 17 文件 / 44MB，形态如上。`is_local=true` 读缓存兜底对 `get_backward_factor` 实测有效（8702 行 / 2s）。详见 `FULL_REGRESSION_TEST_REPORT.md`。
- **缓存会被自动清理（2026-11-06 起）**：每次登录成功后 30 秒（`AD_CACHE_CLEAN_DELAY`），网关会删除整个 `local_data/infodata/`（保留 `local_data` 与其他子目录）。也就是说**每天 3 次重登各清一次**，别指望 `infodata` 里的 h5 长期留存。清理结果写入日志与 `/health` 的 `cache_clean` 字段。此前是 crontab 每 3 天跑一次外部脚本，已废弃。

---

## 2. 命令目录总览（实测结论）

✅ = 实测通过（本文所有示例均已跑通）；❌ = 实测不可用；⬜ = 未验证

### 2.1 BaseData（基础数据）

| 方法 | 关键参数 | 实测 | 数据形态 |
|------|----------|------|----------|
| `get_code_list` | `security_type` | ✅ 1619/5544/315 行 | 标量列表 |
| `get_calendar` | `market`, `data_type`, `date` | ✅ 8702 行 | 标量列表 |
| `get_code_info` | `security_type` | ✅ 10488 行 | 记录列表（含 `code_market` 代码列，2026-08-15 起） |
| `get_backward_factor` | `code_list` | ⚠️ TGW 故障中 | 记录列表 |
| `get_adj_factor` | `code_list`, `is_local` | ⚠️ TGW 故障中 | 记录列表 |
| `get_hist_code_list` | `security_type`, `start_date`, `end_date` | ✅ 5346 行 | 标量列表 |
| `get_etf_pcf` | `code_list` | ⚠️ TGW 故障中 | 记录列表 |
| `get_future_code_list` | `security_type` | ✅ 814 行 | 标量列表 |
| `get_future_code_info` | `security_type` | ✅ 814 行 | 记录列表 |
| `get_option_code_list` | `security_type` | ✅ 1250 行 | 标量列表 |

### 2.2 InfoData（信息 / 财务 / 交易 / 衍生品）

| 方法 | 关键参数 | 实测 | 数据形态 |
|------|----------|------|----------|
| `get_bj_code_mapping` | `is_local` | ✅ 248 行 | 记录列表 |
| `get_history_stock_status` | `code_list`, `begin_date`, `end_date` | ✅ 1 行 | 记录列表 |
| `get_profit_notice` | `code_list` + 日期 | ✅ 0 行（当期无数据） | 记录列表 |
| `get_share_holder` | `code_list` + 日期 | ✅ 20 行 | 记录列表 |
| `get_holder_num` | `code_list` + 日期 | ✅ 2 行 | 记录列表 |
| `get_equity_structure` | `code_list` + 日期 | ✅ 2 行 | 记录列表 |
| `get_equity_pledge_freeze` | `code_list` + 日期 | ✅ 1 行 | 记录列表 |
| `get_equity_restricted` | `code_list` + 日期 | ✅ 1 行 | 记录列表 |
| `get_dividend` | `code_list` + 日期 | ✅ 0 行（当期无） | 记录列表 |
| `get_right_issue` | `code_list` + 日期 | ✅ 0 行（当期无） | 记录列表 |
| `get_margin_summary` | `begin_date`, `end_date` | ✅ 66–351 行 | 记录列表 |
| `get_margin_detail` | `code_list` + 日期 | ✅ 1 行 | 记录列表 |
| `get_long_hu_bang` | `code_list` + 日期 | ✅ 0 行（当期无） | 记录列表 |
| `get_block_trading` | `code_list` + 日期 | ✅ 2 行 | 记录列表 |
| `get_option_basic_info` | `code_list`(可空) | ✅ 0 行 | 记录列表 |
| `get_option_std_ctr_specs` | `code_list`(可空) | ✅ 0 行 | 记录列表 |
| `get_option_mon_ctr_specs` | `code_list` | ✅ 0 行 | 记录列表 |
| `get_fund_share` | `code_list` + 日期 | ✅ 1 行 | 按代码 dict |
| `get_fund_iopv` | `code_list` + 日期 | ✅ 1 行 | 按代码 dict |
| `get_industry_weight` | `code_list` + 日期 | ✅ 1 行 | 记录列表 |
| `get_industry_daily` | `code_list` + 日期 | ✅ 1 行 | 按代码 dict |
| `get_treasury_yield` | `code_list`(如 `["CGB"]`) + 日期 | ✅ 1 行 | 记录列表 |
| `get_announcement_stock_list` | `code_list`, `is_local` | ✅ 318 行 | 记录列表 |
| `get_stock_basic` | `code_list` | ✅ 1 行 | 记录列表 |
| `get_balance_sheet` / `get_cash_flow` / `get_income` | `code_list` + 日期 | ✅ 已通（NaN 序列化已修） | 按代码 dict |
| `get_index_constituent` / `get_industry_constituent` | `code_list` | ✅ 已通 | 按代码 dict |
| `get_kzz_conv` / `get_kzz_call` / `get_kzz_put` | `code_list` | ✅ 已通 | 记录列表 |
| `get_fund_nav` | `code_list` + 日期 | ✅ 已通 | 按代码 dict |
| `get_announcement_bond_list` | `code_list` | ✅ 已通(49 行) | 记录列表 |

### 2.3 MarketData（行情查询）—— ✅ 已打通（2026-08-12 后端修复后）

| 方法 | 关键参数 | 实测 | 数据形态 |
|------|----------|------|----------|
| `query_kline` | `code_list`, `begin_date`, `end_date`, `period`, `begin_time`/`end_time`(分钟线) | ✅ 日K 18根 / 30分K 144 / 5分K 864 / 1分K 4320 | 按代码 dict |
| `query_snapshot` | `code_list`, `begin_date`, `end_date` | ✅ 通 | `{日期: {代码: [记录]}}` |

> `query_kline` 的 `period` 支持字符串(`"day"`/`"min5"`)或枚举整数(`10008` 等)，后端已做自动映射。返回按代码分组的 dict：
> `{"300750.SZ": [{"code":..., "kline_time":..., "open":..., "high":..., "low":..., "close":..., "volume":..., "amount":...}, ...]}`

> 注：`MarketData(calendar)` 构造必须传交易日历，`/api/query` 原本无参实例化会报 `missing 'calendar'`。后端已于 2026-08-12 修复：启动后懒加载并缓存 `get_calendar()`，`cls=="MarketData"` 时用缓存日历构造。同时为 `query_kline` 加了 `period` 字符串→枚举值自动映射（`"day"`→`10008` 等）。

### 2.4 SubscribeData（实时订阅）—— 仅 WS —— ✅ v10 全市场预加载模式（2026-11-09 盘中实测；8 类市场、默认订 4 类）

> 📖 **完整协议见 §3.6**（帧格式 / 字段表 / 错误码 / 多客户端 / 重登影响 / 示例代码）。本节为总览。

**v9 的核心变化：订阅不再由客户端发起，而是网关自己维护。**

```
09:00~09:15  表定版窗口：09:00 的进程内重登作废旧会话（连带旧订阅），09:05 拉当天新表；
             这段时间网关不建订阅，等 09:15 用当天表一次性建好
09:15        用当天全市场表起订阅（一条 TGW 订阅流，进程内）；v10 起按 subscribe_markets
             分市场建组，各市场周期不同，混订时全局 period 显示为 "mixed"
09:15~次日09:00  只要有表就保持订阅：半夜/凌晨/盘中重启或崩溃，开机即用（用的是现有表）
之后         客户端连上来，只需说明「我要这几只」，网关从已有流里转发给你
```

> **v10（2026-11-09）**：代码表扩到 8 类（stock/etf/index/kzz/glra/hkt/future/option），
> 线上实际订阅 `stock,etf,index,glra` = **7891 只**。对客户端而言只有两点变化：
> ① `/health.sub` 多了 `markets` / `periods` / `subscribe_markets` / `market_counts`；
> ② 只能订 `subscribe_markets` 里的市场，其余品种即使代码正确也判表外。协议本身未变。

> **拉表失败不阻塞订阅**：09:05 拉不到（或表没变化）就沿用现有表，09:15 照样起订阅。不是每天都有新股/退市股，所以"用昨天的表"是可接受的默认；当天订阅建好后表即定版，全天不再换（否则会出现"表里有这只、订阅里没有"的静默收不到）。

池子由 `config.ini [preload] subscribe_markets` 决定，改一行就能增减市场（必须是 `fetch_markets` 的子集）：

| 市场键 | 品种 | security_type | period | flag |
|---|---|---|---|---|
| `stock` | A 股 | `EXTRA_STOCK_A` | snapshot | kSnapshot(10) |
| `etf` | ETF | `EXTRA_ETF` | snapshot | kSnapshot(10) |
| `index` | 指数 | `EXTRA_INDEX_A` | snapshot | kIndexSnapshot(13) |
| `kzz` | 可转债 | `EXTRA_KZZ` | snapshot | kSnapshot(10) |
| `glra` | 逆回购 | `EXTRA_GLRA` | snapshot | kSnapshot(10) |
| `hkt` | 港股通（仅港股通标的） | `EXTRA_HKT` | snapshotHKT | kHKTSnapshot(12) |
| `future` | 期货 | `EXTRA_FUTURE` | snapshotfuture | kFutureSnapshot(20) |
| `option` | ETF 期权 | `EXTRA_ETF_OP` | snapshotoption | kOptionSnapshot(11) |

2026-11（v10）起**多市场可以同时订阅**：网关按市场把代码分组、每组带上自己的 period 与 flag，
最后仍合成**一条** TGW 订阅流（账号限制是"一条流"，不是"一个周期"）。
8 类全开实测约 11700 只、稳态 ~1.5GB 内存、tick 4000-4500/s，其中期权占 37%、期货 13%。
唯一进不来的是 K 线（`min1`/`min5`/`day`…）：它是独立通道，与实时推送互斥。

客户端首帧：

```json
{"code_list": ["510300.SH", "600000.SH"]}
```

- `code_list` = **你要转发哪些**，不是「让网关去订哪些」。可以是全市场表里的任意子集，多客户端各订各的互不影响。
- `period` **可以省略**。网关订阅了多个市场时，这里传 `mixed` / `auto` / `all` 都等价于"不挑周期，整条流给我"；
  传了具体周期（如 `snapshot`）而网关实际是别的周期，会被明确拒绝并断连——这是"周期不可按客户端区分"的体现。
- 服务端推送：`{"type":"tick","data":{...}}`。**股票/ETF 走 `Snapshot`（35 字段），指数走 `SnapshotIndex`（11 字段）**，见 §3.6.5。

**动态增删**（首帧之后任意时刻）：

| 帧 | 行为 |
|---|---|
| `{"type":"add","code_list":[...]}` | 表内代码 → 立即开始推送，回 `ack.added`；表外代码 → 回 `error 全推数据中无此代码`，**不重启、连接不断** |
| `{"type":"remove","code_list":[...]}` | 只停止向本连接转发，不影响其他客户端，回 `ack.removed` |

**实测数据**（2026-11-06 盘中，网关 `<网关机器IP>:8000`，当时池子 7237 只）：

| 指标 | 数值 |
|---|---|
| 全市场流吞吐 | 40 秒 75,845 条，覆盖 7208/7237（99.6%） |
| 客户端首 tick 延迟 | 0.6 ~ 1.8 秒 |
| 双客户端并发 | A 订 4 只收 37 条、B 订 2 只收 15 条，互不干扰 |
| 运行中 add | 立即 ack，进程不重启 |
| 单只最高频率 | 约 3 秒一档（快照周期） |

**池子规模**（`/health` 实测）：

| `subscribe_markets` | 池子 | 拉表耗时 |
|---|---|---|
| `stock,etf` | 7237（A股 5579 + ETF 1658） | ~38s |
| `stock,etf,index` | 7873（+ 指数 636） | ~54s |
| `stock,etf,index,glra`（**v10 线上默认**） | **7891**（+ 逆回购 18） | ~56s |
| `all`（8 类） | **~11700**（+ 可转债 / 港股通 1384 / 期货 816 / 期权 1342） | **~116s** |

> 加指数、逆回购对内存几乎无影响：进程 RSS 从 1.34GB → 1.34GB（`ad.login()` 的 1.14GB 固定池才是大头），订阅增量可忽略。

**四个必须知道的约束**：

1. **表外代码永远订阅不到**：池子是 09:05 的快照，当天新上市 / 已退市 / 代码写错的都不在池里，只会回 `error`。第二天自动进池（或等 09:05 刷新）。
2. **K 线与逐笔拿不到**：客户端传 `min1` / `execution` 会被明确拒绝，不是 bug；实时周期由网关按市场自动决定。
3. **收盘时段无 tick 是正常的**：`snapshot` 只在交易时段推送。
4. **指数和股票共用同一条流，但字段不同**：指数（`000300.SH`、`399006.SZ`、`000001.SH` …）会推，但只有 11 个字段，**没有盘口**。客户端不能假设每条 tick 都有买卖五档，见 §3.6.5。

> 旧文档提到的「集合外增订触发服务重启」「`/tmp/amazingdata_subscribe.json` 持久化集合」均已废弃。只有在 `config.ini` 里把 `[preload] enabled` 设为 0（退回 lazy 模式）时，才会重新出现 `restarting` 帧。

---

## 3. 命令用法详解（实测示例）

> 下文 `$B=http://<网关机器IP>:8000`

### 3.1 代码表 / 日历 / 证券信息

```bash
# 全市场 ETF 代码表（1619 只）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_code_list","params":{"security_type":"EXTRA_ETF"}}'
# → {"ok":true,"rows":1619,"data":["516190.SH","513530.SH",...]}

# 上交所+深交所 A 股（5544 只）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_code_list","params":{"security_type":"EXTRA_STOCK_A"}}'

# 可转债（315 只）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_code_list","params":{"security_type":"EXTRA_KZZ"}}'

# 交易日历（默认沪市，返回 8 位整数日期列表，8702 天）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_calendar","params":{"market":"SH"}}'

# 每日证券信息（涨跌停价/昨收/状态等，10493 条）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_code_info","params":{"security_type":"EXTRA_STOCK_A"}}'

# 历史代码表（指定区间，例：2024-01-01~2024-01-10，5346 只）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_hist_code_list","params":{"security_type":"EXTRA_STOCK_A","start_date":20240101,"end_date":20240110}}'

# 期货 / 期权代码表（814 / 1250）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_future_code_list","params":{"security_type":"EXTRA_FUTURE"}}'
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_option_code_list","params":{"security_type":"EXTRA_ETF_OP"}}'
```

### 3.2 因子 / 复权

```bash
# 后复权因子
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_backward_factor","params":{"code_list":["600000.SH"]}}'

# 前复权因子（注意 is_local=false 走 TGW；默认 true 是读本地磁盘 D:// 路径，Linux 上会失败）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"BaseData","method":"get_adj_factor","params":{"code_list":["600000.SH"],"is_local":false}}'
```

### 3.3 股东 / 财务 / 交易明细

```bash
# 十大股东（600000.SH，2024 上半年）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_share_holder","params":{"code_list":["600000.SH"],"begin_date":20240101,"end_date":20240630,"is_local":false}}'
# → rows=20，data 首条：
# {"ANN_DATE":"20240430","MARKET_CODE":"600000.SH","HOLDER_ENDDATE":"20240331",
#  "HOLDER_TYPE":10,"QTY_NUM":1,"HOLDER_NAME":"上海国际集团有限公司",
#  "HOLDER_HOLDER_CATEGORY":"2","HOLDER_QUANTITY":6331322671,"HOLDER_PCT":21.57,
#  "HOLDER_SHARECATEGORYNAME":"A股流通股","FLOAT_QTY":6331322671}

# 融资融券汇总（全市场，按交易日）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_margin_summary","params":{"begin_date":20240101,"end_date":20240131,"is_local":false}}'
# → rows=66，data 首条：
# {"TRADE_DATE":"20240131","EXCHANGE":"SSE","SUM_BORROW_MONEY_BALANCE":792490224505,
#  "SUM_PURCH_WITH_BORROW_MONEY":26596924448,"SUM_REPAYMENT_OF_BORROW_MONEY":34351206639,
#  "SUM_SEC_LENDING_BALANCE":41854391113,"SUM_SALES_OF_BORROWED_SEC":1114919031,
#  "SUM_MARGIN_TRADE_BALANCE":834344615618}

# 股票质押冻结 / 限售股 / 股东户数 / 股权结构 / 分红 / 配股 等，参数格式同上
```

### 3.4 ETF / 指数 / 行业 / 国债

```bash
# ETF 份额（data 为按代码分组 dict）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_fund_share","params":{"code_list":["510300.SH"],"begin_date":20240101,"end_date":20240630,"is_local":false}}'
# → {"ok":true,"rows":1,"data":{"510300.SH":[{"FUND_SHARE":6073968.769,"CHANGE_REASON":"SGSH",
#    "IS_CONSOLIDATED_DATA":0,"MARKET_CODE":"510300.SH","ANN_DATE":"20240719","TOTAL_SHARE":6073968.769,
#    "CHANGE_DATE":"20240630","FLOAT_SHARE":6073968.769}]}}

# ETF IOPV（盘中参考净值）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_fund_iopv","params":{"code_list":["510300.SH"],"begin_date":20240101,"end_date":20240630,"is_local":false}}'

# 指数成分（沪深300）—— 注意：当前 HTTP 下 500，见 §6
# 行业指数日行情（申万，801010.SI=农林牧渔）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_industry_daily","params":{"code_list":["801010.SI"],"begin_date":20240101,"end_date":20240131,"is_local":false}}'
# → {"510300.SH" 形态}: {"801010.SI":[{"OPEN":2524.62,"HIGH":2529.27,"CLOSE":2483.72,"LOW":2483.72,
#    "AMOUNT":10463920000,"VOLUME":1596650000,"PB":2.34,"PE":44.22,"TOTAL_CAP":115958715.4063,
#    "INDEX_CODE":"801010.SI","A_FLOAT_CAP":52444853,"PRE_CLOSE":2546.9}]}

# 国债收益率（CGB=中债国债曲线，返回 3M~30Y 各期限收益率）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"InfoData","method":"get_treasury_yield","params":{"code_list":["CGB"],"begin_date":20240101,"end_date":20240630,"is_local":false}}'
```

### 3.5 K 线 / 快照（MarketData）—— ✅ 实测通过

```bash
# 日K：今天 + 过去10个交易日（300750.SZ，宁德时代）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"MarketData","method":"query_kline",
       "params":{"code_list":["300750.SZ"],"begin_date":20260720,"end_date":20260812,
                 "period":"day","is_local":false}}'
# → 18 根，末根 kline_time=2026-08-12T00:00:00 close=393.93（今天已收盘，拿到完整日K）

# 30分钟K（分钟线必须带 begin_time/end_time，900=9:00, 1700=17:00）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"MarketData","method":"query_kline",
       "params":{"code_list":["300750.SZ"],"begin_date":20260720,"end_date":20260812,
                 "period":"min30","begin_time":900,"end_time":1700,"is_local":false}}'
# → 144 根

# 5分钟K / 1分钟K：period 分别换 "min5" / "min1"，参数同上
# → 864 根 / 4320 根

# 快照（按日期分组：{日期: {代码: [记录]}}）
curl -X POST $B/api/query -H 'Content-Type: application/json' \
  -d '{"cls":"MarketData","method":"query_snapshot",
       "params":{"code_list":["300750.SZ"],"begin_date":20260812,"end_date":20260812,"is_local":false}}'
```

**K线返回字段**（每条记录）：

```json
{"code": "300750.SZ", "kline_time": "2026-08-12T00:00:00",
 "open": 388.43, "high": 395.91, "low": 387.52, "close": 393.93,
 "volume": 20236434, "amount": 7909423541.91}
```

**`period` 取值对照**（后端支持字符串名，自动映射枚举值）：

| 字符串 | 枚举值 | 说明 |
|--------|--------|------|
| `min1` / `min3` / `min5` / `min10` / `min15` | `10000` / `10001` / `10002` / `10003` / `10004` | 分钟线 |
| `min30` / `min60` / `min120` | `10005` / `10006` / `10007` | 分钟线 |
| `day` | `10008` | 日线 |
| `week` / `month` / `season` / `year` | `10009` / `10010` / `10011` / `10012` | 周/月/季/年线 |

> 提示：分钟线**必须**传 `begin_time`/`end_time`（8 位时分整型，如 `900`/`1700`），否则取不到数据；日线及以上不需要。

---

### 3.6 实时行情订阅（WS /ws/market）—— 完整语法 —— ✅ v10 盘中实测通过（2026-11-09；v9 预加载 + v10 多市场）

> **v9 起网关自己持有全市场订阅**，客户端只做两件事：说明要哪几只、收帧。
> 2026-08 版的「首个客户端决定全局集合」「集合外增订触发重启」「`global_codes` 字段」全部废弃。
> **v10 起支持 8 类市场**（stock / etf / index / kzz / glra / hkt / future / option），
> 实际订阅哪些由网关配置决定；客户端协议无变化，但**代码形态与 tick 字段集会随品种不同**。

#### 3.6.1 工作方式

```
网关 09:05 拉代码表（v10: 按 fetch_markets 拉全部 8 类，拉不到就沿用现有表）
     按 subscribe_markets 组织订阅：v10 线上 = A股 5579 + ETF 1658 + 指数 636 + 逆回购 18 = 7891 只
网关 09:00~09:15 是表定版窗口，不建订阅；09:15 用当天表起订阅覆盖全部
     其余时间（09:15~次日09:00）重启：只要有表就立刻订，开机即用
   客户端A ──首帧 {"code_list":[4 只]}──► 网关：把 A 的槽位挂到这 4 只上
   客户端B ──首帧 {"code_list":[2 只]}──► 网关：独立槽位，互不影响
   客户端C ──首帧 {"code_list":"all"}──► 网关：登记为全推，收到什么就转什么
   网关 ──{"type":"tick","data":{...}}──► 只推给订阅了该代码的连接（全推连接全收）
```

> 每个连接持有的是**覆盖式槽位**（`code -> 该只最新一份快照`），不是 FIFO 队列。
> 客户端读得快 → 来一条推一条，延迟微秒级；客户端读得慢 → 同一只代码的新值直接覆盖旧值，
> 内存恒等于订阅的代码数，永远不会因为客户端慢而增长。

一条流服务所有客户端，所以：

- 客户端数量、订阅组合都不受限制；
- 订阅 / 退订是**内存槽位的挂载与摘除**，立即生效，不重启进程、不占新的 TGW 会话；
- **周期由网关按市场自动选择**，客户端不能指定（v9 是全局固定 `snapshot`；v10 多市场混订时
  `/health.sub.period` = `mixed`，客户端传 `period:"mixed"` 表示不挑，见 §3.6.3 ①）；
- 池子里**多品种混在一起**，同一条流、同一种帧格式（`type:"tick"`），但 `data` 的字段集不同 ——
  指数只有 11 个字段（无盘口），见 §3.6.5；
- 想要「不挑、全要」可以传 `code_list:"all"`（见 §3.6.3 ④），但那是 **≈11 Mbps / 21 GB 一天** 的量级，先看带宽再决定。

#### 3.6.2 连接地址

```
ws://<网关机器IP>:8000/ws/market
```

同网段直连；不在同一网络时先做端口转发 `ssh -L 8000:127.0.0.1:8000 root@<网关机器IP>`，再连 `ws://127.0.0.1:8000/ws/market`。

#### 3.6.3 客户端 → 服务端

**① 首帧（订阅，必发一次）**

```json
{"code_list": ["300750.SZ", "510300.SH"], "period": "snapshot"}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `code_list` | ✅ | 要转发的代码子集，重复自动去重。**含义是「从全市场流里选哪些」**，不是「让网关去订哪些」 |
| `period` | ⬜ | 默认 `snapshot`。校验规则（v10）：取值必须 ∈ 网关实际订阅用到的 period 集合（`/health.sub.periods`），否则回 `error` 并关闭连接。传 `mixed` / `auto` / `all` / `""` 表示**不挑周期**，跳过校验；网关按市场自动选择（见下） |

**关于 `period` 的两条实测结论（2026-11-09）：**

1. 线上 `subscribe_markets = stock,etf,index,glra`，四个市场都用 `snapshot`（枚举 10014），
   所以 `/health.sub.periods = [10014]`、`period = "snapshot"`；
   **客户端继续硬编码 `period:"snapshot"` 完全合法**，不会因为 v10 多市场而被拒。
2. 只有当日志出现 `subscribe_markets` 含 hkt / future / option（各自 `snapshotHKT` /
   `snapshotfuture` / `snapshotoption`）时，`periods` 才会变成多个、`period` 变成 `mixed`。
   **这种配置下再传 `period:"snapshot"` 就会被拒**（snapshot 不在集合里）；
   要写死就改传 `mixed`（或干脆不传 `period`），由网关按市场分配。

```json
// v10 推荐写法：不挑周期，任何 subscribe_markets 组合都成立
{"code_list": ["510300.SH", "600519.SH"], "period": "mixed"}
```

代码格式固定为 `代码.市场`：`600000.SH` / `000001.SZ` / `830799.BJ` / `000300.SH`（指数）/ `399006.SZ`（指数）。
**不接受**市场级写法（`SH`、`SZ`、`*.SH`）和裸代码（`600000`）——SDK 层会直接抛异常，网关侧只会把它们当表外代码拒掉。

> ⚠️ **v10 新增：能不能订取决于「订阅市场」，不是「有没有拉表」。**
> `fetch_markets` 拉全部 8 类（stock/etf/index/kzz/glra/hkt/future/option），
> 但只有 `subscribe_markets` 里的市场会被真正订阅。线上当前是 `stock,etf,index,glra`，
> 所以 **可转债（`110059.SH`）、港股通（`00700.HK`）、期货、期权即使代码格式正确，
> 也会被判为「表外代码」**——2026-11-09 实测：
>
> ```
> 首帧 ["00700.HK","110059.SH","510300.SH"]
>   ack   codes=["510300.SH"]  unavailable=["00700.HK","110059.SH"]  universe_size=7891
>   error 全推数据中无此代码: 00700.HK, 110059.SH(...)     ← 连接不关，510300.SH 照推
> ```
>
> 要订新市场，只能改网关 `config.ini` 的 `subscribe_markets` 并重启，客户端无法自助扩市场。

> 指数也在池里，直接按同样的格式写即可（`000300.SH`、`000001.SH`、`399001.SZ`、`399006.SZ`、`000905.SH`、`000688.SH` …）。
> **注意 `000001.SH` 是上证指数、`000001.SZ` 才是平安银行**，两者是不同标的，都能订。

**② 控制帧 `add`（增订）**

```json
{"type": "add", "code_list": ["600519.SH"]}
```

| 情况 | 服务端行为 |
|---|---|
| 代码在今日全市场表内 | 立即挂上槽位，回 `{"type":"ack","action":"add","added":[...]}`，下一笔 tick 起就能收到 |
| 代码不在表内 | 回 `{"type":"error","message":"全推数据中无此代码: ..."}`，**连接保持、可用代码照常推送、进程不重启** |
| 两者混合 | 先回 `ack.added`（可用部分），再回 `error`（缺失部分） |

**③ 控制帧 `remove`（退订）**

```json
{"type": "remove", "code_list": ["510300.SH"]}
```

只摘除**本连接**的槽位，回 `{"type":"ack","action":"remove","removed":[...]}`；其他客户端不受影响。退订不在本连接订阅列表里的代码时，`removed` 为空数组。

**④ 全市场全推（`all`）—— 把网关订阅流收到的一切都推给你**

三种写法等价：

```json
{"code_list": "all",  "period": "snapshot"}
{"code_list": ["all"], "period": "snapshot"}
{"all": true,          "period": "snapshot"}
```

命中后网关把**全市场订阅流收到的每一条 tick 原样转发**给本连接，回：

```json
{"type": "ack", "action": "subscribe", "codes": "all", "all": true,
 "universe_size": 7891, "period": "snapshot"}
```

| 行为 | 说明 |
|---|---|
| `ack.codes` | 是全推模式时**为字符串 `"all"`**（不是数组），另带 `"all": true`。普通订阅仍是数组 |
| `ack.unavailable` | 全推时**不返回**该字段——整个池子都在推，没有「不可用」这一说 |
| `remove 单只代码` | 全推模式下等于**屏蔽**这几只：回 `{"action":"remove","removed":[...],"mode":"all_exclude"}`，其余照推 |
| `add 单只代码` | 全推模式下等于**解除屏蔽**：回 `{"action":"add","added":[...],"mode":"all"}` |
| `remove "all"` | 关闭全推，回 `{"action":"remove","removed":"all"}`；本连接不再接收任何 tick |
| `add "all"` | 运行中切到全推，回 `{"action":"add","added":"all","universe_size":7891}` |

⚠️ **带宽是硬成本，开之前先算清楚**（2026-11-06 实测口径，当时池子 7873；v10 池 7891，约 +0.2%）：

| 指标 | 数值 |
|---|---|
| 全市场流吞吐 | 约 **1900 帧/秒**（7891 只、约 3 秒一档） |
| 单帧大小 | 约 **778 字节**（`Snapshot` 35 字段） |
| 单连接带宽 | **≈ 1.4 MB/s ≈ 11 Mbps** |
| 一个交易日（4 小时） | **≈ 21 GB** |
| 客户端解析压力 | ≈ 1900 msg/s，Python 单线程基本吃满一个核 |

所以 `all` 适合「本地要自己算全市场指标/落库」的场景，不适合「只想看几十只」。只要几十只就老老实实列 `code_list`。

> 慢客户端不会被拖垮也不会拖垮网关：每个连接是**覆盖式槽位**，同一只代码在发出去之前又被更新过，
> 旧的那份直接被新的一份覆盖（`/health.sub.coalesced` 计数）。内存上限 = 订阅的代码数，
> **永远不随时间增长**，也没有「队列满丢多少条」这种事。代价只是中间状态被合并——
> 而快照本来就是 3 秒一档的状态，不是逐笔流水。

#### 3.6.4 服务端 → 客户端

**tick（行情推送）—— 股票 / ETF / 可转债（`Snapshot`，35 字段）**

```json
{"type": "tick", "data": {"code": "510300.SH", "trade_time": "2026-11-06 14:09:14",
 "last": 4.774, "pre_close": 4.748, "open": 4.77, "high": 4.786, "low": 4.766,
 "close": 0.0, "volume": 179426228, "amount": 857281594.0, "num_trades": 24532,
 "high_limited": 5.223, "low_limited": 4.273,
 "ask_price1": 4.774, "ask_volume1": 10400, "bid_price1": 4.773, "bid_volume1": 2183700,
 "ask_price2": 4.775, "ask_volume2": 0, "bid_price2": 4.772, "bid_volume2": 0}}
```

**tick（行情推送）—— 指数（`SnapshotIndex`，11 字段，没有盘口）**

```json
{"type": "tick", "data": {"code": "000300.SH", "trade_time": "2026-11-06 14:09:14",
 "last": 4012.35, "pre_close": 3998.71, "open": 3999.02,
 "high": 4015.88, "low": 3991.44, "close": 0.0,
 "volume": 168234567, "amount": 402345678901.0,
 "trading_phase_code": "T111"}}
```

> 两种 `data` **走同一个 `type:"tick"`**，客户端要自己按代码区分标的类型（见 §3.6.5）。

**ack（订阅确认，首帧的回执）**

```json
{"type": "ack", "action": "subscribe",
 "codes": ["300750.SZ", "510300.SH"],
 "universe_size": 7891,
 "period": "snapshot",
 "unavailable": ["999999.SH"]}
```

| 字段 | 说明 |
|---|---|
| `codes` | **本连接实际生效**的代码（去掉表外代码后的结果） |
| `universe_size` | 全市场池大小（v10 线上 7891 = A股 5579 + ETF 1658 + 指数 636 + 逆回购 18），不是代码列表本身 |
| `period` | 网关全局周期名；v10 多市场混订时为 `"mixed"` |
| `unavailable` | 请求了但不在今日全市场表里的代码，**最多回前 200 个**（超出部分只在 `error` 帧里体现为数量） |

紧跟其后还有一条 `error` 帧说明原因，格式固定为 `全推数据中无此代码: <前 50 只> [等 N 只](不在今日全市场表内: 未上市/已退市/代码写错)`——**超过 50 只时只列前 50 只**，精确名单以 `ack.unavailable` 为准（它也封顶 200）。

> ⚠️ **`add` 的表外代码没有 `ack.unavailable`**：`add` 表外代码时服务端只回一条 `error` 帧（消息里带前 50 只），
> 不返回可用清单的 ack。客户端要自己从 error 文本里捞代码，或**拿自己发出去的 add 列表与后续 `ack.added` 求差集**，
> 否则会以为订上了、只是没行情。

> `global_codes` 字段已删除。想拿全市场清单请走 `POST /api/query` 的 `BaseData.get_code_list`，别指望 ack 里带 7891 只代码。

**ack（增删确认）**

```json
{"type": "ack", "action": "add",    "added":   ["600519.SH"]}
{"type": "ack", "action": "remove", "removed": ["510300.SH"]}
```

**error（错误）**

```json
{"type": "error", "message": "全推数据中无此代码: 999999.SH(不在今日全市场表内: 未上市/已退市/代码写错)"}
```

| message 形态 | 触发场景 | 连接 |
|---|---|---|
| `首帧必须包含 code_list` | 首帧不是对象或没有 `code_list` | 关闭 |
| `unknown period: min1` | period 名不是 `ad.constant.Period` 的成员 | 关闭 |
| `网关全局订阅 period=snapshot, 请求 min1 不一致(单账号只允许一条订阅流, 周期固定)` | 周期与网关不一致 | 关闭 |
| `表定版窗口(09:00-09:15): 09:00 的会话重建会作废旧订阅, 09:15 才用当天全市场表起新订阅, 请 09:15 后重连` | 09:00~09:15 之间连接（该窗口内网关不建订阅） | 关闭 |
| `全市场行情尚未就绪(订阅建立中或会话重建中), 请稍后重连` | 09:15 后重启尚未建好订阅、或重登后订阅正在重建、或订阅线程已死 | 关闭 |
| `service starting: initial TGW login not ready, retry later` | 进程刚启动，首次登录未完成 | 关闭 |
| `全推数据中无此代码: ...(不在今日全市场表内: 未上市/已退市/代码写错)` | 首帧或 add 带了表外代码 | **不关闭** |
| `unknown command: xxx` | 控制帧 `type` 不认识 | 不关闭 |

> 边界：`{"type":"add"}` 或 `{"type":"remove"}` 但 `code_list` 为空 / 全是非字符串时，会被归到 `unknown command` 分支回错（不是静默忽略），连接不关闭。

> `unavailable` 和 error 里的名单都是**截断的**（200 / 50）。若客户端一次提交了大量表外代码，请以自己发出的清单减去 `ack.codes` 来求差集，别依赖服务端回传的截断名单。

**restarting（仅 lazy 模式）**

```json
{"type": "restarting", "message": "新增标的 ['600519.SH'] 不在订阅集合, 服务即将重启换集合, 约 10 秒后请重新连接; 新集合=[...]"}
```

只有在 `config.ini` 把 `[preload] enabled = 0`（退回旧模式）时才会出现。预加载模式下**永远不会**发这个帧。

#### 3.6.5 tick 全量字段表

**同一个 `type:"tick"` 里有多种 `data`**，取决于标的类型：

| 标的 | SDK 模型 | 字段数 | 手册章节 |
|---|---|---|---|
| 股票 / ETF / 可转债 / 逆回购 | `Snapshot` | **35** | 4.2.1 Level-1 快照 |
| **指数** | `SnapshotIndex` | **11** | 4.2.4 指数快照 |
| 港股通 / 期货 / 期权（v10 已支持，**当前未订阅**） | `SnapshotHKT` / `SnapshotFuture` / `SnapshotOption` | 未实测 | 待补 |

> **v10 说明**：`_MARKETS` 里已定义 8 类市场，但线上 `subscribe_markets` 只开了
> `stock,etf,index,glra`。港股通/期货/期权的 tick 字段集**尚未在盘中实测**，
> 文档不臆造字段名；若要开这些市场，先按下面的命令 dump 模型字段再改客户端：
>
> ```bash
> venv/bin/python -c "import AmazingData as ad; \
> print(list(ad.constant.SnapshotHKT.model_fields)); \
> print(list(ad.constant.SnapshotFuture.model_fields)); \
> print(list(ad.constant.SnapshotOption.model_fields))"
> ```
>
> **所有品种的容错写法都一样：`.get()`，不要 `d[...]`。** 品种越多，字段差异越大。

字段名与模型定义可直接在部署机上核对：

```bash
venv/bin/python -c "import AmazingData as ad; \
print(list(ad.constant.Snapshot.model_fields)); \
print(list(ad.constant.SnapshotIndex.model_fields))"
```

##### ① 股票 / ETF（`Snapshot`，35 字段）

2026-08-13 与 2026-11-06 两次盘中实测一致。

| 字段 | 类型 | 示例 | 说明 |
|------|------|------|------|
| `code` | str | `"300750.SZ"` | 证券代码+市场 |
| `trade_time` | datetime | `"2026-11-06 14:09:14"` | **交易所行情数据时间**（秒级，无毫秒） |
| `last` | float | `393.29` | 最新价 |
| `pre_close` | float | `393.93` | 昨收价 |
| `open` / `high` / `low` | float | `393.04 / 396.04 / 391.04` | 开盘价 / 最高价 / 最低价 |
| `close` | float | `0.0` | 收盘价（**盘中为 0**，收盘后才有值） |
| `volume` | float | `9043850` | 成交总量（**单位：股**，`volume×last ≈ amount` 可自验） |
| `amount` | float | `3556077557.47` | 成交总金额（元） |
| `num_trades` | float | `41261` | 成交笔数 |
| `high_limited` | float | `472.72` | 涨停价 |
| `low_limited` | float | `315.14` | 跌停价 |
| `ask_price1`~`ask_price5` | float | `393.29` | 卖 1~5 档价格 |
| `ask_volume1`~`ask_volume5` | int | `100` | 卖 1~5 档量（单位：股） |
| `bid_price1`~`bid_price5` | float | `393.28` | 买 1~5 档价格 |
| `bid_volume1`~`bid_volume5` | int | `200` | 买 1~5 档量（单位：股） |
| `iopv` | float | `4.7739` | 净值估值（**仅基金品种有效**，个股恒为 0） |
| `trading_phase_code` | str | `"T0"` / `"T111"` | 交易阶段代码 |

> ⚠️ `volume` / `ask_volume` / `bid_volume` 单位是**股**（不是手）；`close` 盘中为 0 属正常，别拿它判断涨跌。

##### ② 指数（`SnapshotIndex`，11 字段）—— 没有盘口

| 字段 | 类型 | 示例 | 说明 |
|------|------|------|------|
| `code` | str | `"000300.SH"` | 指数代码+市场 |
| `trade_time` | datetime | `"2026-11-06 14:09:14"` | 交易所行情数据时间（秒级） |
| `last` | float | `4012.35` | 最新点位 |
| `pre_close` | float | `3998.71` | 前收盘点位 |
| `open` / `high` / `low` | float | `3999.02 / 4015.88 / 3991.44` | 今开 / 最高 / 最低 |
| `close` | float | `0.0` | 收盘点位（**仅上海有效**，盘中为 0） |
| `volume` | int | `168234567` | 成交量（**上交所：手；深交所：张**，与股票"股"不同！） |
| `amount` | float | `402345678901.0` | 成交额（元） |
| `trading_phase_code` | str | `"T111"` | 交易阶段代码 |

**指数 tick 明确没有这些字段**（客户端不得假设存在）：

| 缺失字段 | 影响 |
|---|---|
| `ask_price1~5` / `ask_volume1~5` / `bid_price1~5` / `bid_volume1~5` | 无买卖五档 → 盘口类 UI（十档、买卖压力、委比）对指数必须留空 |
| `num_trades` | 无成交笔数 → 均价笔数类指标不可算 |
| `high_limited` / `low_limited` | 指数无涨跌停 → 涨跌停判断要跳过 |
| `iopv` | 无净值估值 → 折溢价率对指数无意义 |

##### ③ 客户端容错写法（重要）

> 网关是 `data.model_dump()` 原样转发，**不会补齐缺失字段**。指数 tick 里就是没有 `bid_price1`、`num_trades` 这些键，`d["bid_price1"]` 会直接 `KeyError`。

**判断标的类型**：不要靠「字段在不在」猜，建议客户端自己维护一份指数代码集合（启动时从
`POST /api/query` → `BaseData.get_code_list({"security_type":"EXTRA_INDEX_A"})` 拉一次，共 636 只），
收到 tick 时 `code in INDEX_SET` 即指数。代码前缀不可靠：`000001.SH` 是上证指数，`000001.SZ` 是平安银行。

```python
IDX = {"000001.SH", "000300.SH", "000905.SH", "399001.SZ", "399006.SZ"}  # 建议启动时拉全量

def normalize(d):
    """把股票/指数两种 tick 统一成同一套字段，缺失一律 None / 0。"""
    is_idx = d["code"] in IDX
    return {
        "code":        d["code"],
        "ts":          d.get("trade_time"),
        "last":        d.get("last", 0.0),
        "pre_close":   d.get("pre_close", 0.0),
        "open":        d.get("open", 0.0),
        "high":        d.get("high", 0.0),
        "low":         d.get("low", 0.0),
        "volume":      d.get("volume", 0),
        "amount":      d.get("amount", 0.0),
        "is_index":    is_idx,
        # —— 只有股票/ETF 才有，指数恒为 None ——
        "bid1":        d.get("bid_price1"),
        "ask1":        d.get("ask_price1"),
        "bid1_vol":    d.get("bid_volume1"),
        "ask1_vol":    d.get("ask_volume1"),
        "num_trades":  d.get("num_trades"),
        "high_limit":  d.get("high_limited"),
        "low_limit":   d.get("low_limited"),
        "iopv":        d.get("iopv"),
    }
```

三条硬性要求：

1. **一律用 `.get(key, default)`**，不要 `d[key]`；
2. **不要对指数算涨跌幅用的"涨跌停价"、不要算委比/十档、不要算折溢价**——数据源就没有；
3. 落库 / 建表时把盘口字段设为可空（`NULL`），否则指数行会插不进去。

#### 3.6.6 多客户端与增订实测（2026-11-06 / v10 复测 2026-11-09）

```
14:50  客户端A 首帧 ["600000.SH","000001.SZ","510300.SH","588000.SH"] → ack，首 tick 0.6s，累计 37 条
14:50  客户端B 首帧 ["601318.SH","159915.SZ"]                       → ack，首 tick 1.8s，累计 15 条
14:58  A 追加 {"type":"add","code_list":["600519.SH","300750.SZ"]}  → 立即 ack.added，两只随即开始推送
      进程 PID 不变，"subscribe restart triggered" 计数 = 0
```

同时段全市场流本身：40 秒 75,845 条，覆盖 7208/7237（99.6%），单只最高约 3 秒一档。

**表外代码实测**：

```
首帧 ["600000.SH","999999.SH","600000"]
  ACK   codes=["600000.SH"]  unavailable=["600000","999999.SH"]  universe_size=7237
  ERROR 全推数据中无此代码: 600000, 999999.SH(不在今日全市场表内: 未上市/已退市/代码写错)
  → 600000.SH 照常收到 tick，进程未重启
```

**指数并入后的实测**（2026-11-06 23:15，收盘后，只能验订阅链路、无 tick）：

```
pool  = 7873（EXTRA_INDEX_A 636 只并入，拉表 53.9s）

首帧 ["600000.SH","000300.SH","399006.SZ","000001.SH","000001.SZ","510300.SH"]
  ACK  codes=6 只全收（指数 000300.SH / 399006.SZ / 000001.SH 均不再被判表外）
       universe_size=7873  unavailable=[]
add    ["399001.SZ","000688.SH"] → ack.added=["000688.SH","399001.SZ"]
remove ["000905.SH"]             → ack.removed=["000905.SH"]
```

> 指数 tick 的真实字段集已于 **2026-11-09 盘中复核**：`000300.SH` 的 `data` 恰好 11 个 key、`510300.SH` 恰好 35 个，
> 与手册 4.2.4 + `SnapshotIndex.model_fields` 一致。缺失字段确认不会出现（不是值为 null，而是键不存在）。

**v10 多市场实测**（2026-11-09 13:50 盘中，`<网关机器IP>:8000`，`subscribe_markets=stock,etf,index,glra`）：

```
pool = 7891（A股 5579 + ETF 1658 + 指数 636 + 逆回购 18；表源 disk，list_date=2026-11-09）
/health.sub  periods=[10014]  period="snapshot"  markets=[stock,etf,index,glra]

首帧 {"code_list":["510300.SH","000300.SH"],"period":"snapshot"}
  ack  codes=2  universe_size=7891  period=snapshot  unavailable=[]
       → 首 tick 0.6s，9 秒内 6 条（510300.SH 与 000300.SH 交替）

add  ["600519.SH"]        → ack.added=["600519.SH"]，**连接对象未变、进程未重启**
add  ["999999.SH"]        → error 全推数据中无此代码: 999999.SH(...)，连接保持
                             （注意：add 路径**没有** ack.unavailable，只能从 error 文本取代码）
remove ["000300.SH","600519.SH","999999.SH"] → ack.removed 只含前两只（999999 本就没挂上）

首帧 ["00700.HK","110059.SH","510300.SH"]   ← 港股通/可转债，已拉表但未订阅
  ack  codes=["510300.SH"]  unavailable=["00700.HK","110059.SH"]  universe_size=7891
  error 全推数据中无此代码: 00700.HK, 110059.SH(...)     ← 连接不断，510300.SH 照推
```

> 结论：**v10 对 WS 协议零破坏**。所有变化都在「池子构成」和「周期命名」上，帧格式、ack 字段、add/remove 语义全部沿用 v9。

#### 3.6.7 定时重登 / 自愈对连接的影响

网关每天在**进程内**换三次 TGW 会话（`config.ini [relogin]`，默认 `04:10 / 09:00 / 21:00`，提前 2 分钟登出）：

| 动作 | 进程 | WS 连接 | 客户端要做的 |
|---|---|---|---|
| 定时重登（04:08→04:10 / 08:58→09:00 / 20:58→21:00） | 不动，PID 不变 | **不断** | 04:10 / 21:00 两次：窗口内约 1~2 分钟收不到 tick，登录后订阅立刻用新会话重建（`sub.resub_count` +1）。<br>09:00 那次：重建**延到 09:15**（表定版窗口，要先拉当天新表），这 15 分钟内新建的 WS 会被拒（`表定版窗口(09:00-09:15)`），已有连接保持不断但不推数据 |
| watchdog 判会话死亡 → `execve` 自愈 | 映像替换，PID 不变 | **断** | 重连并重发首帧 |
| 交易时段连续 180s 零 tick → 自愈重启（每天最多 3 次） | 同上 | **断** | 同上 |
| 手工 `run.sh restart` | 换进程 | **断** | 同上 |

服务端**不会补发**断开期间漏掉的 tick，重连后从当下的行情继续。

#### 3.6.8 客户端实现要点

1. **必须实现自动重连**（建议 5 秒退避），并在重连后重发首帧——服务端不记忆连接状态。
2. 收到 `error: 全推数据中无此代码` 时**不要重试、不要重连**，把该代码标记为不可用即可；反复重试只会刷日志。
3. 收到 `全市场行情尚未就绪` 时，等 5~10 秒再连（通常发生在重启/自愈后正在建订阅、或重登后正在重建）；收到 `表定版窗口(09:00-09:15)` 时不用重试，**等 09:15 之后再连**——那段时间网关故意不建订阅。
4. 判断「行情通不通」请看 `GET /health` 的 `sub.ready` / `sub.last_tick_age`，**不要看 HTTP 状态码**（恒为 200 是刻意设计）。
5. **不要只订不读**：网关对每个连接只保留「每只最新一份」，你读得慢时中间状态会被合并（`/health.sub.coalesced` 增长），最新价仍然会到，网关内存也不会涨。但如果一直不读，TCP 发送缓冲填满后发送协程会一直挂着——这条连接等于僵尸，只能靠你自己重连。
6. **注意标的混流**：池子里同时有股票、ETF、指数（v10 还可能有逆回购/可转债/港股通/期货/期权）。订了指数就要按 §3.6.5 的 `SnapshotIndex` 字段处理 —— 不存在的盘口字段一律 `.get()`，不要 `d["bid_price1"]`。
7. **`ack.codes` 可能是字符串**：全推时是 `"all"`（不是数组）。解析 ack 时别假设它一定是 list，或直接读 `ack.all`。
8. **`add` 的失败没有 `ack.unavailable`**：首帧订阅失败才有 `unavailable`；运行中 `add` 表外代码只回一条 `error` 帧。
   要准确知道哪几只没订上，用「自己发出的 add 列表 − `ack.added`」，或从 error 文本里解析代码。**只读 `unavailable` 会漏。**
9. **「已拉表」≠「可订阅」**：能否订取决于网关 `subscribe_markets`，不是 `fetch_markets`。
   客户端把 `110059.SH`（可转债）、`00700.HK`（港股通）写进 `code_list` 一定被拒 —— 这不是代码写错，是市场没开。
   判断依据看 `GET /health` 的 `sub.subscribe_markets`，不要猜。
10. **周期别硬编码 `snapshot`（推荐 `mixed`）**：只要网关 `subscribe_markets` 里混入 hkt/future/option，
    `snapshot` 就会不在 `sub.periods` 里而被拒。传 `mixed` / 不传 `period` 对所有组合都成立。

最小可运行客户端（Python + `websocket-client`，含重连）：

```python
import json, time, websocket

URL   = "ws://<网关机器IP>:8000/ws/market"
CODES = ["300750.SZ", "510300.SH", "000300.SH"]   # 指数可以和股票混着订
INDEX = {"000001.SH", "000300.SH", "399001.SZ", "399006.SZ"}  # 建议启动时拉全量

while True:
    try:
        ws = websocket.create_connection(URL, timeout=10)
        # period 传 "mixed"：不挑周期，网关按市场自动选，任何 subscribe_markets 组合都成立
        ws.send(json.dumps({"code_list": CODES, "period": "mixed"}))
        while True:
            msg = json.loads(ws.recv())
            t = msg.get("type")
            if t == "tick":
                d = msg["data"]
                # 指数没有盘口: bid1 会是 None，而不是报错
                print(d["code"], d.get("last"), d.get("trade_time"),
                      "bid1=", d.get("bid_price1"), "idx=", d["code"] in INDEX)
            elif t == "ack":
                print("ack", msg.get("action"),
                      msg.get("codes") or msg.get("added") or msg.get("removed"),
                      "pool=", msg.get("universe_size"),
                      "unavailable=", msg.get("unavailable"))
            elif t == "error":
                print("error:", msg["message"])
    except Exception as e:
        print("reconnect in 5s:", e)
        time.sleep(5)
```

#### 3.6.9 相关配置（`config.ini`）

```ini
[preload]
enabled           = 1
fetch_markets     = all                ; 每天拉哪些市场的代码表; all = 8 类全拉
subscribe_markets = stock,etf,index,glra   ; 开盘订阅哪些市场(⊆ fetch_markets) = 7891 只
list_time         = 09:05              ; 拉全市场代码表(拉不到就沿用现有表)
hold_start        = 09:00              ; 定版窗口起点: hold_start~subscribe_time 之间不建订阅
subscribe_time    = 09:15              ; 定版窗口终点: 用当天表起订阅
; 旧键 security_types / period 已废弃(周期改为按市场自动选择), 写了也不生效

[relogin]
times           = 04:10,09:00,21:00   ; 也可写显式区间 03:58-04:01, 21:00
logout_lead_min = 2                   ; 提前几分钟登出

[ws]
; 推送缓冲是覆盖式的(每只代码只留最新一份), 没有"队列上限"可配。
; 旧配置项 queue_max 已废弃, 写了也不生效。
```

**v10 市场键（`_MARKETS`，8 类）—— 决定 `subscribe_markets` 能写什么：**

| 键 | 中文名 | `security_type` | 取表方法 | 周期 | 池子（2026-11-09） |
|---|---|---|---|---|---|
| `stock` | A股股票 | `EXTRA_STOCK_A` | `get_code_list` | `snapshot` | 5579 |
| `etf` | ETF | `EXTRA_ETF` | `get_code_list` | `snapshot` | 1658 |
| `index` | 指数 | `EXTRA_INDEX_A` | `get_code_list` | `snapshot` | 636 |
| `kzz` | 可转债 | `EXTRA_KZZ` | `get_code_list` | `snapshot` | （已拉表，未订阅） |
| `glra` | 逆回购 | `EXTRA_GLRA` | `get_code_list` | `snapshot` | 18 |
| `hkt` | 港股通 | `EXTRA_HKT` | `get_code_list` | `snapshotHKT` | （已拉表，未订阅） |
| `future` | 期货 | `EXTRA_FUTURE` | `get_future_code_list` | `snapshotfuture` | （已拉表，未订阅） |
| `option` | ETF期权 | `EXTRA_ETF_OP` | `get_option_code_list` | `snapshotoption` | （已拉表，未订阅） |

> 只要 `subscribe_markets` 里出现后四类中的任意一类，`periods` 就不再是单一 `[10014]`，
> `sub.period` 变成 `"mixed"` —— 此时客户端必须传 `period:"mixed"` 或不传，硬编码 `snapshot` 会被拒。

- `enabled = 0` 会退回 2026-08 的 lazy 模式（首个客户端决定集合、表外增订触发 `restarting` 重启），**仅供排障**，多客户端场景不要开。
- 代码表每天一份，缓存在 `run/preload_codes.json`，重启后直接读盘，不再花几分钟重拉。
  **改了 `fetch_markets` 但当天想立刻生效，必须删掉这个缓存文件再重启**，否则要等次日 `list_time` 才重拉。
  只改 `subscribe_markets` 不用删缓存，重启即按现有表重建订阅。
- 订阅时机按「跨日 09:15 分界」：`09:15~次日09:00` 只要有表就立刻订（半夜/凌晨重启也能开机即用）；
  `09:00~09:15` 是定版窗口，不建订阅（此时旧会话已被 09:00 重登作废，09:05 才拉新表，
  等 09:15 用当天表一次建好；`/health.sub.preopen_hold` 可看当前是否在窗口内）。
- **当天订阅建好后表即定版**：09:05 拉表失败就沿用现有表、并把"已定版"写回缓存，当天不再重拉。
  这样不会出现"表换了、订阅还是旧的"（客户端能挂上却永远收不到）这种静默故障。
- 调试时可设环境变量 `AD_PRELOAD_NOW=1` 跳过时间判断，立即拉表并订阅。
- 拉表耗时随品种增加：两种 ~38s，加指数后 ~54s（串行 3 次 `get_code_list`，每次 15~20s）。
- 推送缓冲**没有上限可调**了：每连接是「每只代码一份最新快照」的覆盖式槽位，内存恒为
  `代码数 × 3.5KB`（全推 7891 只 ≈ 28 MB），不随客户端快慢变化。慢客户端造成的是
  `/health.sub.coalesced` 增长（旧帧被新帧覆盖），不是丢帧计数。

#### 3.6.10 注意事项

- **逐笔数据仍不可用**：`period` 传 `execution` / `order` 订阅能建立但 0 条回调（账号无逐笔权限），
  实际可用只有快照类周期（`snapshot` / `snapshotHKT` / `snapshotfuture` / `snapshotoption`，v10）。
  `trade_time` 为秒级。
- **周期由网关决定**：客户端不传 `period`，或传 `mixed`/`auto`/`all`（三者等价于"不挑周期"）；
  传具体周期时校验的是「该值是否 ∈ `/health.sub.periods`」——v10 线上四个市场都是 10014，
  所以 `snapshot` 合法；一旦 `subscribe_markets` 混入 hkt/future/option，`snapshot` 就会不在集合里被拒。
  要换周期就改 `config.ini` 的 `subscribe_markets` 并重启。
- **能否订阅取决于 `subscribe_markets`，不是 `fetch_markets`**：拉表 8 类，订阅只 4 类。
  可转债 / 港股通 / 期货 / 期权的代码格式再正确，只要市场没开就会被当表外代码拒掉（§3.6.3）。
- **池子是 09:05 快照**：当日新上市、退市、代码写错的都进不来；次日 09:05 自动刷新。
  09:05 拉表失败时沿用现有表（当天定版），所以"新上市当天就能订到"不保证。
- **指数只有 11 个字段、没有盘口**：`SnapshotIndex` 没有买卖五档 / 成交笔数 / 涨跌停价 / IOPV，客户端必须容错（§3.6.5）。
  指数 `volume` 单位也不同（沪市"手"、深市"张"，股票是"股"），跨品种聚合成交量前先归一。
- **跨品种字段集不同（v10）**：股票/ETF/可转债/逆回购都是 `Snapshot`（35 字段），指数是 `SnapshotIndex`（11 字段）；
  港股通/期货/期权若将来开启，字段集需先 dump 模型确认，**一律 `.get()`**。
- 收盘时段不触发 tick，`last_tick_age` 会一直增长，属正常。
- **`all` 不是"免费"的**：单连接 ≈1.4 MB/s、一天 ≈21 GB、≈1900 帧/秒的解析压力。多个全推客户端是**线性叠加**的，
  网关侧不做去重合并。真要全市场，优先考虑让网关落库/聚合后再给你（或减少到需要的品种）。
- `all` 只在预加载模式下可用（`[preload] enabled=1`）；lazy 模式没有「全市场表」概念，会直接回 `error`。

#### 3.6.11 客户端兼容性核对（v9/v10 升级必查）

按 2026-11-09 对项目内两份客户端的实测核查整理。**协议层不用改，下面 5 条是逻辑层**：

| # | 检查项 | 不改会怎样 | 判据/做法 |
|---|---|---|---|
| 1 | 地址是否指向新网关 | 连的是旧 lazy 实例，v9/v10 的行为一条都看不到（脚本默认值可能写死旧 IP） | 启动日志打印最终 `ws://host:port/path`；最稳做法是环境变量 `AMAZINGDATA_WS_URL` |
| 2 | tick 解析是否全用 `.get()` | 订了指数（或逆回购等字段更少的品种）就 `KeyError`，异常被外层 `except: return` 吞掉 → **那只标的一直不动且无报错** | `d["bid_price1"]` 一律换 `d.get("bid_price1")` |
| 3 | 是否消费 `ack.unavailable` / `error` | 表外代码（新股/退市/写错/市场没开）只回 error 帧，连接不断；客户端不处理就**以为订上了** | 首帧看 `unavailable`；`add` 路径看 error 文本或与 `ack.added` 求差集 |
| 4 | 改关注列表是否还在断连重订 | 预加载网关下每次改列表白丢 `reconnect_interval`（常见 10s）盲区 | 收到 subscribe ack 且 `universe_size` 存在 → 直接发 `add`/`remove`；lazy 模式（ack 带 `global_codes`）才断连 |
| 5 | 僵死检测盯的对象 | uvicorn 默认 20s 发一次 ping，会持续刷新「收到任意帧」的时间戳 → 连接活着但零 tick 时永远判不出来 | 单独记「最后一条**解析成功**的 tick 时间」，交易时段内超过 60s 无 tick 就主动重连 |

> 另有一条不属于协议、但会直接导致「查不到数据」：`period` 硬编码 `snapshot`。
> 当前线上合法，但 `subscribe_markets` 一旦加入 hkt/future/option 就会被拒——
> 建议客户端默认传 `mixed`，或从 `/health.sub.periods` 读一次再决定。

---

## 4. `security_type` / `market` 取值（依据开发手册 4.1）

### 4.1 `security_type`（沪深北，用于 `get_code_list` / `get_code_info`）

| 取值 | 含义 |
|------|------|
| `EXTRA_STOCK_A` | 上交所+深交所+北交所 A 股（默认） |
| `SH_A` / `SZ_A` / `BJ_A` | 单一市场 A 股 |
| `EXTRA_STOCK_A_SH_SZ` | 沪深 A 股 |
| `EXTRA_INDEX_A_SH_SZ` / `EXTRA_INDEX_A` | 沪深指数（624 只）/ 全指数（**636 只**，2026-11-06 实测） |
| `SH_INDEX` / `SZ_INDEX` / `BJ_INDEX` | 单一市场指数 |
| `SH_ETF` / `SZ_ETF` / `EXTRA_ETF` | ETF（`EXTRA_ETF` 实测 1658 只） |
| `SH_KZZ` / `SZ_KZZ` / `EXTRA_KZZ` | 可转债（`EXTRA_KZZ` 实测 315 只） |
| `SH_HKT` / `SZ_HKT` / `EXTRA_HKT` | 沪港通 / 深港通 / 沪深港通 |
| `SH_GLRA` / `SZ_GLRA` / `EXTRA_GLRA` | 沪/深/沪深 逆回购（`EXTRA_GLRA` 实测 18 只） |

> 与实时订阅的关系（**v10 已更新**）：股票 / ETF / 指数 / 可转债 / 逆回购 都用 `snapshot` 周期，
> 可以放进同一条流；v10 起港股通（`snapshotHKT`）、期货（`snapshotfuture`）、期权（`snapshotoption`）
> 也能进同一条订阅（分组建流，周期按市场各自分配），只是**当前 `subscribe_markets` 未开**。
> 想看某些品种实际能订哪些，读 `/health.sub.subscribe_markets`，不要按 `fetch_markets` 推断。

> ⚠️ `EXTRA_FUTURE` / `EXTRA_OPTION` 传给 `get_code_list` 会返回 0 行；期货/期权要用专门的 `get_future_*` / `get_option_*` 方法。
> ⚠️ 这四个"代码表"接口都是**每日最新、无法获取历史**（手册 3.5.2.1~3.5.2.4 明确标注）；历史代码表用 `get_hist_code_list`。

### 4.2 `security_type`（期货，用于 `get_future_code_list` / `get_future_code_info`）

| 取值 | 含义 |
|------|------|
| `EXTRA_FUTURE` | 期货，包含中金所（SDK 默认，实测 814 只） |

> 手册 4.1.2 写作 `ZJ_FUTURE`，与 3.5.2.3 示例的 `EXTRA_FUTURE` 矛盾；实测 SDK 默认 `EXTRA_FUTURE` 且可用，以 SDK 为准。

### 4.3 `security_type`（期权，用于 `get_option_code_list`）

| 取值 | 含义 |
|------|------|
| `EXTRA_ETF_OP` | ETF 期权，沪深（SDK 默认，实测 1250 只） |
| `SH_OPTION` / `SZ_OPTION` | 单市场期权 |

### 4.4 `market`（用于 `get_calendar` 等）

| 取值 | 含义 |
|------|------|
| `SH` / `SZ` / `BJ` | 上交所 / 深交所 / 北交所 |
| `CFE` | 中金所 |
| `SHN` / `SZN` | 沪港通 / 深港通 |
| `HK` | 港交所 |

---

## 5. MCP server.py 旧方法名 → SDK 1.1.9 真实方法名

`mcp/mcp_server/server.py` 是照**旧版 SDK** 写的，以下方法名在 1.1.9 中**已改名或不存在**，通过 `/api/query` 调用会报 `'X' object has no attribute '...'`：

| MCP server.py 里的名字 | SDK 1.1.9 真实方法 / 现状 |
|------------------------|----------------------------|
| `get_history_code_list` | → `get_hist_code_list`（`security_type, start_date, end_date`） |
| `get_etf_fund_share` | → `get_fund_share` |
| `get_etf_iopv` | → `get_fund_iopv` |
| `get_etf_purchase_redemption` | 不存在；ETF 申赎清单用 `BaseData.get_etf_pcf` |
| `get_index_constituent_weight` | 不存在（有属性 `index_weight`，非方法） |
| `get_industry_index_info` | 不存在（有属性 `industry_base_info`） |
| `get_convertible_bond_issue` / `_balance` / `_conversion_change` / `_adjustment` / `_terms` / `_resale_notice` / `_redemption_notice` / `_suspension` | 全部不存在（SDK 里是 `kzz_*` 属性，非可调方法） |
| `get_kzz_conv` / `get_kzz_call` / `get_kzz_put` | 方法存在但当前 HTTP 下抛 BaseException（见 §6） |

---

## 6. 已知问题与限制

### 6.1 MarketData 无法经 /api/query 调用 —— ✅ 已修复（2026-08-12 21:40 部署，22:01 重新合并恢复）
原来 `MarketData(calendar)` 构造必须传交易日历，而 `/api/query` 无参实例化，报：
`{"ok":false,"error":"MarketData.__init__() missing 1 required positional argument: 'calendar'"}`。
**修复**：后端启动后懒加载并缓存 `get_calendar()`，`cls=="MarketData"` 时用缓存日历构造。K 线、快照现已可用（见 §3.5）。

> ⚠️ 回归记录（2026-08-12 22:00 全量测试发现）：21:45/21:55 迭代加入 local_path/is_local 注入时基于旧版本开发，**覆盖丢了 21:40 版本的 calendar/period/BaseException 修复**，导致 query_kline 再次 500。已从 `http_service.py.bak_20260812_2140` 合并恢复并重新部署。当前版本包含：`_get_calendar()` 懒加载 + `MarketData` 特殊构造 + `_normalize_period()` period 字符串映射 + `except BaseException` 兜底 + local_path/is_local 注入。

### 6.2 一批方法报裸 500 —— ✅ 已修复（2026-08-13 09:08，根因=NaN 序列化）
现象：以下方法返回裸 HTTP 500（`Internal Server Error`），**无日志、不进入 Python 异常链**，且 `except BaseException` 兜底无效：
- `InfoData.get_stock_basic`
- `InfoData.get_balance_sheet` / `get_cash_flow` / `get_income`
- `InfoData.get_index_constituent` / `get_industry_constituent`
- `InfoData.get_kzz_conv` / `get_kzz_call` / `get_kzz_put`
- `InfoData.get_fund_nav`
- `InfoData.get_announcement_bond_list`

**真实根因**（通过 `journalctl` 拿到 FastAPI 层 traceback 确认）：这些方法返回的 DataFrame 含 **NaN**（如 `get_stock_basic` 的 `DELISTDATE` 列，未退市股票无退市日期）。`to_jsonable` 原实现把 DataFrame 直接 `to_dict(orient="records")` 返回、**不递归清洗**，NaN 原样混入响应；FastAPI 的 `JSONResponse` 用 `json.dumps(..., allow_nan=False)` 渲染，在**端点返回之后**（`fastapi/routing.py`）抛 `ValueError: Out of range float values are not JSON compliant: nan`——所以日志无记录、两个 except 都接不住。
**修复**：`to_jsonable` 的 DataFrame/pydantic 分支改为**递归清洗**，`float NaN/Inf → null`。实测全部 11 个方法复活，K线等原有功能回归正常。

> ⚠️ **回归记录（2026-08-15）**：后续迭代覆盖丢失了上述 NaN 清洗，`get_stock_basic` 等再次裸 500（日志无记录）。本次修复 `get_code_info` index 丢失问题时，**一并恢复递归清洗**（NaN/Inf→null 在 `to_jsonable` 叶子层），并新增：DataFrame 带语义化 index（如 `code_market` 股票代码）时先 `reset_index()` 再转 records，index 不再丢失。验证：`get_code_info` 返回含 `code_market`（10488 行）、`get_stock_basic`/`get_index_constituent` 复活、标量列表接口不受影响。远端备份 `http_service.py.bak_code_market`。

### 6.3 会话与并发注意
- 会话生命周期：进程启动时登录一次，之后每天 `04:10 / 09:00 / 21:00` 在**进程内**登出再登录（不重启、前端运行计时不归零）。**任何其他进程再用同一账号 `ad.login()` 会顶掉这个会话**（TGW 单账号单会话）。所以调试时不要在网关机器上另起脚本登录 SDK 做压测——两个 1.4GB 进程同时跑还会把内存打爆（2026-11-05 实测触发过 `corrupted double-linked list`）。
- 本机只发 HTTP 请求、不碰 SDK，天然规避 thread-local 会话坑。
- `/ws/market` 已改为 **v9 全市场预加载模式**（详见 §2.4 / §3.6）：网关自持一条全市场订阅，客户端只做选股转发。**表外代码不会再触发重启**，只在 ack 的 `unavailable` 和一条 `error` 里告知。
- 单账号仍只允许**一条订阅流**，但流内可以混多个 period（v10 起按市场分组，一个 `tgw.Subscribe` 提交）。
  客户端看不到分组，也别指望按客户端区分 period。
- 全推池默认含**股票 + ETF + 指数**（`subscribe_markets` 可配，写 `all` 就是 8 类）。指数推送体是 `SnapshotIndex`，只有 11 个字段、没有盘口，客户端要按 §3.6.5 容错。K 线是独立通道，**不能**塞进这条流。
- 会话更换（定时重登、自愈重启）后订阅会自动用新会话重建；进程级重启期间 WS 会断，客户端必须自己重连（服务端不补发漏掉的 tick）。
- 这台机器只有 2GB 内存，`ad.login()` 本身占约 1.14GB，全市场订阅的增量很小但也没有余量再跑第二个 SDK 进程。

### 6.4 返回 `rows=0` 属正常
`get_profit_notice` / `get_dividend` / `get_right_issue` / `get_long_hu_bang` / 期权系列在测试日期区间内无数据返回 `rows=0`，是**数据确实没有**（不是报错），业务上按空数据处理即可。

### 6.5 当日代码表数据源异常 —— ✅ 已恢复（2026-08-13 早间）
2026-08-12 21:00 起 TGW 侧"当日代码表"数据源故障（`QuerySecuritiesInfo` 服务），波及：`get_code_list` / `get_code_info` / `get_future_code_list` / `get_future_code_info` / `get_option_code_list` 以及 `get_backward_factor` / `get_adj_factor` / `get_etf_pcf`，报 `'NoneType' object is not subscriptable`。
**2026-08-13 早间已恢复**（约 9 点代码表例行更新后）：A股 5543 / 证券信息 10489 / ETF 1621 / 可转债 317 / 期货 814 / 期权 1250 全部正常。
**期间兜底**：
- 取全市场代码：`get_hist_code_list(security_type=..., start_date, end_date)`（故障期正常）
- 复权数据：`get_backward_factor(..., is_local=true)` 读本地缓存（`local_data/basedata/backward_factor/backward_factor.h5` 有缓存时有效）
- ⚠️ 故障期间 `get_adj_factor` 读缓存兜底无效（`adj_factor.h5` 已存在仍报 NoneType，属 SDK 层问题）

---

## 7. 自检脚本

本机可复制以下脚本做连通性自检（无需任何依赖，仅 urllib）：

```python
import json, urllib.request

BASE = "http://<网关机器IP>:8000"

def query(cls, method, params):
    body = json.dumps({"cls": cls, "method": method, "params": params}).encode()
    req = urllib.request.Request(BASE + "/api/query", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

# 自检三连
print(query("BaseData", "get_calendar", {"market": "SH"})["rows"])          # 8702
print(query("BaseData", "get_code_list", {"security_type": "EXTRA_ETF"})["rows"])  # 1619
print(query("InfoData", "get_share_holder",
            {"code_list": ["600000.SH"], "begin_date": 20240101,
             "end_date": 20240630, "is_local": False})["rows"])             # 20
```

订阅状态（不用 WS，先看网关自己怎么说）：

```bash
curl -s http://<网关机器IP>:8000/health | python3 -c '
import json,sys; d=json.load(sys.stdin)
print("status=%s login=%s" % (d["status"], d["login"]))
s=d["sub"]; print("mode=%s ready=%s universe=%s markets=%s period=%s periods=%s ticks=%s last_tick_age=%s"
                  % (s["mode"], s["ready"], s["universe"], ",".join(s["markets"]),
                     s["period"], s["periods"], s["ticks"], s["last_tick_age"]))'
# 期望（盘中，v10 线上）：mode=preload ready=True universe=7891 markets=stock,etf,index,glra
#                       period=snapshot periods=[10014] ticks 持续增长 last_tick_age 个位数
```

WS 连通性（需 `pip install websocket-client`）：

```python
import json, time, websocket
ws = websocket.create_connection("ws://<网关机器IP>:8000/ws/market", timeout=10)
ws.send(json.dumps({"code_list": ["600000.SH", "510300.SH", "000300.SH"], "period": "snapshot"}))
n = 0
t0 = time.time()
while time.time() - t0 < 15:
    m = json.loads(ws.recv())
    if m["type"] == "ack":
        print("ack pool=%s unavailable=%s" % (m.get("universe_size"), m.get("unavailable")))
    elif m["type"] == "tick":
        n += 1
        if n == 1:
            # 用 .get() —— 指数 tick 没有盘口字段
            print("first tick:", m["data"]["code"], m["data"].get("last"),
                  m["data"].get("trade_time"), "bid1=", m["data"].get("bid_price1"))
    elif m["type"] == "error":
        print("error:", m["message"])
print("ticks in 15s =", n)
```

> 收盘 / 午休时段 `ticks` 为 0 属正常；只要 `ack` 到了、`universe_size=7891`（或与你网关 `subscribe_markets` 对应的数），就说明订阅链路是通的。
