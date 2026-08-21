# AmazingData HTTP REST 调用规范文档

> 文档日期：2026-08-12
> 测试环境：本机 <客户端机器>（纯 HTTP 客户端，**不装 AmazingData SDK**）→ 远端 `<网关机器IP>:8000`（HTTP 中间层，持有 TGW 登录会话）→ TGW 数据源 `<TGW数据源IP_电信入口>:8600`（电信入口；备选联通入口 `<TGW数据源IP_联通入口>:8600`）
> 后端：`http_service.py`（FastAPI + uvicorn，启动时 `ad.login()` 一次）
> SDK 版本：AmazingData **1.1.9**
> 本文所有示例均在本机用 curl 实测通过（收盘时段，行情类接口无实时 tick 属正常）。

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
| `GET /health` | 存活+登录态：`{"status":"ok","login":true}` |
| `GET /api/stats` | 运行指标：运行时间/请求数/数据行/重启次数/最近日志 |
| `GET /` | 状态面板（独立静态文件 `index.html`，与 http_service.py 同目录，改 UI 覆盖文件即可，无需重启 server） |
| `WS /ws/market` | 实时行情推送（见 §3.4） |

### 1.7 耗时经验（实测）
- `get_code_list` / `get_code_info` / `get_hist_code_list` / `get_adj_factor` / `get_option_code_list`：**15–21 秒**（TGW 全量拉取，正常现象，客户端超时要设长）。
- 其余 InfoData 明细类：0.2–4 秒。
- 登录只在后端进程启动时执行一次，**后续请求复用会话，不会每次重新登录**；请求端与 TGW 没有会话交互。

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

### 2.4 SubscribeData（实时订阅）—— 仅 WS —— ✅ 盘中实测通过（2026-08-13 修复+增删）

> 📖 **完整协议语法见 §3.6**（帧格式表 / 字段说明 / 示例代码 / 场景推演 / 注意事项）。本节为总览。

`WS /ws/market`，客户端连接后首帧发：

```json
{"code_list": ["510300.SH"], "period": "snapshot"}
```

服务端推送每笔行情：

```json
{"type": "tick", "data": { ... 行情字段 ... }}
```

**动态增删订阅**（2026-08-13 新增，方案A）：首帧之后可发控制帧：
- `{"type":"add","code_list":["600519.SH"]}` —— 增订。标已在订阅集合内则立即生效（回 `ack`）；**集合外标的会触发服务自动重启换集合**（回 `restarting` 帧，约 10s 中断，客户端需重连）。
- `{"type":"remove","code_list":["510300.SH"]}` —— 退订。纯客户端过滤，立即生效（回 `ack`）。

服务端响应帧：
- `{"type":"ack","action":"subscribe|add|remove","codes"/"added"/"removed":[...]}` 操作确认
- `{"type":"error","message":...}`
- `{"type":"restarting","message":...}` 集合外增订触发服务重启

**TGW 单订阅会话硬限制（实测确认）**：同进程只有**第一个** `tgw.Subscribe` 能收到数据，`tgw.UnSubscribe` 不会把数据流释放给新的 Subscribe，故"退订→重订新列表"不可行。唯一可靠换集合方式是**重启进程**：增订集合外标的时，服务把期望集合持久化到 `/tmp/amazingdata_subscribe.json` 后 `os.execv` 重启，重启后按"持久化集合 ∪ 首连客户端集合"订阅。想清空持久化集合重新开始：删除该文件后重启服务。

- `period` 取值：`snapshot`（快照）、`min1`~`min120`、`day`、`week` 等（`ad.constant.Period` 枚举名，快照=10014）。period 全局唯一，首帧锁定。
- **盘中实测**（2026-08-13 10:05~10:40，A股上午时段）：订阅 `300750.SZ / 510300.SH / 000001.SH`，快照期约 **1 条/s**，字段含 `trade_time / last / pre_close / open / high / low / volume / amount / num_trades / ask_price1 / bid_price1 ...`；增删标的、集合外自动重启全链路通过。收盘时段不触发 tick，属正常。
- **已修复的两处 bug（2026-08-13）**：
  1. **datetime 序列化崩连接**：`data.model_dump()` 里含 `datetime`，`ws.send_json()` 直接抛 `TypeError('Object of type datetime is not JSON serializable')` 掐断连接——之前盘中测试 0 tick 的真凶。已改为 `to_jsonable()` 递归清洗后 `send_text()`。
  2. **订阅线程泄漏抢占会话**：旧实现每次 WS 连接都 `new SubscribeData + daemon 线程跑 sub.run()`，断开后线程不回收；而 TGW 单账号同时只支持**一个**订阅会话，第一个订阅线程永久霸占，导致后续任何订阅都收不到 tick（实测第二个连接 0 条）。已改为**全局单订阅管理器**：进程内唯一 `sub.run()` 线程，多客户端按 `data.code` 共享扇出。
- 客户端断开后订阅线程继续运行（无害，不占额外资源）；重新连接订阅相同标的即可恢复收流。
- 测试脚本：`python3 ws_subscribe_test.py [秒数]`（纯标准库，无需装依赖；装了 websocket-client 则优先用它）。

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

### 3.6 实时行情订阅（WS /ws/market）—— 完整语法 —— ✅ 盘中实测通过

> 订阅推送**只能用 WebSocket**，REST `/api/query` 只做一次性查询，不推送。本节是 WS 订阅的完整协议规范，实测日期 2026-08-13。

#### 3.6.1 连接地址

```
ws://<网关机器IP>:8000/ws/market
```

同网段直连；不在同一网络时先做端口转发 `ssh -L 8000:127.0.0.1:8000 root@<网关机器IP>`，再连 `ws://127.0.0.1:8000/ws/market`。

#### 3.6.2 消息流向

```
客户端 ──(1) 首帧订阅──────► 服务端
客户端 ──(2) 控制帧 add/remove ─► 服务端
服务端 ──(3) tick/ack/restarting/error ─► 客户端
```

- 首帧**必须**先发，控制帧在首帧之后任意时刻可发。
- 服务端只推送文本 JSON 帧。

#### 3.6.3 客户端 → 服务端 帧规范

**① 首帧（订阅）—— 必发一次**

```json
{"code_list": ["300750.SZ", "510300.SH"], "period": "snapshot"}
```

| 字段 | 必填 | 说明 |
|------|------|------|
| `code_list` | ✅ | 证券代码数组，如 `["510300.SH"]`；重复代码自动去重 |
| `period` | ✅ | 周期名，见 §3.5 period 对照表；订阅用 `snapshot`（快照，枚举 10014） |

**② 控制帧 `add`（增订）**

```json
{"type": "add", "code_list": ["600519.SH"]}
```

| 场景 | 服务端行为 |
|------|-----------|
| 标的全在已订阅集合内 | 立即生效，回 `{"type":"ack","action":"add","added":[...]}` |
| 有标的在集合外 | 自动重启进程换集合 → 回 `{"type":"restarting",...}` 后断连，约 10s 后重连生效（重启后全局集合由首连请求决定） |

**③ 控制帧 `remove`（退订）**

```json
{"type": "remove", "code_list": ["510300.SH"]}
```

| 场景 | 服务端行为 |
|------|-----------|
| 标的是本连接已订阅的 | 立即停止转发，回 `{"type":"ack","action":"remove","removed":[...]}` |
| 标的本连接没订阅 | `removed` 返回空数组，不影响其它连接 |

#### 3.6.4 服务端 → 客户端 帧规范

**tick（行情推送）**

```json
{"type": "tick", "data": {"code": "510300.SH", "trade_time": "2026-08-13 10:05:39",
 "last": 4.774, "pre_close": 4.748, "open": 4.77, "high": 4.786, "low": 4.766,
 "close": 0.0, "volume": 179426228, "amount": 857281594.0, "num_trades": 24532,
 "high_limited": 5.223, "low_limited": 4.273,
 "ask_price1": 4.774, "ask_volume1": 10400, "bid_price1": 4.773, "bid_volume1": 2183700,
 "ask_price2": 4.775, "ask_volume2": 0, "bid_price2": 4.772, "bid_volume2": 0}}
```

主要字段：`code`（代码）、`trade_time`（**交易所行情数据时间**）、`last`（最新价）、`pre_close`（昨收）、`open/high/low/close`、`volume`（成交总量，股）、`amount`（成交额，元）、`num_trades`（笔数）、`high_limited/low_limited`（涨停/跌停）、`ask_price1~5/ask_volume1~5`（卖盘五档）、`bid_price1~5/bid_volume1~5`（买盘五档）、`iopv`（ETF 实时参考净值，股票为 0）、`trading_phase_code`（交易阶段码）。指数无买卖档字段。

**tick 全量字段表**（对应手册 4.2.1 Level-1 快照 Snapshot；2026-08-13 盘中实测，股票/ETF 各 31 字段）：

| 字段 | 类型 | 示例 | 说明（手册原文） |
|------|------|------|------|
| `code` | str | `"300750.SZ"` | 证券代码+市场 |
| `trade_time` | datetime | `"2026-08-13 10:52:21"` | **交易所行情数据时间**（交易所时间戳，秒级，无毫秒） |
| `last` | float | `393.29` | 最新价 |
| `pre_close` | float | `393.93` | 昨收价 |
| `open` / `high` / `low` | float | `393.04 / 396.04 / 391.04` | 开盘价 / 最高价 / 最低价 |
| `close` | float | `0.0` | 收盘价（**盘中为 0**，收盘后才有值） |
| `volume` | float | `9043850` | 成交总量（**单位：股**，实测 `volume×last≈amount` 验证） |
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

**ack（操作确认）**

```json
{"type": "ack", "action": "subscribe", "codes": ["300750.SZ"], "global_codes": ["000001.SH", "300750.SZ", "510300.SH"]}
{"type": "ack", "action": "add", "added": ["600519.SH"]}
{"type": "ack", "action": "remove", "removed": ["510300.SH"]}
```

- `subscribe` 的 `codes` 为本连接订阅列表，`global_codes` 为服务当前全局订阅集合。

**error（错误）**

```json
{"type": "error", "message": "..."}
```

触发场景：首帧缺 `code_list`、`period` 未知、与全局 period 不一致、命令类型未知、重启被跳过等。

**restarting（集合外增订触发重启）**

```json
{"type": "restarting", "message": "新增标的 ['600519.SH'] 不在订阅集合, 服务即将重启换集合, 约 10 秒后请重新连接; 新集合=[...]"}
```

收到后**关闭连接、等待约 10 秒、用相同首帧重连**即可。

#### 3.6.5 <客户端机器> 端完整示例（Python，零依赖）

```python
import json, os, time, socket, struct, base64

WS_URL = "ws://<网关机器IP>:8000/ws/market"
BASE   = "http://<网关机器IP>:8000"

# 简单 WS 客户端（生产建议用 websocket-client 库）
import websocket  # pip3 install websocket-client; 没有则改用 ws_subscribe_client.py 内置纯标准库版
ws = websocket.create_connection(WS_URL, origin=BASE, timeout=10)

# 1) 首帧订阅
ws.send(json.dumps({"code_list": ["300750.SZ", "510300.SH"], "period": "snapshot"}))

# 2) 收行情
t0 = time.time()
while time.time() - t0 < 30:
    msg = json.loads(ws.recv())
    if msg["type"] == "tick":
        d = msg["data"]
        print(d["code"], d["last"], d["trade_time"])
    elif msg["type"] == "ack":
        print("ack:", msg)

# 3) 动态增删（任意时刻发控制帧）
ws.send(json.dumps({"type": "add",    "code_list": ["000001.SH"]}))
ws.send(json.dumps({"type": "remove", "code_list": ["300750.SZ"]}))
```

> 完整可运行客户端见仓库 `ws_subscribe_client.py`（支持断线重连、add/remove 演示，零依赖任何 python3 可跑）。

#### 3.6.6 场景推演

```
1. 客户端A 订阅 [A,B]          → ack(subscribe) + A、B 的 tick
2. 客户端B 订阅 [B,C]          → B、C 不在全局集合(仍只有 A,B) → restarting → 全局集合变 [A,B,C] → 全部重连
3. 客户端A remove B            → ack(remove) → A 不再收到 B 的 tick（全局流里 B 仍在，只是不转发给 A）
4. 客户端A add C（集合内）     → ack(add) → A 开始收到 C 的 tick
```

#### 3.6.7 注意事项

- **逐笔数据不可用（2026-08-13 实测）**：`period` 传 `execution`（逐笔成交）/ `order`（逐笔委托）时订阅能建立（回 ack）但 **0 条回调**——HTTP 层与 SDK 直连均无数据，疑似账号无逐笔权限或 TGW 未开通。当前只有 `snapshot`（快照）有数据，`trade_time` 为**秒级**，无毫秒级时间戳。
- **TGW 单订阅会话硬限制**：同进程只有第一个 `tgw.Subscribe` 能收到数据，"退订→重订"不可行；集合外增订只能靠服务自动重启换集合（约 10s 中断）。
- **period 全局唯一且首帧锁定**：不同 period 的连接会被拒绝（回 error 帧），换 period 需重启服务。
- **全局订阅集合只增不减**：remove 只是客户端过滤，全局流不变；集合随集合外增订而增长。
- **全局订阅集合不持久化**：服务重启后全局集合由首连客户端提交的 `code_list` 决定；集合外增订会触发重启换集合。删除 `/tmp/amazingdata_subscribe.json`（历史遗留文件）不会影响新逻辑。
- 收盘时段不触发 tick，属正常。
- 测试脚本：`python3 ws_subscribe_test.py [秒数]`。

---

## 4. `security_type` / `market` 取值（依据开发手册 4.1）

### 4.1 `security_type`（沪深北，用于 `get_code_list` / `get_code_info`）

| 取值 | 含义 |
|------|------|
| `EXTRA_STOCK_A` | 上交所+深交所+北交所 A 股（默认） |
| `SH_A` / `SZ_A` / `BJ_A` | 单一市场 A 股 |
| `EXTRA_STOCK_A_SH_SZ` | 沪深 A 股 |
| `EXTRA_INDEX_A_SH_SZ` / `EXTRA_INDEX_A` | 沪深指数 / 全指数 |
| `SH_INDEX` / `SZ_INDEX` / `BJ_INDEX` | 单一市场指数 |
| `SH_ETF` / `SZ_ETF` / `EXTRA_ETF` | ETF |
| `SH_KZZ` / `SZ_KZZ` / `EXTRA_KZZ` | 可转债 |
| `SH_HKT` / `SZ_HKT` / `EXTRA_HKT` | 沪港通 / 深港通 / 沪深港通 |
| `SH_GLRA` / `SZ_GLRA` / `EXTRA_GLRA` | 沪/深/沪深 逆回购 |

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
- 登录只发生在后端进程启动时一次；**任何其他进程再用同一账号 `ad.login()` 会顶掉这个会话**（TGW 单账号单会话），服务靠看门狗自愈重启。
- 本机只发 HTTP 请求、不碰 SDK，天然规避 thread-local 会话坑。
- `/ws/market` 实时订阅已修复并盘中验证（2026-08-13）：全局单订阅管理器 + 动态增删（详见 §2.4）。**注意**：TGW 单账号单订阅会话是硬限制，"退订→重订"不可行；集合外增订会触发服务自动重启（约 10s 中断，所有客户端重连）。

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
