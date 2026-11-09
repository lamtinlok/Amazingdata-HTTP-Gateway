# AmazingData HTTP 网关

![替代文字](screen.png)

因为银河证券星耀数智 SDK 只能运行在 x86 架构的机器上，我的策略是运行在arm架构的 Mac mini上 的，没法直接用SDK，还有很多奇奇怪怪的问题比如登录限制，内存溢出等问题，就用 AI 做了个转发的中间层，以提高其兼容性。  
**本项目是在一台x86架构，内存为4GB的 Debian Linux 平台构建和运行的，我用了一段时间已经稳定运行，大部分主要功能都经过测试没有问题，因为是用 AI 搓的，在我这里用可能没问题，可能换个环境就有问题，到时候自行让 AI 来帮你解决，解决不了就让 AI 帮你重写整个项目也行，这里只是提供一个思路**

把 **AmazingData** 封装成一个 HTTP 中间层 + 内置状态面板，对外提供 HTTP REST 查询、WebSocket 实时行情推送，并自带守护进程与  
`/health` 健康检查接口。

---

## 1. 目录结构

```
amazingdata-http-gateway/
├── http_service.py      # 主程序(FastAPI): REST/WS/看门狗/UI
├── index.html           # 内置状态面板(随 http_service.py 一起提供服务)
├── config.ini           # 配置文件
├── run.sh               # 启动 + 存活检测脚本(配合 crontab 守护)
├── requirements.txt     # pip 依赖
├── HTTP_API_SPEC.md     # 完整接口文档
├── LICENSE              # 许可证
├── .gitignore           # 忽略 local_data/ run/ 日志等运行产物
└── README.md
```

---

## 2. 依赖安装

```bash
pip install -r requirements.txt
```

`AmazingData` SDK 为数据服务商提供的私有 Python 包，不通过 pip 公开分发，  
请按服务商指引单独安装（安装后 `import AmazingData` 可用即可）。

---

## 3. 配置

仓库里只带一份模板 `config.example.ini`，**先复制成 `config.ini`，再把 `<...>` 占位符换成你的真实值**：

```bash
cp config.example.ini config.ini
vim config.ini          # 填账号 / 密码 / 数据源地址
```

`config.ini` 含登录凭据，已列入 `.gitignore`，不随仓库分发；改完重启服务生效。
完整键名与注释见 `config.example.ini`，下面是需要你动手的部分：

```ini
[tgw]
user     = <你的TGW账号>           # 数据源登录账号
password = <你的TGW密码>           # 数据源登录密码
host     = <数据源服务器IP或域名>   # 即"数据库"入口, 跑数据服务的机器
port     = 8600

[server]
host     = 0.0.0.0                # HTTP 监听地址(本机填 127.0.0.1 仅本机访问)
port     = 8000                   # HTTP 监听端口

[paths]
local_data = local_data          # 行情/数据本地缓存目录

[preload]
enabled           = 1            # 全市场预加载订阅; 0 = 退回旧模式(仅供排障)
fetch_markets     = all          # 每天拉哪些市场的代码表; all = 8 类全拉
subscribe_markets = stock,etf,index
                                 # 开盘订阅哪些(⊆ fetch_markets), 逗号分隔
list_time         = 09:05        # 拉全市场代码表(拉不到就沿用现有表)
hold_start        = 09:00        # 定版窗口起点: hold_start~subscribe_time 不建订阅
subscribe_time    = 09:15        # 定版窗口终点: 用当天表起订阅

# 旧版的 security_types / period 两个键已废弃: 周期按市场自动选择,
# 写了也会被忽略(不报错), 不用再配。

[relogin]
times           = 04:10,09:00,21:00   # 也可写显式区间 03:58-04:01, 21:00
logout_lead_min = 2                   # 登录前几分钟先登出

[ws]
# 推送缓冲是覆盖式的(每只代码只留最新一份), 没有"队列上限"可配
# queue_max 已废弃, 写了也不生效
```

`[preload]` 是 2026-11 起的默认架构：网关自己拉代码表、自己保持一条订阅流，
客户端连 WS 后只声明「要转发哪几只」。`[relogin]` 让网关每天在**进程内**换三次 TGW 会话，
不重启进程、前端运行计时不归零。

**要订阅哪些市场完全由配置决定**，不用改代码。两个键共用同一套市场名，写 `all` 或 `*` 就是 8 类全选：

| 键 | 市场 | `security_type` | period |
|---|---|---|---|
| `stock` | A股 | `EXTRA_STOCK_A` | snapshot |
| `etf` | ETF | `EXTRA_ETF` | snapshot |
| `index` | 指数 | `EXTRA_INDEX_A` | snapshot |
| `kzz` | 可转债 | `EXTRA_KZZ` | snapshot |
| `glra` | 逆回购 | `EXTRA_GLRA` | snapshot |
| `hkt` | 港股通 | `EXTRA_HKT` | snapshotHKT |
| `future` | 期货 | `EXTRA_FUTURE` | snapshotfuture |
| `option` | ETF期权 | `EXTRA_ETF_OP` | snapshotoption |

- `fetch_markets`：每天拉哪些市场的代码表（拉表很便宜，建议 `all`，表全了以后想加市场只改下一行）；
- `subscribe_markets`：开盘后真正向上游订阅哪些市场，必须是前者的子集。
  默认 `stock,etf,index`；写 `all` 即全市场（本机实测约 11700 只、稳态 ~1.5 GB 内存、tick 4000–4500/s）。
- 改了 `subscribe_markets` **必须重启**；配了但没拉到的市场会被跳过并打 WARNING，不会报错退出。

订阅时机只有两条规则：**09:15~次日 09:00 只要有表就立刻订**（半夜/凌晨重启也能开机即用）；
**09:00~09:15 是表定版窗口，不订**（09:00 换会话会作废旧订阅、`list_time` 才拉新表，等 09:15 一次建好）。
拉表失败就沿用现有表，当天定版后不再换表 —— 不是每天都有新股/退市股。

> 每个连接只保留「每只代码的最新一份」：有数据立刻推，没数据不发帧；客户端读得慢只是合并中间状态
> （`/health.sub.coalesced`），内存恒等于订阅的代码数，不会随时间增长。

> 想「全都要」不必逐个列代码，首帧传 `{"code_list": "all"}` 即可（也接受 `["all"]` 或 `{"all": true}`）。
> 它表示「网关当前订阅的全部标的」，网关没订的市场不会出现在里面。
> 代价是单连接约 **1.4 MB/s、一个交易日约 21 GB、约 1900 帧/秒**，只适合本地要算全市场的场景。

> 股票 / ETF / 指数 / 可转债 / 逆回购同属 `snapshot` 周期，可以混在一条流里；
> 港股通、期货、期权各用各的周期，网关会按市场自动分组订阅（一个账号仍只有**一条订阅流**）。
> K 线不走推送通道：用 `POST /api/query`（`cls="MarketData"`, `method="query_kline"`）一次性取。
> 它和 `/ws/market` 推送**并行、互不干扰**，同一进程里可以一边订阅实时行情一边查历史 K 线。
>
> 指数推送的是 `SnapshotIndex`（11 个字段），**没有买卖五档、涨跌停价、成交笔数、IOPV**，
> 客户端必须按缺字段容错，详见 `HTTP_API_SPEC.md` §3.6.5。

配置优先级：**环境变量 > config.ini > 默认值**  
（环境变量名：`TGW_USER` `TGW_PASSWORD` `TGW_HOST` `TGW_PORT` `HTTP_HOST` `HTTP_PORT` `AMAZINGDATA_CONFIG`）。  
为避开系统自带的 `USER`/`HOST`/`PORT` 等环境变量，这里统一加了 `TGW_`/`HTTP_` 前缀。

还有几个不常用但确实存在的：`AMAZINGDATA_SDK_LOG_DIR`（SDK 日志目录）；
`AD_PRELOAD_NOW=1` 跳过时间判断立即拉表建订阅（**仅盘后/盘中验证用，正常运行别设**）。
`run.sh` 自己另认 `PYTHON_BIN` / `HEALTH_TIMEOUT` / `STARTUP_GRACE`，见第 4 节。

> `config.ini` 不入库，凭据只留在部署机上。若想让配置也能随代码走，把密码放进环境变量，
> `config.ini` 里保留占位值即可。
---

## 4. 启动

### 方式 A：直接运行（调试/前台）

```bash
python3 http_service.py
# 或
python3 -m uvicorn http_service:app --host 0.0.0.0 --port 8000
```

打开浏览器访问 `http://<机器IP>:8000/` 即可看到状态面板。

### 方式 B：run.sh + crontab（推荐，生产守护）

`run.sh` 一个脚本同时完成**启动**和**存活检测**：它探测 `/health`，  
若进程不存在或无响应就自动拉起，因此配合 crontab 每分钟调用即可实现进程守护。

```bash
chmod +x run.sh

# 手动启动 / 查看状态 / 停止 / 重启
./run.sh           # 检测存活, 未运行则启动
./run.sh status
./run.sh stop
./run.sh restart
```

加入 crontab（每分钟执行一次存活检测 + 自拉起）：

```bash
# 把下面一行追加进当前用户的 crontab(请改成你的实际绝对路径)
(crontab -l 2>/dev/null; echo "* * * * * /绝对路径/amazingdata-http-gateway/run.sh >> /绝对路径/amazingdata-http-gateway/run/cron.log 2>&1") | crontab -

# 查看
crontab -l
```



> **`run.sh` 默认用的是 `./venv/bin/python`，不是 PATH 里的 `python3`。**  
> 所以要么先在脚本同目录建好 venv（`python3 -m venv venv && venv/bin/pip install -r requirements.txt`），  
> 要么显式指定解释器：`PYTHON_BIN=/path/to/python3 ./run.sh`。  
> crontab 里同样要写全，例如：  
> `* * * * * PYTHON_BIN=/opt/venv/bin/python3 /绝对路径/.../run.sh >> /绝对路径/.../run/cron.log 2>&1`
>
> 另外两个可调变量：`HEALTH_TIMEOUT`（单次 `/health` 探活超时，默认 5 秒）、  
> `STARTUP_GRACE`（启动宽限秒数，默认 120：端口迟迟不就绪时先等待，而不是立刻判失败重拉）。

日志与 PID 文件位于 `run/`：`run/server.log`（服务输出）、  
`run/http_service.pid`、`run/cron.log`（crontab 调用记录）。

---

## 5. 接口一览

| 接口           | 方法   | 说明                                      |
| ------------ | ---- | --------------------------------------- |
| `/`          | GET  | 内置状态面板(HTML)                            |
| `/api/query` | POST | 通用查询 `body={"cls","method","params"}`   |
| `/ws/market` | WS   | 实时行情推送。网关自持全市场订阅，首帧 `code_list` 表示**要转发哪几只**（传 `"all"` 则全市场全推），支持动态增删 |
| `/api/stats` | GET  | 运行指标(JSON, 供 UI 拉取)                     |
| `/health`    | GET  | 健康检查(恒 200), 含 `login` / `ready` / `sub` 等 |

完整请求/响应格式见 [HTTP\_API\_SPEC.md](./HTTP_API_SPEC.md)。

健康检查示例：

```bash
curl http://127.0.0.1:8000/health
# {"status":"ok","login":true,"ready":true,"probe_ok_age":3,...,
#  "sub":{"mode":"preload","ready":true,"universe":7891,"markets":["stock","etf","index","glra"],...}}
```

> `universe` 的数字取决于 `subscribe_markets`：默认三市场 `stock,etf,index` 约 7873，
> 加逆回购 `glra` 后约 7891（生产实测值）。不是固定常量，别拿它做断言。

> `/health` **永远返回 200**（进程活着就算），会话抖动由进程内看门狗处理。
> 想看“行情到底通不通”，主指标是 **`probe_ok_age`**（距最近一次真实查询成功多少秒，
> 超过 `probe_interval` 的若干倍就说明上游取不到数），其次才是 `sub.ready` / `sub.last_tick_age`。
> `hb_age` / `hb_qry_age` 这类心跳字段**仅供参考**：v6 起它们不参与判活，
> 2026-09-22 就出现过心跳全程健康、查询已挂 8 小时而 `/health` 仍回 `status=ok` 的情况。
> 不要用 HTTP 状态码判行情可用性。

---

## 6. 与 AmazingData 主程序的关系

本网关是**独立的中间层**，只依赖 AmazingData 的 Python SDK（`import AmazingData`），  
不需要 AmazingData 桌面/客户端主程序一起部署。它可以单独放在任意一台能访问数据源  
服务器、且装有 SDK 的机器上运行。因此本目录与"主程序"解耦，适合单独开源。

---

## 7. 安全须知（脱敏说明）

- `config.ini` 含登录凭据、**不入库**；仓库里只有占位符版本 `config.example.ini`。
  想连配置一起纳入版本管理，就用环境变量 `TGW_USER`/`TGW_PASSWORD`/`TGW_HOST` 注入，
  `config.ini` 里保留占位值即可。
- 本文档与 `HTTP_API_SPEC.md` 中的 `<网关机器IP>`、`<你的TGW账号>` 等均为占位符，
  真实地址与凭据一律未写入仓库。
- `local_data/`、`run/`、`*.log` 等运行产物已列入 `.gitignore`，无需手工清理。
- 部署机登录用的 `*.exp` / `server_auth` 等运维脚本属于私有资产，未纳入本目录。
- 生产环境建议将 `server.host` 设为 `127.0.0.1` 并在前面加一层反向代理/Nginx 做鉴权，  
  不要将 8000 端口直接暴露公网。
