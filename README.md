# 停电单核查 API

FastAPI + SQLite 实现的停电单核查服务。联络开关切换后，台区可能从另一路电源继续供电，
因此**台区只有连不到任何电源才算失电**。服务按变位单自动核算各台区失电时段，
并据此核查人工报停区间，列出**应补报**与**应剔除**的区间。

## 固定拓扑

启动时自动加载（首次启动写入 SQLite，幂等）：

```
P1 ─K1─ T1 ─K2─ T2 ─K3─ T3 ─K4─ T4 ─K5─ P2
                （K3 为联络开关，初始断开）
```

| 开关 | 连线 | 初始通断 |
| ---- | -------- | -------- |
| K1 | P1 — T1 | 通 |
| K2 | T1 — T2 | 通 |
| K3 | T2 — T3（联络开关） | 断 |
| K4 | T3 — T4 | 通 |
| K5 | T4 — P2 | 通 |

正常运行时 T1/T2 由 P1 供电、T3/T4 由 P2 供电；一侧失电时合上 K3 即可由另一侧转供。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `8000` 端口，打开 <http://localhost:8000/docs> 即可在 Swagger UI 中
补录、更正、撤销变位单并做核查。SQLite 数据持久化在命名卷 `outage-data` 中。

### 本地开发

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest                      # 运行测试
uvicorn app.main:app --reload
```

环境变量 `DB_PATH` 可指定 SQLite 文件路径（默认 `./data/outage.db`，容器内 `/data/outage.db`）。

## API 一览

| 方法 | 路径 | 说明 |
| ------ | --------------------------- | -------------------------------------------- |
| GET | `/topology` | 固定拓扑：电源、台区、双向开关连线及各开关初始/当前通断 |
| GET | `/switches/{id}/timeline` | 开关状态时间轴（物化段，初始段起点为 null） |
| POST | `/orders` | **补录**变位单：单号 + 固定开关 + UTC 时刻 + 通断 |
| GET | `/orders`、`/orders/{单号}` | 查询变位单 |
| PUT | `/orders/{单号}` | **更正**变位单（开关固定，只能改时刻与通断） |
| DELETE | `/orders/{单号}` | **撤销**变位单 |
| GET | `/loss-periods` | 查询已算好的台区失电时段（按范围裁剪） |
| POST | `/verify` | **核查**：列出应补报与应剔除的区间 |

### 变位单规则

- 每张单固定一个开关，填写 UTC 时刻（ISO 8601，不带时区按 UTC 处理）和通断（`通`/`断`）。
- **同一开关同一时刻已有另一张单时拒绝提交**（HTTP 409），由唯一索引
  `(switch_id, change_epoch)` 兜底。
- **同刻变位一起算**：同一时刻多个开关的变位落在同一核算 epoch，一起生效。
  例如 10:00 同时“拉开 K1、合上 K3”，T1/T2 全程不失电。
- 变位单与重算后的失电时段在**同一事务**中生效；任何一步失败整体回滚，
  核查仍使用原结果。

### 核查规则

- 填台区、时间范围和人工报停区间；所有区间均为 **[起点, 终点)：起点计入、终点不计**。
- 直接用已算好的失电时段比对：
  - **应补报** = 失电时段 − 人工报停区间
  - **应剔除** = 人工报停区间 − 失电时段

## 示例：联络开关转供

```bash
# 1) 10:00 拉开 K1（甲站出线检修）→ T1、T2 失电
curl -X POST localhost:8000/orders -H 'Content-Type: application/json' -d '{
  "order_no": "PB001", "switch_id": "K1",
  "change_time": "2026-11-23T10:00:00Z", "state": "断"}'

# 2) 10:05 合上联络开关 K3 → T1、T2 由 P2 转供，失电时段 [10:00, 10:05) 封口
curl -X POST localhost:8000/orders -H 'Content-Type: application/json' -d '{
  "order_no": "PB002", "switch_id": "K3",
  "change_time": "2026-11-23T10:05:00Z", "state": "通"}'

# 3) 核查 T1：人工报停 09:30-10:03
curl -X POST localhost:8000/verify -H 'Content-Type: application/json' -d '{
  "area_id": "T1",
  "range_start": "2026-11-23T00:00:00Z",
  "range_end": "2026-11-24T00:00:00Z",
  "reported": [{"start": "2026-11-23T09:30:00Z", "end": "2026-11-23T10:03:00Z"}]}'
```

返回：

```json
{
  "loss_periods":   [{"start": "...T10:00:00Z", "end": "...T10:05:00Z"}],
  "should_report":  [{"start": "...T10:03:00Z", "end": "...T10:05:00Z"}],
  "should_remove":  [{"start": "...T09:30:00Z", "end": "...T10:00:00Z"}]
}
```

## 增量重算设计

- `switch_segments` 物化各开关状态时间轴，定位任意时刻（含段首）状态只需
  一次索引点查（`start_epoch <= t ORDER BY start_epoch DESC LIMIT 1`），
  **无需扫描此前的变位历史**。
- 补录 / 更正 / 撤销时，先算出该开关新旧状态不同的时段
  `[t, 下一变位时刻)`（更正为撤销 + 补录两段的并集），**只重算这些时段**的
  失电时段并与范围外相邻段接合；状态无差异的冗余单完全不触发重算。
- 重算按 epoch（相邻变位时刻之间）推进，每 epoch 用并查集判定各台区是否
  连到至少一路电源，结果写回 `loss_periods`。

## 目录结构

```
app/
  main.py       FastAPI 路由与请求模型
  core.py       变位时间轴、增量重算、核查比对
  db.py         SQLite 连接、建表、写事务
  topology.py   固定拓扑种子数据
tests/
  test_core.py  单元 / 端到端测试（含增量重算与暴力全量重算对照）
Dockerfile、docker-compose.yml、requirements*.txt
```
