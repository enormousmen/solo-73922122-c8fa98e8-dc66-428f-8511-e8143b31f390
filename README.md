# PostgreSQL 主备单向切换 + 节点 C 物理备库重建演示平台

一个 FastAPI 网页，连接**真实运行流复制的 PostgreSQL 15** 集群（主 A、备 B、
按需重建的备 C），支持：

- 录入 / 查询示例记录（写入只走当前主库，可从 A/B/C 任意节点查询）
- 发起**单向**主备切换（旧主 → 备库提升）
- 暂停 / 恢复备库 WAL 回放，用于现场演示「回放超时回滚 → 恢复回放后切换成功」
- **B 接管写入后，一键从 B 给节点 C 建物理备库**（独立数据卷、网页可观测、
  中断可重试、断流自动转未就绪）

## 快速开始

```bash
cd compose
cp .env.example .env          # 可改密码与超时参数
docker compose up -d --build
# 打开 http://localhost:8000
```

一键端到端演示（需要本机有 `curl`、`jq`、`docker compose`）：

```bash
./demo.sh                     # 默认 http://localhost:8000
```

`demo.sh` 会依次完成：写入记录 → 暂停备库回放 → 切换并观察 503 与超时回滚 →
恢复回放 → 重新切换成功 → 校验旧记录在新主可查、新写入只走新主 → 重启网页容器再校验 →
**从 B 给 C 建物理备库（含重复点击幂等、建库期间持续录入）→ 核对 C 流复制与回放达标后显示可用 → 录入新记录并从 C 查到**。

## 节点 C 物理备库重建（与需求逐条对应）

1. **确认 B 已接管写入才受理**：`POST /api/standby/c/build` 仅当当前写入主库为 B、
   且 B 不在 recovery 时受理，否则返回 409。受理瞬间锁定 B 的
   `pg_current_wal_lsn()` 作为**受理 WAL 位置**并原子落盘。
2. **C 使用独立数据卷**：compose 中 `node-c-data` 与 A/B 完全隔离；C 容器平时只跑
   一个代理，等待网页通过共享卷 `c-control` 下发 `request.env`，绝不自动建库。
3. **建库期间 B 照常接收网页录入**：建库是独立状态机（`app/build_state.py`），
   不触碰切换状态、不设置只读围栏；代理用 `pg_basebackup -Xs -R` 做非排他基础备份，
   B 全程可写。
4. **重复点击显示同一次任务**：BUILDING 阶段再次点击直接返回原任务号（幂等），
   不重复下发请求、不新建任务。
5. **C 只从完整基础备份启动**：代理只有在 `pg_basebackup` 正常结束后才写
   `backup.complete` 标记并启动 postgres；基础备份中断（含故障注入
   `FAIL_BASEBACKUP_ONCE=1`）时清理半成品、写 `PHASE=FAILED`，网页提示失败，
   可重新发起。
6. **已建好的数据目录重复点击/容器重启后继续使用**：代理启动时若
   `backup.complete` 与 `PG_VERSION` 都在，直接以流复制备库启动，绝不重做基础备份。
7. **备库可用的判定（每次查询实时核对，非持久化结论）**：C 必须
   `pg_is_in_recovery()=t`、`pg_stat_wal_receiver.status='streaming'` 且上游
   host/port 与 B 一致，并且 `pg_last_wal_replay_lsn() >= 受理 WAL 位置`，
   网页才显示 READY；探测到断流立即变未就绪并给出原因，重连后重新核对。
8. **崩溃安全**：建库状态与控制文件都走「临时文件 + fsync + 原子替换」，
   web 或 C 容器重启后按磁盘上的实际状态续跑。

### 建库状态机

```
IDLE ──受理(锁定 B 的 LSN)──▶ BUILDING ──完整备份+流复制+回放达标──▶ IDLE(任务归档, 实时核对 READY)
                                 │
                                 └──基础备份中断──▶ FAILED ──重新发起──▶ BUILDING
```

“备库可用”不是持久化阶段：任务归档后每次 `/api/status` 都重新查 C 的真实流复制
状态与回放位置，断流即未就绪。

## 切换语义（与需求逐条对应）

1. **切换受理后新写入提示稍后重试**：受理瞬间状态落盘为 `SWITCH_PENDING`，
   之后所有 `POST /api/records` 返回 `503 + Retry-After: 5`。
2. **已接单的写入在旧主收尾**：受理时通过 `ALTER SYSTEM SET
   default_transaction_read_only=on` + reload 围栏旧主，只拦截**新事务**，
   已在执行的写事务继续提交；后台等待旧主无活动写事务（`wait_for_wal_quiesce`）
   后才锁定**最终目标 LSN**，保证覆盖围栏前接单的最后一个事务。
3. **回放完旧主已提交事务才可提升**：轮询备库
   `pg_last_wal_replay_lsn() >= 最终目标 LSN` 后才调用 `pg_promote()`；
   未追平绝不提升。
4. **回放超时恢复旧主写入**：超过 `SWITCH_TIMEOUT` 未追平（例如回放被暂停），
   状态机进入 `ROLLBACK`，把旧主只读围栏解除并恢复写入，历史记录标记 `TIMEOUT`，
   恢复回放后可重新发起切换。
5. **提升后已确认记录在新主可查**：提升基于物理流复制，备库已回放旧主全部
   已提交 WAL，因此 `demo_records` 里所有已确认记录在新主完整可查。
6. **网页服务在提升后、更新写入目标前退出也安全**：所有状态经
   `write 临时文件 + fsync + os.replace + fsync 目录` 原子落盘到
   `/data/state/switch.json`；启动时 `reconcile_on_startup()` 以数据库**实际角色
   + 只读围栏**为准核对：目标已是主库则续完成功收尾，旧主仍是主库则按超时回滚，
   两个节点都可写（真脑裂）则拒绝自动选择。重启后的新写入只走真正的主库。

## 状态机

```
NORMAL ──发起切换──▶ SWITCH_PENDING ──回放追平+promote──▶ PROMOTED ──收尾──▶ NORMAL(leader=B)
                          │
                          └──超过 SWITCH_TIMEOUT──▶ ROLLBACK ──解除旧主只读──▶ NORMAL(leader=A)
```

## 角色与权限（最小权限）

| 角色 | 权限 | 用途 |
| --- | --- | --- |
| `app` | 仅 `appdb.demo_records` 的 SELECT/INSERT | 网页记录读写 |
| `repl` | 仅 `REPLICATION` | 持续 WAL 流复制（`primary_conninfo`） |
| `postgres`（超级用户） | 全部 | 建库建角色、`ALTER SYSTEM`、`pg_promote`、一次性 `pg_basebackup` |

> PG15 的 `pg_basebackup` 需要向 checkpointer 发信号，仅超级用户可做；
> 备库 B 与节点 C 入口用超级用户完成一次性基础备份后，都会把
> `primary_conninfo` 改写为最小权限的 `repl` 角色，持续复制不使用超级用户。

## 目录结构

```
compose/
├── docker-compose.yml          # primary(A) + standby(B) + node-c(C) + web 四服务
├── .env.example
├── demo.sh                     # 端到端演示脚本（含 C 建库与查询）
├── postgres/
│   ├── init/01-app-schema.sh   # 主库首启：建角色/表/授权
│   ├── standby/entrypoint.sh   # 备库 B：pg_basebackup 建库后以流复制启动
│   └── node-c/entrypoint.sh    # 节点 C 代理：按网页请求做基础备份/复用/启动
└── web/
    ├── Dockerfile
    └── requirements.txt
app/                            # FastAPI 应用（与 compose 解耦，可独立运行）
├── config.py                   # 环境变量配置，app/admin 两套 DSN，含节点 C
├── state.py                    # 切换状态机 + 原子持久化（纯标准库，可离线单测）
├── build_state.py              # C 建库任务状态机 + 原子持久化（纯标准库）
├── switcher.py                 # 切换编排器（适配器协议，可注入假实现）
├── builder.py                  # C 建库编排器（控制卷协议 + 实时就绪核对）
├── database.py                 # 真实 PostgreSQL 适配器（psycopg 3）
├── main.py                     # HTTP 接口 + 演示页面 + 后台工作线程
└── static/index.html
tests/                          # unittest（状态机/编排器/Web 层，共 51 个）
dev/
├── local_cluster.sh            # 无 Docker 时用本机 postgres 二进制起主备的辅助脚本
├── agent_it.sh                 # C 代理 + 真实 PG 的中断/重试/复用集成自测
└── web_it.py                   # Web + 真实 PG 适配器的建库流程集成自测
```

## 本地开发与自测

```bash
pip install -r compose/web/requirements.txt
PYTHONPATH=. python -m unittest discover -s tests -v
```

- `tests/test_state.py`：原子持久化、状态机非法流转、成功/超时生命周期
- `tests/test_switcher.py`：回放追平提升、暂停回放超时回滚、写事务收尾锁定最终
  LSN、提升后崩溃的启动核对、脑裂拒绝
- `tests/test_web.py`：503 + Retry-After、切换受理/回滚/成功全链路、重启后写入路由、
  C 建库受理/幂等/中断重试/断流翻转
- `tests/test_build_state.py`、`tests/test_builder.py`：建库状态机与编排器

无 Docker 但本机有解压版 PG 二进制时（仓库自带 `.local-pg`），可跑真实 PG 集成自测：

```bash
dev/agent_it.sh    # 真实主库 + 真实代理：故障注入中断→FAILED→重试成功→重启复用
dev/web_it.py      # 真实 PgClusterAdapter：受理 LSN、幂等、断流未就绪、真实读写
```

> 受限沙箱里 hot-standby postmaster 无法拉起 walreceiver（信号被 seccomp 拦截），
> 真实可连接备库只能在 Docker 内运行；`agent_it.sh` 用 `POSTGRES_BIN=/bin/true`
> 测试缝模拟 postgres 启动，基础备份、控制卷、状态机全部为真实路径。

## 已知边界

- 这是**演示级**单向切换，不做自动故障检测与 fence-old-leader 的 STONITH；
  旧主在新主提升后仍可能被人手工恢复写入，启动核对会把「两个节点都可写」识别为
  脑裂并拒绝自动选择，需人工介入。
- 切换后旧主不会自动反向重建为新主的备库，再次切回需要先重建流复制。
- 节点 C 的设计定位是「B 接管写入后的新物理备库」：建库入口只受理 B 为写入主库的
  场景；C 不参与主备切换（没有提升按钮），切换状态机仍只在 A/B 之间工作。
- 写事务收尾判定基于 `pg_stat_activity` 中无其他活动事务后端；超长事务会推迟
  最终 LSN 锁定，但仍受 `SWITCH_TIMEOUT` 约束，超时即回滚。
