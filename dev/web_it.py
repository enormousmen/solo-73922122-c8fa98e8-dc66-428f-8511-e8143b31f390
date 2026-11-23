#!/usr/bin/env python3
"""Web + 真实 PgClusterAdapter 集成自测（连真实 PostgreSQL 15）。

沙箱里无法运行可连接的 hot-standby（辅助进程信号被拦截），因此：
- A/B 都指向同一个真实主库（验证真实连接、真实 LSN、真实记录读写、真实控制卷）
- C 指向一个无监听端口：standby_streaming 真实地连接失败 -> 实时核对未就绪
- 代理状态文件用真实文件模拟（与 agent_it.sh 的产物格式一致）

覆盖：
  1. /api/status 含节点 C，C 不可达时 ready=false
  2. leader=A 时建库被拒；leader=B 后受理，request.env 含 B 的真实 pg_current_wal_lsn
  3. 建库进行中重复点击幂等，返回同一任务
  4. 代理 FAILED -> 状态 FAILED；重新发起 -> 新任务
  5. 代理 RUNNING 但 C 连不上：仍未就绪（不能只凭代理状态宣布可用）
  6. 建库期间对 B 的记录写入/查询真实可用
用法: python3 -I dev/web_it.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from app.config import NodeConfig, Settings  # noqa: E402
from app.main import create_app  # noqa: E402

PORT_B = int(os.getenv("IT_PG_PORT", "55552"))
SOCK = os.getenv("IT_PG_SOCK", "/tmp/agent-it3/sock")
DEAD_PORT = int(os.getenv("IT_DEAD_PORT", "55999"))

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(("ok  " if cond else "FAIL") + f" {name}" + (f" :: {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def main() -> int:
    tmp = tempfile.TemporaryDirectory()
    control_dir = os.path.join(tmp.name, "control")
    os.makedirs(control_dir, exist_ok=True)
    # 真实主库：initdb trust HBA 仍生效，密码字段会被忽略。
    def live_node(name: str) -> NodeConfig:
        return NodeConfig(name, "127.0.0.1", PORT_B, "postgres", "x", "postgres",
                          "postgres", "x")

    settings = Settings(
        node_a=live_node("A"),
        node_b=live_node("B"),
        node_c=NodeConfig("C", "127.0.0.1", DEAD_PORT, "postgres", "x", "postgres",
                          "postgres", "x"),
        state_file=os.path.join(tmp.name, "switch.json"),
        switch_timeout=30.0, poll_interval=0.2, promote_wait_seconds=5,
        background_worker=False,
        build_state_file=os.path.join(tmp.name, "build.json"),
        build_control_dir=control_dir,
    )
    app = create_app(settings)
    with TestClient(app) as client:
        builder = app.state.builder
        build_store = app.state.build_store
        switch_store = app.state.store

        # 1. 状态接口与 C 不可达
        r = client.get("/api/status")
        check("status 200", r.status_code == 200)
        names = {n["name"]: n for n in r.json()["nodes"]}
        check("返回 A/B/C 三节点", set(names) == {"A", "B", "C"})
        check("C 不可达", names["C"]["reachable"] is False)
        check("C 默认未就绪", r.json()["standby_build"]["ready"] is False)

        # 2. leader=A 拒绝；切到 B（手工模拟切换结果）后受理
        r = client.post("/api/standby/c/build", json={})
        check("leader=A 时建库 409", r.status_code == 409, r.text)

        state = switch_store.load()
        state.leader = "B"
        switch_store.save(state)

        r = client.post("/api/standby/c/build", json={})
        check("leader=B 时受理 200", r.status_code == 200, r.text)
        task1 = r.json()["task_id"]
        request_path = Path(control_dir) / "request.env"
        check("request.env 已落盘", request_path.exists())
        request_text = request_path.read_text()
        check("请求含任务号", f"TASK_ID={task1}" in request_text)
        # 受理 LSN 必须是 B 的真实 WAL 位置
        import psycopg
        with psycopg.connect(settings.node_b.dsn(admin=True), autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            real_lsn = cur.fetchone()[0]
        accepted = request_text.split("ACCEPTED_LSN=")[1].splitlines()[0]
        check("request 含真实格式受理 LSN", accepted.startswith("0/"), request_text)
        # 受理先于本次查询，受理 LSN 不应晚于当前 LSN（数值比较）。
        from app.switcher import lsn_int
        check("受理 LSN 不晚于 B 当前 LSN", lsn_int(accepted) <= lsn_int(real_lsn),
              f"{accepted} vs {real_lsn}")

        # 3. 重复点击幂等
        r2 = client.post("/api/standby/c/build", json={})
        check("重复点击幂等", r2.status_code == 200 and r2.json()["idempotent"]
              and r2.json()["task_id"] == task1, r2.text)

        # 4. 代理上报中断 -> FAILED -> 重新发起新任务
        (Path(control_dir) / "status.env").write_text(
            f"PHASE=FAILED\nTASK_ID={task1}\nERROR=基础备份未完成（fail_injected=1）\n")
        builder.run_once()
        check("中断后 FAILED", build_store.load().phase == "FAILED")
        r3 = client.post("/api/standby/c/build", json={})
        task2 = r3.json()["task_id"]
        check("重试产生新任务", r3.status_code == 200 and task2 != task1, r3.text)
        check("失败任务归档", len(build_store.load().history) == 1)

        # 5. 代理 RUNNING 但 C 连不上：不得宣布可用
        (Path(control_dir) / "status.env").write_text(
            f"PHASE=RUNNING\nTASK_ID={task2}\n")
        result = builder.run_once()
        check("C 不可达时保持 BUILDING", result is not None and result["phase"] == "RUNNING",
              str(result))
        ready, reason = builder.check_ready()
        check("实时核对未就绪且给出原因", ready is False and "无法连接 C" in reason, reason)

        # 6. 建库期间 B 的真实写入/查询
        marker = f"web-it-{int(time.time())}"
        r = client.post("/api/records", json={"content": marker})
        check("写入 B 成功", r.status_code == 201 and r.json()["node"] == "B", r.text)
        r = client.get("/api/records?node=B&limit=200")
        contents = {row["content"] for row in r.json()["records"]}
        check("从 B 查回新记录", marker in contents)
        # C 不可达时查询应返回 502 而不是崩溃
        r = client.get("/api/records?node=C")
        check("C 不可达查询返回 502", r.status_code == 502)

    tmp.cleanup()
    if failures:
        print(f"\n{len(failures)} 项失败：{failures}")
        return 1
    print("\nWeb 集成自测全部通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
