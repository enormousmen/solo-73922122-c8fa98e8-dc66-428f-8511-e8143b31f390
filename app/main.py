"""FastAPI 入口：状态/记录接口、切换受理、回放控制、节点 C 建库和演示页面。"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .build_state import BUILD_BUILDING, BUILD_IDLE, BuildStateStore, BuildError
from .builder import StandbyBuilder
from .config import Settings
from .database import PgClusterAdapter
from .state import PHASE_NORMAL, StateStore, SwitchError
from .switcher import Switcher

logger = logging.getLogger("switch-demo")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


class RecordIn(BaseModel):
    content: str = Field(min_length=1, max_length=500)


class SwitchIn(BaseModel):
    target: Optional[str] = None
    timeout: Optional[float] = Field(default=None, ge=1.0, le=600.0)


class ReplayIn(BaseModel):
    node: Optional[str] = None


def _make_lifespan(settings: Settings, store: StateStore,
                   adapter: PgClusterAdapter, switcher: Switcher,
                   builder: Optional[StandbyBuilder], build_store: BuildStateStore,
                   wake: threading.Event):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 1. 以数据库实际角色为准核对状态（覆盖“提升后、更新目标前进程退出”）。
        try:
            switcher.reconcile_on_startup()
        except Exception as exc:
            logger.exception("启动核对失败：%r", exc)
        # 2. 等主库可达后建表（DDL 经流复制到备库）。
        for attempt in range(60):
            try:
                adapter.ensure_schema()
                break
            except Exception as exc:
                logger.info("等待主库就绪以建表（%s/60）：%r", attempt + 1, exc)
                wake.wait(2.0)
        # 3. 建库任务续跑：容器重启后按 C 代理写出的实际状态推进，
        #    已建好的数据目录会被代理直接复用，不会重做基础备份。
        if builder is not None:
            try:
                builder.run_once()
            except Exception as exc:
                logger.exception("建库任务启动续跑失败：%r", exc)

        def worker() -> None:
            # 用 Event 等待而不是 time.sleep：切换受理、回放恢复后可立即唤醒推进。
            while True:
                try:
                    state = store.load()
                    if state.phase != PHASE_NORMAL:
                        switcher.run_once()
                    if builder is not None:
                        builder.run_once()
                        # 每轮实时核对 C 的流复制与回放位置：
                        # 探测到断流立即把可用性改为未就绪，重连后重新核对。
                        builder.poll_and_cache_readiness()
                except Exception as exc:
                    logger.exception("后台工作线程出错：%r", exc)
                wake.wait(timeout=max(0.5, settings.poll_interval))
                wake.clear()

        if settings.background_worker:
            thread = threading.Thread(target=worker, name="switch-worker", daemon=True)
            thread.start()
        logger.info("应用启动完成，当前状态：%s", store.load().public_view())
        yield

    return lifespan


def create_app(
    settings: Optional[Settings] = None,
    adapter: Optional[PgClusterAdapter] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    store = StateStore(settings.state_file)
    adapter = adapter or PgClusterAdapter(settings)
    switcher = Switcher(
        store,
        adapter,
        poll_interval=settings.poll_interval,
        promote_wait_seconds=settings.promote_wait_seconds,
    )
    build_store = BuildStateStore(settings.build_state_file)
    # 未配置节点 C（离线单测）时建库功能整体停用。
    builder: Optional[StandbyBuilder] = (
        StandbyBuilder(build_store, adapter, settings.build_control_dir,
                       poll_interval=settings.poll_interval)
        if settings.node_c is not None
        else None
    )

    wake = threading.Event()
    app = FastAPI(title="PostgreSQL 主备切换演示",
                  lifespan=_make_lifespan(settings, store, adapter, switcher,
                                          builder, build_store, wake))
    app.state.settings = settings
    app.state.store = store
    app.state.adapter = adapter
    app.state.switcher = switcher
    app.state.build_store = build_store
    app.state.builder = builder
    switch_lock = threading.Lock()
    build_lock = threading.Lock()

    # ---------------- 页面 ---------------- #
    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html_path = Path(__file__).parent / "static" / "index.html"
        return HTMLResponse(html_path.read_text(encoding="utf-8"))

    # ---------------- 状态 ---------------- #
    @app.get("/api/status")
    def status() -> dict:
        state = store.load()
        node_names = ["A", "B"] + (["C"] if settings.node_c is not None else [])
        nodes = [adapter.node_status(name) for name in node_names]
        response = {
            "cluster": state.public_view(),
            "nodes": nodes,
        }
        if builder is not None:
            build_state = build_store.load()
            # 权威可用性每次现查：断流立即体现为未就绪，重连后重新核对。
            try:
                builder.poll_and_cache_readiness()
                build_state = build_store.load()
            except Exception as exc:
                logger.warning("C 备库可用性核对失败：%r", exc)
            response["standby_build"] = build_state.public_view()
        return response

    # ---------------- 示例记录 ---------------- #
    @app.post("/api/records", status_code=201)
    def create_record(payload: RecordIn) -> dict:
        state = store.load()
        if state.phase != PHASE_NORMAL:
            # 切换受理后新写入一律提示稍后重试；旧主也已被置只读做双保险。
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "SWITCH_IN_PROGRESS",
                    "message": "主备切换进行中，写入已暂停，请稍后重试",
                    "phase": state.phase,
                },
                headers={"Retry-After": "5"},
            )
        try:
            return adapter.insert_record(state.leader, payload.content)
        except Exception as exc:
            # SQLSTATE 25006 = read_only_sql_transaction，数据库层围栏生效。
            sqlstate = getattr(exc, "sqlstate", None)
            text = str(exc).strip()
            if sqlstate == "25006" or "read-only" in text or "read only" in text:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "LEADER_READ_ONLY",
                        "message": "当前主库处于只读（切换中），请稍后重试",
                    },
                    headers={"Retry-After": "5"},
                )
            logger.exception("写入失败")
            raise HTTPException(status_code=502, detail={"error": "DB_ERROR", "message": text})

    @app.get("/api/records")
    def get_records(node: Optional[str] = None, limit: int = 50) -> dict:
        state = store.load()
        target = node or state.leader
        allowed = ("A", "B") + (("C",) if settings.node_c is not None else ())
        if target not in allowed:
            raise HTTPException(
                400,
                detail={"error": "BAD_NODE", "message": f"node 只能是 {'/'.join(allowed)}"},
            )
        try:
            rows = adapter.list_records(target, limit=min(max(1, limit), 200))
        except Exception as exc:
            raise HTTPException(502, detail={"error": "DB_ERROR", "message": str(exc).strip()})
        return {"node": target, "count": len(rows), "records": rows}

    # ---------------- 单向切换 ---------------- #
    @app.post("/api/switch")
    def start_switch(payload: SwitchIn) -> dict:
        with switch_lock:
            state = store.load()
            if state.phase != PHASE_NORMAL:
                raise HTTPException(
                    409,
                    detail={
                        "error": "SWITCH_IN_PROGRESS",
                        "message": f"已有切换进行中（{state.phase}）",
                    },
                )
            target = payload.target or ("B" if state.leader == "A" else "A")
            if target not in ("A", "B"):
                raise HTTPException(400, detail={"error": "BAD_NODE", "message": "target 只能是 A 或 B"})
            timeout = payload.timeout or settings.switch_timeout
            try:
                new_state = switcher.start_switch(target, timeout)
            except SwitchError as exc:
                raise HTTPException(409, detail={"error": "SWITCH_REJECTED", "message": str(exc)})
            wake.set()
            return {
                "accepted": True,
                "message": "切换已受理：新写入请稍后重试，旧主已置只读，等待备库回放后提升",
                "switch": new_state.public_view(),
            }

    # ---------------- 回放暂停/恢复（演示用） ---------------- #
    @app.post("/api/replay/pause")
    def pause_replay(payload: ReplayIn) -> dict:
        node = payload.node or "B"
        try:
            adapter.pause_replay(node)
        except SwitchError as exc:
            raise HTTPException(409, detail={"error": "NOT_STANDBY", "message": str(exc)})
        wake.set()
        return {"node": node, "replay_paused": True}

    @app.post("/api/replay/resume")
    def resume_replay(payload: ReplayIn) -> dict:
        node = payload.node or "B"
        try:
            adapter.resume_replay(node)
        except SwitchError as exc:
            raise HTTPException(409, detail={"error": "NOT_STANDBY", "message": str(exc)})
        wake.set()
        return {"node": node, "replay_paused": False}

    # ---------------- 节点 C 物理备库建库 ---------------- #
    @app.post("/api/standby/c/build")
    def build_standby_c() -> dict:
        """确认 B 已接管写入后，从 B 给节点 C 建物理备库。

        - 仅当当前写入主库为 B 时受理（B 未接管写入直接拒绝）
        - 建库进行中重复点击返回同一次任务，不新建任务
        - 基础备份中断后页面提示失败，可重新发起；C 已有完整数据目录则复用
        - 建库期间 B 照常接收网页录入（不改动切换状态机，不拦截写入）
        """
        if builder is None:
            raise HTTPException(
                503,
                detail={"error": "NODE_C_DISABLED",
                        "message": "未配置节点 C（PG_C_HOST），建库功能不可用"},
            )
        with build_lock:
            state = store.load()
            current = build_store.load()
            # 幂等：建库进行中，重复点击仍显示同一次任务。
            if current.phase == BUILD_BUILDING and current.current is not None:
                return {
                    "accepted": True,
                    "idempotent": True,
                    "message": "建库任务已在进行中，显示同一次任务",
                    "task_id": current.current.task_id,
                    "build": current.public_view(),
                }
            # 已成功建过完整备库：C 代理已 exec postgres、不再轮询请求文件，
            # 此时再下发请求不会被消费。改为返回当前实时可用性，避免任务悬挂。
            if current.phase == BUILD_IDLE and builder.is_complete_built():
                ready, reason = builder.check_ready()
                build_store.update_readiness(ready, reason)
                return {
                    "accepted": False,
                    "idempotent": True,
                    "already_built": True,
                    "message": (
                        "C 的物理备库已建好并复用现有数据目录；当前"
                        + ("备库可用（正从 B 流复制且回放达标）" if ready
                           else f"未就绪：{reason}")
                    ),
                    "task_id": current.history[-1].task_id,
                    "build": build_store.load().public_view(),
                }
            try:
                task_id = builder.start_build(state.leader)
            except BuildError as exc:
                raise HTTPException(
                    409, detail={"error": "BUILD_REJECTED", "message": str(exc)}
                )
            wake.set()
            return {
                "accepted": True,
                "idempotent": False,
                "message": (
                    "建库已受理：C 将从 B 做完整物理基础备份，"
                    "期间 B 照常接收网页录入；C 回放达到受理时 B 的 WAL 位置后才显示可用"
                ),
                "task_id": task_id,
                "build": build_store.load().public_view(),
            }

    @app.get("/api/standby/c")
    def standby_c_status() -> dict:
        if builder is None:
            raise HTTPException(
                503,
                detail={"error": "NODE_C_DISABLED",
                        "message": "未配置节点 C（PG_C_HOST），建库功能不可用"},
            )
        ready = builder.poll_and_cache_readiness()
        reason = build_store.load().last_not_ready_reason
        return {
            "ready": ready,
            "reason": reason,
            "build": build_store.load().public_view(),
            "node": adapter.node_status("C"),
        }

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("WEB_PORT", "8000")),
    )
