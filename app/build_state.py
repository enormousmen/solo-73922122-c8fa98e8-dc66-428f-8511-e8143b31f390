"""节点 C 物理备库建库任务的状态机与原子持久化（纯标准库，可离线单测）。

建库任务与主备切换状态机相互独立：建库期间 B 照常接收网页录入，
切换状态保持 ``NORMAL``，本模块只描述“给 C 做基础备份并接入流复制”这一件事。

状态流转::

    IDLE      --(受理建库，记录 B 受理时 WAL 位置)--> BUILDING
    BUILDING  --(C 已从完整基础备份启动且回放追平受理 LSN)--> READY
    BUILDING  --(基础备份中断/代理报错)--> FAILED --(重新发起)--> BUILDING

持久化沿用与 :mod:`app.state` 相同的「临时文件 + fsync + os.replace +
fsync 目录」原子写法，容器重启后读到的状态始终完整。

“备库可用”不是一个持久化阶段，而是每次查询时对 C 的实时核对结果：
只有 C 正在从 B 流复制、且回放已达到受理时 B 的 WAL 位置才显示可用；
探测到断流立即变为未就绪，重连后重新核对。
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# 建库任务阶段
BUILD_IDLE = "IDLE"
BUILD_BUILDING = "BUILDING"
BUILD_FAILED = "FAILED"

BUILD_ALL_PHASES = (BUILD_IDLE, BUILD_BUILDING, BUILD_FAILED)

# 建库结果（写入历史）
BUILD_RESULT_PENDING = "PENDING"
BUILD_RESULT_FAILED = "FAILED"
BUILD_RESULT_READY = "READY"


class BuildError(RuntimeError):
    """建库流程中的可预期错误（如 B 还没接管写入）。"""


@dataclass
class BuildRecord:
    task_id: str
    started_at: float
    accepted_lsn: str
    source_node: str = "B"
    finished_at: Optional[float] = None
    result: str = BUILD_RESULT_PENDING
    message: str = ""


@dataclass
class BuildState:
    phase: str = BUILD_IDLE
    current: Optional[BuildRecord] = None
    history: List[BuildRecord] = field(default_factory=list)
    # 最近一次实时核对的备库可用性（仅作展示缓存，权威结论每次现查）
    ready: bool = False
    last_checked_at: Optional[float] = None
    last_not_ready_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase,
            "current": asdict(self.current) if self.current else None,
            "history": [asdict(h) for h in self.history],
            "ready": self.ready,
            "last_checked_at": self.last_checked_at,
            "last_not_ready_reason": self.last_not_ready_reason,
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "BuildState":
        current = BuildRecord(**raw["current"]) if raw.get("current") else None
        history = [BuildRecord(**h) for h in raw.get("history", [])]
        return cls(
            phase=raw.get("phase", BUILD_IDLE),
            current=current,
            history=history,
            ready=raw.get("ready", False),
            last_checked_at=raw.get("last_checked_at"),
            last_not_ready_reason=raw.get("last_not_ready_reason"),
        )

    def public_view(self) -> Dict[str, Any]:
        return {
            "phase": self.phase,
            "ready": self.ready,
            "last_checked_at": self.last_checked_at,
            "last_not_ready_reason": self.last_not_ready_reason,
            "current_build": asdict(self.current) if self.current else None,
            "history": [asdict(h) for h in self.history[-10:]],
        }


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".build-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_dir(directory)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


class BuildStateStore:
    """BuildState 的持久化门面，所有写操作都原子落盘。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> BuildState:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                return BuildState.from_dict(json.load(fh))
        except FileNotFoundError:
            state = BuildState()
            self.save(state)
            return state

    def save(self, state: BuildState) -> None:
        atomic_write_json(self.path, state.to_dict())

    def begin_build(self, task_id: str, accepted_lsn: str,
                    source_node: str = "B", now: Optional[float] = None) -> BuildState:
        """受理一次建库。

        - BUILDING 中重复点击：返回进行中的同一任务（幂等，不新建）。
        - FAILED 后重新发起：归档失败记录并开启新任务。
        - IDLE 且无历史：开启首个任务。
        """
        state = self.load()
        if state.phase == BUILD_BUILDING:
            return state
        ts = now if now is not None else time.time()
        if state.phase == BUILD_FAILED and state.current is not None:
            state.history.append(state.current)
        state.current = BuildRecord(
            task_id=task_id,
            started_at=ts,
            accepted_lsn=accepted_lsn,
            source_node=source_node,
        )
        state.phase = BUILD_BUILDING
        state.ready = False
        state.last_not_ready_reason = None
        self.save(state)
        return state

    def mark_failed(self, message: str, now: Optional[float] = None) -> BuildState:
        """基础备份中断或代理报错：网页提示失败，允许重新发起。"""
        state = self.load()
        if state.current is None:
            return state
        state.current.result = BUILD_RESULT_FAILED
        state.current.finished_at = now if now is not None else time.time()
        state.current.message = message
        state.phase = BUILD_FAILED
        state.ready = False
        state.last_not_ready_reason = message
        self.save(state)
        return state

    def mark_idle_after_ready(self, message: str, now: Optional[float] = None) -> BuildState:
        """C 已从完整基础备份启动并追平受理 LSN：任务归档，阶段回到 IDLE。

        之后“备库可用”完全由实时核对决定（断流即未就绪，重连重新核对）。
        """
        state = self.load()
        if state.current is None:
            return state
        state.current.result = BUILD_RESULT_READY
        state.current.finished_at = now if now is not None else time.time()
        state.current.message = message
        state.history.append(state.current)
        state.current = None
        state.phase = BUILD_IDLE
        self.save(state)
        return state

    def update_readiness(self, ready: bool, reason: Optional[str],
                         now: Optional[float] = None) -> BuildState:
        """缓存最近一次实时核对结论（权威结论仍以查询时现查为准）。"""
        state = self.load()
        state.ready = ready
        state.last_checked_at = now if now is not None else time.time()
        state.last_not_ready_reason = None if ready else reason
        self.save(state)
        return state
