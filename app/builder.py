"""节点 C 物理备库建库编排器。

编排器不 import psycopg，只依赖 :class:`StandbyBuildAdapter` 协议，
因此可以用内存假适配器在没有数据库的环境里完整单测。
真实适配器见 :mod:`app.database`，C 容器内的代理脚本见
``compose/postgres/node-c/entrypoint.sh``。

协作方式（web 容器与 C 容器通过共享 Docker 卷交换文件）：

- web 受理建库 → 原子写 ``request.env``（含任务号、受理时 B 的 WAL 位置）
- C 代理轮询到新任务 → 仅在数据卷为空/无完整标记时跑 ``pg_basebackup``，
  全程把阶段/进度/错误写入 ``status.env``
- web 轮询 ``status.env`` 推进状态；基础备份中断 → FAILED，可重新发起；
  C 已建好的数据目录在重复点击或容器重启后继续使用，不会重做基础备份
"""
from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Dict, Optional, Protocol

from .build_state import (
    BUILD_BUILDING,
    BUILD_IDLE,
    BUILD_RESULT_READY,
    BuildStateStore,
    BuildError,
)
from .switcher import lsn_int

logger = logging.getLogger(__name__)

REQUEST_FILE = "request.env"
STATUS_FILE = "status.env"

# C 代理状态机取值（与 entrypoint.sh 保持一致）
AGENT_IDLE = "IDLE"
AGENT_BUILDING = "BUILDING"
AGENT_RUNNING = "RUNNING"
AGENT_FAILED = "FAILED"


class StandbyBuildAdapter(Protocol):
    """建库流程需要的数据库/控制卷原子操作。"""

    def is_in_recovery(self, node: str) -> bool: ...

    def current_wal_lsn(self, node: str) -> str: ...

    def standby_streaming(self, upstream: str, standby: str) -> Dict[str, Any]:
        """核对备库是否正从指定上游流复制，返回流式状态与接收/回放 LSN。"""

    def write_control_file(self, name: str, lines: Dict[str, str]) -> None: ...

    def read_control_file(self, name: str) -> Optional[Dict[str, str]]: ...


def _parse_env_file(raw: str) -> Dict[str, str]:
    """解析 C 代理写的简单 KEY=VALUE 文件（值不含换行，按白名单读取）。"""
    result: Dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


class StandbyBuilder:
    def __init__(
        self,
        store: BuildStateStore,
        adapter: StandbyBuildAdapter,
        control_dir: str,
        poll_interval: float = 1.0,
        clock=time.time,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.control_dir = control_dir
        self.poll_interval = poll_interval
        self._clock = clock

    # ------------------------------------------------------------------ #
    # 受理建库
    # ------------------------------------------------------------------ #
    def start_build(self, leader: str) -> str:
        """受理一次“从 B 给 C 建物理备库”。

        硬性前置：B 已接管写入（B 是不在 recovery 的主库）。
        重复点击时若任务仍在 BUILDING，直接返回原任务号，显示同一次任务。
        返回任务号。
        """
        if leader != "B":
            raise BuildError("只有确认 B 已接管写入（当前写入主库为 B）后才能给 C 建备库")
        try:
            if self.adapter.is_in_recovery("B"):
                raise BuildError("B 当前仍是备库，尚未接管写入，不能作为建库上游")
        except BuildError:
            raise
        except Exception as exc:
            raise BuildError(f"无法确认 B 的角色：{exc}") from exc

        state = self.store.load()
        if state.phase == BUILD_BUILDING and state.current is not None:
            logger.info("建库任务 %s 进行中，重复点击返回同一任务", state.current.task_id)
            return state.current.task_id

        # 受理瞬间锁定 B 的 WAL 位置：C 只有回放达到该位置才算备库可用。
        try:
            accepted_lsn = self.adapter.current_wal_lsn("B")
        except Exception as exc:
            raise BuildError(f"无法读取 B 当前 WAL 位置：{exc}") from exc

        task_id = uuid.uuid4().hex[:12]
        state = self.store.begin_build(
            task_id, accepted_lsn, source_node="B", now=self._clock()
        )
        # 通知 C 代理。原子替换，代理只在任务号变化时动作。
        self.adapter.write_control_file(REQUEST_FILE, {
            "TASK_ID": task_id,
            "SOURCE_NODE": "B",
            "ACCEPTED_LSN": accepted_lsn,
            "REQUESTED_AT": str(self._clock()),
        })
        logger.info("建库任务已受理：task=%s 受理 LSN=%s", task_id, accepted_lsn)
        return task_id

    # ------------------------------------------------------------------ #
    # 后台 / 重启续跑：推进一次建库状态机
    # ------------------------------------------------------------------ #
    def run_once(self) -> Optional[dict]:
        state = self.store.load()
        if state.phase != BUILD_BUILDING or state.current is None:
            return None
        task = state.current
        try:
            status = self.adapter.read_control_file(STATUS_FILE)
        except Exception as exc:
            logger.warning("读取 C 代理状态失败，下轮重试：%r", exc)
            return None
        if status is None:
            logger.info("C 代理尚未写出状态（任务 %s），下轮再查", task.task_id)
            return None

        agent_phase = status.get("PHASE", "")
        status_task = status.get("TASK_ID", "")
        # 代理还在处理上一个任务（容器重启、请求文件尚未被观察到）：等待即可。
        if status_task and status_task != task.task_id and agent_phase != AGENT_IDLE:
            logger.info("C 代理仍在执行旧任务 %s，等待其回到 IDLE", status_task)
            return None

        if agent_phase == AGENT_FAILED:
            message = "基础备份中断或失败：" + status.get(
                "ERROR", "C 代理报告失败"
            )
            logger.warning("任务 %s 失败：%s", task.task_id, message)
            self.store.mark_failed(message, now=self._clock())
            return {"phase": AGENT_FAILED, "message": message}

        if agent_phase == AGENT_RUNNING:
            # C 已从完整基础备份启动。是否“可用”还要实时核对流复制与回放位置。
            ready, reason = self.check_ready()
            if ready:
                message = (
                    f"C 已从完整基础备份启动并接入 B 流复制，回放达到受理位置 "
                    f"{task.accepted_lsn}"
                )
                self.store.mark_idle_after_ready(message, now=self._clock())
                self.store.update_readiness(True, None, now=self._clock())
                logger.info("任务 %s 完成，C 备库可用", task.task_id)
                return {"phase": "READY", "message": message}
            logger.info("C 已启动但尚未就绪：%s", reason)
            return {"phase": AGENT_RUNNING, "reason": reason}

        if agent_phase == AGENT_BUILDING:
            logger.info(
                "任务 %s 基础备份进行中：%s（%s）",
                task.task_id, status.get("STAGE", ""), status.get("PROGRESS", ""),
            )
            return {"phase": AGENT_BUILDING}

        # IDLE 但没有进入 BUILDING：代理可能刚重启，保持等待，下轮再观察。
        logger.info("C 代理处于 IDLE，等待其接手任务 %s", task.task_id)
        return None

    # ------------------------------------------------------------------ #
    # 实时核对：C 是否正从 B 流复制且回放达到受理时 B 的 WAL 位置
    # ------------------------------------------------------------------ #
    def check_ready(self) -> tuple[bool, Optional[str]]:
        state = self.store.load()
        target_lsn = state.current.accepted_lsn if state.current else None
        if target_lsn is None and state.history:
            # 任务已归档：用最近一次建库受理的 LSN 作为最低回放门槛。
            target_lsn = state.history[-1].accepted_lsn
        if target_lsn is None:
            return False, "没有建库受理记录"
        try:
            info = self.adapter.standby_streaming("B", "C")
        except Exception as exc:
            return False, f"无法连接 C：{exc}"
        if not info.get("in_recovery"):
            return False, "C 不在 recovery 状态（不是备库）"
        if not info.get("streaming"):
            return False, "C 未在从 B 流复制（流已断开）"
        replay_lsn = info.get("replay_lsn") or "0/0"
        if lsn_int(replay_lsn) < lsn_int(target_lsn):
            return False, (
                f"回放位置 {replay_lsn} 尚未达到受理时 B 的 WAL 位置 {target_lsn}"
            )
        return True, None

    def poll_and_cache_readiness(self) -> bool:
        """供后台线程/状态接口周期调用：实时核对并缓存结论，断流即改为未就绪。

        结论没变化时不落盘，避免共享卷上每秒一次的无意义写入。
        """
        ready, reason = self.check_ready()
        state = self.store.load()
        if state.ready != ready or state.last_not_ready_reason != reason:
            self.store.update_readiness(ready, reason, now=self._clock())
        return ready

    def is_complete_built(self) -> bool:
        """是否已成功建过完整备库（最近一次任务结果为 READY）。"""
        state = self.store.load()
        return bool(state.history and state.history[-1].result == BUILD_RESULT_READY)


def write_env_file(path: str, values: Dict[str, str]) -> None:
    """共享卷上的 KEY=VALUE 文件也走临时文件 + 原子替换 + 目录 fsync。"""
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}.{uuid.uuid4().hex[:6]}"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            for key, value in values.items():
                fh.write(f"{key}={value}\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


def read_env_file(path: str) -> Optional[Dict[str, str]]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return _parse_env_file(fh.read())
    except FileNotFoundError:
        return None
