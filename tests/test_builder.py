"""建库编排器测试：受理前置、幂等、中断失败、实时就绪核对与断流翻转。"""
from __future__ import annotations

import os
import tempfile
import unittest

from app.build_state import (
    BUILD_BUILDING,
    BUILD_FAILED,
    BUILD_IDLE,
    BuildStateStore,
)
from app.builder import REQUEST_FILE, STATUS_FILE, StandbyBuilder
from app.build_state import BuildError


class ManualClock:
    def __init__(self) -> None:
        self.t = 2000.0

    def time(self) -> float:
        return self.t


class FakeBuildAdapter:
    """内存假适配器：模拟 B 主库、C 代理控制卷与流复制状态。"""

    def __init__(self, clock: ManualClock) -> None:
        self.clock = clock
        self.b_in_recovery = False
        self.b_lsn = "0/3000"
        # C 的实时状态
        self.c_in_recovery = True
        self.c_streaming = True
        self.c_replay_lsn = "0/0"
        self.c_upstream = ("bhost", "5432")
        # 控制卷
        self.files: dict[str, dict[str, str]] = {}
        # 代理将返回的状态（测试用脚本驱动）
        self.agent_status: dict[str, str] | None = None
        self.requests: list[dict[str, str]] = []

    def is_in_recovery(self, node: str) -> bool:
        if node == "B":
            return self.b_in_recovery
        if node == "C":
            return self.c_in_recovery
        raise KeyError(node)

    def current_wal_lsn(self, node: str) -> str:
        assert node == "B"
        return self.b_lsn

    def standby_streaming(self, upstream: str, standby: str) -> dict:
        return {
            "in_recovery": self.c_in_recovery,
            "streaming": self.c_streaming and self.c_upstream == ("bhost", "5432"),
            "upstream_host": self.c_upstream[0],
            "upstream_port": self.c_upstream[1],
            "receive_lsn": self.c_replay_lsn,
            "replay_lsn": self.c_replay_lsn,
            "status": "streaming" if self.c_streaming else None,
        }

    def write_control_file(self, name: str, lines: dict[str, str]) -> None:
        self.files[name] = dict(lines)
        if name == REQUEST_FILE:
            self.requests.append(dict(lines))

    def read_control_file(self, name: str) -> dict | None:
        if name == STATUS_FILE:
            return self.agent_status
        return self.files.get(name)


class StandbyBuilderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = ManualClock()
        self.adapter = FakeBuildAdapter(self.clock)
        self.store = BuildStateStore(os.path.join(self.tmp.name, "build.json"))
        self.builder = StandbyBuilder(
            self.store, self.adapter,
            control_dir=os.path.join(self.tmp.name, "control"),
            poll_interval=0.01, clock=self.clock.time,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_rejects_when_b_is_not_leader(self) -> None:
        with self.assertRaises(BuildError):
            self.builder.start_build("A")

    def test_rejects_when_b_still_in_recovery(self) -> None:
        self.adapter.b_in_recovery = True
        with self.assertRaises(BuildError):
            self.builder.start_build("B")

    def test_accept_writes_request_with_accepted_lsn(self) -> None:
        task_id = self.builder.start_build("B")
        self.assertEqual(len(task_id), 12)
        request = self.adapter.files[REQUEST_FILE]
        self.assertEqual(request["TASK_ID"], task_id)
        self.assertEqual(request["ACCEPTED_LSN"], "0/3000")
        state = self.store.load()
        self.assertEqual(state.phase, BUILD_BUILDING)
        self.assertEqual(state.current.accepted_lsn, "0/3000")

    def test_duplicate_start_returns_same_task_and_single_request(self) -> None:
        first = self.builder.start_build("B")
        second = self.builder.start_build("B")  # 重复点击
        self.assertEqual(first, second)
        self.assertEqual(len(self.adapter.requests), 1)  # 没有重复下发请求

    def test_agent_building_keeps_building(self) -> None:
        task_id = self.builder.start_build("B")
        self.adapter.agent_status = {
            "PHASE": "BUILDING", "TASK_ID": task_id, "STAGE": "basebackup",
            "PROGRESS": "42%",
        }
        result = self.builder.run_once()
        self.assertEqual(result["phase"], "BUILDING")
        self.assertEqual(self.store.load().phase, BUILD_BUILDING)

    def test_agent_failure_marks_failed_and_retry_starts_new_task(self) -> None:
        task_id = self.builder.start_build("B")
        self.adapter.agent_status = {
            "PHASE": "FAILED", "TASK_ID": task_id,
            "ERROR": "基础备份未完成（fail_injected=1）",
        }
        result = self.builder.run_once()
        self.assertEqual(result["phase"], "FAILED")
        state = self.store.load()
        self.assertEqual(state.phase, BUILD_FAILED)
        self.assertIn("基础备份", state.current.message)

        # 重新发起：新任务号，失败记录归档。
        new_task = self.builder.start_build("B")
        self.assertNotEqual(new_task, task_id)
        state = self.store.load()
        self.assertEqual(state.phase, BUILD_BUILDING)
        self.assertEqual(len(state.history), 1)

    def test_ready_requires_streaming_and_replay_at_accepted_lsn(self) -> None:
        task_id = self.builder.start_build("B")  # accepted 0/3000
        # C 已启动但回放还没追上：不能就绪。
        self.adapter.agent_status = {"PHASE": "RUNNING", "TASK_ID": task_id}
        self.adapter.c_replay_lsn = "0/2000"
        result = self.builder.run_once()
        self.assertEqual(result["phase"], "RUNNING")
        self.assertEqual(self.store.load().phase, BUILD_BUILDING)
        ready, reason = self.builder.check_ready()
        self.assertFalse(ready)
        self.assertIn("尚未达到", reason)

        # 回放追平受理位置：任务完成、显示可用。
        self.adapter.c_replay_lsn = "0/3000"
        ready, reason = self.builder.check_ready()
        self.assertTrue(ready, reason)
        result = self.builder.run_once()
        self.assertEqual(result["phase"], "READY")
        state = self.store.load()
        self.assertEqual(state.phase, BUILD_IDLE)
        self.assertTrue(state.ready)
        self.assertEqual(state.history[-1].result, "READY")

    def test_stream_break_flips_not_ready_and_reconnect_rechecks(self) -> None:
        self.builder.start_build("B")
        self.adapter.c_replay_lsn = "0/3000"
        self.builder.run_once()  # 完成建库
        self.assertTrue(self.builder.poll_and_cache_readiness())

        # 断流：实时核对立即变未就绪。
        self.adapter.c_streaming = False
        ready, reason = self.builder.check_ready()
        self.assertFalse(ready)
        self.assertIn("流已断开", reason)
        self.assertFalse(self.builder.poll_and_cache_readiness())

        # 重连：重新核对后恢复可用，无需重建备库。
        self.adapter.c_streaming = True
        self.assertTrue(self.builder.poll_and_cache_readiness())

    def test_not_ready_when_c_not_in_recovery(self) -> None:
        self.builder.start_build("B")
        self.adapter.c_replay_lsn = "0/3000"
        self.adapter.c_in_recovery = False
        ready, reason = self.builder.check_ready()
        self.assertFalse(ready)
        self.assertIn("recovery", reason)

    def test_stale_agent_task_is_ignored_until_idle(self) -> None:
        task_id = self.builder.start_build("B")
        # 代理还在跑旧任务（容器重启场景）：不允许串任务。
        self.adapter.agent_status = {"PHASE": "BUILDING", "TASK_ID": "older-task"}
        result = self.builder.run_once()
        self.assertIsNone(result)
        self.assertEqual(self.store.load().phase, BUILD_BUILDING)

    def test_no_status_file_waits(self) -> None:
        self.builder.start_build("B")
        self.adapter.agent_status = None
        self.assertIsNone(self.builder.run_once())
        self.assertEqual(self.store.load().phase, BUILD_BUILDING)

    def test_ready_check_without_any_build(self) -> None:
        ready, reason = self.builder.check_ready()
        self.assertFalse(ready)
        self.assertIn("没有建库受理记录", reason)


if __name__ == "__main__":
    unittest.main()
