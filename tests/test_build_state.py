"""建库状态机测试：原子持久化、幂等受理、失败重试、就绪归档。"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from app.build_state import (
    BUILD_BUILDING,
    BUILD_FAILED,
    BUILD_IDLE,
    BuildStateStore,
)


class BuildStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "build.json")
        self.store = BuildStateStore(self.path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_load_missing_file_returns_idle(self) -> None:
        state = self.store.load()
        self.assertEqual(state.phase, BUILD_IDLE)
        self.assertIsNone(state.current)

    def test_begin_build_persists_accepted_lsn(self) -> None:
        state = self.store.begin_build("task-1", "0/ABCD")
        self.assertEqual(state.phase, BUILD_BUILDING)
        self.assertEqual(state.current.task_id, "task-1")
        self.assertEqual(state.current.accepted_lsn, "0/ABCD")
        # 文件内容为完整 JSON（原子落盘）。
        with open(self.path, encoding="utf-8") as fh:
            raw = json.load(fh)
        self.assertEqual(raw["current"]["task_id"], "task-1")

    def test_duplicate_begin_while_building_returns_same_task(self) -> None:
        first = self.store.begin_build("task-1", "0/1000")
        second = self.store.begin_build("task-2", "0/2000")  # 进行中重复点击
        self.assertEqual(second.phase, BUILD_BUILDING)
        self.assertEqual(second.current.task_id, "task-1")
        self.assertEqual(first.current.task_id, second.current.task_id)
        self.assertEqual(second.current.accepted_lsn, "0/1000")

    def test_failure_then_retry_creates_new_task_and_keeps_history(self) -> None:
        self.store.begin_build("task-1", "0/1000")
        self.store.mark_failed("基础备份中断")
        self.assertEqual(self.store.load().phase, BUILD_FAILED)

        again = self.store.begin_build("task-2", "0/2000")
        self.assertEqual(again.phase, BUILD_BUILDING)
        self.assertEqual(again.current.task_id, "task-2")
        # 失败任务归档进历史，可追溯。
        self.assertEqual(len(again.history), 1)
        self.assertEqual(again.history[0].task_id, "task-1")
        self.assertEqual(again.history[0].message, "基础备份中断")

    def test_ready_archives_task_and_returns_idle(self) -> None:
        self.store.begin_build("task-1", "0/1000")
        state = self.store.mark_idle_after_ready("C 已就绪")
        self.assertEqual(state.phase, BUILD_IDLE)
        self.assertIsNone(state.current)
        self.assertEqual(state.history[-1].result, "READY")

    def test_readiness_cache_flips_on_disconnect(self) -> None:
        self.store.begin_build("task-1", "0/1000")
        self.store.mark_idle_after_ready("ok")
        self.store.update_readiness(True, None)
        self.assertTrue(self.store.load().ready)
        self.store.update_readiness(False, "C 未在从 B 流复制（流已断开）")
        state = self.store.load()
        self.assertFalse(state.ready)
        self.assertIn("流已断开", state.last_not_ready_reason)


if __name__ == "__main__":
    unittest.main()
