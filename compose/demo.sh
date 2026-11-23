#!/usr/bin/env bash
# 端到端演示脚本：
#   1) 写入/查询示例记录
#   2) 暂停备库回放 -> 发起切换 -> 等待超时回滚（演示失败路径）
#   3) 恢复回放 -> 重新切换成功（演示成功路径）
#   4) 校验旧记录在新主可查、新写入只走新主
#   5) 重启 web 容器，校验重启后写入仍只走新主
# 用法: ./demo.sh [BASE_URL]
set -euo pipefail

BASE="${1:-http://localhost:8000}"
command -v curl >/dev/null || { echo "需要 curl"; exit 1; }
command -v jq >/dev/null || { echo "需要 jq"; exit 1; }

api() {  # api METHOD PATH [JSON_BODY]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" -H 'Content-Type: application/json' \
      -d "$body" "$BASE$path"
  else
    curl -fsS -X "$method" "$BASE$path"
  fi
}
wait_phase() {  # wait_phase PHASE [TIMEOUT]
  local want="$1" timeout="${2:-40}" deadline=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local phase
    phase=$(api GET /api/status | jq -r '.cluster.phase')
    [ "$phase" = "$want" ] && return 0
    sleep 1
  done
  echo "等待阶段 $want 超时" >&2
  api GET /api/status | jq . >&2
  exit 1
}
slog() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

slog "0. 初始状态"
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, nodes: [.nodes[] | {name, role, replay_paused}]}'

slog "1. 在旧主 A 写入两条已确认记录"
api POST /api/records '{"content":"record-before-switch-1"}' | jq .
api POST /api/records '{"content":"record-before-switch-2"}' | jq .
api GET '/api/records?node=A' | jq '{node, count}'

slog "2. 暂停备库 B 的 WAL 回放（制造追不上）"
api POST /api/replay/pause '{"node":"B"}' | jq .

slog "3. 发起切换（20s 内回放追不上 -> 超时回滚）"
api POST /api/switch '{"target":"B","timeout":20}' | jq .
slog "3a. 切换受理后新写入应被拒绝（期望 HTTP 503）"
curl -sS -o /tmp/switch-503.json -w 'HTTP %{http_code}\n' \
  -X POST -H 'Content-Type: application/json' \
  -d '{"content":"should-be-rejected"}' "$BASE/api/records"
jq . /tmp/switch-503.json
wait_phase NORMAL 35
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, last: .cluster.history[-1].result}'

slog "4. 恢复回放，重新发起切换 -> 成功"
api POST /api/replay/resume '{"node":"B"}' | jq .
api POST /api/switch '{"target":"B","timeout":20}' | jq .
wait_phase NORMAL 40
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, last: .cluster.history[-1]}'

slog "5. 校验：旧记录在新主 B 可查，新写入只走 B"
api GET '/api/records?node=B' | jq '{node, count, contents: [.records[].content]}'
for c in record-before-switch-1 record-before-switch-2; do
  api GET '/api/records?node=B' | jq -e --arg c "$c" '.records[] | select(.content==$c)' >/dev/null \
    && echo "OK: $c 在新主 B 可查"
done
api POST /api/records '{"content":"record-after-promotion"}' | jq .

slog "6. 重启网页服务，验证重启后写入仍只走新主 B"
docker compose restart web >/dev/null
sleep 3
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader}'
api POST /api/records '{"content":"record-after-web-restart"}' | jq .
api GET /api/records | jq '{node, latest: .records[0].content}'

slog "7. 从 B 给节点 C 建物理备库（建库期间 B 照常接收录入）"
# 受理前校验：当前主库必须是 B，否则接口 409。
api POST /api/standby/c/build | jq '{accepted, idempotent, task_id}'
# 建库进行中重复点击：必须返回同一个任务号（幂等）。
TASK_ID=$(api GET /api/status | jq -r '.standby_build.current_build.task_id')
api POST /api/standby/c/build | jq -e --arg t "$TASK_ID" '.idempotent==true and .task_id==$t' >/dev/null \
  && echo "OK: 重复点击返回同一任务 $TASK_ID"
# 建库期间继续往 B 录入，写入不中断。
api POST /api/records '{"content":"record-during-c-build"}' | jq '{id, node, content}'

# 轮询直到建库任务归档（IDLE），再核对实时可用性。
deadline=$((SECONDS + 120))
while [ "$SECONDS" -lt "$deadline" ]; do
  phase=$(api GET /api/status | jq -r '.standby_build.phase')
  [ "$phase" = "FAILED" ] && { api GET /api/status | jq .standby_build >&2; echo "建库失败" >&2; exit 1; }
  [ "$phase" = "IDLE" ] && break
  sleep 2
done
[ "$phase" = "IDLE" ] || { echo "等待 C 建库完成超时" >&2; exit 1; }

# 备库可用 = C 正从 B 流复制 且 回放达到受理时 B 的 WAL 位置（每次现查）。
for _ in $(seq 1 30); do
  ready=$(api GET /api/standby/c | jq -r '.ready')
  [ "$ready" = "true" ] && break
  sleep 2
done
api GET /api/standby/c | jq '{ready, reason, build_phase: .build.phase, node: {name: .node.name, role: .node.role, streaming: .node.streaming}}'
[ "$ready" = "true" ] || { echo "C 未达到可用条件" >&2; exit 1; }

slog "8. C 可用后再录入一条，并从 C 查到它"
FINAL="record-on-c-$RANDOM"
api POST /api/records "{\"content\":\"$FINAL\"}" | jq .
for _ in $(seq 1 30); do
  api GET '/api/records?node=C&limit=200' | jq -e --arg c "$FINAL" '.records[]|select(.content==$c)' >/dev/null && break
  sleep 2
done
api GET '/api/records?node=C&limit=200' | jq -e --arg c "$FINAL" '.records[]|select(.content==$c)' >/dev/null \
  && echo "OK: 记录 $FINAL 已从 C 查到"
api GET '/api/records?node=C&limit=200' \
  | jq -e '.records[]|select(.content=="record-during-c-build")' >/dev/null \
  && echo "OK: 建库期间的录入也已流到 C"

slog "全部演示步骤完成（含节点 C 物理备库重建）"
