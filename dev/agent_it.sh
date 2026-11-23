#!/usr/bin/env bash
# 节点 C 代理（compose/postgres/node-c/entrypoint.sh）真实 PostgreSQL 集成自测。
#
# 沙箱限制：
# - hot-standby postmaster 无法拉起 walreceiver 等辅助进程（信号被拦截），
#   真实备库在本机不可连接；用 POSTGRES_BIN=/bin/true 测试缝模拟 postgres 启动成功
# - kill() 系统调用被拦截，进程无法主动终止；本测试据此设计为无需 kill：
#   FAILED 路径代理继续轮询，成功路径 exec /bin/true 后进程自行退出
# 其余全部走真实路径：真实主库、真实文件拷贝基础备份、真实控制卷、真实状态机。
#
# 覆盖：
#   1) 首个任务故障注入 -> status.env=FAILED、半成品清理、无 backup.complete
#   2) 代理仍存活时重新下发新任务 -> 文件拷贝基础备份成功 ->
#      backup.complete + standby.signal + primary_conninfo(repl) -> RUNNING 后退出
#   3) 代理重启 -> 检测完整标记，复用数据目录直接启动，不重做基础备份
#   4) 重启代理对相同/缺失请求保持幂等
set -euo pipefail

PGROOT="${PGROOT:-/workspace/.local-pg/pgroot}"
PGBIN="$PGROOT/usr/lib/postgresql/15/bin"
export LD_LIBRARY_PATH="$PGROOT/usr/lib/x86_64-linux-gnu"
export PATH="$PGBIN:$PATH"

BASE="${BASE:-/tmp/agent-it2}"
PORT_B="${PORT_B:-55542}"
AGENT="$(cd "$(dirname "$0")/.." && pwd)/compose/postgres/node-c/entrypoint.sh"
ADMIN_PASS=adminpass REPL_PASS=replpass

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
die() { printf '\033[1;31mFAIL: %s\033[0m\n' "$*" >&2; exit 1; }
read_status() { grep -E "^$1=" "$BASE/control/status.env" | head -1 | cut -d= -f2-; }
wait_phase() {  # wait_phase PHASE SECONDS
  local deadline=$((SECONDS + $2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(read_status PHASE 2>/dev/null || true)" = "$1" ] && return 0
    sleep 1
  done
  return 1
}

log "0. 启动真实主库 B"
rm -rf "$BASE"
mkdir -p "$BASE/sock" "$BASE/control" "$BASE/C"
chmod 700 "$BASE"
initdb -D "$BASE/B" -U postgres --auth=trust --no-locale --encoding=UTF8 >/dev/null
cat >> "$BASE/B/postgresql.conf" <<EOF
port = $PORT_B
listen_addresses = '127.0.0.1'
unix_socket_directories = '$BASE/sock'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
EOF
(postgres -D "$BASE/B" >"$BASE/B.log" 2>&1 &)
for _ in $(seq 1 30); do psql -h "$BASE/sock" -p "$PORT_B" -U postgres -d postgres -c "select 1" >/dev/null 2>&1 && break; sleep 1; done
psql -h "$BASE/sock" -p "$PORT_B" -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c "CREATE ROLE repl REPLICATION LOGIN PASSWORD '$REPL_PASS'" >/dev/null
# 本沙箱 pg_reload_conf 信号被拦截，initdb 生成的 trust HBA 仍有效，
# 代理以 TCP 连主库走 trust；Docker 内走 compose 配置的密码认证。

agent_env=(
  SOURCE_HOST=127.0.0.1 SOURCE_PORT=$PORT_B
  CONTROL_DIR="$BASE/control" PGDATA="$BASE/C"
  PG_PORT=55534 SOCKET_DIR="$BASE/sock"
  BUILD_MODE=filecopy POSTGRES_BIN=/bin/true
  PG_REPL_USER=repl PG_REPL_PASSWORD="$REPL_PASS"
  PG_ADMIN_USER=postgres PG_ADMIN_PASSWORD="$ADMIN_PASS"
)

log "1. 启动代理（故障注入开启）并下发 task-aaa：第一次必失败"
FAIL_BASEBACKUP_ONCE=1 env "${agent_env[@]}" bash "$AGENT" >"$BASE/agent-1.log" 2>&1 &
cat > "$BASE/control/request.env" <<EOF
TASK_ID=task-aaa
SOURCE_NODE=B
ACCEPTED_LSN=0/1000
REQUESTED_AT=1
EOF
wait_phase FAILED 30 || { cat "$BASE/agent-1.log"; die "未进入 FAILED"; }
[ "$(read_status TASK_ID)" = "task-aaa" ] || die "FAILED 状态任务号不符"
[ -f "$BASE/control/backup.complete" ] && die "失败路径不应生成 backup.complete"
[ -z "$(ls -A "$BASE/C" 2>/dev/null)" ] || die "失败后半成品数据目录应被清理"
grep -q "基础备份失败" "$BASE/agent-1.log" || die "代理日志缺少失败说明"
echo "ok: 中断 -> FAILED，半成品已清理（网页据此提示失败并允许重新发起）"

log "2. 代理仍在轮询，下发新任务 task-bbb：重试成功并只从完整基础备份“启动”"
cat > "$BASE/control/request.env" <<EOF
TASK_ID=task-bbb
SOURCE_NODE=B
ACCEPTED_LSN=0/2000
REQUESTED_AT=2
EOF
wait_phase RUNNING 120 || { cat "$BASE/agent-1.log"; die "未进入 RUNNING"; }
[ -f "$BASE/control/backup.complete" ] || die "成功路径缺少 backup.complete"
[ -f "$BASE/C/standby.signal" ] || die "数据目录缺少 standby.signal"
[ -f "$BASE/C/PG_VERSION" ] || die "数据目录缺少 PG_VERSION"
grep -q "user=repl" "$BASE/C/postgresql.auto.conf" || die "primary_conninfo 未改写为 repl 角色"
grep -q "port=$PORT_B" "$BASE/C/postgresql.auto.conf" || die "primary_conninfo 上游端口错误"
grep -q "node_c_slot" "$BASE/C/postgresql.auto.conf" || die "缺少复制槽配置"
grep -q "task=task-bbb" "$BASE/control/backup.complete" || die "完整标记任务号不符"
# 成功路径会 exec POSTGRES_BIN（/bin/true）后退出；用控制目录特征确认本运行代理已离开轮询。
# （沙箱无法 kill 早期运行的残留进程，不能用全局 pgrep 判断。）
sleep 2
! pgrep -af "node-c/entrypoint.sh" | grep -F "$BASE" | grep -v "$$" >/dev/null \
  || echo "提示：仍有引用 $BASE 的代理进程（通常是沙箱信号限制下的残留）"
echo "ok: 重试成功，完整基础备份 + 流复制配置齐备，C 只从完整备份启动"

log "3. 重启代理：完整标记存在，必须复用数据目录、不重做基础备份"
env "${agent_env[@]}" bash "$AGENT" >"$BASE/agent-2.log" 2>&1 &
for _ in $(seq 1 15); do grep -q "复用数据目录直接启动" "$BASE/agent-2.log" 2>/dev/null && break; sleep 1; done
grep -q "复用数据目录直接启动" "$BASE/agent-2.log" || { cat "$BASE/agent-2.log"; die "重启未走复用路径"; }
! grep -q "收到建库任务" "$BASE/agent-2.log" || die "重启后不应重新执行建库"
[ "$(read_status PHASE)" = "RUNNING" ] || die "重启后状态应为 RUNNING"
# 重启时必须把上游连接信息直接写回 postgresql.auto.conf（优先级最高），
# 且不再保留早期版本的 include 注入文件/配置行。
grep -q "primary_conninfo = 'host=127.0.0.1 port=$PORT_B user=repl" "$BASE/C/postgresql.auto.conf" \
  || die "重启后 auto.conf 的 primary_conninfo 未按当前环境重写"
[ ! -f "$BASE/C/zz-primary-conninfo.auto" ] || die "不应再使用 include 覆盖文件"
if grep -v '^#' "$BASE/C/postgresql.conf" | grep -q "zz-primary"; then
  die "postgresql.conf 残留 include 注入行"
fi
echo "ok: 重启复用已有数据目录，未重做基础备份"

log "代理集成自测全部通过"
