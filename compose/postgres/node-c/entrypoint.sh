#!/bin/bash
# 节点 C 容器入口（代理）：
#
# 节点 C 不做任何自动建库，只在网页（web 容器）通过共享卷下发 request.env 后，
# 从当前写入主库 B 做一次完整的 pg_basebackup，随后以物理备库身份启动并接入
# B 的流复制。全程把阶段/进度/错误原子写入共享卷上的 status.env 供网页轮询。
#
# 关键语义（与需求逐条对应）：
# - C 使用独立数据卷（PGDATA），与 A/B 完全隔离
# - 仅当数据卷为空、或不存在“基础备份完整标记” backup.complete 时才做基础备份；
#   已建好的数据目录在重复点击、容器重启后直接复用，绝不重做
# - 基础备份中断（pg_basebackup 非零退出 / 故障注入）：清理半成品数据目录、
#   写 PHASE=FAILED，网页提示失败并可重新发起；C 不会从不完整备份启动
# - 只有 backup.complete 存在（即 pg_basebackup 正常结束）才启动 postgres
#
# 受限沙箱（无 Docker 的开发机）可用 BUILD_MODE=filecopy 走“复制槽+文件拷贝”
# 退化路径，逻辑与 dev/local_cluster.sh 一致；容器内默认标准 pg_basebackup。
#
# 故障注入（仅自测用）：FAIL_BASEBACKUP_ONCE=1 时第一次基础备份必失败。
set -uo pipefail

DATA_DIR="${PGDATA:-/var/lib/postgresql/data}"
CONTROL_DIR="${CONTROL_DIR:-/data/c-control}"
REQUEST_FILE="$CONTROL_DIR/request.env"
STATUS_FILE="$CONTROL_DIR/status.env"
# 非 root 自测时允许覆盖 socket 目录；容器内保持默认（/var/run/postgresql）。
SOCKET_DIR="${SOCKET_DIR:-}"

: "${SOURCE_HOST:?missing SOURCE_HOST}"
: "${PG_REPL_USER:?missing PG_REPL_USER}"
: "${PG_REPL_PASSWORD:?missing PG_REPL_PASSWORD}"
: "${PG_ADMIN_USER:?missing PG_ADMIN_USER}"
: "${PG_ADMIN_PASSWORD:?missing PG_ADMIN_PASSWORD}"

BUILD_MODE="${BUILD_MODE:-basebackup}"
PORT="${PG_PORT:-5432}"              # C 本地监听端口
SOURCE_PORT="${SOURCE_PORT:-5432}"   # 上游 B 的连接端口
# 测试缝：默认就是 postgres；离线自测可指向 /bin/true 模拟“启动成功”。
POSTGRES_BIN="${POSTGRES_BIN:-postgres}"
FAIL_MARK="$CONTROL_DIR/.fail-injected"

mkdir -p "$CONTROL_DIR" "$DATA_DIR"
if [ "$(id -u)" = "0" ]; then
  chown -R postgres:postgres /var/lib/postgresql "$CONTROL_DIR"
  chmod 0700 "$DATA_DIR"
else
  # 非 root（无 Docker 的开发机自测）：当前用户必须拥有数据/控制目录。
  chmod 0700 "$DATA_DIR" 2>/dev/null || true
fi

# --- 状态文件原子写：先写临时文件再 mv，网页永远读到完整内容 --- #
write_status() {
  local tmp="$STATUS_FILE.tmp.$$"
  {
    echo "PHASE=$1"
    [ -n "${2:-}" ] && echo "TASK_ID=$2"
    [ -n "${3:-}" ] && echo "STAGE=$3"
    [ -n "${4:-}" ] && echo "PROGRESS=$4"
    [ -n "${5:-}" ] && echo "ERROR=$5"
    echo "UPDATED_AT=$(date +%s)"
  } > "$tmp"
  mv "$tmp" "$STATUS_FILE"
}

read_request_value() {  # read_request_value KEY
  [ -f "$REQUEST_FILE" ] || return 1
  local line
  line=$(grep -E "^$1=" "$REQUEST_FILE" | head -1) || return 1
  printf '%s' "${line#*=}"
}

reset_data_dir() {
  # 中断后清理半成品；backup.complete 在数据目录之外，成功路径不会被删。
  rm -rf "$DATA_DIR"/* "$DATA_DIR"/.[!.]* 2>/dev/null || true
  if [ "$(id -u)" = "0" ]; then
    chown -R postgres:postgres "$DATA_DIR"
    chmod 0700 "$DATA_DIR"
  fi
}

run_as_postgres() {
  if [ "$(id -u)" = "0" ]; then gosu postgres "$@"; else "$@"; fi
}

# ---------------------------------------------------------------------------
# 启动路径一：数据卷里已有完整基础备份 → 直接启动，不重做（重启/重复点击复用）
# ---------------------------------------------------------------------------
if [ -f "$CONTROL_DIR/backup.complete" ] && [ -s "$DATA_DIR/PG_VERSION" ]; then
  echo "node-c: 完整基础备份已存在，复用数据目录直接启动流复制备库"
  write_status RUNNING "$(read_request_value TASK_ID 2>/dev/null || echo reused)" "starting" "" ""
  # 容器重启后若上游连接信息变化，以当前环境变量为准重写 primary_conninfo。
  # 注意：postgresql.auto.conf 优先级高于 postgresql.conf 里的 include，
  # 因此必须直接替换 auto.conf 中的 primary_conninfo 行，不能用 include 覆盖。
  new_conninfo="primary_conninfo = 'host=${SOURCE_HOST} port=${SOURCE_PORT} user=${PG_REPL_USER} password=${PG_REPL_PASSWORD} application_name=node-c'"
  if grep -q "^primary_conninfo" "$DATA_DIR/postgresql.auto.conf" 2>/dev/null; then
    # 用 # 作 sed 分隔符，避免连接串里的 / 等字符干扰。
    sed -i "s#^primary_conninfo.*#${new_conninfo}#" "$DATA_DIR/postgresql.auto.conf"
  else
    echo "$new_conninfo" >> "$DATA_DIR/postgresql.auto.conf"
  fi
  [ -f "$DATA_DIR/zz-primary-conninfo.auto" ] && rm -f "$DATA_DIR/zz-primary-conninfo.auto"
  # 清理早期版本可能留下的 include 行（目标文件已删除，留着只是噪音）。
  if [ -f "$DATA_DIR/postgresql.conf" ]; then
    sed -i "\#zz-primary-conninfo.auto#d" "$DATA_DIR/postgresql.conf"
  fi
  if [ "$(id -u)" = "0" ]; then
    chown postgres:postgres "$DATA_DIR/postgresql.auto.conf"
  fi
  # 端口用 -c 命令行指定：文件拷贝退化路径里数据目录带着上游的 postgresql.conf，
  # 命令行参数优先级更高，保证 C 监听自己的端口。
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres "$POSTGRES_BIN" -D "$DATA_DIR" -c port="$PORT" ${SOCKET_DIR:+-c unix_socket_directories="$SOCKET_DIR"}
  fi
  exec "$POSTGRES_BIN" -D "$DATA_DIR" -c port="$PORT" ${SOCKET_DIR:+-c unix_socket_directories="$SOCKET_DIR"}
fi

# 数据目录存在残留但没有完整标记：说明上次中断，清掉半成品等待重新发起。
if [ -n "$(ls -A "$DATA_DIR" 2>/dev/null)" ]; then
  echo "node-c: 发现无完整标记的残留数据目录，按中断半成品清理"
  reset_data_dir
fi

write_status IDLE "" "waiting-request" "" ""
echo "node-c: 代理就绪，等待网页下发建库请求（$REQUEST_FILE）"

last_task=""
# ---------------------------------------------------------------------------
# 主循环：轮询 request.env，任务号变化才执行一次建库
# ---------------------------------------------------------------------------
while true; do
  task_id="$(read_request_value TASK_ID 2>/dev/null || true)"
  if [ -z "$task_id" ] || [ "$task_id" = "$last_task" ]; then
    sleep 1
    continue
  fi

  accepted_lsn="$(read_request_value ACCEPTED_LSN 2>/dev/null || echo unknown)"
  echo "node-c: 收到建库任务 $task_id（受理 LSN=$accepted_lsn，模式=$BUILD_MODE）"
  write_status BUILDING "$task_id" "basebackup" "0%" ""
  reset_data_dir

  fail_injected=0
  if [ "${FAIL_BASEBACKUP_ONCE:-0}" = "1" ] && [ ! -f "$FAIL_MARK" ]; then
    touch "$FAIL_MARK"
    fail_injected=1
  fi

  backup_ok=0
  if [ "$fail_injected" = "0" ] && [ "$BUILD_MODE" = "basebackup" ]; then
    # 标准路径：pg_basebackup -Xs -R（-R 生成 standby.signal 与 primary_conninfo）
    if run_as_postgres pg_basebackup \
         -h "$SOURCE_HOST" -p "$SOURCE_PORT" -U "$PG_ADMIN_USER" \
         -D "$DATA_DIR" -Fp -Xs -R; then
      backup_ok=1
    fi
  elif [ "$fail_injected" = "0" ]; then
    # 受限沙箱退化路径：先尝试标准 pg_basebackup（部分环境 repl 角色即可），
    # 信号被拦截失败时，再退化为“物理复制槽 + 运行中文件拷贝 + 崩溃恢复”，
    # 与 dev/local_cluster.sh 的自测降级方式保持一致。
    slot="node_c_slot"
    if run_as_postgres pg_basebackup \
         -h "$SOURCE_HOST" -p "$SOURCE_PORT" -U "$PG_REPL_USER" \
         -D "$DATA_DIR" -Fp -Xs -R; then
      backup_ok=1
    else
      echo "node-c: pg_basebackup 不可用，退化到复制槽 + 数据目录文件拷贝"
      reset_data_dir
      PGPASSWORD="$PG_ADMIN_PASSWORD" run_as_postgres psql \
        -h "$SOURCE_HOST" -p "$SOURCE_PORT" -U "$PG_ADMIN_USER" -d postgres -tAc \
        "SELECT pg_create_physical_replication_slot('$slot')
         WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots
                           WHERE slot_name='$slot')" >/dev/null
      src_data="$(PGPASSWORD="$PG_ADMIN_PASSWORD" run_as_postgres psql \
        -h "$SOURCE_HOST" -p "$SOURCE_PORT" -U "$PG_ADMIN_USER" -d postgres -tAc \
        'SHOW data_directory' 2>/dev/null | tr -d '[:space:]')"
      if [ -n "$src_data" ] && [ -d "$src_data" ] && \
         run_as_postgres cp -a "$src_data/." "$DATA_DIR/"; then
        rm -f "$DATA_DIR/postmaster.pid"
        touch "$DATA_DIR/standby.signal"
        cat > "$DATA_DIR/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=${SOURCE_HOST} port=${SOURCE_PORT} user=${PG_REPL_USER} password=${PG_REPL_PASSWORD} application_name=node-c'
primary_slot_name = '${slot}'
EOF
        backup_ok=1
      fi
    fi
  fi

  if [ "$fail_injected" = "1" ] || [ "$backup_ok" != "1" ]; then
    # 基础备份中断：网页提示失败，可重新发起；绝不让 C 从不完整备份启动。
    write_status FAILED "$task_id" "basebackup" "" \
      "基础备份未完成（fail_injected=$fail_injected），已清理半成品数据目录，可重新发起建库"
    echo "node-c: 任务 $task_id 基础备份失败，半成品已清理"
    reset_data_dir
    rm -f "$CONTROL_DIR/backup.complete"
    last_task="$task_id"
    continue
  fi

  chmod 0700 "$DATA_DIR" 2>/dev/null || true
  # 把 -R 生成的管理员连接信息改写为最小权限的流复制角色。
  if [ -f "$DATA_DIR/postgresql.auto.conf" ]; then
    sed -i "s|user=${PG_ADMIN_USER}|user=${PG_REPL_USER}|; s|password=${PG_ADMIN_PASSWORD}|password=${PG_REPL_PASSWORD}|" \
      "$DATA_DIR/postgresql.auto.conf"
  fi
  # 完整标记：只有走到这里才存在，作为“允许启动 postgres”的唯一凭据。
  echo "task=$task_id finished_at=$(date +%s) accepted_lsn=$accepted_lsn" \
    > "$CONTROL_DIR/backup.complete"
  write_status RUNNING "$task_id" "starting" "100%" ""
  echo "node-c: 任务 $task_id 基础备份完整，启动流复制备库"
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres "$POSTGRES_BIN" -D "$DATA_DIR" -c port="$PORT" ${SOCKET_DIR:+-c unix_socket_directories="$SOCKET_DIR"}
  fi
  exec "$POSTGRES_BIN" -D "$DATA_DIR" -c port="$PORT" ${SOCKET_DIR:+-c unix_socket_directories="$SOCKET_DIR"}
done
