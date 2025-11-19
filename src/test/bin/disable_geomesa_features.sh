#!/bin/bash
# ==============================================================================
# Shell脚本：禁用 GeoMesa 分区架构特性以进行高速批量导入 (v2 - 增强版)
#
# v2 更新:
# - 增加了对 "performance_wa" 表自身触发器的禁用，以获得最大写入性能。
#
# 操作:
# 1. 暂停所有与 "performance" 相关的 pg_cron 定时任务。
# 2. 删除视图 "performance" 上的 INSTEAD OF 触发器。
# 3. 禁用写入缓冲区表 "performance_wa" 上的 BEFORE INSERT 触发器。
# 4. 删除写入缓冲区表 "performance_wa" 上的所有索引。
# ==============================================================================

set -e

# --- 配置区 ---
CONTAINER_NAME="my-postgis-container"
DB_USER="postgres"
DB_NAME="postgres"
DB_PASSWD="ds123456"

# --- 函数定义 ---
run_psql() {
  local sql_command="$1"
  echo "--------------------------------------------------"
  echo "$(date '+%Y-%m-%d %H:%M:%S') - 执行SQL命令..."
  echo "${sql_command}"
  echo "--------------------------------------------------"
  docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
    psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${sql_command}"
}

# --- 主逻辑 ---

echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始禁用 GeoMesa 分区架构特性..."

# 1. 暂停 pg_cron 定时任务
echo -e "\n=== 步骤 1: 暂停 pg_cron 任务 ==="
PAUSE_TASKS_SQL=$(cat <<'EOF'
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    UPDATE cron.job SET active = false WHERE command ILIKE '%performance%';
    RAISE NOTICE '已暂停 % 个与 "performance" 相关的 cron 任务。', (SELECT count(*) FROM cron.job WHERE command ILIKE '%performance%' AND active = false);
  ELSE
    RAISE NOTICE '未找到 pg_cron 扩展，跳过。';
  END IF;
END
$$;
EOF
)
run_psql "${PAUSE_TASKS_SQL}"

# 2. 删除视图上的触发器
echo -e "\n=== 步骤 2: 删除视图 performance 上的触发器 ==="
DROP_VIEW_TRIGGERS_SQL=$(cat <<'EOF'
DROP TRIGGER IF EXISTS delete_from_performance_trigger ON public.performance;
DROP TRIGGER IF EXISTS insert_to_performance_trigger ON public.performance;
DROP TRIGGER IF EXISTS update_to_performance_trigger ON public.performance;
SELECT '已成功删除视图 "performance" 上的 INSTEAD OF 触发器。';
EOF
)
run_psql "${DROP_VIEW_TRIGGERS_SQL}"

# 3. **[新增]** 禁用 performance_wa 表上的触发器
# 我们使用 ALTER TABLE ... DISABLE TRIGGER 来禁用它，而不是删除，这样恢复时更方便。
echo -e "\n=== 步骤 3: 禁用表 performance_wa 上的触发器 ==="
DISABLE_TABLE_TRIGGER_SQL=$(cat <<'EOF'
ALTER TABLE public.performance_wa DISABLE TRIGGER insert_to_wa_writes_performance_trigger;
SELECT '已成功禁用表 "performance_wa" 上的 "insert_to_wa_writes_performance_trigger" 触发器。';
EOF
)
run_psql "${DISABLE_TABLE_TRIGGER_SQL}"

# 4. 删除 performance_wa 表上的索引
echo -e "\n=== 步骤 4: 删除表 performance_wa 上的所有索引 ==="
DROP_INDEXES_SQL=$(cat <<'EOF'
DO $$
DECLARE
    index_name TEXT;
    index_count INT := 0;
BEGIN
    RAISE NOTICE '开始删除表 public.performance_wa 上的所有索引...';
    FOR index_name IN
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'performance_wa'
    LOOP
        RAISE NOTICE '  -> 正在删除索引: %', index_name;
        EXECUTE format('DROP INDEX IF EXISTS %I;', index_name);
        index_count := index_count + 1;
    END LOOP;
    RAISE NOTICE '操作完成，共删除了 % 个索引。', index_count;
END;
$$;
EOF
)
run_psql "${DROP_INDEXES_SQL}"

echo -e "\n=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【成功】所有 GeoMesa 特性已禁用。"
echo "数据库现在已为高速批量导入准备就绪。"
echo "=================================================="