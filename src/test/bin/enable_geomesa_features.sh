#!/bin/bash
# ==============================================================================
# Shell脚本：恢复 GeoMesa 分区架构特性 (v2 - 增强版)
#
# v2 更新:
# - 增加了对 "performance_wa" 表自身触发器的重新启用。
#
# 操作:
# 1. 在 "performance_wa" 表上重新创建必要的索引。
# 2. 重新创建视图 "performance" 上的 INSTEAD OF 触发器。
# 3. 重新启用 "performance_wa" 上的 BEFORE INSERT 触发器。
# 4. 重新激活之前暂停的 pg_cron 定时任务。
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
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始恢复 GeoMesa 分区架构特性..."

# 1. 重新创建 performance_wa 表上的索引
echo -e "\n=== 步骤 1: 重新创建表 performance_wa 上的索引 ==="
CREATE_INDEXES_SQL=$(cat <<'EOF'
-- 主键通常在建表时已定义，这里假设它存在
CREATE INDEX IF NOT EXISTS performance_wa_dtg_idx ON public.performance_wa (dtg);
CREATE INDEX IF NOT EXISTS performance_wa_geom_idx ON public.performance_wa USING GIST (geom);
CREATE INDEX IF NOT EXISTS performance_wa_taxi_id_idx ON public.performance_wa (taxi_id);
SELECT '已成功在 "performance_wa" 表上重新创建二级索引。';
EOF
)
run_psql "${CREATE_INDEXES_SQL}"

# 2. 重新创建视图上的触发器
echo -e "\n=== 步骤 2: 重新创建视图 performance 上的触发器 ==="
CREATE_VIEW_TRIGGERS_SQL=$(cat <<'EOF'
-- 确保触发器函数存在
CREATE OR REPLACE TRIGGER insert_to_performance_trigger
    INSTEAD OF INSERT ON public.performance
    FOR EACH ROW
    EXECUTE FUNCTION insert_to_performance();

CREATE OR REPLACE TRIGGER delete_from_performance_trigger
    INSTEAD OF DELETE ON public.performance
    FOR EACH ROW
    EXECUTE FUNCTION delete_from_performance();

CREATE OR REPLACE TRIGGER update_to_performance_trigger
    INSTEAD OF UPDATE ON public.performance
    FOR EACH ROW
    EXECUTE FUNCTION update_to_performance();

SELECT '已成功在视图 "performance" 上重新创建 INSTEAD OF 触发器。';
EOF
)
run_psql "${CREATE_VIEW_TRIGGERS_SQL}"

# 3. **[新增]** 重新启用 performance_wa 表上的触发器
echo -e "\n=== 步骤 3: 重新启用表 performance_wa 上的触发器 ==="
ENABLE_TABLE_TRIGGER_SQL=$(cat <<'EOF'
ALTER TABLE public.performance_wa ENABLE TRIGGER insert_to_wa_writes_performance_trigger;
SELECT '已成功重新启用表 "performance_wa" 上的 "insert_to_wa_writes_performance_trigger" 触发器。';
EOF
)
run_psql "${ENABLE_TABLE_TRIGGER_SQL}"

# 4. 重新激活 pg_cron 定时任务
echo -e "\n=== 步骤 4: 重新激活 pg_cron 任务 ==="
REACTIVATE_TASKS_SQL=$(cat <<'EOF'
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    UPDATE cron.job SET active = true WHERE command ILIKE '%performance%';
    RAISE NOTICE '已重新激活 % 个与 "performance" 相关的 cron 任务。', (SELECT count(*) FROM cron.job WHERE command ILIKE '%performance%' AND active = true);
  ELSE
    RAISE NOTICE '未找到 pg_cron 扩展，跳过。';
  END IF;
END
$$;
EOF
)
run_psql "${REACTIVATE_TASKS_SQL}"

echo -e "\n=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【成功】所有 GeoMesa 特性已恢复。"
echo "数据库现在已恢复正常运作。"
echo "=================================================="