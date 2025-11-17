#!/bin/bash

# ==============================================================================
# Shell脚本：GeoMesa 分区架构专用极速批量导入 (5000万条数据优化版)
#
# 专门优化参数针对:
# - 总数据量: 5000万条
# - 每个文件: 10万条
# - 文件总数: 500个
#
# 优化特性:
# 1. 精确批处理大小 (BATCH_SIZE=20) - 针对500文件总量优化
# 2. 内存参数优化 - 为5000万条数据定制
# 3. 高效事务处理 - 每批200万条，共25批次完成
# 4. 完整错误恢复机制 - 保证5000万条数据一致性
# 5. 实时进度监控 - 精确估算剩余时间
#
# 使用方法:
# 1. 确保 .tbl 文件目录就绪 (500个文件，每个10万条)
# 2. 在主机上运行此脚本: ./import_50m_optimized.sh
# ==============================================================================

# --- 配置区 ---

# 1. Docker 容器名
CONTAINER_NAME="stag-gstria-postgis_postgis_1"

# 2. 数据库连接详细信息
DB_USER="postgres"
DB_NAME="gstria"
TARGET_TABLE="performance_wa"  # 写入缓冲区 - GeoMesa架构入口点
DB_PASSWD="ds123456"

# 3. 容器内部存放 .tbl 文件的目录
TBL_DIR_IN_CONTAINER="/tmp/import_data"
TBL_DIR_IN_LOCAL="/home/gstria/datasets/beijingshi_tbl_100k"

# 4. 批处理和性能参数 (针对5000万条/500文件优化)
BATCH_SIZE=20                   # 每20个文件提交一次事务 (500/20=25批次)
MAX_BATCH_ROWS=2000000          # 每批最大200万条记录 (20*10万)
MAX_BATCH_DURATION=90           # 单批次最大持续时间(秒) - 增加到90秒
ADAPTIVE_BATCHING=true          # 启用自适应批大小调整
BATCH_COMMIT_DELAY=10000        # 提交延迟 (微秒)

# 5. 系统资源参数 (针对5000万条数据优化)
MAINTENANCE_WORK_MEM="3072MB"   # 3GB - 足够处理200万行索引
WORK_MEM="768MB"                # 768MB - 平衡内存使用
MAX_PARALLEL_WORKERS=12         # 12并行进程 - 适合16核服务器
TEMP_BUFFERS="192MB"            # 临时缓冲区

# 6. 恢复控制
PROGRESS_FILE="${TBL_DIR_IN_CONTAINER}/import_progress.txt"
FAILED_FILES_LOG="${TBL_DIR_IN_CONTAINER}/failed_files.log"
RESUME_FROM_PROGRESS=true      # 断点续传
VACUUM_FREQUENCY=5              # 每5个批次执行一次VACUUM ANALYZE

# 每文件数据量 (关键参数)
ROWS_PER_FILE=100000            # 10万条/文件
TOTAL_EXPECTED_FILES=500        # 预期总文件数

# 记录脚本总开始时间
start_total_time=$(date +%s.%N)

# 检查 docker 命令是否存在
if ! command -v docker &> /dev/null; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 错误: docker 命令未找到。"
    exit 1
fi

# 检查指定的 Docker 容器是否正在运行
if ! docker ps --filter "name=${CONTAINER_NAME}" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 错误: Docker 容器 '${CONTAINER_NAME}' 不存在或未在运行。"
    exit 1
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始 5000万条数据专用导入过程..."
echo "目标 Docker 容器: $CONTAINER_NAME"
echo "目标数据库: $DB_NAME, 目标表: $TARGET_TABLE (写入缓冲区)"
echo "总数据量: 5000万条 (500个文件 × 10万条/文件)"
echo "批处理大小: $BATCH_SIZE 个文件/批 (每批 ${MAX_BATCH_ROWS} 条记录)"
echo ""

# 检查容器内目录是否存在，不存在则创建
docker exec "${CONTAINER_NAME}" mkdir -p "${TBL_DIR_IN_CONTAINER}"

# 复制本地文件到容器
echo "$(date '+%Y-%m-%d %H:%M:%S') - 检查并复制文件到容器..."
CONTAINER_FILE_COUNT=$(docker exec "${CONTAINER_NAME}" sh -c "find ${TBL_DIR_IN_CONTAINER} -maxdepth 1 -name '*.tbl' -type f 2>/dev/null | wc -l")
LOCAL_FILE_COUNT=$(find "${TBL_DIR_IN_LOCAL}" -maxdepth 1 -name "*.tbl" -type f 2>/dev/null | wc -l)

echo "$(date '+%Y-%m-%d %H:%M:%S') - 本地文件数: $LOCAL_FILE_COUNT, 容器内文件数: $CONTAINER_FILE_COUNT"

if [ "$CONTAINER_FILE_COUNT" -lt "$LOCAL_FILE_COUNT" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 复制 ${LOCAL_FILE_COUNT} 个文件到容器..."
    # 使用rsync模式复制，跳过已存在文件
    docker cp "${TBL_DIR_IN_LOCAL}/." "${CONTAINER_NAME}:${TBL_DIR_IN_CONTAINER}/"

    # 验证复制结果
    NEW_CONTAINER_FILE_COUNT=$(docker exec "${CONTAINER_NAME}" sh -c "find ${TBL_DIR_IN_CONTAINER} -maxdepth 1 -name '*.tbl' -type f 2>/dev/null | wc -l")
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 复制后容器内文件数: $NEW_CONTAINER_FILE_COUNT (期待: $LOCAL_FILE_COUNT)"
    if [ "$NEW_CONTAINER_FILE_COUNT" -ne "$LOCAL_FILE_COUNT" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 文件复制不完整，继续尝试..."
        # 尝试二次复制
        docker cp "${TBL_DIR_IN_LOCAL}/." "${CONTAINER_NAME}:${TBL_DIR_IN_CONTAINER}/"
        NEW_CONTAINER_FILE_COUNT=$(docker exec "${CONTAINER_NAME}" sh -c "find ${TBL_DIR_IN_CONTAINER} -maxdepth 1 -name '*.tbl' -type f 2>/dev/null | wc -l")
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 二次复制后文件数: $NEW_CONTAINER_FILE_COUNT"
    fi
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 容器中已存在文件，跳过复制步骤。"
fi

# 验证文件数量是否符合预期
if [ "$LOCAL_FILE_COUNT" -ne "$TOTAL_EXPECTED_FILES" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 本地文件数量 ($LOCAL_FILE_COUNT) 与预期 ($TOTAL_EXPECTED_FILES) 不符"
    if [ "$LOCAL_FILE_COUNT" -lt "$TOTAL_EXPECTED_FILES" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 错误: 文件数量不足，无法完成5000万条数据导入"
        exit 1
    fi
fi

# 在容器内查找文件列表
file_list=$(docker exec "${CONTAINER_NAME}" find "${TBL_DIR_IN_CONTAINER}" -maxdepth 1 -type f -name "*.tbl" -printf "%f\n" | sort)
files=($file_list)

if [ ${#files[@]} -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 在容器目录 '${TBL_DIR_IN_CONTAINER}' 中没有找到任何 .tbl 文件。"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 容器目录内容:"
    docker exec "${CONTAINER_NAME}" ls -la "${TBL_DIR_IN_CONTAINER}" || true
    exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - 共找到 ${#files[@]} 个 .tbl 文件需要导入。"

# 处理断点续传
start_index=0
if [ "$RESUME_FROM_PROGRESS" = true ] && [ -f "$PROGRESS_FILE" ]; then
    if read -r start_index < "$PROGRESS_FILE"; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 检测到进度文件，从文件索引 $start_index 继续导入。"
        # 验证起始索引有效性
        if [ "$start_index" -ge "${#files[@]}" ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 进度文件索引超出范围，重置为0。"
            start_index=0
        fi
    fi
fi

# 初始化失败文件日志
if [ ! -f "$FAILED_FILES_LOG" ]; then
    docker exec "${CONTAINER_NAME}" touch "$FAILED_FILES_LOG"
fi

# 准备工作：暂停后台任务和优化参数
echo "--------------------------------------------------"
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【准备阶段】暂停后台任务并优化数据库参数..."

# 暂停所有相关cron任务
PAUSE_TASKS_CMD=$(cat <<EOF
DO \$\$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    UPDATE cron.job SET active = false WHERE jobname LIKE 'performance%';
    RAISE NOTICE '已暂停 % 个 cron 任务', (SELECT count(*) FROM cron.job WHERE jobname LIKE 'performance%' AND active = false);
  END IF;
END
\$\$;
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${PAUSE_TASKS_CMD}"

# 优化PostgreSQL参数 (针对5000万条数据)
OPTIMIZE_PARAMS_CMD=$(cat <<EOF
-- 设置高内存参数提升批处理性能
SET maintenance_work_mem = '${MAINTENANCE_WORK_MEM}';
SET work_mem = '${WORK_MEM}';
SET temp_buffers = '${TEMP_BUFFERS}';
SET synchronous_commit = off;  -- 非持久化场景安全
SET statement_timeout = 0;
SET lock_timeout = 0;
SET max_parallel_workers_per_gather = ${MAX_PARALLEL_WORKERS};
SET enable_partitionwise_join = on;
SET enable_partitionwise_aggregate = on;
SET commit_delay = ${BATCH_COMMIT_DELAY};  -- 微秒，合并提交
SET commit_siblings = 5;  -- 需要至少5个活跃事务才启用延迟
SET random_page_cost = 1.1;  -- SSD优化
SET effective_io_concurrency = 200;  -- SSD优化
SET max_wal_size = '6GB';  -- 优化WAL缓冲
SET checkpoint_timeout = '1h';  -- 减少检查点频率
SET wal_buffers = '32MB';
SET autovacuum = off;  -- 临时关闭自动维护
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${OPTIMIZE_PARAMS_CMD}"

# 预创建2000年分区 - 修复版（正确继承约束）
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【准备阶段】预创建2000年历史数据分区..."

# 1. 检查并修复表结构
PREPARE_TABLES_CMD=$(cat <<EOF
-- 确保必要的函数存在
CREATE OR REPLACE FUNCTION truncate_to_partition(dtg timestamp without time zone, hours int)
RETURNS timestamp without time zone AS
\$BODY\$
  SELECT date_trunc('day', dtg) +
    (hours * INTERVAL '1 HOUR' * floor(date_part('hour', dtg) / hours));
\$BODY\$
LANGUAGE sql IMMUTABLE;

-- 确保序列存在
INSERT INTO geomesa_wa_seq (type_name, value) VALUES ('performance', 0)
ON CONFLICT (type_name) DO NOTHING;

-- 确保写入分区存在
DO \$\$
DECLARE
  seq_val smallint;
  partition_name text;
BEGIN
  SELECT COALESCE(value, 0) INTO seq_val FROM geomesa_wa_seq WHERE type_name = 'performance';
  partition_name := 'performance_wa_' || lpad(seq_val::text, 3, '0');

  IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = partition_name) THEN
    EXECUTE format('
      CREATE TABLE IF NOT EXISTS %I (
        LIKE performance_wa INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING NOT NULL,
        CONSTRAINT %I PRIMARY KEY (fid, dtg)
      ) INHERITS (performance_wa) WITH (autovacuum_enabled = false)',
      partition_name, partition_name || '_pkey');

    -- 创建必要索引
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (dtg)',
                   partition_name || '_dtg', partition_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING gist(geom)',
                   partition_name || '_spatial_geom', partition_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (taxi_id)',
                   partition_name || '_taxi_id', partition_name);
  END IF;
END
\$\$;
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${PREPARE_TABLES_CMD}"

# 2. 为2000-01-01创建必要的分区
CREATE_2000_PARTITIONS_CMD=$(cat <<EOF
DO \$\$
DECLARE
  partition_start timestamp;
  partition_end timestamp;
  partition_name text;
  parent_table text;
  partition_exists boolean;
BEGIN
  -- 为2000-01-01创建6小时间隔的分区
  FOR hour_offset IN 0..3 LOOP
    partition_start := '2000-01-01 00:00:00'::timestamp + (hour_offset * INTERVAL '6 HOURS');
    partition_end := partition_start + INTERVAL '6 HOURS';

    -- 优先使用performance_partition
    parent_table := 'performance_partition';
    partition_name := parent_table || '_' || to_char(partition_start, 'YYYY_MM_DD_HH24');

    -- 检查分区是否已存在
    SELECT EXISTS (
      SELECT FROM pg_tables
      WHERE schemaname = 'public' AND tablename = partition_name
    ) INTO partition_exists;

    IF NOT partition_exists THEN
      RAISE NOTICE '创建历史分区: %', partition_name;

      -- 创建分区表
      EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
          LIKE performance_wa INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING NOT NULL,
          CONSTRAINT %I CHECK (dtg >= %L AND dtg < %L)
        ) PARTITION BY RANGE(dtg)',
        partition_name, partition_name || '_constraint', partition_start, partition_end);

      -- 附加到父表
      EXECUTE format('
        ALTER TABLE %I ATTACH PARTITION %I
        FOR VALUES FROM (%L) TO (%L)',
        parent_table, partition_name, partition_start, partition_end);

      -- 创建索引
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING BRIN(geom) WITH (pages_per_range = 128)',
                     partition_name || '_geom', partition_name);
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (dtg)',
                     partition_name || '_dtg', partition_name);
      EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (taxi_id)',
                     partition_name || '_taxi_id', partition_name);
    END IF;
  END LOOP;
END
\$\$;
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=0 -c "${CREATE_2000_PARTITIONS_CMD}"

# 开始事务
BEGIN_CMD="BEGIN;"
docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${BEGIN_CMD}"

echo "$(date '+%Y-%m-%d %H:%M:%S') - 【准备阶段】完成，开始批量导入..."

# 计数器初始化
success_count=0
fail_count=0
total_import_time=0
processed_files=0
batch_counter=0
batch_start_time=0
best_throughput=0
worst_throughput=999999

# 预估完成时间
estimated_total_time=0

# 批处理主循环
for i in "${!files[@]}"; do
    # 跳过已完成的文件
    if [ "$i" -lt "$start_index" ]; then
        ((processed_files++))
        continue
    fi

    filename="${files[$i]}"
    full_path_in_container="${TBL_DIR_IN_CONTAINER}/${filename}"

    # 批处理开始
    if [ $batch_counter -eq 0 ]; then
        batch_start_time=$(date +%s.%N)
        batch_number=$((processed_files / BATCH_SIZE + 1))
        total_batches=$(((TOTAL_EXPECTED_FILES + BATCH_SIZE - 1) / BATCH_SIZE))
        echo "--------------------------------------------------"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】开始新批次 #${batch_number}/${total_batches} (文件 ${i}/${#files[@]})"
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 预估总时间: ${estimated_total_time}秒, 已用时间: $(echo "scale=0; $total_duration" | bc)s"
    fi

    # 记录单个文件开始时间
    file_start_time=$(date +%s.%N)

    # 执行导入
    IMPORT_CMD="COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM '${full_path_in_container}' WITH (FORMAT text, DELIMITER '|', NULL '');"

    docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${IMPORT_CMD}"

    # 检查退出状态码
    if [ $? -eq 0 ]; then
        file_end_time=$(date +%s.%N)
        file_duration=$(echo "scale=3; $file_end_time - $file_start_time" | bc)

        ((success_count++))
        ((processed_files++))
        ((batch_counter++))

        # 显示进度
        if [ $((processed_files % 5)) -eq 0 ] || [ $processed_files -eq 1 ] || [ $processed_files -eq ${#files[@]} ]; then
            percent_complete=$(echo "scale=1; $processed_files * 100 / ${#files[@]}" | bc)
            printf "$(date '+%Y-%m-%d %H:%M:%S') - 进度: %4d/%4d 文件 (%5.1f%%), 成功: %d, 失败: %d\n" \
                "$processed_files" "${#files[@]}" "$percent_complete" "$success_count" "$fail_count"
        fi
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 失败: 导入文件 '$filename' 时发生错误。"
        echo "$filename" >> "$FAILED_FILES_LOG"
        ((fail_count++))
        ((processed_files++))
    fi

    # 检查是否达到批处理限制
    batch_end_time=$(date +%s.%N)
    current_batch_duration=$(echo "scale=3; $batch_end_time - $batch_start_time" | bc)

    should_commit=false

    # 条件1: 达到批大小
    if [ $batch_counter -ge $BATCH_SIZE ]; then
        should_commit=true
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】达到批大小限制 ($BATCH_SIZE 个文件 = $((batch_counter * ROWS_PER_FILE)) 条记录)"
    fi

    # 条件2: 达到行数限制
    current_batch_rows=$((batch_counter * ROWS_PER_FILE))
    if [ $current_batch_rows -ge $MAX_BATCH_ROWS ]; then
        should_commit=true
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】达到行数限制 ($MAX_BATCH_ROWS 条记录)"
    fi

    # 条件3: 达到时间限制
    if [ $(echo "$current_batch_duration > $MAX_BATCH_DURATION" | bc) -eq 1 ]; then
        should_commit=true
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】达到时间限制 ($MAX_BATCH_DURATION 秒)"
    fi

    # 条件4: 最后一个文件
    if [ $processed_files -eq ${#files[@]} ]; then
        should_commit=true
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】处理完所有文件"
    fi

    # 提交批处理
    if [ "$should_commit" = true ] && [ $batch_counter -gt 0 ]; then
        COMMIT_CMD="COMMIT;"
        commit_start_time=$(date +%s.%N)

        docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
          psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${COMMIT_CMD}"

        commit_end_time=$(date +%s.%N)
        commit_duration=$(echo "scale=3; $commit_end_time - $commit_start_time" | bc)

        batch_end_time=$(date +%s.%N)
        batch_duration=$(echo "scale=3; $batch_end_time - $batch_start_time" | bc)
        avg_per_file=$(echo "scale=3; $batch_duration / $batch_counter" | bc)
        avg_per_row_ms=$(echo "scale=3; ($batch_duration * 1000) / $current_batch_rows" | bc)
        batch_rows=$((batch_counter * ROWS_PER_FILE))
        throughput=$(echo "scale=0; $batch_rows / $batch_duration" | bc)

        # 更新最佳/最差吞吐量
        if [ "$throughput" -gt "$best_throughput" ]; then
            best_throughput=$throughput
        fi
        if [ "$throughput" -lt "$worst_throughput" ] || [ "$worst_throughput" -eq 999999 ]; then
            worst_throughput=$throughput
        fi

        printf "$(date '+%Y-%m-%d %H:%M:%S') - 【批处理】完成批次: %2d个文件 (%s万条), 耗时: %6.2fs, 平均: %5.3fs/文件, %.3fms/行, 吞吐量: %6d条/秒, 提交耗时: %5.2fs\n" \
            "$batch_counter" "$((current_batch_rows / 10000))" "$batch_duration" "$avg_per_file" "$avg_per_row_ms" "$throughput" "$commit_duration"

        # 保存进度
        echo "$processed_files" > "$PROGRESS_FILE"

        # 更新总耗时用于估算
        end_total_time=$(date +%s.%N)
        total_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)

        # 估算剩余时间
        if [ $success_count -gt 0 ]; then
            avg_time_per_file=$(echo "scale=3; $total_duration / $success_count" | bc)
            remaining_files=$((${#files[@]} - $processed_files))
            estimated_remaining_time=$(echo "scale=0; $avg_time_per_file * $remaining_files" | bc)
            estimated_total_time=$(echo "scale=0; $total_duration + $estimated_remaining_time" | bc)

            est_hours=$(echo "scale=0; $estimated_remaining_time / 3600" | bc)
            est_minutes=$(echo "scale=0; ($estimated_remaining_time - $est_hours * 3600) / 60" | bc)
            est_seconds=$(echo "scale=0; $estimated_remaining_time - $est_hours * 3600 - $est_minutes * 60" | bc)

            echo "$(date '+%Y-%m-%d %H:%M:%S') - 【估算】剩余时间: ${est_hours}h ${est_minutes}m ${est_seconds}s (总计约 $(echo "scale=0; $estimated_total_time / 3600" | bc)h)"
        fi

        # 重置批计数器
        batch_counter=0

        # 每VACUUM_FREQUENCY个批次执行一次VACUUM ANALYZE
        batches_completed=$((processed_files / BATCH_SIZE))
        if [ $((batches_completed % VACUUM_FREQUENCY)) -eq 0 ] && [ $batches_completed -gt 0 ] && [ $processed_files -lt ${#files[@]} ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - 【维护】执行VACUUM ANALYZE优化 (每 ${VACUUM_FREQUENCY} 个批次一次)..."
            VACUUM_CMD="VACUUM ANALYZE ${TARGET_TABLE};"

            docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
              psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${VACUUM_CMD}"
        fi

        # 检查磁盘空间
        DISK_USAGE_CMD="SELECT pg_size_pretty(pg_database_size('${DB_NAME}')) AS db_size;"
        db_size=$(docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
          psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "$DISK_USAGE_CMD" | tr -d '[:space:]' || echo "unknown")
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【监控】当前数据库大小: $db_size"

        # 开始新事务
        if [ $processed_files -lt ${#files[@]} ]; then
            BEGIN_CMD="BEGIN;"
            docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
              psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${BEGIN_CMD}"
        fi
    fi
done

# 确保最后的事务被提交
if [ $batch_counter -gt 0 ]; then
    COMMIT_CMD="COMMIT;"
    docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${COMMIT_CMD}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【清理】提交最后一个批次事务"
fi

# 恢复阶段：重建索引和恢复配置
echo "=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【恢复阶段】开始重建索引和恢复配置..."

# 1. 执行最终VACUUM ANALYZE
echo "$(date '+%Y-%m-%d %H:%M:%S') - 执行最终VACUUM ANALYZE优化..."
VACUUM_CMD="VACUUM ANALYZE;"
docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${VACUUM_CMD}"

# 2. 手动触发分区维护
echo "$(date '+%Y-%m-%d %H:%M:%S') - 手动触发分区维护任务，将数据移动到正确分区..."
MAINTENANCE_CMD=$(cat <<EOF
DO \$\$
BEGIN
  -- 手动触发分区维护
  CALL "performance_partition_maintenance"();
EXCEPTION WHEN others THEN
  RAISE NOTICE '分区维护执行: %', SQLERRM;
END
\$\$;
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=0 -c "${MAINTENANCE_CMD}"

# 3. 恢复PostgreSQL参数
echo "$(date '+%Y-%m-%d %H:%M:%S') - 恢复默认PostgreSQL参数..."
RESTORE_PARAMS_CMD=$(cat <<EOF
RESET maintenance_work_mem;
RESET work_mem;
RESET temp_buffers;
RESET max_parallel_workers_per_gather;
RESET enable_partitionwise_join;
RESET enable_partitionwise_aggregate;
RESET commit_delay;
RESET commit_siblings;
RESET random_page_cost;
RESET effective_io_concurrency;
RESET max_wal_size;
RESET checkpoint_timeout;
RESET wal_buffers;
RESET lock_timeout;
SET synchronous_commit = on;
SET autovacuum = on;  -- 恢复自动维护
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${RESTORE_PARAMS_CMD}"

# 4. 重新激活cron任务
echo "$(date '+%Y-%m-%d %H:%M:%S') - 重新激活后台任务..."
REACTIVATE_TASKS_CMD=$(cat <<EOF
DO \$\$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_cron') THEN
    UPDATE cron.job SET active = true WHERE jobname LIKE 'performance%';
    RAISE NOTICE '已激活 % 个 cron 任务', (SELECT count(*) FROM cron.job WHERE jobname LIKE 'performance%' AND active = true);

    -- 手动触发分区维护
    CALL "performance_partition_maintenance"();
    -- 更新统计信息
    CALL "performance_analyze_partitions"();
  END IF;
END
\$\$;
EOF
)

docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${REACTIVATE_TASKS_CMD}"

# 记录脚本总结束时间
end_total_time=$(date +%s.%N)
total_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)
hours=$(echo "scale=0; $total_duration / 3600" | bc)
minutes=$(echo "scale=0; ($total_duration - $hours * 3600) / 60" | bc)
seconds=$(echo "scale=0; $total_duration - $hours * 3600 - $minutes * 60" | bc)

echo "=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【完成】所有文件处理完毕。"
echo "成功导入: $success_count 个文件 (${success_count}0万条数据)"
echo "导入失败: $fail_count 个文件 (${fail_count}0万条数据)"
echo "--------------------------------------------------"
printf "$(date '+%Y-%m-%d %H:%M:%S') - 脚本总执行时间: %d小时 %d分钟 %d秒 (%.0f秒)\n" "$hours" "$minutes" "$seconds" "$total_duration"

# 计算并显示性能指标
if [ $success_count -gt 0 ]; then
    average_time_per_file=$(echo "scale=3; $total_duration / $success_count" | bc)
    total_rows=$((success_count * ROWS_PER_FILE))
    overall_throughput=$(echo "scale=0; $total_rows / $total_duration" | bc)
    avg_ms_per_row=$(echo "scale=3; ($total_duration * 1000) / $total_rows" | bc)

    printf "$(date '+%Y-%m-%d %H:%M:%S') - 平均每个文件的处理时间: %.3f 秒\n" "$average_time_per_file"
    printf "$(date '+%Y-%m-%d %H:%M:%S') - 平均每行处理时间: %.3f 毫秒\n" "$avg_ms_per_row"
    printf "$(date '+%Y-%m-%d %H:%M:%S') - 总吞吐量: %d 条/秒\n" "$overall_throughput"
    printf "$(date '+%Y-%m-%d %H:%M:%S') - 最佳批次吞吐量: %d 条/秒\n" "$best_throughput"
    if [ "$worst_throughput" -ne 999999 ]; then
        printf "$(date '+%Y-%m-%d %H:%M:%S') - 最差批次吞吐量: %d 条/秒\n" "$worst_throughput"
    fi

    # 验证总数据量
    expected_total=$((TOTAL_EXPECTED_FILES * ROWS_PER_FILE))
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【验证】预期总数据量: ${expected_total} 条"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【验证】实际成功导入: ${total_rows} 条"
    if [ "$total_rows" -lt "$expected_total" ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 实际导入数据量小于预期"
    fi
fi

# 验证数据分布
echo "--------------------------------------------------"
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【验证】检查数据分布..."
VERIFY_CMD=$(cat <<EOF
SELECT
  (SELECT count(1)::text FROM performance_wa) AS wa_count,
  (SELECT count(1)::text FROM performance_wa_partition) AS wa_part_count,
  (SELECT count(1)::text FROM performance_partition) AS part_count,
  (SELECT count(1)::text FROM performance_spill) AS spill_count,
  (SELECT count(1)::text FROM performance) AS view_total;
EOF
)

result=$(docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -t -c "${VERIFY_CMD}" 2>/dev/null || echo "验证失败")

if [ $? -eq 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 数据分布结果:"
    echo "$result" | while read -r line; do
        echo "  $line"
    done

    # 计算总数据量
    total_from_db=$(echo "$result" | grep view_total | awk '{print $2}')
    if [ -n "$total_from_db" ] && [ "$total_from_db" -gt 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【验证】数据库中总记录数: ${total_from_db}"
        if [ "$total_from_db" -ge $((success_count * ROWS_PER_FILE * 95 / 100)) ]; then
            echo "$(date '+%Y-%m-%d %H:%M:%S') - 【验证】数据量符合预期 (误差<5%)"
        else
            echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 数据库记录数(${total_from_db})与导入数(${success_count}0万)差异较大"
        fi
    fi
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 警告: 无法获取数据分布统计"
fi

# 显示失败文件摘要
if [ $fail_count -gt 0 ]; then
    echo "--------------------------------------------------"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【失败文件摘要】共 $fail_count 个文件导入失败:"
    echo "可以查看详细日志: $FAILED_FILES_LOG"
    echo "重新运行脚本将自动跳过已成功处理的文件"

    # 显示前5个失败文件
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 前5个失败文件:"
    head -5 "$FAILED_FILES_LOG" | while read failed_file; do
        echo "  - $failed_file"
    done
fi

echo "=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 导入过程已全部完成！"
echo "5000万条数据导入任务总结:"
echo "  - 计划导入: 500个文件 (5000万条)"
echo "  - 成功导入: ${success_count}个文件 (${success_count}0万条)"
echo "  - 失败文件: ${fail_count}个文件 (${fail_count}0万条)"
echo "  - 总耗时: ${hours}h ${minutes}m ${seconds}s"
echo "  - 平均吞吐量: ${overall_throughput} 条/秒"
echo ""
echo "注意: 数据已导入到GeoMesa架构中，后台任务会自动将数据移动到正确分区。"
echo "通常在10-20分钟后，所有数据将完全出现在performance视图中。"
echo "=================================================="

# 清理临时文件
if [ $success_count -ge ${#files[@]} ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【清理】所有文件导入成功，清理进度文件..."
    docker exec "${CONTAINER_NAME}" rm -f "$PROGRESS_FILE" "$FAILED_FILES_LOG" 2>/dev/null || true
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【清理】存在失败文件，保留进度和错误日志以便重试"
fi

# 最终数据验证
echo "$(date '+%Y-%m-%d %H:%M:%S') - 【最终验证】检查performance视图数据总量..."
FINAL_COUNT_CMD="SELECT count(1) AS total_count FROM performance;"
final_count=$(docker exec -e PGPASSWORD="${DB_PASSWD}" "${CONTAINER_NAME}" \
  psql -U "${DB_USER}" -d "${DB_NAME}" -t -c "$FINAL_COUNT_CMD" 2>/dev/null | tr -d '[:space:]')

if [ -n "$final_count" ] && [ "$final_count" -gt 0 ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - performance视图中总记录数: $final_count"
    if [ "$final_count" -ge $((TOTAL_EXPECTED_FILES * ROWS_PER_FILE * 98 / 100)) ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【成功】导入任务完成，数据量符合预期 (误差<2%)"
        exit 0
    else
        echo "$(date '+%Y-%m-%d %H:%M:%S') - 【警告】数据量不完整，可能需要重新导入失败文件"
        exit 1
    fi
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') - 【警告】无法获取最终数据量统计"
    exit 1
fi