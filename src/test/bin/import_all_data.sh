#!/bin/bash
# ==============================================================================
# Shell脚本：全量数据导入与性能测试 (v5 - 调试版)
#
# v5 更新:
# - 移除了循环内部所有的 `> /dev/null 2>&1` 输出重定向。
# - 增加了详细的 DEBUG 日志，以精确定位是哪一步操作失败。
# - 这个版本是为了暴露隐藏的错误，而不是为了干净的输出。
# ==============================================================================

set -e

# ... (脚本配置区和前面1-3阶段完全不变) ...
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
CONTAINER_NAME="my-postgis-container"
DB_USER="postgres"
DB_NAME="postgres"
TARGET_TABLE="performance_wa"
TBL_DIR_IN_LOCAL="/data6/zhangdw/datasets/beijingshi_tbl_100k"
ROWS_PER_FILE=100000
DISABLE_SCRIPT="${SCRIPT_DIR}/disable_geomesa_features.sh"
ENABLE_SCRIPT="${SCRIPT_DIR}/enable_geomesa_features.sh"
echo "=================================================="
echo "$(date '+%Y-%m-%d %H:%M:%S') - 开始全量数据导入流程..."
echo "=================================================="
echo -e "\n>>> 阶段 1: 禁用 GeoMesa 特性..."
bash "${DISABLE_SCRIPT}"
echo -e "\n>>> 阶段 2: 清空写入缓冲区表 '${TARGET_TABLE}'..."
docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -c "DELETE FROM ${TARGET_TABLE};"
echo "表已清空。"
echo -e "\n>>> 阶段 3: 查找并准备导入文件..."
file_list=$(find "${TBL_DIR_IN_LOCAL}" -maxdepth 1 -type f -name "*.tbl" | sort)
files=($file_list)
total_files=${#files[@]}
if [ "$total_files" -eq 0 ]; then
    echo "错误: 在目录 '${TBL_DIR_IN_LOCAL}' 中没有找到任何 .tbl 文件。"
    bash "${ENABLE_SCRIPT}"
    exit 1
fi
echo "共找到 ${total_files} 个文件需要导入。"


# 4. 循环导入并计时 (调试模式)
echo -e "\n>>> 阶段 4: 开始循环导入文件 (调试模式)..."
success_count=0
fail_count=0
total_copy_duration=0
start_total_time=$(date +%s.%N)

for i in "${!files[@]}"; do
    local_full_path="${files[$i]}"
    filename=$(basename "$local_full_path")
    container_temp_path="/tmp/${filename}"
    current_file_num=$((i + 1))

    echo "--------------------------------------------------"
    echo "处理文件 ${current_file_num}/${total_files}: ${filename}"

    # [MODIFIED] 移除所有 > /dev/null 2>&1，并增加详细日志
    copy_start_time=$(date +%s.%N)
    exit_code=0

    echo "DEBUG: 步骤 A - 正在复制 '${local_full_path}' 到容器的 '${container_temp_path}'..."
    docker cp "${local_full_path}" "${CONTAINER_NAME}:${container_temp_path}"
    if [ $? -ne 0 ]; then
        echo "DEBUG: 步骤 A - docker cp 失败！"
        exit_code=1
    fi

    if [ "$exit_code" -eq 0 ]; then
        echo "DEBUG: 步骤 B - 正在从 '${container_temp_path}' COPY 数据..."
        IMPORT_SQL="COPY ${TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM '${container_temp_path}' WITH (FORMAT text, DELIMITER '|', NULL '');"
        docker exec -i "${CONTAINER_NAME}" psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${IMPORT_SQL}"
        if [ $? -ne 0 ]; then
            echo "DEBUG: 步骤 B - psql COPY 失败！"
            exit_code=1
        fi
    fi

    copy_end_time=$(date +%s.%N)

    echo "DEBUG: 步骤 C - 正在从容器中删除临时文件 '${container_temp_path}'..."
    # 即使 B 失败了，也尝试清理
    docker exec -i "${CONTAINER_NAME}" rm -f "${container_temp_path}"

    if [ "$exit_code" -eq 0 ]; then
        copy_duration=$(echo "scale=3; $copy_end_time - $copy_start_time" | bc)
        total_copy_duration=$(echo "scale=3; $total_copy_duration + $copy_duration" | bc)
        ((success_count++))
        echo "结果: 成功 (耗时: ${copy_duration}s)"
    else
        echo "结果: 失败！脚本将因 set -e 退出。"
        ((fail_count++))
        # 因为 set -e，脚本在这里就会停止，所以不需要 break
    fi
done

# ... (脚本后面报告部分不变) ...
end_total_time=$(date +%s.%N)
total_script_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)
echo "所有文件导入尝试完毕。"
echo -e "\n>>> 阶段 5: 恢复 GeoMesa 特性..."
bash "${ENABLE_SCRIPT}"
echo -e "\n>>> 阶段 6: 生成最终报告..."
# ...