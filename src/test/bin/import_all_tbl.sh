#!/bin/bash

# ==============================================================================
# Shell脚本：控制 Docker 容器内部的文件，进行批量导入并计时（精确到毫秒）
#
# 使用方法:
# 1. 确保你已经手动将 .tbl 文件目录复制到容器中。
#    例如: docker cp /path/on/host/beijingshi_tbl my_container:/import-data
# 2. 修改下面的 "--- 配置区 ---"。
# 3. 在主机上运行此脚本: ./import_all_tbl.sh
# ==============================================================================

# --- 配置区 ---

# 1. Docker 容器名
CONTAINER_NAME="stag-gstria-postgis_postgis_1"

# 2. 数据库连接详细信息
DB_USER="postgres"
DB_NAME="gstria"
TABLE_NAME="performance"
DB_PASSWD="ds123456"

# 3. 容器内部存放 .tbl 文件的目录
TBL_DIR_IN_CONTAINER="/tmp/import-data"
TBL_DIR_IN_LOCAL="/home/gstria/datasets/beijingshi_tbl"


# 记录脚本总开始时间
start_total_time=$(date +%s.%N)

# 检查 docker 命令是否存在
if ! command -v docker &> /dev/null; then
    echo "错误: docker 命令未找到。"
    exit 1
fi

# 检查指定的 Docker 容器是否正在运行
if ! docker ps --filter "name=${CONTAINER_NAME}" --format "{{.Names}}" | grep -q "^${CONTAINER_NAME}$"; then
    echo "错误: Docker 容器 '${CONTAINER_NAME}' 不存在或未在运行。"
    exit 1
fi

# 检查容器内目录是否存在，不存在则创建
docker exec "${CONTAINER_NAME}" mkdir -p "${TBL_DIR_IN_CONTAINER}"
# 复制本地目录下的所有.tbl文件到容器目标目录（注意末尾的/*）
if ! docker exec "${CONTAINER_NAME}" test -f "${TBL_DIR_IN_CONTAINER}/*.tbl"; then
    docker cp "${TBL_DIR_IN_LOCAL}"/*.tbl "${CONTAINER_NAME}:${TBL_DIR_IN_CONTAINER}/"
fi

echo "开始批量导入过程..."
echo "目标 Docker 容器: $CONTAINER_NAME"
echo "目标数据库: $DB_NAME, 目标表: $TABLE_NAME"
echo "容器内 TBL 文件目录: $TBL_DIR_IN_CONTAINER"
echo ""

# 关键改动：通过 docker exec 在容器内部查找文件列表
# - 'find ... -printf "%f\n"' 会在容器内找到所有 .tbl 文件并只打印文件名
file_list=$(docker exec "${CONTAINER_NAME}" find "${TBL_DIR_IN_CONTAINER}" -maxdepth 1 -type f -name "*.tbl" -printf "%f\n")
# 将找到的文件名字符串转换成 Bash 数组
files=($file_list)

# 检查是否找到了任何 .tbl 文件
if [ ${#files[@]} -eq 0 ]; then
    echo "在容器目录 '${TBL_DIR_IN_CONTAINER}' 中没有找到任何 .tbl 文件。"
    exit 0
fi

echo "共找到 ${#files[@]} 个 .tbl 文件需要导入。"

# 计数器
success_count=0
fail_count=0
total_import_time=0

# 遍历在容器中找到的文件名数组
for filename in "${files[@]}"; do
    # 构建容器内部文件的完整路径
    full_path_in_container="${TBL_DIR_IN_CONTAINER}/${filename}"

    echo "--------------------------------------------------"
    echo "准备导入文件: ${full_path_in_container} (在容器内)"

    # 构建服务器端的 COPY 命令，使用容器内的绝对路径
    COMMAND="COPY ${TABLE_NAME}(fid,geom,dtg,taxi_id) FROM '${full_path_in_container}' WITH (FORMAT text, DELIMITER '|', NULL '')"

    # 记录单个文件开始时间
    start_file_time=$(date +%s.%N)

    # 在容器内执行 psql 命令
    docker exec \
      -e PGPASSWORD="${DB_PASSWD}" \
      "${CONTAINER_NAME}" \
      psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c "${COMMAND}"

    # 检查退出状态码
    if [ $? -eq 0 ]; then
        end_file_time=$(date +%s.%N)
        file_duration=$(echo "scale=3; $end_file_time - $start_file_time" | bc)
        printf "成功: 文件 '%s' 已成功导入。耗时: %.3f 秒。\n" "$filename" "$file_duration"
        ((success_count++))
        total_import_time=$(echo "scale=3; $total_import_time + $file_duration" | bc)
    else
        echo "失败: 导入文件 '$filename' 时发生错误。"
        ((fail_count++))
    fi
done

# 记录脚本总结束时间
end_total_time=$(date +%s.%N)
total_duration=$(echo "scale=3; $end_total_time - $start_total_time" | bc)

echo "=================================================="
echo "所有文件处理完毕。"
echo "成功导入: $success_count 个文件"
echo "导入失败: $fail_count 个文件"
echo "--------------------------------------------------"
printf "脚本总执行时间: %.3f 秒。\n" "$total_duration"

# 计算并显示平均导入时间
if [ $success_count -gt 0 ]; then
    average_time=$(echo "scale=3; $total_import_time / $success_count" | bc)
    printf "平均每个文件的导入时间: %.3f 秒。\n" "$average_time"
fi
echo "=================================================="