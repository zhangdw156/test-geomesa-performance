#!/usr/bin/env python3
import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# ==================== 日志配置 ====================
# 创建日志目录（如不存在）
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)  # 不存在则创建，存在则忽略

# 日志文件名（包含当前日期）
LOG_FILE = LOG_DIR / f"import_log_{time.strftime('%Y%m%d')}.log"

# 配置日志：同时输出到控制台和文件，格式包含时间、级别、消息
logging.basicConfig(
    level=logging.INFO,  # 日志级别：INFO及以上会被记录
    format="%(asctime)s - %(levelname)s - %(message)s",  # 日志格式
    datefmt="%Y-%m-%d %H:%M:%S",  # 时间格式
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),  # 写入文件（支持中文）
        logging.StreamHandler(sys.stdout)  # 输出到控制台
    ]
)

# ==================== 配置参数 ====================
CONTAINER_NAME = "my-postgis-container"
DB_USER = "postgres"
DB_NAME = "postgres"
TARGET_TABLE = "performance_wa"
TBL_DIR_IN_LOCAL = Path("/data6/zhangdw/datasets/beijingshi_tbl_100k")
ROWS_PER_FILE = 100000

# 辅助脚本路径
DISABLE_SCRIPT = SCRIPT_DIR.parent.parent / "bin" / "disable_geomesa_features.sh"
ENABLE_SCRIPT = SCRIPT_DIR.parent.parent / "bin" / "enable_geomesa_features.sh"

# ==================== 工具函数 ====================
def run_command(cmd, check=True, capture_output=False):
    """执行 shell 命令，返回结果或记录错误日志"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            check=check,
            capture_output=capture_output,
            text=True
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"命令执行失败: {e.cmd}")
        logging.error(f"错误输出: {e.stderr}")
        sys.exit(1)

# ==================== 主逻辑 ====================
def main():
    logging.info("=" * 50)
    start_total_time = time.time()
    logging.info(f"开始全量数据导入流程...")
    logging.info("=" * 50)

    # 1. 禁用 GeoMesa 特性
    logging.info("\n>>> 阶段 1: 禁用 GeoMesa 特性...")
    run_command(f"bash {DISABLE_SCRIPT}")

    # 2. 清空目标表
    logging.info(f"\n>>> 阶段 2: 清空写入缓冲区表 '{TARGET_TABLE}'...")
    run_command(
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -c 'DELETE FROM {TARGET_TABLE};'"
    )
    logging.info("表已清空。")

    # 3. 查找要导入的文件列表
    logging.info("\n>>> 阶段 3: 查找数据文件...")
    tbl_files = sorted(TBL_DIR_IN_LOCAL.glob("*.tbl"))
    total_files = len(tbl_files)
    if total_files == 0:
        logging.error(f"在目录 '{TBL_DIR_IN_LOCAL}' 中未找到任何 .tbl 文件。")
        run_command(f"bash {ENABLE_SCRIPT}")
        sys.exit(1)
    logging.info(f"共找到 {total_files} 个文件需要导入。")

    # 4. 循环导入文件并计时
    logging.info("\n>>> 阶段 4: 开始循环导入文件...")
    success_count = 0
    fail_count = 0
    total_import_duration = 0.0  # 总导入耗时（秒）

    for i, file_path in enumerate(tbl_files, 1):
        filename = file_path.name
        current_file_num = i
        # 控制台实时显示进度，同时记录日志
        logging.info(f"  -> 正在导入文件 {current_file_num}/{total_files}: {filename} ... ")

        # 记录单个文件导入开始时间
        import_start = time.time()

        # 执行导入（通过管道传递数据到 docker exec）
        cmd = (
            f"cat {file_path} | docker exec -i {CONTAINER_NAME} "
            f"psql -U {DB_USER} -d {DB_NAME} -q -v ON_ERROR_STOP=1 "
            f'-c "COPY {TARGET_TABLE}(fid,geom,dtg,taxi_id) FROM STDIN WITH (FORMAT text, DELIMITER \'|\', NULL \'\');"'
        )
        result = run_command(cmd, check=False, capture_output=True)

        # 计算单个文件导入耗时
        import_end = time.time()
        import_duration = import_end - import_start

        # 检查执行结果
        if result.returncode == 0:
            success_count += 1
            total_import_duration += import_duration
            logging.info(f"  -> 导入文件 {current_file_num}/{total_files}: {filename} ... ✅ (耗时: {import_duration:.3f}s)")
        else:
            fail_count += 1
            logging.error(f"  -> 导入文件 {current_file_num}/{total_files}: {filename} ... ❌")
            logging.error(f"错误信息: {result.stderr}")
            # 如需遇到失败即停止，取消下面的注释
            # run_command(f"bash {ENABLE_SCRIPT}")
            # sys.exit(1)

    logging.info("\n所有文件导入尝试完毕。")

    # 5. 恢复 GeoMesa 特性
    logging.info(f"\n>>> 阶段 5: 恢复 GeoMesa 特性...")
    run_command(f"bash {ENABLE_SCRIPT}")

    # 6. 生成最终报告
    logging.info(f"\n>>> 阶段 6: 生成最终报告...")
    logging.info("=" * 50)
    logging.info(" 全量数据导入完成 - 性能报告")
    logging.info("=" * 50)

    # 总脚本执行时间
    end_total_time = time.time()
    total_script_duration = end_total_time - start_total_time
    logging.info(f"脚本总执行时间: {total_script_duration:.3f} 秒")
    logging.info("-" * 50)
    logging.info("文件处理统计:")
    logging.info(f"  - 成功导入文件数: {success_count}")
    logging.info(f"  - 失败导入文件数: {fail_count}")
    logging.info(f"  - 文件总数: {total_files}")
    logging.info("-" * 50)

    # 性能指标（仅成功时计算）
    if success_count > 0:
        total_rows_imported = success_count * ROWS_PER_FILE
        avg_time_per_file = total_import_duration / success_count

        # 吞吐量计算（避免除以 0）
        if total_import_duration > 0:
            overall_throughput = int(total_rows_imported / total_import_duration)
            avg_ms_per_row = (total_import_duration * 1000) / total_rows_imported
        else:
            overall_throughput = 0
            avg_ms_per_row = 0.0

        logging.info("性能指标 (仅计算导入命令耗时):")
        logging.info(f"  - 纯数据导入总耗时: {total_import_duration:.3f} 秒")
        logging.info(f"  - 成功导入总行数 (估算): {total_rows_imported}")
        logging.info(f"  - 平均每个文件的导入时间: {avg_time_per_file:.3f} 秒")
        logging.info(f"  - 平均每行处理时间: {avg_ms_per_row:.3f} 毫秒")
        logging.info(f"  - 纯导入吞吐量: {overall_throughput} 条/秒")

    logging.info("-" * 50)

    # 最终数据量验证
    logging.info("最终数据量验证...")
    cmd = (
        f"docker exec -i {CONTAINER_NAME} psql -U {DB_USER} -d {DB_NAME} -t -c 'SELECT count(1) FROM performance;'"
        " 2>/dev/null | tr -d '[:space:]'"
    )
    result = run_command(cmd, check=False, capture_output=True)
    final_count = result.stdout.strip()

    if result.returncode == 0 and final_count:
        logging.info(f"  -> 'performance' 视图中的总记录数: {final_count}")
        expected_min = success_count * ROWS_PER_FILE * 98 // 100  # 98% 预期值
        if int(final_count) >= expected_min:
            logging.info("  -> 【成功】数据量符合预期！")
        else:
            logging.warning("  -> 【警告】最终数据量与成功导入文件数不符，请检查分区维护任务。")
    else:
        logging.warning("  -> 【警告】无法获取最终数据量统计。")

    logging.info("=" * 50)

if __name__ == "__main__":
    main()