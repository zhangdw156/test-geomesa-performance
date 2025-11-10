import os
import csv
from datetime import datetime

def generate_sql_from_csv_folder(input_folder):
    """
    读取一个文件夹中的所有CSV文件，将每一行转换为SQL INSERT语句，
    并将结果保存到同级的_sql文件夹中。

    :param input_folder: 包含CSV文件的输入文件夹路径。
    """
    if not os.path.isdir(input_folder):
        print(f"错误：输入目录 '{input_folder}' 不存在或不是一个目录。")
        return

    parent_dir = os.path.dirname(input_folder)
    folder_name = os.path.basename(input_folder)
    output_folder = os.path.join(parent_dir, f"{folder_name}_sql")

    try:
        os.makedirs(output_folder, exist_ok=True)
        print(f"SQL文件将被保存到: {output_folder}")
    except OSError as e:
        print(f"错误：创建输出目录 '{output_folder}' 失败: {e}")
        return

    # --- 新增代码：开始 ---

    # 1. 首先，筛选出所有需要处理的CSV文件列表，以便计算总数
    csv_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.csv')]
    total_files = len(csv_files)

    if total_files == 0:
        print("在指定目录中未找到任何CSV文件。")
        return

    print(f"共找到 {total_files} 个CSV文件需要处理。")

    # --- 新增代码：结束 ---

    # 3. 遍历我们已经筛选好的CSV文件列表
    #    使用 enumerate 来同时获取索引和文件名，方便显示进度
    for i, filename in enumerate(csv_files, start=1):
        input_file_path = os.path.join(input_folder, filename)

        output_filename = os.path.splitext(filename)[0] + '.sql'
        output_file_path = os.path.join(output_folder, output_filename)

        # --- 修改代码：在打印信息中加入进度 ---
        print(f"\n[进度: {i}/{total_files}] 正在处理文件: '{filename}' ...")
        # --- 修改结束 ---

        try:
            process_single_csv(input_file_path, output_file_path)
        except Exception as e:
            print(f"  处理文件 '{filename}' 时发生未知错误: {e}")

    print("\n所有文件处理完毕！")

def process_single_csv(csv_path, sql_path):
    """
    处理单个CSV文件，生成对应的SQL文件。
    """
    statement_count = 0
    # 注意：这种方式在非Windows系统上可能会因路径分隔符'\'而出错
    taxi_id = int(os.path.basename(csv_path).split('.')[0])
    with open(csv_path, mode='r', encoding='utf-8') as infile, \
         open(sql_path, mode='w', encoding='utf-8') as outfile:

        # 根据您提供的CSV示例，分隔符是制表符(Tab)
        csv_reader = csv.reader(infile, delimiter=',')

        # 跳过表头
        try:
            header = next(csv_reader)
            # 验证表头是否符合预期（可选，但推荐）
            expected_header = ['dtg_str', 'lat', 'lng', 'speed', 'geohash']
            if header != expected_header:
                 print(f"  警告: 文件 '{os.path.basename(csv_path)}' 的表头与预期不符。当前表头: {header}")
        except StopIteration:
            print(f"  警告: 文件 '{os.path.basename(csv_path)}' 是空的。")
            return

        # 逐行处理数据
        for j, row in enumerate(csv_reader, start=1):
            try:
                dtg_str = row[0]
                lat_str = row[1]
                lng_str = row[2]

                # 注意：如果您的日期格式再次变化，这里可能需要修改
                sql_dtg = datetime.strptime(dtg_str, '%Y-%m-%d %H:%M:%S')

                lat = float(lat_str)
                lng = float(lng_str)

                sql_statement = (
                    f"INSERT INTO performance (geom, dtg,taxi_id) VALUES ("
                    f"ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326), "
                    f"'{sql_dtg}',"
                    f"{taxi_id}"
                    f");\n" # 每个语句后都换行
                )

                # 9. 将语句写入.sql文件
                outfile.write(sql_statement)
                statement_count += 1

            except (ValueError, IndexError) as e:
                # 在报错信息中加入行号，方便定位
                print(f"  [行号: {j+1}] 跳过该行，因为格式错误: {row} -> 错误: {e}")

    print(f"  处理完成，成功生成 {statement_count} 条 INSERT 语句到 '{os.path.basename(sql_path)}'。")


# --- 主程序入口 ---
if __name__ == "__main__":

    csv_folder_path = r"D:\datasets\beijingshi"

    # 调用主函数开始转换
    generate_sql_from_csv_folder(csv_folder_path)