import os

def prepend_string_to_files(folder_path, prefix_string, file_extension=".csv"):
    """
    遍历指定文件夹中具有特定扩展名的所有文件，
    并在每个文件的开头写入一个字符串。

    :param folder_path: 要处理的文件夹的路径。
    :param prefix_string: 要写入文件开头的字符串。
    :param file_extension: 要处理的文件的扩展名 (例如 ".csv", ".txt")。
    """
    print(f"开始处理文件夹 '{folder_path}' 中的 '{file_extension}' 文件...")

    # 1. 遍历文件夹中的所有条目
    try:
        filenames = os.listdir(folder_path)
    except FileNotFoundError:
        print(f"错误：找不到文件夹 '{folder_path}'。请检查路径是否正确。")
        return

    processed_count = 0
    for filename in filenames:
        # 2. 检查文件是否具有我们想要的扩展名
        if filename.lower().endswith(file_extension):
            # 构建完整的文件路径
            full_path = os.path.join(folder_path, filename)

            try:
                # 3. 读取文件的全部原始内容
                with open(full_path, 'r', encoding='utf-8') as f:
                    original_content = f.read()

                # 4. 重新以写入模式打开文件（这会清空文件），
                #    然后写入前缀字符串和原始内容的组合。
                with open(full_path, 'w', encoding='utf-8') as f:
                    # 这里的 '+' 操作确保了前缀和原文之间没有换行
                    f.write(prefix_string + original_content)

                print(f"  [成功] 已处理文件: {filename}")
                processed_count += 1

            except Exception as e:
                print(f"  [失败] 处理文件 {filename} 时发生错误: {e}")

    if processed_count == 0:
        print(f"\n未在文件夹中找到任何 '{file_extension}' 文件进行处理。")
    else:
        print(f"\n处理完成！共修改了 {processed_count} 个文件。")


# --- 主程序 ---
if __name__ == "__main__":
    # --- 请在这里修改为您自己的文件夹路径 ---
    folder_to_process = "D:\\datasets\\beijingshi"  # <--- 修改这里

    # 要写入的字符串
    header_to_add = "dtg_str"

    # 调用函数开始处理
    prepend_string_to_files(folder_to_process, header_to_add, file_extension=".csv")