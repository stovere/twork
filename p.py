from collections import Counter

def count_range_substring_combinations(file_path, start_pos, end_pos):
    """
    统计从 start_pos 开始，到 end_pos（包含） 的不同子字符串组合数量，并统计不同长度的字符串数量分布
    :param file_path: 文件路径
    :param start_pos: 起始索引（从 0 开始）
    :param end_pos: 结束索引（包含该索引）
    :return: 该范围内不同字符串组合的数量, 长度分布字典
    """
    if start_pos > end_pos:
        raise ValueError("start_pos 必须小于或等于 end_pos")

    counter = Counter()
    length_distribution = Counter()  # 记录不同长度的字符串数量

    # 读取文件
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            line = line.strip()
            if line:  # 过滤空行
                length_distribution[len(line)] += 1  # 记录当前字符串长度的出现次数

                # 统计前缀子字符串
                if len(line) >= end_pos + 1:  # 确保字符串足够长
                    substring = line[start_pos:end_pos + 1]  # 取出子字符串
                    counter[substring] += 1  # 统计出现次数

    # 计算唯一组合数量
    unique_count = len(counter)

    return unique_count, length_distribution

# 示例调用
file_path = "test.txt"  # 替换为你的文本文件路径
start_position = 1  # 从索引 1（第二个字符）开始
end_position = 14  # 到索引 3（第四个字符），共取 3 个字符
unique_count, length_distribution = count_range_substring_combinations(file_path, start_position, end_position)
print(f"从索引 {start_position} 到索引 {end_position}（共 {end_position - start_position + 1} 个字符）的不同组合数量: {unique_count}")

file_path = "test.txt"  # 替换为你的文本文件路径
start_position = 15  # 从索引 1（第二个字符）开始
end_position = 28  # 到索引 3（第四个字符），共取 3 个字符
unique_count, length_distribution = count_range_substring_combinations(file_path, start_position, end_position)

# 输出结果
print(f"从索引 {start_position} 到索引 {end_position}（共 {end_position - start_position + 1} 个字符）的不同组合数量: {unique_count}")
print("\n字符串长度分布:")
for length, count in sorted(length_distribution.items()):
    print(f"长度 {length}: {count} 个字符串")
#68-92