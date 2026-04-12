#!/usr/bin/env python3
"""
安全地移除未使用的头文件，只处理那些确实可以安全移除的
"""

import re
import sys
import os
from collections import defaultdict

def parse_unused_headers(filename):
    """解析未使用头文件报告"""
    cpp_files = defaultdict(list)
    
    # 匹配模式：文件路径:行号:列号: warning: included header 头文件名 is not used directly
    pattern = r'(/home/xujia/starrocks/[^:]+\.cpp):(\d+):\d+: warning: included header ([^\s]+) is not used directly'
    
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
        
    matches = re.findall(pattern, content)
    
    for cpp_file, line_num, header in matches:
        cpp_files[cpp_file].append((int(line_num), header))
    
    return cpp_files

def is_safe_to_remove(cpp_file, header):
    """检查是否可以安全移除头文件"""
    # 一些绝对不能移除的头文件
    critical_headers = {
        'fmt/format.h', 'fmt/core.h',  # 格式化库
        'gutil/strings/split.h', 'gutil/strings/strip.h',  # gutil库
        'boost/uuid/uuid.hpp', 'boost/uuid/uuid_generators.hpp', 'boost/uuid/uuid_io.hpp',  # boost库
        'runtime/exec_env.h',  # 执行环境
        'column/nullable_column.h', 'column/json_column.h', 'column/object_column.h',  # 列类型
        'types/logical_type.h',  # 逻辑类型
        'avro.h', 'avro_errors.h',  # avro库
    }
    
    # 检查是否是关键头文件
    for critical in critical_headers:
        if critical in header or header in critical:
            return False
    
    # 检查是否是系统头文件（通常可以安全移除）
    system_headers = {
        'algorithm', 'memory', 'vector', 'string', 'map', 'set', 'unordered_map', 'unordered_set',
        'iostream', 'fstream', 'sstream', 'istream', 'ostream',
        'chrono', 'thread', 'mutex', 'condition_variable',
        'sys/types.h', 'sys/stat.h', 'sys/time.h', 'sys/socket.h',
        'pthread.h', 'unistd.h', 'fcntl.h', 'errno.h', 'string.h', 'strings.h',
        'cstring', 'cstdlib', 'cstdio', 'cassert', 'cmath', 'climits',
        'functional', 'utility', 'tuple', 'optional', 'variant',
        'regex', 'random', 'numeric', 'iterator', 'type_traits'
    }
    
    # 系统头文件通常可以安全移除
    if header in system_headers:
        return True
    
    # 对于项目内部头文件，需要更谨慎
    if header.startswith('"') or header.startswith('<'):
        # 移除引号和尖括号
        clean_header = header.strip('"<>')
        if clean_header in system_headers:
            return True
    
    return False

def remove_safe_headers_from_file(cpp_file, headers_to_remove):
    """从指定的cpp文件中安全地移除未使用的头文件"""
    
    if not os.path.exists(cpp_file):
        print(f"Warning: File {cpp_file} does not exist")
        return False
    
    try:
        with open(cpp_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {cpp_file}: {e}")
        return False
    
    # 过滤出可以安全移除的头文件
    safe_headers = []
    for line_num, header in headers_to_remove:
        if is_safe_to_remove(cpp_file, header):
            safe_headers.append((line_num, header))
    
    if not safe_headers:
        return False
    
    # 按行号倒序排序，这样删除时不会影响后面的行号
    safe_headers.sort(key=lambda x: x[0], reverse=True)
    
    removed_count = 0
    original_lines = len(lines)
    
    for line_num, header in safe_headers:
        if line_num <= len(lines):
            line_index = line_num - 1  # 转换为0基索引
            line = lines[line_index]
            
            # 检查这一行是否是include语句且包含目标头文件
            if re.match(r'\s*#include\s*[<"]', line):
                # 提取include语句中的头文件名
                include_match = re.search(r'#include\s*[<"]([^>"]+)[>"]', line)
                if include_match:
                    include_header = include_match.group(1)
                    # 检查是否匹配（支持路径前缀匹配）
                    if (header in include_header or 
                        include_header.endswith('/' + header) or
                        include_header == header):
                        # 移除这一行
                        lines.pop(line_index)
                        removed_count += 1
                        print(f"  Removed: {line.strip()}")
    
    if removed_count > 0:
        try:
            with open(cpp_file, 'w', encoding='utf-8') as f:
                f.writelines(lines)
            print(f"  Modified {cpp_file}: removed {removed_count} headers")
            return True
        except Exception as e:
            print(f"Error writing {cpp_file}: {e}")
            return False
    
    return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 safe_remove_headers.py <analysis_file> [max_files]")
        sys.exit(1)
    
    analysis_file = sys.argv[1]
    max_files = int(sys.argv[2]) if len(sys.argv) > 2 else None
    
    cpp_files = parse_unused_headers(analysis_file)
    
    print(f"Found {len(cpp_files)} cpp files with unused headers")
    
    processed_count = 0
    modified_count = 0
    
    for cpp_file, headers in cpp_files.items():
        if max_files and processed_count >= max_files:
            break
            
        print(f"\nProcessing {cpp_file}...")
        if remove_safe_headers_from_file(cpp_file, headers):
            modified_count += 1
        
        processed_count += 1
    
    print(f"\n=== Summary ===")
    print(f"Processed files: {processed_count}")
    print(f"Modified files: {modified_count}")
    print(f"Unmodified files: {processed_count - modified_count}")

if __name__ == "__main__":
    main()
