#!/bin/bash

# 批量检查 cow_error_files 中所有文件的错误数量

while IFS= read -r file; do
    if [ -z "$file" ]; then
        continue
    fi
    
    # 运行 clangd_check.sh 并统计 COW 相关错误
    error_count=$(bash clangd_check.sh "$file" 2>&1 | grep -E '^\[E\]' | grep -E '(member_function_call_bad_cvr|ovl_no_viable|init_conversion_failed|typecheck_convert_discards_qualifiers)' | wc -l)
    
    echo "$error_count|$file"
done < cow_error_files | sort -n

