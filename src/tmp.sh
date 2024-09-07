#!/bin/bash

# 指定目标目录
target_directory="~/Downloads/factor_datas/"

# 遍历目标目录下的所有文件夹
for dir in "$target_directory"/*; do
    # 检查是否是目录
    if [ -d "$dir" ]; then
        # 遍历目录中的所有文件
        for file in "$dir"/*; do
            # 检查是否是文件
            if [ -f "$file" ]; then
                # 移动文件到目标目录
                mv "$file" "$target_directory"
            fi
        done
        # 删除空目录
        rmdir "$dir"
    fi
done
