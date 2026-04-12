# 修复脚本

这个脚本将批量修复代码中的编译错误，主要针对column->append相关调用。

#!/bin/bash

# 修复column->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/column->append(/column->as_mutable_raw_ptr()->append(/g'

# 修复column->resize调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/column->resize(/column->as_mutable_raw_ptr()->resize(/g'

# 修复column->append_nulls调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/column->append_nulls(/column->as_mutable_raw_ptr()->append_nulls(/g'

# 修复column->append_default调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/column->append_default(/column->as_mutable_raw_ptr()->append_default(/g'

# 修复column->reserve调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/column->reserve(/column->as_mutable_raw_ptr()->reserve(/g'

# 修复elements_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/elements_column()->append(/elements_column()->as_mutable_raw_ptr()->append(/g'

# 修复elements_column()->resize调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/elements_column()->resize(/elements_column()->as_mutable_raw_ptr()->resize(/g'

# 修复elements_column()->append_nulls调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/elements_column()->append_nulls(/elements_column()->as_mutable_raw_ptr()->append_nulls(/g'

# 修复elements_column()->append_default调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/elements_column()->append_default(/elements_column()->as_mutable_raw_ptr()->append_default(/g'

# 修复elements_column()->reserve调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/elements_column()->reserve(/elements_column()->as_mutable_raw_ptr()->reserve(/g'

# 修复keys_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/keys_column()->append(/keys_column()->as_mutable_raw_ptr()->append(/g'

# 修复values_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/values_column()->append(/values_column()->as_mutable_raw_ptr()->append(/g'

# 修复offsets_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/offsets_column()->append(/offsets_column()->as_mutable_raw_ptr()->append(/g'

# 修复offsets_column()->resize调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/offsets_column()->resize(/offsets_column()->as_mutable_raw_ptr()->resize(/g'

# 修复data_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/data_column()->append(/data_column()->as_mutable_raw_ptr()->append(/g'

# 修复null_column()->append调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/null_column()->append(/null_column()->as_mutable_raw_ptr()->append(/g'

# 修复null_column()->resize调用
find be/src -name "*.cpp" -o -name "*.h" | xargs sed -i 's/null_column()->resize(/null_column()->as_mutable_raw_ptr()->resize(/g'

echo "修复完成"