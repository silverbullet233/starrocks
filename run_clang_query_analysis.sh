#!/bin/bash

# Script to find dangerous ImmutPtr method calls using clang-query
# Using the correct path for clang-query

CLANG_QUERY_BIN="/bin/clang-query-19"
SEARCH_DIR="be/src"

if [ ! -x "$CLANG_QUERY_BIN" ]; then
    echo "Error: clang-query not found at $CLANG_QUERY_BIN"
    exit 1
fi

echo "Using clang-query from: $CLANG_QUERY_BIN"
echo "Analyzing directory: $SEARCH_DIR"

# Create a temporary query file
QUERY_FILE=$(mktemp)
cat > "$QUERY_FILE" << 'EOF'
// Match calls to non-const get() method on ImmutPtr
match cxxMemberCallExpr(
    on(hasType(recordDecl(hasName("ImmutPtr")))),
    callee(cxxMethodDecl(hasName("get"))),
    unless(callee(cxxMethodDecl(isConst())))
)

// Match calls to non-const operator->() on ImmutPtr
match cxxMemberCallExpr(
    on(hasType(recordDecl(hasName("ImmutPtr")))),
    callee(cxxMethodDecl(hasName("operator->"))),
    unless(callee(cxxMethodDecl(isConst())))
)

// Match calls to non-const operator*() on ImmutPtr
match cxxMemberCallExpr(
    on(hasType(recordDecl(hasName("ImmutPtr")))),
    callee(cxxMethodDecl(hasName("operator*"))),
    unless(callee(cxxMethodDecl(isConst())))
)
EOF

# Find all C++ files
CPP_FILES=$(find "$SEARCH_DIR" -name "*.h" -o -name "*.cpp" -o -name "*.cc" | head -5)

echo "Found $(echo "$CPP_FILES" | wc -l) C++ files to analyze (limited to first 5 for demo)"
echo "=================================================="

# Analyze each file
for file in $CPP_FILES; do
    echo "Analyzing: $file"
    if [ -f "$file" ]; then
        # Run clang-query on the file
        "$CLANG_QUERY_BIN" "$file" -c "run $QUERY_FILE" 2>/dev/null | grep -E "(match|ImmutPtr)" || echo "  No matches found"
        echo ""
    fi
done

# Clean up
rm -f "$QUERY_FILE"

echo "Analysis complete"