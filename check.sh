#!/usr/bin/env bash
# ============================================================
#  analyze_clangd_all_json.sh
#  Description:
#    Run clangd semantic analysis on all C++ source files
#    under be/src, using compile_commands.json from be/build_Release,
#    and export all diagnostics as JSON with parallel execution support.
# ============================================================

set -euo pipefail

# --- Default Configuration ---
PROJECT_ROOT="$(pwd)"
SRC_DIR="$PROJECT_ROOT/be/src"
BUILD_DIR="$PROJECT_ROOT/be/build_Release"
OUTPUT_JSON="$PROJECT_ROOT/clangd_diagnostics.json"
JOBS=$(nproc)  # Default to number of CPU cores

# --- Usage ---
usage() {
  cat << EOF
Usage: $0 [OPTIONS]

Run clangd --check on C++ source files in parallel and export diagnostics to JSON.

OPTIONS:
  -j, --jobs N      Number of parallel jobs (default: $JOBS)
  -s, --src-dir     Source directory to scan (default: be/src)
  -b, --build-dir   Build directory with compile_commands.json (default: be/build_Release)
  -o, --output      Output JSON file (default: clangd_diagnostics.json)
  -h, --help        Show this help message

EXAMPLES:
  $0                    # Use default settings
  $0 -j 8               # Run with 8 parallel jobs
  $0 -j 4 -s be/src/runtime  # Check only runtime directory with 4 jobs

EOF
  exit 0
}

# --- Parse Arguments ---
while [[ $# -gt 0 ]]; do
  case $1 in
    -j|--jobs)
      JOBS="$2"
      shift 2
      ;;
    -s|--src-dir)
      SRC_DIR="$PROJECT_ROOT/$2"
      shift 2
      ;;
    -b|--build-dir)
      BUILD_DIR="$PROJECT_ROOT/$2"
      shift 2
      ;;
    -o|--output)
      OUTPUT_JSON="$PROJECT_ROOT/$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo "❌ Unknown option: $1"
      usage
      ;;
  esac
done

# --- Checks ---
if ! command -v clangd &> /dev/null; then
  echo "❌ Error: clangd not found in PATH."
  echo "Install clangd (e.g., via brew install llvm or apt install clangd)."
  exit 1
fi

if [ ! -f "$BUILD_DIR/compile_commands.json" ]; then
  echo "❌ Error: compile_commands.json not found in $BUILD_DIR"
  exit 1
fi

# --- Temporary storage ---
TMP_DIR="$PROJECT_ROOT/.clangd_check_tmp_$$"
mkdir -p "$TMP_DIR"
TMP_LOG="$PROJECT_ROOT/clangd_raw_log.txt"

# Cleanup on exit
trap "rm -rf '$TMP_DIR'" EXIT

# --- Function to check a single file ---
check_file() {
  local file="$1"
  local build_dir="$2"
  local tmp_dir="$3"
  local file_hash=$(echo "$file" | md5sum | cut -d' ' -f1)
  local output_file="$tmp_dir/$file_hash.log"
  
  # Add file marker
  echo "FILE: $file" > "$output_file"
  
  # Run clangd check
  clangd --check="$file" \
         --compile-commands-dir="$build_dir" \
         --log=error \
         --background-index=0 \
         2>> "$output_file" || true
}

# Export function and variables for parallel execution
export -f check_file
export BUILD_DIR
export TMP_DIR

# --- Scan files ---
echo "🔍 Scanning C++ source and header files under $SRC_DIR ..."
mapfile -t FILES < <(find "$SRC_DIR" -type f \( -name "*.cpp" -o -name "*.h" -o -name "*.hpp" \))

echo "🧠 Running clangd --check on ${#FILES[@]} files with $JOBS parallel jobs ..."

# --- Parallel execution ---
printf "%s\n" "${FILES[@]}" | xargs -P "$JOBS" -I {} bash -c 'check_file "$@"' _ {} "$BUILD_DIR" "$TMP_DIR"

# --- Merge all temporary logs ---
echo "📦 Merging results..."
> "$TMP_LOG"
for log_file in "$TMP_DIR"/*.log; do
  if [ -f "$log_file" ]; then
    cat "$log_file" >> "$TMP_LOG"
  fi
done

# --- Parse diagnostics to JSON with function name detection ---
echo "📦 Parsing diagnostics to JSON ..."
python3 - <<'PYCODE'
import re, json, sys, os
log_path = "clangd_raw_log.txt"
out_path = "clangd_diagnostics.json"

# Pattern for FILE: marker
file_pattern = re.compile(r'^FILE: (.+)$')
# Pattern for clangd diagnostics: E[timestamp] [error_code] Line XX: message
diag_pattern = re.compile(r'^E\[[^\]]+\] \[([^\]]+)\] Line (\d+): (.+)$')

# Cache for file contents
file_cache = {}

def find_function_name(file_path, target_line):
    """Find the function name for a given line in a file."""
    if file_path not in file_cache:
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                file_cache[file_path] = f.readlines()
        except:
            return None
    
    lines = file_cache[file_path]
    if target_line > len(lines) or target_line < 1:
        return None
    
    # Function patterns to match (simple heuristic)
    # Match patterns like: return_type function_name(...) { or Class::method(...) {
    func_patterns = [
        # C++ method/function with implementation: Type Name::func(...) {
        re.compile(r'^\s*(?:[\w:]+\s+)*?([\w~]+::[\w~]+)\s*\([^)]*\)\s*(?:const)?\s*(?:override)?\s*\{?'),
        # Regular function: return_type func_name(...) {
        re.compile(r'^\s*(?:[\w:]+\s+)+([\w~]+)\s*\([^)]*\)\s*(?:const)?\s*(?:override)?\s*\{?'),
        # Constructor/Destructor
        re.compile(r'^\s*(~?[\w]+::[\w]+)\s*\([^)]*\)\s*(?::|,)?\s*\{?'),
    ]
    
    # Search backwards from target line
    for i in range(target_line - 1, -1, -1):
        line = lines[i].rstrip()
        # Stop at namespace or class definition (to avoid going too far)
        if re.match(r'^\s*(?:namespace|class|struct)\s+', line):
            break
        
        for pattern in func_patterns:
            match = pattern.match(line)
            if match:
                return match.group(1)
    
    return None

results = []
current_file = None

with open(log_path) as f:
    for line in f:
        line = line.strip()
        
        # Check for FILE: marker
        file_match = file_pattern.match(line)
        if file_match:
            current_file = os.path.abspath(file_match.group(1))
            continue
        
        # Check for diagnostic
        diag_match = diag_pattern.match(line)
        if diag_match and current_file:
            error_code = diag_match.group(1)
            line_num = int(diag_match.group(2))
            message = diag_match.group(3)
            
            # Try to find function name
            function_name = find_function_name(current_file, line_num)
            
            result = {
                "file": current_file,
                "line": line_num,
                "column": 0,  # clangd doesn't provide column in this format
                "level": "error",  # E[...] indicates error
                "error_code": error_code,
                "message": message,
            }
            
            if function_name:
                result["function"] = function_name
            
            results.append(result)

with open(out_path, "w") as f:
    json.dump(results, f, indent=2)

print(f"✅ Exported {len(results)} diagnostics to {out_path}")
PYCODE

echo "✅ Done. JSON output saved to: $OUTPUT_JSON"
echo "📊 Parallel execution with $JOBS jobs completed."
