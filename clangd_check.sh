#!/usr/bin/env bash
# ============================================================
#  clangd_check.sh
#  Description:
#    Run clangd semantic analysis on a single C++ source file
#    using compile_commands.json from be/build_Release.
# ============================================================

set -euo pipefail

# --- Default Configuration ---
PROJECT_ROOT="$(pwd)"
BUILD_DIR="$PROJECT_ROOT/be/build_Release"

# --- Usage ---
usage() {
  cat << EOF
Usage: $0 [OPTIONS] <file>

Run clangd --check on a single C++ source file and output diagnostics.

OPTIONS:
  -b, --build-dir   Build directory with compile_commands.json (default: be/build_Release)
  -h, --help        Show this help message

EXAMPLES:
  $0 be/src/runtime/exec/exec_node.cpp     # Check a specific file
  $0 -b be/build_Debug be/src/main.cpp     # Use a different build directory

EOF
  exit 0
}

# --- Parse Arguments ---
FILE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    -b|--build-dir)
      BUILD_DIR="$PROJECT_ROOT/$2"
      shift 2
      ;;
    -h|--help)
      usage
      ;;
    -*)
      echo "❌ Unknown option: $1"
      usage
      ;;
    *)
      if [ -z "$FILE" ]; then
        FILE="$1"
      else
        echo "❌ Too many arguments: $1"
        usage
      fi
      shift
      ;;
  esac
done

# --- Checks ---
if [ -z "$FILE" ]; then
  echo "❌ Error: No file specified"
  usage
fi

if [ ! -f "$FILE" ]; then
  echo "❌ Error: File $FILE does not exist"
  exit 1
fi

if ! command -v clangd &> /dev/null; then
  echo "❌ Error: clangd not found in PATH."
  echo "Install clangd (e.g., via brew install llvm or apt install clangd)."
  exit 1
fi

if [ ! -f "$BUILD_DIR/compile_commands.json" ]; then
  echo "❌ Error: compile_commands.json not found in $BUILD_DIR"
  exit 1
fi

# Run clangd and output directly to stdout/stderr
clangd --check="$FILE" \
       --compile-commands-dir="$BUILD_DIR" \
       --log=error \
       --background-index=0
