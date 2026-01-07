#!/usr/bin/env bash
set -e

# 切换到项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> 运行 rustfmt..."
BEFORE_DIFF="$(mktemp)"
AFTER_DIFF="$(mktemp)"
trap 'rm -f "$BEFORE_DIFF" "$AFTER_DIFF"' EXIT

git diff >"$BEFORE_DIFF"
cargo fmt --all
git diff >"$AFTER_DIFF"

if ! cmp -s "$BEFORE_DIFF" "$AFTER_DIFF"; then
  echo "==> rustfmt 产生了格式化改动，请先在本地运行 'cargo fmt --all' 并提交结果。" >&2
  git --no-pager diff
  exit 1
fi
echo "==> rustfmt 检查通过！"
