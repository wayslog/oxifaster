#!/usr/bin/env bash
set -e

# 切换到项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "==> Installing git hooks..."

# 确保 .git/hooks 目录存在
mkdir -p .git/hooks

# 创建 pre-commit hook 的符号链接
HOOK_TARGET="$SCRIPT_DIR/hooks/pre-commit"
HOOK_LINK=".git/hooks/pre-commit"

if [ -L "$HOOK_LINK" ]; then
    echo "==> Removing existing symlink..."
    rm "$HOOK_LINK"
elif [ -f "$HOOK_LINK" ]; then
    echo "==> Backing up existing pre-commit hook to pre-commit.backup..."
    mv "$HOOK_LINK" "$HOOK_LINK.backup"
fi

ln -s "$HOOK_TARGET" "$HOOK_LINK"

echo "==> Git hooks installed successfully!"
echo ""
echo "The pre-commit hook will now run:"
echo "  - cargo fmt --all -- --check"
echo "  - cargo clippy --all-targets --all-features -- -D warnings"
echo ""
echo "To skip the hook (not recommended), use: git commit --no-verify"
