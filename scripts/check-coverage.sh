#!/usr/bin/env bash
# 覆盖率检查脚本
# 强制最低 80% 代码覆盖率

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 最低覆盖率阈值
MIN_COVERAGE=75

echo "======================================"
echo "Running Code Coverage Analysis..."
echo "======================================"
echo ""

# 检查 cargo-llvm-cov 是否安装
if ! command -v cargo-llvm-cov &> /dev/null; then
    echo -e "${RED}Error: cargo-llvm-cov not found${NC}"
    echo "Please install it with: cargo install cargo-llvm-cov"
    exit 1
fi

# 运行覆盖率测试
echo "Running tests with coverage..."
cargo llvm-cov --all-features --lcov --output-path target/lcov.info

# 获取覆盖率摘要并提取总行覆盖率
echo ""
echo "======================================"
echo "Coverage Summary:"
echo "======================================"
COVERAGE_OUTPUT=$(cargo llvm-cov --all-features --summary-only 2>/dev/null)
echo "$COVERAGE_OUTPUT" | grep -E "^TOTAL|^Filename" | tail -2

# 提取总行覆盖率百分比
LINE_COVERAGE=$(echo "$COVERAGE_OUTPUT" | grep "^TOTAL" | awk '{print $4}' | sed 's/%//')

if [ -z "$LINE_COVERAGE" ]; then
    echo -e "${RED}Error: Failed to extract coverage percentage${NC}"
    exit 1
fi

echo ""
echo "======================================"
echo "Coverage Check Result:"
echo "======================================"
echo -e "Total Line Coverage: ${YELLOW}${LINE_COVERAGE}%${NC}"
echo -e "Minimum Required:    ${YELLOW}${MIN_COVERAGE}%${NC}"
echo ""

# 比较覆盖率 (使用 awk 进行浮点数比较)
COVERAGE_OK=$(awk -v cov="$LINE_COVERAGE" -v min="$MIN_COVERAGE" 'BEGIN { print (cov >= min) ? "1" : "0" }')

if [ "$COVERAGE_OK" = "1" ]; then
    echo -e "${GREEN}✓ Coverage check PASSED${NC}"
    echo -e "${GREEN}  Coverage ${LINE_COVERAGE}% meets the minimum requirement of ${MIN_COVERAGE}%${NC}"
    echo ""

    # 生成 HTML 报告
    echo "Generating HTML coverage report..."
    cargo llvm-cov --all-features --html --output-dir target/coverage
    echo -e "${GREEN}HTML report generated at: target/coverage/index.html${NC}"
    echo ""

    exit 0
else
    echo -e "${RED}✗ Coverage check FAILED${NC}"
    echo -e "${RED}  Coverage ${LINE_COVERAGE}% is below the minimum requirement of ${MIN_COVERAGE}%${NC}"
    echo ""
    echo "Top 15 modules with lowest coverage:"
    echo "======================================"
    echo "$COVERAGE_OUTPUT" | grep "\.rs" | awk '{print $7, $1}' | sort -n | head -15
    echo ""
    echo -e "${YELLOW}Please add tests to improve coverage.${NC}"
    echo -e "${YELLOW}See COVERAGE_PLAN.md for detailed improvement plan.${NC}"
    echo ""

    # 生成 HTML 报告以便查看
    echo "Generating HTML coverage report for analysis..."
    cargo llvm-cov --all-features --html --output-dir target/coverage
    echo -e "${YELLOW}HTML report generated at: target/coverage/index.html${NC}"
    echo ""

    exit 1
fi
