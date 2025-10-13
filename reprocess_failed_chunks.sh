#!/bin/bash

# 실패한 청크 재처리 스크립트
# post_proc.csv 파일을 읽어서 실패한 청크들을 재처리합니다.

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 사용법 출력
usage() {
    echo "Usage: $0 [OPTIONS] [CSV_FILE]"
    echo ""
    echo "Options:"
    echo "  --dry-run        실제 처리 없이 확인만 (Dry-run 모드)"
    echo "  --ship SHIP_ID   특정 선박만 처리 (예: IMO9986063)"
    echo "  -d, --delay N    청크 처리 사이 지연 시간 (초, 기본값: 0.5)"
    echo "  -h, --help       도움말 표시"
    echo ""
    echo "Examples:"
    echo "  $0                              # 기본 실행"
    echo "  $0 --dry-run                    # Dry-run 모드"
    echo "  $0 --ship IMO9986063            # 특정 선박만 처리"
    echo "  $0 -d 1.0                       # 1초 지연"
    echo "  $0 my_failed_chunks.csv         # 다른 CSV 파일 사용"
    exit 1
}

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}   Failed Chunk Reprocessor${NC}"
echo -e "${BLUE}================================================${NC}"

# 인자 파싱
DRY_RUN=""
SHIP_FILTER=""
DELAY=""
CSV_FILE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --ship)
            SHIP_FILTER="--ship $2"
            shift 2
            ;;
        -d|--delay)
            DELAY="-d $2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            CSV_FILE="$1"
            shift
            ;;
    esac
done

# CSV 파일 기본값 설정
if [ -z "$CSV_FILE" ]; then
    CSV_FILE="post_proc.csv"
fi

# CSV 파일 확인
if [ ! -f "$CSV_FILE" ]; then
    echo -e "${RED}❌ Error: CSV file not found: $CSV_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Found CSV file: $CSV_FILE${NC}"

# 라인 수 확인
LINE_COUNT=$(wc -l < "$CSV_FILE" | tr -d ' ')
echo -e "${BLUE}📊 Total chunks in file: $LINE_COUNT${NC}"

# Dry-run 모드 표시
if [ -n "$DRY_RUN" ]; then
    echo -e "${YELLOW}🔍 Running in DRY-RUN mode (no actual processing)${NC}"
fi

# 선박 필터 표시
if [ -n "$SHIP_FILTER" ]; then
    SHIP_ID=$(echo "$SHIP_FILTER" | cut -d' ' -f2)
    echo -e "${BLUE}🚢 Filtering by ship: $SHIP_ID${NC}"
fi

# 확인 메시지 (Dry-run이 아닐 경우만)
if [ -z "$DRY_RUN" ]; then
    echo ""
    echo -e "${YELLOW}⚠️  This will reprocess all failed chunks.${NC}"
    echo -e "${YELLOW}   Press Ctrl+C to cancel, or Enter to continue...${NC}"
    read
fi

# Python 스크립트 실행
echo -e "${BLUE}🚀 Starting reprocessing...${NC}"
echo ""

# 명령어 구성
CMD="python3 reprocess_failed_chunks.py -f $CSV_FILE $DRY_RUN $SHIP_FILTER $DELAY"

echo -e "${BLUE}📝 Command: $CMD${NC}"
echo ""

eval $CMD

# 결과 확인
EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}   ✅ Reprocessing completed successfully!${NC}"
    echo -e "${GREEN}================================================${NC}"
else
    echo -e "${RED}================================================${NC}"
    echo -e "${RED}   ❌ Reprocessing completed with errors${NC}"
    echo -e "${RED}================================================${NC}"
    
    # 실패한 청크가 있는지 확인 (Dry-run이 아닐 경우만)
    if [ -z "$DRY_RUN" ] && [ -f "failed_chunks_after_reprocess.csv" ]; then
        FAILED_COUNT=$(wc -l < "failed_chunks_after_reprocess.csv" | tr -d ' ')
        echo -e "${YELLOW}⚠️  $FAILED_COUNT chunks still failed${NC}"
        echo -e "${YELLOW}   Check failed_chunks_after_reprocess.csv for details${NC}"
    fi
fi

echo ""
echo -e "${BLUE}📝 로그 파일:${NC}"
if [ -L "logs/reprocess_latest.log" ]; then
    ACTUAL_LOG=$(readlink "logs/reprocess_latest.log")
    echo -e "${BLUE}   logs/$ACTUAL_LOG${NC}"
    echo -e "${BLUE}   logs/reprocess_latest.log (심볼릭 링크)${NC}"
else
    echo -e "${BLUE}   logs/reprocess_*.log${NC}"
fi

echo ""
echo -e "${BLUE}💡 로그 확인:${NC}"
echo -e "${BLUE}   tail -f logs/reprocess_latest.log${NC}"
echo -e "${BLUE}   또는${NC}"
echo -e "${BLUE}   ls -lt logs/reprocess_*.log | head -1${NC}"

exit $EXIT_CODE

