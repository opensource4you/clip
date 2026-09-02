#!/bin/bash

# ==========================================
# 1. 自動偵測系統資源並計算一半作為預設值
# ==========================================
# 取得系統 CPU 核心數
if command -v nproc &> /dev/null; then
    TOTAL_CPU=$(nproc)
elif command -v sysctl &> /dev/null; then
    TOTAL_CPU=$(sysctl -n hw.ncpu)
else
    TOTAL_CPU=4 # 預設 fallback
fi
HALF_CPU=$(( TOTAL_CPU / 2 ))
[[ $HALF_CPU -lt 1 ]] && HALF_CPU=1

# 取得系統實體記憶體並計算一半 (轉換為 MB)
if [ -f /proc/meminfo ]; then
    TOTAL_MEM_KB=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
    HALF_MEM_MB=$(( TOTAL_MEM_KB / 1024 / 2 ))
elif command -v sysctl &> /dev/null; then
    TOTAL_MEM_BYTES=$(sysctl -n hw.memsize)
    HALF_MEM_MB=$(( TOTAL_MEM_BYTES / 1024 / 1024 / 2 ))
else
    HALF_MEM_MB=4096 # 預設 fallback 4GB
fi

# ==========================================
# 2. 設定預設變數
# ==========================================
DEFAULT_DIR="$HOME/project/kafka"
TARGET_CPU=$HALF_CPU
TARGET_MEM="${HALF_MEM_MB}m"
PROJECT_DIR_ARG=""

# ==========================================
# 3. 解析命令列參數
# ==========================================
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--cpus) TARGET_CPU="$2"; shift ;;
        -m|--memory) TARGET_MEM="$2"; shift ;;
        -h|--help)
            echo "用法: $0 [選項] [本機專案目錄路徑]"
            echo "選項:"
            echo "  -c, --cpus <數量>     設定容器使用的 CPU 核心數 (預設: 系統一半, 即 $HALF_CPU)"
            echo "  -m, --memory <大小>   設定容器記憶體大小，支援單位 m, g (預設: 系統一半, 即 ${HALF_MEM_MB}m)"
            echo "  -h, --help            顯示此幫助訊息"
            echo ""
            echo "範例: $0 -c 4 -m 8g ~/my-java-project"
            echo "若未提供路徑，預設使用: $DEFAULT_DIR"
            exit 0
            ;;
        *) PROJECT_DIR_ARG="$1" ;; # 捕捉路徑參數
    esac
    shift
done

# ==========================================
# 4. 路徑與環境檢查
# ==========================================
# 如果沒有提供路徑參數，就使用預設路徑
PROJECT_DIR=$(realpath "${PROJECT_DIR_ARG:-$DEFAULT_DIR}")
GRADLE_USER_HOME="${HOME}/.gradle"

# 將工作目錄移至容器內所有使用者皆可寫入的 /tmp 目錄
CONTAINER_WORKDIR="/tmp/workspace"
CONTAINER_GRADLE_HOME="/tmp/gradle-cache"

if [ ! -d "$PROJECT_DIR" ]; then
  echo "錯誤: 找不到指定的專案目錄 '$PROJECT_DIR'"
  exit 1
fi

mkdir -p "$GRADLE_USER_HOME"

# ==========================================
# 5. 確保映像檔包含 Git (動態建置)
# ==========================================
BASE_IMAGE="docker.io/library/eclipse-temurin:21-jdk"
CUSTOM_IMAGE="eclipse-temurin-21-jdk-with-git"

# 檢查自訂映像檔是否已存在，若不存在則透過標準輸入 (stdin) 動態建置
if ! podman image inspect "$CUSTOM_IMAGE" &> /dev/null; then
    echo "📦 首次執行或找不到自訂映像檔：正在建立包含 Git 的環境映像檔..."
    podman build -t "$CUSTOM_IMAGE" - <<EOF
FROM $BASE_IMAGE
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
EOF
    echo "✅ 自訂映像檔建置完成！"
fi

# ==========================================
# 6. 啟動 Podman (實體複製隔離)
# ==========================================
echo "🚀 啟動獨立並行測試容器 (實體複製隔離模式)..."
echo "📂 唯讀掛載: $PROJECT_DIR -> /source_ro"
echo "📦 專案工作區: $CONTAINER_WORKDIR (容器內獨立空間)"
echo "⚙️  資源限制: CPU: $TARGET_CPU 核心, 記憶體: $TARGET_MEM"
echo "---------------------------------------------------"

podman run --rm -it \
  --userns=keep-id \
  --cpus="$TARGET_CPU" \
  --memory="$TARGET_MEM" \
  --pids-limit=-1 \
  --ulimit nproc=16384:16384 \
  --ulimit nofile=65536:65536 \
  -e GRADLE_USER_HOME="$CONTAINER_GRADLE_HOME" \
  -v "$PROJECT_DIR":"/source_ro:ro,z" \
  -v "$GRADLE_USER_HOME":"$CONTAINER_GRADLE_HOME:O" \
  "$CUSTOM_IMAGE" \
  bash -c "
    echo '🔄 正在建立獨立測試環境 (複製檔案中)...'
    mkdir -p $CONTAINER_WORKDIR
    # 加上 --no-preserve=ownership 避免容器內非 root 權限無法更改檔案擁有者
    cp -a --no-preserve=ownership /source_ro/. $CONTAINER_WORKDIR/
    cd $CONTAINER_WORKDIR

    echo '✅ 環境準備完成！現在可以執行測試 (建議使用 ./gradlew test --no-daemon)'
    echo '💡 Git 已安裝，可使用 git status 等指令進行版控操作。'
    exec bash
  "