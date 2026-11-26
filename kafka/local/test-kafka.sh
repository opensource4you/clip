#!/bin/bash

# 在獨立的 Podman 容器裡跑 Kafka 測試。三種用法：
#   1. 互動模式：    ./test-kafka.sh
#   2. 執行一次指令：./test-kafka.sh -- ./gradlew :clients:test --tests ProducerConfigTest
#   3. 壓力測試：    ./test-kafka.sh -p 4 -r 30 -- ./gradlew :clients:test --tests ProducerConfigTest
#      同時開多個容器、每個重複跑多輪，用來驗證測試是否穩定

# ==========================================
# 1. 自動偵測系統資源
# ==========================================
# 取得系統 CPU 核心數
if command -v nproc &> /dev/null; then
    TOTAL_CPU=$(nproc)
elif command -v sysctl &> /dev/null; then
    TOTAL_CPU=$(sysctl -n hw.ncpu)
else
    TOTAL_CPU=4 # 預設 fallback
fi

# 取得系統實體記憶體 (轉換為 MB)
if [ -f /proc/meminfo ]; then
    TOTAL_MEM_MB=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 ))
elif command -v sysctl &> /dev/null; then
    TOTAL_MEM_MB=$(( $(sysctl -n hw.memsize) / 1024 / 1024 ))
else
    TOTAL_MEM_MB=8192 # 預設 fallback 8GB
fi

# ==========================================
# 2. 設定預設變數
# ==========================================
DEFAULT_DIR="$HOME/project/kafka"
DEFAULT_LOG_ROOT="$HOME/test-kafka"
TARGET_CPU=""
TARGET_MEM=""
PROJECT_DIR_ARG=""
PARALLEL=1
ROUNDS=1
LOG_ROOT="$DEFAULT_LOG_ROOT"
INTERVAL=30
CMD_ARGS=()

usage() {
    echo "用法: $(basename "$0") [選項] [本機專案目錄路徑] [-- 指令]"
    echo ""
    echo "不帶 '-- 指令' 時進入容器的互動 shell。"
    echo "帶 '-- 指令' 時在專案工作區執行該指令，結束後容器即關閉，結束碼就是指令的結束碼。"
    echo "再加上 -p 或 -r 就是壓力測試：同時開多個容器、每個重複執行指令多輪，任何一輪失敗該容器就停止。"
    echo ""
    echo "選項:"
    echo "  -c, --cpus <數量>      每個容器的 CPU 核心數"
    echo "                         (預設: 單一容器用系統一半；多個容器時為系統核心數 / 容器數)"
    echo "  -m, --memory <大小>    每個容器的記憶體，支援單位 m, g"
    echo "                         (預設: 單一容器用系統一半；多個容器時為系統的 3/4 / 容器數)"
    echo "  -p, --parallel <數量>  同時執行的容器數 (預設: 1)"
    echo "  -r, --rounds <次數>    每個容器重複執行指令的輪數 (預設: 1)"
    echo "  -l, --log-dir <路徑>   壓力測試 log 的根目錄 (預設: $DEFAULT_LOG_ROOT)"
    echo "  -i, --interval <秒>    壓力測試的進度回報間隔 (預設: 30)"
    echo "  -h, --help             顯示此幫助訊息"
    echo ""
    echo "範例:"
    echo "  $(basename "$0") -c 4 -m 8g ~/my-java-project"
    echo "  $(basename "$0") -- ./gradlew :clients:test --tests ProducerConfigTest"
    echo "  $(basename "$0") -p 4 -r 30 -- ./gradlew :clients:test --tests ProducerConfigTest"
    echo ""
    echo "若未提供路徑，預設使用: $DEFAULT_DIR"
    echo "重複執行 ./gradlew 時會自動加上 --rerun-tasks，否則第二輪起 Gradle 會直接跳過測試。"
    echo "指令需要用到 && 或 | 等 shell 語法時，把整段指令用引號包成一個參數。"
}

# ==========================================
# 3. 解析命令列參數
# ==========================================
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--cpus) TARGET_CPU="$2"; shift ;;
        -m|--memory) TARGET_MEM="$2"; shift ;;
        -p|--parallel) PARALLEL="$2"; shift ;;
        -r|--rounds) ROUNDS="$2"; shift ;;
        -l|--log-dir) LOG_ROOT="$2"; shift ;;
        -i|--interval) INTERVAL="$2"; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; CMD_ARGS=("$@"); break ;;
        *) PROJECT_DIR_ARG="$1" ;; # 捕捉路徑參數
    esac
    shift
done

for value in "$PARALLEL" "$ROUNDS" "$INTERVAL"; do
    if ! [[ "$value" =~ ^[1-9][0-9]*$ ]]; then
        echo "錯誤: -p、-r、-i 必須是正整數，收到 '$value'"
        exit 1
    fi
done

STRESS_MODE=false
if [ "$PARALLEL" -gt 1 ] || [ "$ROUNDS" -gt 1 ]; then
    STRESS_MODE=true
    if [ "${#CMD_ARGS[@]}" -eq 0 ]; then
        echo "錯誤: 使用 -p 或 -r 時必須用 '-- 指令' 指定要重複執行的內容"
        exit 1
    fi
fi

# 依容器數平分資源，避免多個容器互相搶 CPU 造成假的逾時
if [ -z "$TARGET_CPU" ]; then
    if [ "$PARALLEL" -gt 1 ]; then
        TARGET_CPU=$(( TOTAL_CPU / PARALLEL ))
    else
        TARGET_CPU=$(( TOTAL_CPU / 2 ))
    fi
    [[ $TARGET_CPU -lt 1 ]] && TARGET_CPU=1
fi
if [ -z "$TARGET_MEM" ]; then
    if [ "$PARALLEL" -gt 1 ]; then
        TARGET_MEM="$(( TOTAL_MEM_MB * 3 / 4 / PARALLEL ))m"
    else
        TARGET_MEM="$(( TOTAL_MEM_MB / 2 ))m"
    fi
fi

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
# 6. 組出容器內要執行的指令
# ==========================================
RUN_CMD=""
if [ "${#CMD_ARGS[@]}" -eq 1 ]; then
    # 只有一個參數時當成完整的 shell 指令，可以包含 && 或 | 等語法
    RUN_CMD="${CMD_ARGS[0]}"
elif [ "${#CMD_ARGS[@]}" -gt 1 ]; then
    # 重複執行 gradle 時必須強制重跑，否則第二輪起測試會被視為 up-to-date 而跳過
    if [ "$ROUNDS" -gt 1 ] && [[ "${CMD_ARGS[0]}" == *gradlew ]] && [[ " ${CMD_ARGS[*]} " != *" --rerun-tasks "* ]]; then
        CMD_ARGS=("${CMD_ARGS[0]}" --rerun-tasks "${CMD_ARGS[@]:1}")
    fi
    # 用 %q 把每個參數各自跳脫，帶引號或空白的參數才能原樣傳進容器
    RUN_CMD=$(printf '%q ' "${CMD_ARGS[@]}")
fi

if [ "$STRESS_MODE" = true ]; then
    RUN_CMD="I=0
while [ \$I -lt $ROUNDS ] && { $RUN_CMD
}; do
  I=\$(( I + 1 ))
  echo \"Completed run: \$I\"
done
if [ \$I -lt $ROUNDS ]; then
  echo \"STRESS FAILED in run \$(( I + 1 ))\"
  if [ -d /out ]; then
    find . -type d \\( -path '*/build/reports/tests' -o -path '*/build/test-results' \\) -print0 2>/dev/null \\
      | tar --null -czf /out/reports.tar.gz -T - 2>/dev/null
  fi
  exit 1
fi
echo \"STRESS PASSED: \$I runs\""
fi

# ==========================================
# 7. 啟動 Podman (實體複製隔離)
# ==========================================
# run_container <容器名稱，可空> <掛載到 /out 的本機目錄，可空>
run_container() {
    local name="$1" output_dir="$2"
    local flags=()

    # 有終端機時才要求 TTY，這樣也能在背景執行並把輸出導到檔案
    if [ -t 0 ] && [ -t 1 ]; then
        flags+=(-it)
    fi
    [ -n "$name" ] && flags+=(--name "$name")
    if [ -n "$output_dir" ]; then
        mkdir -p "$output_dir"
        flags+=(-v "$(realpath "$output_dir"):/out:z")
    fi

    echo "🚀 啟動獨立並行測試容器 (實體複製隔離模式)..."
    echo "📂 唯讀掛載: $PROJECT_DIR -> /source_ro"
    echo "📦 專案工作區: $CONTAINER_WORKDIR (容器內獨立空間)"
    echo "⚙️  資源限制: CPU: $TARGET_CPU 核心, 記憶體: $TARGET_MEM"
    echo "---------------------------------------------------"

    podman run --rm "${flags[@]}" \
      --init \
      --stop-timeout=0 \
      --userns=keep-id \
      --cpus="$TARGET_CPU" \
      --memory="$TARGET_MEM" \
      --pids-limit=-1 \
      --ulimit nproc=16384:16384 \
      --ulimit nofile=65536:65536 \
      -e GRADLE_USER_HOME="$CONTAINER_GRADLE_HOME" \
      -e RUN_CMD="$RUN_CMD" \
      -v "$PROJECT_DIR":"/source_ro:ro,z" \
      -v "$GRADLE_USER_HOME":"$CONTAINER_GRADLE_HOME:O" \
      "$CUSTOM_IMAGE" \
      bash -c "
        echo '🔄 正在建立獨立測試環境 (複製檔案中，略過 results)...'
        mkdir -p $CONTAINER_WORKDIR
        # 用 tar 取代 cp 以便排除頂層的 results 目錄
        # --no-same-owner 避免容器內非 root 權限無法更改檔案擁有者
        tar -C /source_ro --exclude=./results -cf - . | tar -C $CONTAINER_WORKDIR --no-same-owner -xf -
        cd $CONTAINER_WORKDIR

        if [ -n \"\$RUN_CMD\" ]; then
          echo '✅ 環境準備完成！開始執行指定的指令'
          exec bash -c \"\$RUN_CMD\"
        fi

        echo '✅ 環境準備完成！現在可以執行測試 (建議使用 ./gradlew test --no-daemon)'
        echo '💡 Git 已安裝，可使用 git status 等指令進行版控操作。'
        exec bash
      "
}

# 互動模式，或只執行一次指令：直接在前景跑一個容器
if [ "$STRESS_MODE" = false ]; then
    if [ -z "$RUN_CMD" ] && ! { [ -t 0 ] && [ -t 1 ]; }; then
        echo "錯誤: 沒有終端機時必須用 '-- 指令' 指定要執行的內容"
        exit 1
    fi
    run_container "" ""
    exit $?
fi

# ==========================================
# 8. 壓力測試：同時開多個容器並回報進度
# ==========================================
RUN_ID="$(date +%Y%m%d-%H%M%S)"
LOG_DIR="$LOG_ROOT/$RUN_ID"
mkdir -p "$LOG_DIR"

NAMES=()
PIDS=()
cleanup() {
    echo ""
    echo "🛑 收到中斷，正在停止容器..."
    podman stop -t 0 "${NAMES[@]}" &> /dev/null
    exit 130
}
trap cleanup INT TERM

echo "🚀 啟動 $PARALLEL 個容器，每個跑 $ROUNDS 輪 (每個容器 CPU: $TARGET_CPU 核心, 記憶體: $TARGET_MEM)"
echo "📂 專案目錄: $PROJECT_DIR"
echo "📝 log 目錄: $LOG_DIR"
echo "   即時輸出: tail -f $LOG_DIR/run-1.log"
echo "🧪 指令: ${CMD_ARGS[*]}"
echo "---------------------------------------------------"

for i in $(seq 1 "$PARALLEL"); do
    name="test-kafka-$RUN_ID-$i"
    NAMES+=("$name")
    run_container "$name" "$LOG_DIR/run-$i" < /dev/null &> "$LOG_DIR/run-$i.log" &
    PIDS+=($!)
done

completed() {
    grep -c '^Completed run: ' "$LOG_DIR/run-$1.log" 2>/dev/null || true
}

while :; do
    alive=0
    line="[$(date +%H:%M:%S)]"
    for i in $(seq 1 "$PARALLEL"); do
        if kill -0 "${PIDS[$((i-1))]}" 2>/dev/null; then
            alive=$(( alive + 1 ))
            line+="  #$i $(completed "$i")/$ROUNDS"
        else
            line+="  #$i $(completed "$i")/$ROUNDS(結束)"
        fi
    done
    echo "$line"
    [ "$alive" -eq 0 ] && break
    # sleep 放背景再 wait，Ctrl+C 才能立刻觸發 cleanup
    sleep "$INTERVAL" &
    wait $!
done

echo "---------------------------------------------------"
failed=0
for i in $(seq 1 "$PARALLEL"); do
    wait "${PIDS[$((i-1))]}"
    code=$?
    if [ "$code" -eq 0 ]; then
        echo "✅ #$i 通過 $(completed "$i")/$ROUNDS 輪"
    else
        failed=$(( failed + 1 ))
        echo "❌ #$i 在第 $(( $(completed "$i") + 1 )) 輪失敗 (結束碼 $code)，log: $LOG_DIR/run-$i.log"
        grep -E ' FAILED$' "$LOG_DIR/run-$i.log" | sort -u | head -20 | sed 's/^/     /'
        [ -f "$LOG_DIR/run-$i/reports.tar.gz" ] && echo "     測試報告: $LOG_DIR/run-$i/reports.tar.gz"
    fi
done

if [ "$failed" -eq 0 ]; then
    echo "🎉 全部通過: $PARALLEL 個容器 x $ROUNDS 輪"
    exit 0
fi
echo "💥 $failed / $PARALLEL 個容器失敗"
exit 1