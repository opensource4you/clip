#!/bin/bash

# 在獨立的 Podman 容器裡跑 Kafka 測試。三種用法：
#   1. 互動模式：    ./test-kafka.sh
#   2. 執行一次指令：./test-kafka.sh -- ./gradlew :clients:test --tests ProducerConfigTest
#   3. 壓力測試：    ./test-kafka.sh -p 4 -r 30 -- ./gradlew :clients:test --tests ProducerConfigTest
#      同時開多個容器、每個重複跑多輪，用來驗證測試是否穩定
# 2 和 3 的 gradle 指令可以用 -t 代替：只給測試名稱，module 自動找
#                    ./test-kafka.sh -p 4 -r 30 -t ProducerConfigTest

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
TESTS=()

usage() {
    echo "用法: $(basename "$0") [選項] [本機專案目錄路徑] [-t 測試 | -- 指令]"
    echo ""
    echo "不帶 -t 也不帶 '-- 指令' 時進入容器的互動 shell。"
    echo "帶 '-- 指令' 時在專案工作區執行該指令，結束後容器即關閉，結束碼就是指令的結束碼。"
    echo "帶 -t 時只要給測試名稱，腳本會在專案目錄裡找出它屬於哪個 module 並組出 gradle 指令。"
    echo "再加上 -p 或 -r 就是壓力測試：同時開多個容器、每個重複執行指令多輪，任何一輪失敗就停掉所有容器。"
    echo ""
    echo "選項:"
    echo "  -c, --cpus <數量>      每個容器的 CPU 核心數"
    echo "                         (預設: 單一容器用系統一半；多個容器時為系統核心數 / 容器數)"
    echo "  -m, --memory <大小>    每個容器的記憶體，支援單位 m, g"
    echo "                         (預設: 單一容器用系統一半；多個容器時為系統的 3/4 / 容器數)"
    echo "  -t, --test <測試>      要跑的測試：Class、Class.method 或只填 method，可重複指定，"
    echo "                         也可以用逗號或空白分開多個。名稱對應到多個檔案時全部都跑。"
    echo "                         有 @Flaky 的會自動加上 -Pkafka.test.run.flaky=true"
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
    echo "  $(basename "$0") -p 4 -r 30 -t ProducerConfigTest -t testClassicRebalanceCompletesWhenOverCapacityMemberIsRejected"
    echo ""
    echo "若未提供路徑，預設使用: $DEFAULT_DIR"
    echo "重複執行 ./gradlew 時會自動在每個測試 task 後面加上 --rerun，否則第二輪起 Gradle 會直接跳過測試。"
    echo "指令需要用到 && 或 | 等 shell 語法時，把整段指令用引號包成一個參數。"
}

# ==========================================
# 3. 解析命令列參數
# ==========================================
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -c|--cpus) TARGET_CPU="$2"; shift ;;
        -m|--memory) TARGET_MEM="$2"; shift ;;
        -t|--test) read -ra _tests <<< "${2//,/ }"; TESTS+=("${_tests[@]}"); shift ;;
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

if [ "${#TESTS[@]}" -gt 0 ] && [ "${#CMD_ARGS[@]}" -gt 0 ]; then
    echo "錯誤: -t 和 '-- 指令' 只能擇一"
    exit 1
fi

STRESS_MODE=false
if [ "$PARALLEL" -gt 1 ] || [ "$ROUNDS" -gt 1 ]; then
    STRESS_MODE=true
    if [ "${#CMD_ARGS[@]}" -eq 0 ] && [ "${#TESTS[@]}" -eq 0 ]; then
        echo "錯誤: 使用 -p 或 -r 時必須用 -t 或 '-- 指令' 指定要重複執行的內容"
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
# 4.1 把 -t 的測試名稱解析成 gradle 指令
# ==========================================
# 作法和 .github/workflows/kafka-loop.yml 的 Resolve command 相同，只是改成找本機的專案目錄
if [ "${#TESTS[@]}" -gt 0 ]; then
    mapfile -t TEST_FILES < <(cd "$PROJECT_DIR" && find . \( -name build -o -name .git -o -name .gradle -o -name results \) -prune \
        -o -type f -path '*/src/test/*' \( -name '*.java' -o -name '*.scala' \) -print | sed 's|^\./||' | sort)

    declare -A BY_MODULE=()
    MODULES=()
    FLAKY_YES=()
    FLAKY_NO=()
    for t in "${TESTS[@]}"; do
        if ! [[ "$t" =~ ^[A-Za-z0-9_.]+$ ]]; then
            echo "錯誤: 看不懂「$t」：要填 Class、Class.method 或 method"
            exit 1
        fi
        # 第一個大寫開頭的段落當 class，前面是 package (可省略)，後面是 method (可省略)
        IFS=. read -ra parts <<< "$t"
        cls=""; pkg=""; method=""
        for idx in "${!parts[@]}"; do
            if [[ "${parts[$idx]}" =~ ^[A-Z] ]]; then
                cls="${parts[$idx]}"
                pkg=$(IFS=/; echo "${parts[*]:0:$idx}")
                method="${parts[$((idx + 1))]:-}"
                break
            fi
        done

        if [ -n "$cls" ]; then
            if [ -n "$pkg" ]; then
                re="/src/test/(java|scala)/${pkg}/${cls}\.(java|scala)$"
            else
                re="/src/test/(java|scala)/(.*/)?${cls}\.(java|scala)$"
            fi
            mapfile -t files < <(printf '%s\n' "${TEST_FILES[@]}" | grep -E "$re" || true)
            if [ "${#files[@]}" -eq 0 ]; then
                echo "錯誤: 在 $PROJECT_DIR 找不到測試 class ${cls}"
                exit 1
            fi
            # 同名的 class 有好幾個時，指定了 method 就只留有宣告該 method 的檔案
            # (都沒有的話可能定義在父類別，全部保留)；其餘情況全部都跑
            if [ "${#files[@]}" -gt 1 ] && [ -n "$method" ]; then
                mapfile -t declaring < <(cd "$PROJECT_DIR" && grep -l -E "(void|def)[[:space:]]+${method}[[:space:]]*\(" "${files[@]}" 2>/dev/null || true)
                [ "${#declaring[@]}" -gt 0 ] && files=("${declaring[@]}")
            fi
        else
            # 沒有 class，整個當成 method 名稱，找出宣告它的測試檔
            method="$t"
            mapfile -t files < <(cd "$PROJECT_DIR" && printf '%s\0' "${TEST_FILES[@]}" \
                | xargs -0 grep -l -E "(void|def)[[:space:]]+${method}[[:space:]]*\(" 2>/dev/null || true)
            if [ "${#files[@]}" -eq 0 ]; then
                echo "錯誤: 在 $PROJECT_DIR 找不到測試 method ${method}"
                exit 1
            fi
        fi
        if [ "${#files[@]}" -gt 1 ]; then
            echo "🔎 ${t} 對應到 ${#files[@]} 個檔案，全部都跑 (只想跑其中一個請加上 package 或 class)"
        fi
        bare_method=false; [ -z "$cls" ] && bare_method=true
        for file in "${files[@]}"; do
            if [ "$bare_method" = true ]; then
                cls=$(basename "$file"); cls="${cls%.*}"
                t="${cls}.${method}"
            fi
            # clients/clients-integration-tests/src/test/... -> :clients:clients-integration-tests
            dir="${file%%/src/test/*}"
            module=":${dir//\//:}"
            [ -n "${BY_MODULE[$module]:-}" ] || MODULES+=("$module")
            # 同一個 module 裡同名的 class 用同一個 --tests 就涵蓋了，不要重複加
            [[ " ${BY_MODULE[$module]:-} " == *" $t "* ]] || BY_MODULE[$module]+=" $t"
            echo "🔎 ${t} 在 ${file}"

            # Kafka 預設跳過 @Flaky 的測試，帶 -Pkafka.test.run.flaky=true 則「只跑」@Flaky 的，所以要先知道這個測試有沒有標。
            # @Flaky 可以標在 class 或 method 上。判斷方式：@Flaky 出現後、下一個敘述結束前碰到的宣告就是它標的對象
            scala=0; [[ "$file" == *.scala ]] && scala=1
            read -r class_flaky method_flaky method_found total < <(awk -v cls="$cls" -v m="$method" -v scala="$scala" '
                /@Flaky/ { f = 1; total++ }
                !seen && $0 ~ ("(class|object)[ \t]+" cls "([^A-Za-z0-9_]|$)") { seen = 1; if (f) cf = 1; f = 0; next }
                m != "" && $0 ~ ("(void|def)[ \t]+" m "[ \t]*\\(") { found = 1; if (f) mf = 1 }
                /;[ \t]*$/ || /^(import|package)[ \t]/ || (scala && /^[ \t]*}[ \t]*$/) { f = 0 }
                END { print cf + 0, mf + 0, found + 0, total + 0 }
            ' "$PROJECT_DIR/$file")
            if [ "$class_flaky" = 1 ] || [ "$method_flaky" = 1 ]; then
                FLAKY_YES+=("$t")
            else
                FLAKY_NO+=("$t")
                if [ -n "$method" ] && [ "$method_found" = 0 ]; then
                    echo "⚠️  在 ${file} 找不到 method ${method} (可能定義在父類別)，當作沒有 @Flaky"
                elif [ -z "$method" ] && [ "$total" -gt 0 ]; then
                    echo "⚠️  ${cls} 裡有 ${total} 個標了 @Flaky 的測試，這次不會跑到；要跑它們請填 ${cls}.method"
                fi
            fi
        done
    done

    CMD_ARGS=(./gradlew)
    for m in "${MODULES[@]}"; do
        CMD_ARGS+=("${m}:test")
        for t in ${BY_MODULE[$m]}; do CMD_ARGS+=(--tests "$t"); done
    done
    if [ "${#FLAKY_YES[@]}" -gt 0 ]; then
        # flaky 屬性是「只跑 @Flaky」，所以有標和沒標的不能放在同一次
        if [ "${#FLAKY_NO[@]}" -gt 0 ]; then
            echo "錯誤: 有 @Flaky 的 (${FLAKY_YES[*]}) 和沒有的 (${FLAKY_NO[*]}) 不能一起跑，請分兩次"
            exit 1
        fi
        CMD_ARGS+=(-Pkafka.test.run.flaky=true)
        echo "🔎 ${FLAKY_YES[*]} 有 @Flaky，自動加上 -Pkafka.test.run.flaky=true"
    fi
fi

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
    # 重複執行 gradle 時必須強制重跑測試，否則第二輪起測試會被視為 up-to-date 而跳過。
    # 在每個測試 task 後面加 --rerun 只重跑該 task；--rerun-tasks 會連編譯都重來，每輪慢很多
    if [ "$ROUNDS" -gt 1 ] && [[ "${CMD_ARGS[0]}" == *gradlew ]] \
        && [[ " ${CMD_ARGS[*]} " != *" --rerun-tasks "* ]] && [[ " ${CMD_ARGS[*]} " != *" --rerun "* ]]; then
        NEW_ARGS=()
        FOUND_TEST_TASK=false
        PREV=""
        for arg in "${CMD_ARGS[@]}"; do
            NEW_ARGS+=("$arg")
            # --tests 後面那個是測試名稱 (常以 Test 結尾)，不是 task
            if [[ "$PREV" != "--tests" ]] && [[ "$arg" =~ ^:?([A-Za-z0-9_-]+:)*[A-Za-z]*[tT]est$ ]]; then
                NEW_ARGS+=(--rerun)
                FOUND_TEST_TASK=true
            fi
            PREV="$arg"
        done
        if [ "$FOUND_TEST_TASK" = true ]; then
            CMD_ARGS=("${NEW_ARGS[@]}")
        else
            # 認不出測試 task 時退回整個重跑
            CMD_ARGS=("${CMD_ARGS[0]}" --rerun-tasks "${CMD_ARGS[@]:1}")
        fi
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
    [ "${#TESTS[@]}" -gt 0 ] && echo "🧪 指令: ${CMD_ARGS[*]}"
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
    # 清理期間忽略後續的 Ctrl+C，避免重複觸發
    trap '' INT TERM
    echo ""
    echo "🛑 收到中斷，正在停止容器 (需要幾秒)..."
    podman stop -t 0 "${NAMES[@]}" &> /dev/null
    wait
    echo "✅ 已停止，log 保留在 $LOG_DIR"
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

# 每 2 秒檢查一次容器有沒有結束，每 $INTERVAL 秒印一次進度。
# 任何一個容器失敗就把其他容器都停掉：失敗一次就要去看 log 了，不用等其他容器跑完
declare -A EXIT_CODE=()
ABORTED=false
elapsed=$INTERVAL
while :; do
    alive=0
    for i in $(seq 1 "$PARALLEL"); do
        [ -n "${EXIT_CODE[$i]:-}" ] && continue
        pid="${PIDS[$((i-1))]}"
        if kill -0 "$pid" 2>/dev/null; then
            alive=$(( alive + 1 ))
            continue
        fi
        wait "$pid"
        EXIT_CODE[$i]=$?
        if [ "${EXIT_CODE[$i]}" -ne 0 ] && [ "$ABORTED" = false ]; then
            ABORTED=true
            echo "[$(date +%H:%M:%S)]  #$i 失敗，停止其他容器..."
            for j in $(seq 1 "$PARALLEL"); do
                # 還沒結束的才是被我們停掉的，記成 stopped 和真正的失敗區分
                if [ -z "${EXIT_CODE[$j]:-}" ] && kill -0 "${PIDS[$((j-1))]}" 2>/dev/null; then
                    EXIT_CODE[$j]=stopped
                fi
            done
            podman stop -t 0 "${NAMES[@]}" &> /dev/null
            wait
        fi
    done
    [ "$ABORTED" = true ] && break

    if [ "$elapsed" -ge "$INTERVAL" ] || [ "$alive" -eq 0 ]; then
        line="[$(date +%H:%M:%S)]"
        for i in $(seq 1 "$PARALLEL"); do
            if [ -n "${EXIT_CODE[$i]:-}" ]; then
                line+="  #$i $(completed "$i")/$ROUNDS(結束)"
            else
                line+="  #$i $(completed "$i")/$ROUNDS"
            fi
        done
        echo "$line"
        elapsed=0
    fi
    [ "$alive" -eq 0 ] && break
    # sleep 放背景再 wait，Ctrl+C 才能立刻觸發 cleanup
    sleep 2 &
    wait $!
    elapsed=$(( elapsed + 2 ))
done

echo "---------------------------------------------------"
failed=0
stopped=0
for i in $(seq 1 "$PARALLEL"); do
    code="${EXIT_CODE[$i]:-1}"
    if [ "$code" = 0 ]; then
        echo "✅ #$i 通過 $(completed "$i")/$ROUNDS 輪"
    elif [ "$code" = stopped ]; then
        stopped=$(( stopped + 1 ))
        echo "⏹️  #$i 已中止，停下來之前通過 $(completed "$i")/$ROUNDS 輪"
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
echo "💥 $failed / $PARALLEL 個容器失敗，另外 $stopped 個被中止"
exit 1
