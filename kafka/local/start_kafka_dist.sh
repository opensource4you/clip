#!/bin/bash

# 預設值
BROKERS=0
CONTROLLERS=0
BROKER_CONFIGS=""
CONTROLLER_CONFIGS=""
DIST_FILE=""

# 1. 參數解析
while [[ $# -gt 0 ]]; do
  case $1 in
    --brokers) BROKERS="$2"; shift 2 ;;
    --controllers) CONTROLLERS="$2"; shift 2 ;;
    --broker-configs) BROKER_CONFIGS="$2"; shift 2 ;;
    --controller-configs) CONTROLLER_CONFIGS="$2"; shift 2 ;;
    --distribution) DIST_FILE="$2"; shift 2 ;;
    *) echo "未知的參數: $1"; exit 1 ;;
  esac
done

# 2. 驗證參數與檔案
if [[ $CONTROLLERS -le 0 || $BROKERS -le 0 ]]; then
  echo "錯誤: --brokers 和 --controllers 的數量必須大於 0。"
  exit 1
fi

if [[ -n "$DIST_FILE" && ! -f "$DIST_FILE" ]]; then
  echo "錯誤: 找不到指定的 Kafka 壓縮檔 $DIST_FILE"
  exit 1
fi

if ! command -v java &> /dev/null; then
    echo "錯誤: 找不到 java 指令，本機啟動需要安裝 Java JRE/JDK。"
    exit 1
fi

# 3. 準備工作環境 (加入時間戳記來隔離每次的啟動)
BASE_WORKSPACE="/tmp/kafka_workspace"
RUN_ID=$(date +"%Y%m%d_%H%M%S")
RUN_WORKSPACE="$BASE_WORKSPACE/run_$RUN_ID"

KAFKA_HOME="$BASE_WORKSPACE/kafka" # 共用同一套 Kafka 主程式位置
CONFIGS_DIR="$RUN_WORKSPACE/configs"
DATA_DIR="$RUN_WORKSPACE/data"
LOGS_DIR="$RUN_WORKSPACE/logs"

echo "==== 準備 Kafka 工作環境 ===="
mkdir -p "$CONFIGS_DIR" "$DATA_DIR" "$LOGS_DIR"

# 檢查是否需要解壓縮 (依照是否傳入 distribution 來決定)
if [[ -n "$DIST_FILE" ]]; then
  echo "偵測到指定了新的 Kafka 壓縮檔，正在清理舊有程式並重新解壓縮 $DIST_FILE 至 $KAFKA_HOME ..."
  rm -rf "$KAFKA_HOME"
  mkdir -p "$KAFKA_HOME"
  tar -xzf "$DIST_FILE" -C "$KAFKA_HOME" --strip-components=1
else
  if [ ! -f "$KAFKA_HOME/bin/kafka-server-start.sh" ]; then
    echo "錯誤: 尚未建立 Kafka 主程式，必須使用 --distribution 指定壓縮檔以進行初始化。"
    exit 1
  fi
  echo "未指定新的壓縮檔，發現已解壓縮的 Kafka 主程式，將直接重複使用: $KAFKA_HOME"
fi

echo "本次執行的專屬工作目錄: $RUN_WORKSPACE"

# 4. 取得 KRaft Cluster ID
echo "正在產生 KRaft Cluster ID..."
CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
echo "Cluster ID: $CLUSTER_ID"

# 5. 取得本機對外的 IP 位址
HOST_ADDRESS=$(hostname -I 2>/dev/null | awk '{print $1}')
if [[ -z "$HOST_ADDRESS" ]]; then
  HOST_ADDRESS="127.0.0.1" # 如果抓不到，退回到 localhost 安全模式
  echo "警告: 無法偵測外部 IP，將回退使用 127.0.0.1。"
else
  echo "偵測到本機對外位址為: $HOST_ADDRESS"
fi

# 6. 組合 Controller Quorum Voters 字串
VOTERS=""
for i in $(seq 1 $CONTROLLERS); do
  CTRL_PORT=$((19080 + i))
  VOTERS="${VOTERS}${i}@${HOST_ADDRESS}:${CTRL_PORT},"
done
VOTERS=${VOTERS%,}

# 處理自訂配置函式
append_custom_configs() {
  local configs="$1"
  local target_file="$2"
  if [[ -n "$configs" ]]; then
    IFS=',' read -ra CONF_ARRAY <<< "$configs"
    for conf in "${CONF_ARRAY[@]}"; do
      echo "$conf" >> "$target_file"
    done
  fi
}

# 7. 啟動 Controllers
echo "==== 準備啟動 $CONTROLLERS 個 Controllers ===="
for i in $(seq 1 $CONTROLLERS); do
  CONF_FILE="$CONFIGS_DIR/controller-$i.properties"
  CTRL_PORT=$((19080 + i))
  NODE_DATA_DIR="$DATA_DIR/controller-$i"

  cat <<EOF > "$CONF_FILE"
process.roles=controller
node.id=$i
controller.quorum.voters=$VOTERS
controller.listener.names=CONTROLLER
listeners=CONTROLLER://0.0.0.0:$CTRL_PORT
log.dirs=$NODE_DATA_DIR
EOF
  append_custom_configs "$CONTROLLER_CONFIGS" "$CONF_FILE"

  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_FILE" > /dev/null

  CTRL_JMX_PORT=$((9980 + i))
  JMX_PORT=$CTRL_JMX_PORT nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONF_FILE" > "$LOGS_DIR/controller-$i.log" 2>&1 &
  echo "已在背景啟動 controller-$i (Port: $CTRL_PORT, JMX: $CTRL_JMX_PORT, 日誌: $LOGS_DIR/controller-$i.log)"
done

sleep 3

# 8. 啟動 Brokers
echo "==== 準備啟動 $BROKERS 個 Brokers ===="
for i in $(seq 1 $BROKERS); do
  node_id=$((CONTROLLERS + i))
  CONF_FILE="$CONFIGS_DIR/broker-$i.properties"
  BROKER_PORT=$((19090 + i))
  NODE_DATA_DIR="$DATA_DIR/broker-$i"

  cat <<EOF > "$CONF_FILE"
process.roles=broker
node.id=$node_id
controller.quorum.voters=$VOTERS
controller.listener.names=CONTROLLER
listeners=PLAINTEXT://0.0.0.0:$BROKER_PORT
advertised.listeners=PLAINTEXT://$HOST_ADDRESS:$BROKER_PORT
log.dirs=$NODE_DATA_DIR
EOF
  append_custom_configs "$BROKER_CONFIGS" "$CONF_FILE"

  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$CLUSTER_ID" -c "$CONF_FILE" > /dev/null

  BROKER_JMX_PORT=$((9990 + i))
  JMX_PORT=$BROKER_JMX_PORT nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$CONF_FILE" > "$LOGS_DIR/broker-$i.log" 2>&1 &
  echo "已在背景啟動 broker-$i (Node ID: $node_id, 對外 Port: $BROKER_PORT, JMX: $BROKER_JMX_PORT)"
done

echo "==== 啟動完成！ ===="
echo "本次執行的資料與配置皆已放置於: $RUN_WORKSPACE"
echo "--- 外部連線資訊 (Broker) ---"
echo "Brokers 端點: $HOST_ADDRESS:19091, $HOST_ADDRESS:19092 ..."
echo "Brokers JMX:  $HOST_ADDRESS:9991, $HOST_ADDRESS:9992 ..."
echo "--- 外部連線資訊 (Controller) ---"
echo "Controllers 端點: $HOST_ADDRESS:19081, $HOST_ADDRESS:19082 ..."
echo "Controllers JMX:  $HOST_ADDRESS:9981, $HOST_ADDRESS:9982 ..."
echo ""
echo "若要查看服務啟動狀況，請檢查: $LOGS_DIR 底下的 log 檔案。"
echo "再次啟動前，請確保使用 'pkill -f kafka.Kafka' 指令清除舊的行程，避免 Port 被佔用。"