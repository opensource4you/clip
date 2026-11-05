#!/usr/bin/env bash
# 檢查 kafka-loop 的 command 參數只包含 Gradle 指令，避免手滑或亂打。
# 允許：./gradlew 開頭，後面是 task、--tests <pattern>、-x <task>、-Pkey=value、少數常用 flag。
# 用法: check_gradle_command.sh "<command>"
# 注意：這擋不了 fork 裡的 build script 本身，只是限制輸入的格式。
set -f -u

err() { echo "::error::command 不合法：$*" >&2; exit 1; }

read -ra args <<< "${1:-}"
[[ ${#args[@]} -ge 2 && "${args[0]}" == "./gradlew" ]] || err "要以 ./gradlew 開頭，後面至少一個 task"

task_re='^:?[A-Za-z][A-Za-z0-9_.-]*(:[A-Za-z][A-Za-z0-9_.-]*)*$'
pattern_re='^[A-Za-z0-9_.*-]+$'
prop_re='^-P[A-Za-z][A-Za-z0-9_.]*=[A-Za-z0-9_.,:*-]*$'
flags=" --rerun-tasks --rerun --info --stacktrace --continue --no-build-cache --offline "

for ((i = 1; i < ${#args[@]}; i++)); do
  a=${args[i]}
  case "$a" in
    --tests)
      ((i++)); [[ "${args[i]:-}" =~ $pattern_re ]] || err "--tests 後面要接測試名稱（英數、. _ - *），收到「${args[i]:-}」" ;;
    -x|--exclude-task)
      ((i++)); [[ "${args[i]:-}" =~ $task_re ]] || err "$a 後面要接 task，收到「${args[i]:-}」" ;;
    -P*)
      [[ "$a" =~ $prop_re ]] || err "不支援的屬性「$a」" ;;
    -*)
      [[ "$flags" == *" $a "* ]] || err "不支援的選項「$a」，可用：$flags" ;;
    *)
      [[ "$a" =~ $task_re ]] || err "不是合法的 task「$a」" ;;
  esac
done
echo "command ok: ${args[*]}"
