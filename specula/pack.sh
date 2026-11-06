#!/usr/bin/env bash
# Pick the files a person should review for this phase, plus the whole
# specula-output tar.gz when FULL_RESULTS=true. Writes ATTACH and PACKED to $GITHUB_ENV.
# Env: WORK, TARGET, REPORT_PHASE, FULL_RESULTS, RUNNER_TEMP, GITHUB_RUN_ID, GITHUB_ENV
set -euo pipefail

out="$WORK/.specula-output"
[[ -d "$WORK/kafka/.git" ]] && git -C "$WORK/kafka" diff > "$out/kafka-harness.patch"
case "$REPORT_PHASE" in
  code-analysis)       want="modeling-brief.md" ;;
  validation-workflow) want="spec/bug-report.md" ;;
  bug-confirmation)    want="confirmed-bugs.md" ;;
  bug-classification)  want="confirmed-bugs.md bug-severity.md summary.md" ;;
  *)                   want="" ;;
esac
attach=""
for f in $want; do
  [[ -f "$out/$f" ]] || continue
  dst="$RUNNER_TEMP/specula-attach/${TARGET}-$(basename "$f")"
  mkdir -p "$(dirname "$dst")" && cp "$out/$f" "$dst" && attach="$attach $dst"
done
if [[ "$FULL_RESULTS" == "true" ]]; then
  pack="$RUNNER_TEMP/specula-attach/specula-${TARGET}-${REPORT_PHASE}-${GITHUB_RUN_ID}.tar.gz"
  mkdir -p "$(dirname "$pack")"
  # 解開後是一般的 specula-output/，不是隱藏的 .specula-output/
  tar -czf "$pack" -C "$WORK" --transform 's,^\.specula-output,specula-output,' .specula-output
  attach="$attach $pack"
fi
echo "ATTACH=$attach" >> "$GITHUB_ENV"
echo "PACKED=true" >> "$GITHUB_ENV"
ls -la "$RUNNER_TEMP/specula-attach" 2>/dev/null || echo "no extra files for $REPORT_PHASE"
