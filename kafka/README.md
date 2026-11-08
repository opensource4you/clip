# kafka/

## ci/ — GitHub Actions 用（`.github/workflows/kafka-*.yml`）

| 檔案 | 用途 |
|---|---|
| `check_gradle_command.sh` | 檢查 workflow 輸入的 Gradle 指令只含 task、`--tests`、`-P`、少數 flag |
| `remove_cache_containers.sh` | 清掉掛著同一個 Gradle cache 的殘留 container，避免 cache lock 卡住 |
| `junit_failures.py` | 讀 JUnit XML，列出失敗 / flaky 的測試，`--summary` 給 `GITHUB_STEP_SUMMARY` |
| `ducktape_summary.py` | 讀 ducktape `report.json`，產 `GITHUB_STEP_SUMMARY` |

## local/ — 本機工具

| 檔案 | 用途 |
|---|---|
| `test_kafka.sh` | 用 Podman 把 Kafka 原始碼複製進容器跑測試，不動本機目錄 |
| `start_kafka_dist.sh` | 從 release tarball 在本機起 KRaft cluster（多 controller / broker） |
| `scan_cve.sh` | 從任一 git ref 建 release tarball，用 grype 掃 `libs/` |
| `pr-review.sh` | 把 PR 的 diff、改到的檔案、討論串整理成一份 markdown |
| `find_slow_tests.py` | 讀 JUnit XML，列出最慢的測試方法 |
