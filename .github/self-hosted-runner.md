# Self-hosted runner（社群 VM）

`kafka-full` 和 `kafka-e2e` 跑在社群 VM 上；`kafka-loop` 可以在表單的 `runner` 選 `github`（GitHub 的機器，預設）或 `community-vm`（社群 VM）。下表是排到社群 VM 時用的 runner。runner 主動往外連 GitHub 拉 job，VM 不用開任何對外 port。

| workflow   | runs-on 的 label | 每台 runner 數 | 說明 |
|------------|------------------|----------------|------|
| kafka-loop | `kafka`          | 多個           | 一個 runner 跑一個容器，要平行幾個就註冊幾個 |
| kafka-full | `kafka-full`     | **一個**       | 完整跑一次 JUnit，Gradle 自己平行，吃滿整台機器 |
| kafka-e2e  | `kafka-e2e`      | **一個**       | e2e 會吃滿整台機器，同時只能跑一個 |
| specula    | `specula`        | **一個**       | Specula（TLA+）跑 Kafka，TLC 吃滿整台機器，一跑可能一兩天；用專屬的機器 |

一個 runner 同時只跑一個 job，所以「一台機器同時跑幾個 job」由那台註冊幾個 runner 決定。
要吃滿整台機器的 workflow（full、e2e）只能排在「只註冊一個 runner」的機器上，
目前做法是 e2e 機器的 runner 同時帶 `kafka-e2e` 和 `kafka-full` 兩個 label：
同一台一次只會跑其中一個，也不會被 loop 擠到。

已經註冊好的 runner 可以直接加 label，不用重裝（`<ID>` 用下面第一行查）：

```bash
gh api repos/opensource4you/clip/actions/runners -q '.runners[] | [.id, .name, ([.labels[].name] | join(","))] | @tsv'
gh api -X POST repos/opensource4you/clip/actions/runners/<ID>/labels -f 'labels[]=kafka-full'
```

## 安全設定（clip 是 public repo，務必先做）

任何人都能對 public repo 開 PR，而 `pull_request` 事件會用 PR 裡的 workflow 檔，
所以有人可以在 PR 裡新增一個 `runs-on: self-hosted` 的 workflow，在 VM 上跑任意程式。

1. 外人的 PR 要 approve 才跑 workflow（網頁在 Settings -> Actions -> General）：
   ```bash
   gh api -X PUT repos/opensource4you/clip/actions/permissions/fork-pr-contributor-approval \
     -f approval_policy=all_external_contributors
   ```
   之後看到外人的 PR 等待 approve 時，先確認沒改到 `.github/` 再按。
2. VM 上不要放任何憑證（SSH key、雲端 token、gh 登入），網路跟內部環境隔開。
3. log 是公開的，workflow 不要印出任何機密。`github/Jenkinsfile` 那種用參數傳 token 的作法
   不能直接搬過來（workflow_dispatch 的 input 會顯示在 run 頁面）。

## 需要的工具

- 共通：git、python3、zip、podman（rootless 即可）。loop 和 full 都在 eclipse-temurin 容器裡跑，host 不需要 JDK
- e2e 另外需要 host 上的 JDK（含 `javac`），因為 build 是在 host 上跑 `./gradlew`。
  - 新版 Kafka 的 ducker-ak 會自動偵測 docker 或 podman，trunk 用 podman 就能跑。
    較舊的分支可能只支援 docker。
  - Host 上的 JDK 版本決定能 build 哪些分支，例如 JDK 25 可以 build trunk，但舊分支的 Gradle 可能不支援。
- specula 需要 host 上的 JDK、Maven、uv、pip（Fedora：`sudo dnf install -y python3-pip uv maven java-25-openjdk-devel`）。
  Claude Code 第一次跑時會自動裝到 `~/.local/bin`。

## specula

結果可能是還沒公開的漏洞，所以 Claude 的輸出只寫進 VM 上的檔案，
結果用 Slack 私訊給觸發的人（`specula-bot`，只有 `chat:write`、`files:write`、`im:write`），不上傳 artifact。
public 的 summary 只有狀態、輪數、花費和時間。

- Secrets 放在 environment `specula`（只有 `main` 能用）：
  `ANTHROPIC_API_KEY`、`SLACK_BOT_TOKEN`、`SPECULA_SLACK_USERS`（`{"github 帳號": "Slack member ID"}`）。
  `SPECULA_SLACK_USERS` 也是名單：不在裡面的人觸發，job 一開始就失敗，不會花錢。
- 每個 target 的工作目錄在 `~/specula-work/<target>`，phase 之間接著用；表單勾 `reset` 才會從頭開始。
  每次 Claude 的完整 log 在 `~/specula-work/<target>/.specula-output/runs/`。

## 安裝

先在自己電腦上拿註冊 token（1 小時內有效，可重複用來註冊多個 runner）：

```bash
gh api -X POST repos/opensource4you/clip/actions/runners/registration-token -q .token
```

在 VM 上（以 Fedora 為例），`LABEL` 和 `N` 依上表設定：

```bash
TOKEN=貼上token
LABEL=kafka   # e2e 機器用 kafka-e2e,kafka-full
N=3           # e2e 機器用 1
ARCH=x64      # ARM 機器改成 arm64

sudo dnf install -y policycoreutils-python-utils
sudo loginctl enable-linger "$USER"   # rootless podman 在 systemd service 底下需要

VER=$(curl -fsSL https://api.github.com/repos/actions/runner/releases/latest | sed -n 's/.*"tag_name": "v\([^"]*\)".*/\1/p')
for i in $(seq 1 $N); do
  d=~/actions-runner-${LABEL%%,*}-$i
  mkdir -p $d && cd $d
  curl -fsSL https://github.com/actions/runner/releases/download/v$VER/actions-runner-linux-$ARCH-$VER.tar.gz | tar xz
  ./config.sh --unattended --replace \
    --url https://github.com/opensource4you/clip \
    --token "$TOKEN" \
    --name "$(hostname)-${LABEL%%,*}-$i" \
    --labels "$LABEL"
  sudo ./svc.sh install "$USER"
  # SELinux 預設不讓 systemd 執行家目錄裡的檔案，要改標籤，不然 service 會 203/EXEC
  sudo semanage fcontext -a -t bin_t "$d/runsvc\.sh"
  sudo restorecon -v "$d/runsvc.sh"
  sudo ./svc.sh start
done
```

## 狀態和結果在哪看

- Runner 狀態（Idle、Active、Offline、label）：
  https://github.com/opensource4you/clip/settings/actions/runners （只有 admin 看得到）
- 每次 run 的 log 和 summary：https://github.com/opensource4you/clip/actions
- 沒登入只看得到 run 的狀態，log、summary 和 artifact 要登入 GitHub 才能看（任何帳號都可以）。
  artifact（test report、ducktape results）保留 14 天。
- 在 VM 上：
  ```bash
  systemctl list-units 'actions.runner.*'
  podman ps
  ```
