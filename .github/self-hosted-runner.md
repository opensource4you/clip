# Self-hosted runner（社群 VM）

`kafka-loop` 和 `kafka-e2e` 跑在社群 VM 上，label 為 `self-hosted, linux, kafka`。
runner 主動往外連 GitHub 拉 job，VM 不用開任何對外 port。

## 安全設定（clip 是 public repo，務必先做）

任何人都能對 public repo 開 PR，而 `pull_request` 事件會用 PR 裡的 workflow 檔，
所以有人可以在 PR 裡新增一個 `runs-on: self-hosted` 的 workflow，在 VM 上跑任意程式。

1. Settings -> Actions -> General -> Fork pull request workflows from outside collaborators
   選 **Require approval for all external contributors**。之後看到外人的 PR 等待 approve 時，
   先確認沒改到 `.github/` 再按。
2. VM 上不要放任何憑證（SSH key、雲端 token、gh 登入），網路跟內部環境隔開。
3. runner 用專用帳號跑，不給 sudo。
4. log 是公開的，workflow 不要印出任何機密。`github/Jenkinsfile` 那種用參數傳 token 的作法
   不能直接搬過來（workflow_dispatch 的 input 會顯示在 run 頁面）。

## 安裝

VM 需要：git、python3、zip、podman（loop 用）、docker（e2e 的 ducker-ak 用）。

在 Settings -> Actions -> Runners -> New self-hosted runner 拿到下載指令和 token，然後用 runner 專用帳號：

```bash
mkdir -p ~/actions-runner-1 && cd ~/actions-runner-1
# 照頁面上的指令下載、解壓
./config.sh --unattended \
  --url https://github.com/opensource4you/clip \
  --token <TOKEN> \
  --name "$(hostname)-1" \
  --labels kafka
sudo ./svc.sh install "$USER" && sudo ./svc.sh start
```

一個 runner 同時只跑一個 job。想讓同一台 VM 平行跑多個 loop 容器，就用不同目錄
（`actions-runner-2`、`-3`…）各註冊一個 runner。e2e 會吃滿整台機器的記憶體，
如果要避免它跟 loop 擠在同一台，可以給大機器另一個 label，再改 `kafka-e2e.yml` 的 `runs-on`。

## 結果在哪看

- https://github.com/opensource4you/clip/actions ：每次 run 的 log 和 summary，任何人都能看。
- run 頁面下方的 artifact（test report、ducktape results）要登入 GitHub 才能下載，保留 14 天。
