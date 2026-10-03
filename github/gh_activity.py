#!/usr/bin/env python3
"""
下載指定 GitHub 帳號在指定 repo 的活動紀錄，並替他開的 PR 算一個「重要性」分數。

只在終端機印出統計摘要。API 回應會連同 ETag 存在 ~/.cache/gh_activity/，重跑時沒變的回 304，不扣額度。
另外每個 PR 抓到的原始資料也會存一份（~/.cache/gh_activity/pr/），搜尋結果裡的 updated_at 沒變的 PR
整包重用、一個請求都不發，所以重跑只會重抓這段時間有動過的 PR。
stderr 會印每個階段的秒數、請求數、304 命中數和等待額度的時間，方便看時間花在哪。

用法：
  export GITHUB_TOKEN=ghp_xxx
  python gh_activity.py --repo apache/kafka --user chia7712              # 單人單 repo，預設看過去 60 天
  python gh_activity.py --config people.json [--detail]                  # 多人多 repo，預設只印總表
  python gh_activity.py --config https://gist.githubusercontent.com/.../raw/people.json
  python gh_activity.py --config people.json --since 2026-01-01 --until 2026-09-17
  python gh_activity.py --config people.json --dump out/                  # 另外替每個人寫一個 out/<帳號>.md：
                                                                           # 評分明細 + 區間內所有留言全文（含行內留言的 diff 片段）

people.json 格式（每個人都查全部 repo）：
  {
    "since": "2026-08-03",            # 可省略
    "until": "2026-09-17",            # 可省略
    "users": ["jason810496", "chia7712"],
    "repos": ["apache/airflow", "apache/kafka"]
  }
  舊格式（每個人各自列 repo）也接受：{"people": [{"user": "...", "repos": [...]}]}

搜尋是一個 repo 一次搜一批人（重複的 author: 是「或」），每個 PR 的留言只抓一次再拆給有參與的人，
所以人數多的時候比一個人一個 repo 分開跑省很多搜尋額度。
搜尋走 GraphQL：REST 的搜尋每分鐘只有 30 次，GraphQL 走每小時 5000 點的額度（一頁約 1 點），
而且一個請求可以用 alias 塞 SEARCHES_PER_REQUEST 個搜尋。其他資料（留言、reviews、files）仍走 REST，有 ETag 快取。

總分 = review 分 + 重要 PR 分 + 一般 PR 分，目標排序是
  review ≈ 重要貢獻 > 一般貢獻
一個 PR 先算「重要性」分數（有沒有 production code、討論多深、改動多大），達 IMPORTANT_PR_MIN 的全額計入，
未達的打折；沒 merge 的 PR 先算 1/4，被關掉的不計。
Review 分只把「實質意見」當成有在認真 review 的證據：非 nit 的行內留言，或 40 字以上的結論。短留言和只挑格式的
nit（nit: 開頭、行尾空白、換行、typo、縮排，見 is_nit）深度分打折，而且不觸發 request changes、首評、帶非 committer 的加分。

多 repo 的合併規則：總分各 repo 相加；品質分把該人所有 repo 的 PR 放在一起取前 N。
"""
import argparse, hashlib, http.client, json, math, os, re, sys, threading, time, unicodedata, urllib.parse, urllib.request, urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

API = "https://api.github.com"
GRAPHQL = "https://api.github.com/graphql"
TOKEN = os.environ.get("GITHUB_TOKEN")
SEARCHES_PER_REQUEST = 5    # 一個 GraphQL 請求裡用 alias 塞幾個搜尋（每個搜尋一頁 100 筆約 1 點）。
                            # GitHub 是逐個處理，塞越多單一請求越慢，所以塞少一點、多個請求同時送
SEARCH_WORKERS = 10         # 同時送幾個 GraphQL 搜尋請求。126 人 × 3 種關係約 40 個搜尋 = 8 個請求，10 個 worker 一輪跑完

# ---- 權重，自行調整 ----------------------------------------------------------
WEIGHTS = {
    "has_prod_code": 1,        # 有動到 production code
    "no_prod_code": 0.5,       # 只動 test / docs / ci / config，給少少的分
    # 討論深度用連續值，讓大 PR 有機會拿高分（reviewer 人數不算分，那只反映 repo 的 review 慣例）：
    "discussion_log": True,    # + log2(1 + 別人在這個 PR 上的留言數)：5 則 ≈ 2.6、20 則 ≈ 4.4、80 則 ≈ 6.3
    # 改動規模（只算有 production code 的 PR）：+ 0.5 × log2(1 + 增刪行數 / 50)，最多 +3
    # 50 行 ≈ 0.5、300 行 ≈ 1.4、1000 行 ≈ 2.2
    "size_log_scale": 0.5,
    "size_unit": 50,
    "size_cap": 3.0,
    "trivial_title": -2,       # 標題以 MINOR / HOTFIX / typo 開頭
    "merged_under_2h": -1,
}
# ---- 總分怎麼組成 -----------------------------------------------------------
# 總分 = review 分 + 重要 PR 分 + 一般 PR 分
# 目標排序：review ≈ 重要貢獻 > 一般貢獻。大致的單位分數：
#   實質 review（幾則行內意見、approve 且 merge）≈ 4～6，深度 review（行內 ≥ DEEP_REVIEW_INLINE）≈ 8～10，
#   帶非 committer 再 +8；只按 approve 的 review ≈ 1
#   重要 PR（prod code、有討論、有規模）≈ 4～8；一般 PR 乘 0.5 ≈ 0.5～1.5
#   也就是：帶一個非 committer 的 PR 到 merge ≈ 兩個重要 PR；深度 review ≈ 重要 PR；純 approve 幾乎不計
IMPORTANT_PR_MIN = 4.0     # PR 重要性分數達此值算「重要貢獻」。約等於 prod code 且（3 則討論 + 300 行，或 5 則討論 + 50 行）
PR_BUCKET_FACTOR = {"important": 1.0, "general": 0.5}
PR_STATE_FACTOR = {
    "merged": 1.0,
    "open": 0.25,     # 還沒 merge 的先算 1/4，merge 後補齊（不鼓勵灌 PR）
    "closed": 0.0,    # 關掉沒 merge 的不計分
}
WORKERS = 8         # 同時下載的 thread 數。每個 thread 一條持久連線；超過 10 可能觸發 secondary rate limit
DEFAULT_DAYS = 60   # 未指定 --since/--until 時，看過去幾天
QUALITY_TOP_N_PR = 5       # 自己開的 PR 品質分：取分數最高的幾個平均
QUALITY_TOP_N_REVIEW = 5   # Review 品質分：同上。取 5 個而不是 10 個，讓最好的幾個 review 能拉開差距
DEEP_REVIEW_INLINE = 10    # 行內留言達這個數量算「深度 review」，總表單獨列出個數，並額外加 REVIEW_WEIGHTS["deep_review"]
# Review 品質 = 深度（前 N 名平均）＋ 廣度。廣度用對數，只區分量級，不線性放大（量已在數量分算過）
REVIEW_BREADTH = {
    "substantive_log": 1.0,   # + log2(1 + 實質 review 數)：有非 nit 的行內意見或 40 字以上結論的 review（見 is_nit）
    "mentored_log": 1.5,      # + 1.5 × log2(1 + 帶非 committer 數)
}
# 樣本不足 N 個時，缺的名額用基準值補（拉向一般水準，不當 0 也不當高估）。
# 多人模式的基準值是全體受評者的中位數；單人模式用這裡的固定值。
BASELINE_PR_DEFAULT = 1.0      # 一個普通的 production code PR
BASELINE_REVIEW_DEFAULT = 1.0  # 一個純 approve 且 merge 的 review（= REVIEW_WEIGHTS["approved_merged"]）

# ---- Review 活動的權重（只算別人開的 PR）------------------------------------
REVIEW_WEIGHTS = {
    "review_comment": 1.0,      # 程式碼行內留言，每則
    "issue_comment": 0.5,       # PR 下方一般留言，每則
    "review_body": 1.0,         # 有文字內容的 review 結論，每則
    "approved_merged": 1.0,     # 給了 approve 且 PR 後來有 merge（每個 PR 一次）。原 2.0；降低是為了讓純 approve 不值錢，實質意見才有分
    "changes_requested": 2.0,   # 給過 request changes，且在這個 PR 上至少有一則實質意見（每個 PR 一次）。只為 nit 按 request changes 不加分
    "short_factor": 0.25,       # 短留言（少於 SHORT_LEN 字，LGTM、+1 之類）和只挑格式的 nit（見 is_nit）打折
    # 留言累加後取對數再乘 comment_log_scale：1.5 × log2(1 + 累加值)。
    # 1 則 = 1.5、5 則 ≈ 3.9、10 則 ≈ 5.2、30 則 ≈ 7.4。避免無上限膨脹，但深的 review 仍要拉得開
    "comment_log": True,
    "comment_log_scale": 1.5,
    "first_reviewer": 1.0,      # 這個 PR 上第一個留下實質意見的人（非 nit 的行內意見或 40 字以上結論；純 approve、只有 nit、沒文字的 request changes 都不算）
    "acted_on": 1.0,            # 留下實質意見後 PR 有再 push 新 commit（可能是 review 起了作用，訊號不確定所以分數低，每個 PR 一次）
    "mentored": 8.0,            # 帶非 committer：作者不是 committer、本人留 MENTOR_MIN_INLINE 則以上非 nit 的行內意見或（有實質意見的）request changes、PR 最後 merge。原 4.0
    "deep_review": 3.0,         # 深度 review：行內留言達 DEEP_REVIEW_INLINE 則（每個 PR 一次）。留言的對數分壓得很扁，這裡把深度拉開
    "newcomer_factor": 1.5,     # 作者是第一次貢獻者或沒有關聯的人，留言深度的部分乘上這個倍數（固定加分不乘）
    "contributor_factor": 1.2,  # 作者是非 committer 的貢獻者
}
NEWCOMER = {"FIRST_TIME_CONTRIBUTOR", "FIRST_TIMER", "NONE"}
NON_COMMITTER = NEWCOMER | {"CONTRIBUTOR"}
MENTOR_MIN_INLINE = 3
# 不算成「別人」的機器人帳號（k8s 這類專案 bot 留言非常多，會灌高討論數）
BOT_LOGINS = {"k8s-ci-robot", "k8s-triage-robot", "k8s-bot", "kubernetes-bot", "github-actions", "dependabot",
              "codecov", "codecov-commenter", "netlify", "sonarcloud", "boring-cyborg", "asf-ci", "asfgit"}


def is_bot(login):
    return not login or login.endswith("[bot]") or login.lower() in BOT_LOGINS

SHORT_LEN = 40
# 只挑格式的 nit：以 nit 開頭，或整則很短且只提行尾空白、換行、typo、縮排這類東西。
# 視同短留言：深度分打 short_factor，而且不算實質意見（不觸發 first_reviewer / changes_requested / mentored / acted_on）。
# 長留言即使提到 indent、newline 也不算 nit，避免誤殺真正的意見；開頭標 nit: 的一律算 nit，尊重作者自己的分類。
NIT_MAX_LEN = 120
NIT_PREFIX = re.compile(r"^\s*(nit|nits|nitpick|minor|style)\b", re.I)
NIT_KEYWORDS = re.compile(
    r"trailing\s*(white)?space|white\s*space|new\s*line|end of file|\bEOF\b|\btypos?\b|\bspelling\b"
    r"|\bindent(ation|ed)?\b|unused import|import order|blank line|extra space|missing space|\bperiod\b|\bcomma\b", re.I)


def is_nit(body):
    """短留言（LGTM、+1、Done）或只挑格式的 nit。"""
    b = (body or "").strip()
    if len(b) < SHORT_LEN or NIT_PREFIX.match(b):
        return True
    return len(b) <= NIT_MAX_LEN and bool(NIT_KEYWORDS.search(b))


def is_substantive(kind, body, state=None):
    """一則留言算不算實質意見：非 nit 的行內留言，或非 nit 且 40 字以上的 review 結論。
    一般留言（issue comment）不算：多半是 CI、rebase、進度之類的對話。沒文字的 request changes 也不算。"""
    if kind == "review_comment":
        return not is_nit(body)
    if kind == "review":
        return len((body or "").strip()) >= SHORT_LEN and not is_nit(body)
    return False


CONFIG_FILES = {"pom.xml", "build.gradle", "settings.gradle", "gradlew", "gradlew.bat", "package.json",
                "package-lock.json", "requirements.txt", "pyproject.toml", "setup.py", "makefile",
                "dockerfile", "codeowners", "license", "notice"}
MIN_PR_SCORE = 0     # 單一 PR 分數下限，瑣碎 PR 不扣分只是不加分
TRIVIAL_TITLE = re.compile(r"^\s*(minor|hotfix|typo|nit)\b", re.I)
# backport / cherry-pick 的 PR 不計分（自己開的和 review 的都不算），只在摘要列出數量
BACKPORT_TITLE = re.compile(
    r"^\s*\[[^\]]*(v?\d+[.-]\d+|branch|test|release)[^\]]*\]"   # [v3-3-test]、[branch-4.1]
    r"|\(#\d+\)\s*$"                                             # 結尾 (#12345) 引用原 PR
    r"|\(\d+\.\d+(\.\d+)?\)\s*$"                                # 結尾 (4.1)
    r"|\b(backport|cherry[- ]?pick)", re.I)


def is_backport(title):
    return bool(BACKPORT_TITLE.search(title))


# ETag 快取目錄：預設 ~/.cache/gh_activity，可用環境變數 GH_ACTIVITY_CACHE 或 --cache-dir 改；--no-cache 關閉。
# 304 回應不扣 API 額度，重跑幾乎免費
CACHE_DIR = os.environ.get("GH_ACTIVITY_CACHE") or os.path.join(os.path.expanduser("~"), ".cache", "gh_activity")
_cache_enabled = True


def _cache_path(url):
    return os.path.join(CACHE_DIR, hashlib.sha1(url.encode()).hexdigest() + ".json")


# ---- 計時與請求計數，只用來印在 stderr -----------------------------------------
_stats, _stats_lock = Counter(), threading.Lock()


def _count(**kw):
    with _stats_lock:
        for k, v in kw.items():
            _stats[k] += v


def _snapshot():
    with _stats_lock:
        return dict(_stats)


def _elapsed(t0, s0):
    """從 t0 / s0 到現在：秒數、請求數、304 命中、等待額度、PR 快取命中。"""
    s1 = _snapshot()
    d = {k: s1.get(k, 0) - s0.get(k, 0) for k in ("requests", "not_modified", "wait", "pr_cached", "pr_fetched")}
    parts = [f"{d['requests']} 次請求"]
    if d["not_modified"]:
        parts.append(f"304 命中 {d['not_modified']}")
    if d["wait"]:
        parts.append(f"等待額度 {d['wait']:.0f}s")
    if d["pr_cached"] or d["pr_fetched"]:
        parts.append(f"PR 沒變直接用快取 {d['pr_cached']}、重抓 {d['pr_fetched']}")
    return f"{time.time() - t0:.1f}s（{'、'.join(parts)}）"


def _wait(seconds):
    _count(wait=seconds)
    time.sleep(seconds)


_local = threading.local()


def _request(url, headers, method="GET", body=None):
    """用每個 thread 各自的持久連線送請求（keep-alive，省掉每次 TLS 握手）。
    回傳 (status, headers, body_bytes)。"""
    u = urllib.parse.urlsplit(url)
    conn = getattr(_local, "conn", None)
    if conn is None or getattr(_local, "host", None) != u.netloc:
        if conn is not None:
            conn.close()
        conn = http.client.HTTPSConnection(u.netloc, timeout=60)
        _local.conn, _local.host = conn, u.netloc
    path = u.path + ("?" + u.query if u.query else "")
    for attempt in (1, 2):
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            return resp.status, resp.headers, resp.read()
        except (http.client.HTTPException, OSError):
            # 連線被對方關掉或壞了，重建一次再試
            conn.close()
            conn = http.client.HTTPSConnection(u.netloc, timeout=60)
            _local.conn = conn
            if attempt == 2:
                raise


def get(url, params=None, accept="application/vnd.github+json"):
    """呼叫 API，處理 rate limit 與 ETag 快取，回傳 (json, next_url)。"""
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    headers = {"Accept": accept, "User-Agent": "gh-activity"}
    if TOKEN:
        headers["Authorization"] = f"Bearer {TOKEN}"
    cached = None
    if _cache_enabled and os.path.exists(_cache_path(url)):
        try:
            cached = json.load(open(_cache_path(url)))
            headers["If-None-Match"] = cached["etag"]
        except (ValueError, KeyError):
            cached = None
    while True:
        status, h, body = _request(url, headers)
        _count(requests=1)
        if status == 200:
            nxt = None
            for part in h.get("Link", "").split(","):
                if 'rel="next"' in part:
                    nxt = part[part.find("<") + 1:part.find(">")]
            data = json.loads(body.decode())
            etag = h.get("ETag")
            if _cache_enabled and etag:
                os.makedirs(CACHE_DIR, exist_ok=True)
                tmp = _cache_path(url) + ".tmp"
                json.dump({"etag": etag, "next": nxt, "data": data}, open(tmp, "w"))
                os.replace(tmp, _cache_path(url))
            return data, nxt
        if status == 304 and cached:
            _count(not_modified=1)
            return cached["data"], cached.get("next")
        if status in (301, 302, 307, 308) and h.get("Location"):
            url = h["Location"]
            continue
        w = _retry_wait(status, h, body, url)
        if w is not None:
            _wait(w)
            continue
        print(f"  HTTP {status}: {url}", file=sys.stderr)
        raise RuntimeError(f"HTTP {status}: {url} {body[:200]!r}")


def _retry_wait(status, h, body, url):
    """403 / 429 / 5xx：印出原因，回傳該等幾秒後重試；其他狀態回傳 None。REST 和 GraphQL 共用。"""
    if status in (403, 429):
        try:
            msg = json.loads(body.decode()).get("message", "")
        except Exception:
            msg = ""
        if h.get("Retry-After"):
            wait = int(h["Retry-After"]) + 1
            why = "secondary rate limit（Retry-After）"
        elif "secondary" in msg.lower() or "abuse" in msg.lower():
            wait = 60
            why = "secondary rate limit（請求太密集）"
        elif h.get("X-RateLimit-Remaining") == "0":
            if h.get("X-RateLimit-Limit") == "60":
                sys.exit(f"這個請求被當成未登入（額度上限 60）：{url}\n"
                         "token 對這個 repo 無效。fine-grained token 請把 Repository access 設為 "
                         "Public repositories (read-only)，或改用 classic token。")
            wait = int(h.get("X-RateLimit-Reset", time.time() + 60)) - int(time.time()) + 5
            why = f"主要額度用完（{h.get('X-RateLimit-Resource', 'core')}）"
        else:
            wait = 60
            why = f"HTTP {status}"
        print(f"  {why}，等待 {wait}s ... {msg[:80]}", file=sys.stderr)
        return max(wait, 5)
    if status >= 500:
        print(f"  HTTP {status}，10 秒後重試：{url}", file=sys.stderr)
        return 10
    return None


def graphql(query, variables):
    """POST 一個 GraphQL 查詢，處理額度（header 的 403 和 body 裡的 RATE_LIMITED 都有可能），回傳 data。"""
    payload = json.dumps({"query": query, "variables": variables}).encode()
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "gh-activity",
               "Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    while True:
        status, h, body = _request(GRAPHQL, headers, "POST", payload)
        _count(requests=1)
        if status == 200:
            resp = json.loads(body.decode())
            errors = resp.get("errors") or []
            if any(e.get("type") == "RATE_LIMITED" for e in errors):
                wait = max(int(h.get("X-RateLimit-Reset", time.time() + 60)) - int(time.time()) + 5, 5)
                print(f"  GraphQL 額度用完，等待 {wait}s ...", file=sys.stderr)
                _wait(wait)
                continue
            if errors:
                raise RuntimeError("GraphQL 錯誤：" + "；".join(e.get("message", "?") for e in errors))
            return resp["data"]
        w = _retry_wait(status, h, body, GRAPHQL)
        if w is not None:
            _wait(w)
            continue
        raise RuntimeError(f"GraphQL HTTP {status}: {body[:200]!r}")


# 搜尋結果只取後面會用到的欄位。不抓 labels：它會讓每頁成本從 1 點變 11 點，而且目前沒有用到
SEARCH_FIELDS = """issueCount pageInfo { hasNextPage endCursor }
    nodes { ... on PullRequest { number title url state createdAt updatedAt mergedAt authorAssociation
                                 author { login } comments { totalCount } } }"""


def _search_batch(batch):
    """batch 是 [(搜尋字串, cursor)]，一個 GraphQL 請求送完，回傳 [(搜尋字串, 該頁結果)]。"""
    decl = ", ".join(f"$q{i}: String!, $c{i}: String" for i in range(len(batch)))
    parts = " ".join(f"s{i}: search(query: $q{i}, type: ISSUE, first: 100, after: $c{i}) {{ {SEARCH_FIELDS} }}"
                     for i in range(len(batch)))
    variables = {}
    for i, (q, cursor) in enumerate(batch):
        variables[f"q{i}"], variables[f"c{i}"] = q, cursor
    data = graphql(f"query({decl}) {{ {parts} }}", variables)
    return [(q, data[f"s{i}"]) for i, (q, _) in enumerate(batch)]


def search_many(queries):
    """一次送多個搜尋字串：每 SEARCHES_PER_REQUEST 個塞成一個 GraphQL 請求（alias），
    最多 SEARCH_WORKERS 個請求同時送，各自翻頁到完。
    回傳 {搜尋字串: (items, issueCount)}。issueCount 是真正的總數，超過 1000 表示結果被截斷。"""
    results = {q: ([], 0) for q in queries}
    pending = {q: None for q in queries}   # 搜尋字串 → 下一頁的 cursor
    while pending:
        todo = list(pending.items())
        batches = [todo[i:i + SEARCHES_PER_REQUEST] for i in range(0, len(todo), SEARCHES_PER_REQUEST)]
        with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as pool:
            pages = [p for out in pool.map(_search_batch, batches) for p in out]
        for q, r in pages:
            items, _ = results[q]
            items.extend(n for n in r["nodes"] if n)   # is:pr 之外的 node 會是空物件
            results[q] = (items, r["issueCount"])
            if r["pageInfo"]["hasNextPage"]:
                pending[q] = r["pageInfo"]["endCursor"]
            else:
                del pending[q]
    return results


def get_all(url, params=None, key=None, accept="application/vnd.github+json"):
    params = dict(params or {}, per_page=100)
    data, nxt = get(url, params, accept)
    out = list(data[key] if key else data)
    while nxt:
        data, nxt = get(nxt, accept=accept)
        out.extend(data[key] if key else data)
    return out


def in_range(ts, since, until):
    d = (ts or "")[:10]
    return bool(d) and (not since or d >= since) and (not until or d <= until)


def classify(path):
    p = path.lower()
    if p.startswith((".github/", ".ci/", "ci/")) or "jenkinsfile" in p or ".gitlab-ci" in p:
        return "ci"
    name = p.rsplit("/", 1)[-1]
    if name.startswith(".") or name in CONFIG_FILES or p.startswith(("gradle/", "config/", "checkstyle/")) \
            or name.endswith((".gradle", ".gradle.kts", ".toml", ".yaml", ".yml", ".properties", ".cfg", ".ini")):
        return "config"
    if p.startswith(("docs/", "doc/")) or p.endswith((".md", ".rst", ".adoc", ".txt")):
        return "docs"
    if re.search(r"(^|/)(tests?|__tests__|spec|testing)/", p) or re.search(r"(test|spec)s?\.\w+$", p) \
            or re.search(r"(^|/)test_[^/]+$", p):
        return "test"
    return "prod"


def hours_between(a, b):
    f = "%Y-%m-%dT%H:%M:%SZ"
    return (datetime.strptime(b, f) - datetime.strptime(a, f)).total_seconds() / 3600


def score(pr):
    s, reasons = 0, []
    if pr["files_prod"] > 0:
        s += WEIGHTS["has_prod_code"]; reasons.append("prod_code")
    else:
        s += WEIGHTS["no_prod_code"]; reasons.append("no_prod_code")
    discussion = pr["review_comments"] + pr["issue_comments"]
    if WEIGHTS["discussion_log"] and discussion > 0:
        d = math.log2(1 + discussion)
        s += d; reasons.append(f"discussion({discussion})+{d:.1f}")
    lines = (pr.get("additions") or 0) + (pr.get("deletions") or 0)
    if pr["files_prod"] > 0 and lines > 0:
        z = min(WEIGHTS["size_cap"], WEIGHTS["size_log_scale"] * math.log2(1 + lines / WEIGHTS["size_unit"]))
        s += z; reasons.append(f"size({lines})+{z:.1f}")
    if TRIVIAL_TITLE.match(pr["title"]):
        s += WEIGHTS["trivial_title"]; reasons.append("trivial_title")
    if pr["hours_to_merge"] is not None and pr["hours_to_merge"] < 2:
        s += WEIGHTS["merged_under_2h"]; reasons.append("merged<2h")
    if s < MIN_PR_SCORE:
        reasons.append(f"floor({s:+g})")
        s = MIN_PR_SCORE
    pr["score"], pr["score_reasons"] = round(s, 2), reasons
    # 計入總分的分數：重要 PR 全額、一般 PR 打折；沒 merge 的再依狀態打折
    pr["important"] = s >= IMPORTANT_PR_MIN
    pr["bucket"] = "important" if pr["important"] else "general"
    state = "merged" if pr["merged_at"] else ("open" if pr.get("state") == "open" else "closed")
    pr["pr_state"] = state
    pr["counted"] = round(s * PR_BUCKET_FACTOR[pr["bucket"]] * PR_STATE_FACTOR[state], 2)


def review_score(pr, mine, acted_on=False, first_reviewer=False):
    """mine 是本人在這個（別人開的）PR 上的留言，回傳該 PR 的 review 分數。
    分數 = 留言深度（對數、非 committer 倍率）＋ 固定加分（approve 且 merge、request changes、首評、深度 review、帶非 committer、起作用）。"""
    W = REVIEW_WEIGHTS
    depth, cnt = 0.0, Counter()
    for c in mine:
        factor = W["short_factor"] if is_nit(c["body"]) else 1.0
        if c["type"] == "review_comment":
            depth += W["review_comment"] * factor; cnt["review_comment"] += 1
            if is_substantive("review_comment", c["body"]):
                cnt["inline_sub"] += 1
        elif c["type"] == "issue_comment":
            depth += W["issue_comment"] * factor; cnt["issue_comment"] += 1
        elif c["type"] == "review":
            cnt["review"] += 1
            if c["body"].strip():
                depth += W["review_body"] * factor
            if is_substantive("review", c["body"]):
                cnt["review_sub"] += 1
    if W["comment_log"] and depth > 0:
        depth = W["comment_log_scale"] * math.log2(1 + depth)
    states = {c["review_state"] for c in mine if c["type"] == "review"}
    # 這個 PR 上有沒有實質意見（非 nit 的行內留言，或 40 字以上的結論）。沒有的話，
    # request changes、首評、帶非 committer 這些「有在認真 review」的加分都不給
    has_sub = cnt["inline_sub"] + cnt["review_sub"] > 0
    reasons = []
    assoc = pr.get("author_association") or "NONE"
    if assoc in NEWCOMER:
        depth *= W["newcomer_factor"]; reasons.append(f"newcomer×{W['newcomer_factor']}")
    elif assoc == "CONTRIBUTOR":
        depth *= W["contributor_factor"]; reasons.append(f"contributor×{W['contributor_factor']}")
    s = depth
    if "APPROVED" in states and pr["merged_at"]:
        s += W["approved_merged"]; reasons.append("approved_merged")
    changes_requested = "CHANGES_REQUESTED" in states and has_sub
    if changes_requested:
        s += W["changes_requested"]; reasons.append("changes_requested")
    elif "CHANGES_REQUESTED" in states:
        reasons.append("changes_requested_nit_only")
    if first_reviewer:
        s += W["first_reviewer"]; reasons.append("first_reviewer")
    deep = cnt["review_comment"] >= DEEP_REVIEW_INLINE
    if deep:
        s += W["deep_review"]; reasons.append("deep_review")
    mentored = (assoc in NON_COMMITTER and pr["merged_at"]
                and (cnt["inline_sub"] >= MENTOR_MIN_INLINE or changes_requested))
    if mentored:
        s += W["mentored"]; reasons.append("mentored")
    if acted_on:
        s += W["acted_on"]; reasons.append("acted_on")
    short_n = sum(1 for c in mine if c["type"] != "review" and is_nit(c["body"]))
    return {
        "number": pr["number"], "title": pr["title"], "author": pr["author"], "url": pr["url"],
        "merged_at": pr["merged_at"], "author_association": assoc, "acted_on": acted_on,
        "first_reviewer": first_reviewer, "mentored": bool(mentored),
        "substantive": has_sub, "inline_substantive": cnt["inline_sub"],
        "review_comments": cnt["review_comment"], "issue_comments": cnt["issue_comment"],
        "reviews": cnt["review"], "review_states": sorted(st for st in states if st),
        "short_comments": short_n,
        "score": round(s, 2), "score_reasons": reasons,
    }


def _width(text):
    """終端機顯示寬度：全形字算 2。"""
    return sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)


def _pad(text, width, right=False):
    gap = width - _width(text)
    return (" " * gap + text) if right else (text + " " * gap)


def render_table(rows):
    """rows 是每列的 cell 清單，第一列是表頭。數字欄靠右。"""
    ncol = max(len(r) for r in rows)
    rows = [r + [""] * (ncol - len(r)) for r in rows]
    widths = [max(_width(r[i]) for r in rows) for i in range(ncol)]
    numeric = [all(re.fullmatch(r"[+-]?\d[\d.,]*", r[i]) for r in rows[1:] if r[i]) for i in range(ncol)]
    out = ["  ".join(_pad(c, widths[i], numeric[i]) for i, c in enumerate(rows[0])).rstrip(),
           "  ".join("-" * w for w in widths)]
    for r in rows[1:]:
        out.append("  ".join(_pad(c, widths[i], numeric[i]) for i, c in enumerate(r)).rstrip())
    return out


def render(lines, fmt="text"):
    """把摘要的 markdown 子集轉成純文字（表格對齊、標題加底線），fmt=markdown 則原樣輸出。"""
    lines = [l for line in lines for l in line.split("\n")]
    if fmt == "markdown":
        return "\n".join(lines)
    out, table = [], []
    def flush():
        if table:
            out.extend(render_table(table)); table.clear()
    for line in lines:
        st = line.strip()
        if st.startswith("|"):
            cells = [c.strip() for c in st.strip("|").split("|")]
            if not all(set(c) <= set("-: ") for c in cells):
                table.append(cells)
            continue
        flush()
        if st.startswith("### "):
            out.append(f"[{st[4:]}]")
        elif st.startswith("## "):
            out.append(st[3:]); out.append("=" * _width(st[3:]))
        elif st.startswith("# "):
            out.append(st[2:].upper() if st[2:].isascii() else st[2:]); out.append("#" * max(_width(st[2:]), 20))
        elif st == "---":
            out.append("")
        else:
            out.append(line)
    flush()
    return "\n".join(out)


def load_config(path_or_url):
    """--config 可以是本機路徑或 http(s) 網址（例如 gist 的 raw 連結）。"""
    if re.match(r"^https?://", path_or_url):
        print(f"下載設定檔：{path_or_url}", file=sys.stderr)
        req = urllib.request.Request(path_or_url, headers={"User-Agent": "gh-activity"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.load(r)
        except urllib.error.HTTPError as e:
            sys.exit(f"無法下載設定檔（HTTP {e.code}）：{path_or_url}")
        except ValueError:
            sys.exit(f"設定檔不是合法的 JSON：{path_or_url}（gist 要用 raw 連結）")
    try:
        return json.load(open(path_or_url))
    except FileNotFoundError:
        sys.exit(f"找不到設定檔：{path_or_url}")
    except ValueError:
        sys.exit(f"設定檔不是合法的 JSON：{path_or_url}")


def normalize_repo(repo):
    """接受 owner/repo 或完整網址。"""
    repo = re.sub(r"^(https?://)?(www\.)?github\.com/", "", repo.strip()).rstrip("/")
    repo = re.sub(r"\.git$", "", repo)
    if repo.count("/") != 1:
        sys.exit(f"repo 格式錯誤：{repo}，請用 owner/repo")
    return repo


class RepoInaccessible(Exception):
    pass


def ensure_quota(repo, min_needed=300):
    """額度不夠就等到重置再繼續，不中斷。
    不用 /rate_limit 端點，它有時會回到沒同步的快取，顯示滿額但其實已用完；
    改對目標 repo 打一個 HEAD 請求，讀真正的 X-RateLimit-* header。"""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "gh-activity",
               "Authorization": f"Bearer {TOKEN}"}
    while True:
        req = urllib.request.Request(f"{API}/repos/{repo}", headers=headers, method="HEAD")
        try:
            with urllib.request.urlopen(req) as r:
                h = r.headers
        except urllib.error.HTTPError as e:
            if e.code not in (403, 429):
                raise RepoInaccessible(f"無法存取 {repo}（HTTP {e.code}）。私有 repo 需要 classic token 勾 repo scope。")
            h = e.headers
        remaining, limit = int(h.get("X-RateLimit-Remaining", 0)), h.get("X-RateLimit-Limit", "?")
        reset = int(h.get("X-RateLimit-Reset", time.time() + 60))
        if remaining >= min_needed:
            print(f"API 額度：剩餘 {remaining}/{limit}", file=sys.stderr)
            return
        wait = reset - int(time.time()) + 5
        print(f"額度剩 {remaining}/{limit}，等到 {datetime.fromtimestamp(reset).strftime('%H:%M')} "
              f"重置後繼續（約 {wait // 60} 分鐘）...", file=sys.stderr)
        _wait(max(wait, 5))


SEARCH_QUERY_MAX = 256   # GitHub 搜尋字串上限


def user_batches(users, base_query, rel):
    """把帳號分批，讓 base_query + 'rel:user ...' 不超過 256 字元。"""
    batches, cur, length = [], [], len(base_query)
    for u in users:
        term = len(f" {rel}:{u}")
        if cur and length + term > SEARCH_QUERY_MAX:
            batches.append(cur); cur, length = [], len(base_query)
        cur.append(u); length += term
    if cur:
        batches.append(cur)
    return batches


def search_prs(repo, since, users):
    """搜出這群人在 repo 裡開過、留言過、review 過的 PR（去重）。回傳 (pr 清單, 搜尋次數)。
    三種關係各分批，所有批次一起丟給 search_many；撞到 1000 筆上限的批次對半拆開重搜。"""
    base = f"is:pr repo:{repo} updated:>={since}"
    todo = [(rel, batch) for rel in ("author", "commenter", "reviewed-by") for batch in user_batches(users, base, rel)]
    found, n_queries = {}, 0
    while todo:
        qs = {base + "".join(f" {rel}:{u}" for u in batch): (rel, batch) for rel, batch in todo}
        n_queries += len(qs)
        todo = []
        for q, (items, total) in search_many(list(qs)).items():
            rel, batch = qs[q]
            if total > 1000 and len(batch) > 1:
                mid = len(batch) // 2
                todo += [(rel, batch[:mid]), (rel, batch[mid:])]
                continue
            if total > 1000:
                print(f"  警告：{rel}:{batch[0]} 搜尋結果超過 1000 筆上限，請縮小日期區間", file=sys.stderr)
            for i in items:
                found.setdefault(i["number"], i)
    prs = [{"number": i["number"], "title": i["title"], "author": (i.get("author") or {}).get("login") or "ghost",
            "state": i["state"].lower(), "created_at": i["createdAt"], "updated_at": i["updatedAt"],
            "url": i["url"], "merged_at": i.get("mergedAt"),
            "n_issue_comments": i["comments"]["totalCount"], "author_association": i.get("authorAssociation")}
           for i in found.values()]
    return prs, n_queries


def verify_or_semantics(repo, since, users):
    """確認重複的 author: 是「或」：兩個帳號合搜的筆數不能少於單一帳號的筆數。只在第一個 repo 檢查一次。"""
    if len(users) < 2:
        return
    base = f"is:pr repo:{repo} updated:>={since}"
    single, both = f"{base} author:{users[0]}", f"{base} author:{users[0]} author:{users[1]}"
    res = search_many([single, both])
    if res[both][1] < res[single][1]:
        sys.exit("GitHub 搜尋對重複的 author: 不是「或」的語意，無法批次搜尋。請回報這個問題。")


class PRCache:
    """一個 PR 的原始 API 回應（留言、reviews、commits、detail、files）。
    以搜尋結果的 updated_at 當版本：GitHub 在有新留言、review、commit、merge、關閉時都會更新它，
    沒變就整包重用、一個請求都不發。每個欄位是需要時才抓（lazy），所以受評者名單變了也只會補抓缺的部分。"""

    def __init__(self, repo, pr):
        self.path = os.path.join(CACHE_DIR, "pr", hashlib.sha1(f"{repo}#{pr['number']}".encode()).hexdigest() + ".json")
        self.updated_at = pr.get("updated_at")
        self.data, self.dirty = {}, False
        if _cache_enabled and self.updated_at and os.path.exists(self.path):
            try:
                c = json.load(open(self.path))
                if c.get("updated_at") == self.updated_at:
                    self.data = c["data"]
            except (ValueError, KeyError, OSError):
                pass

    def get(self, key, fetch):
        if key not in self.data:
            self.data[key] = fetch(); self.dirty = True
        return self.data[key]

    def save(self):
        _count(pr_fetched=1) if self.dirty else _count(pr_cached=1)
        if not (_cache_enabled and self.dirty and self.updated_at):
            return
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            tmp = self.path + ".tmp"
            json.dump({"updated_at": self.updated_at, "data": self.data}, open(tmp, "w"))
            os.replace(tmp, self.path)
        except OSError:
            pass


def analyze_repo(repo, users, since, until, with_commits=True):
    """抓一群人在一個 repo 的活動，回傳 {user: 統計結果}。
    搜尋是分批合併查的，每個 PR 的留言只抓一次，再拆給有參與的人。
    with_commits 目前保留參數但一律會抓 commits（判斷關掉但有進 default branch 的 PR 要用）。"""
    users = list(users)
    user_set = set(users)
    print(f"\n== {repo}（{len(users)} 人）==", file=sys.stderr)
    t_repo, s_repo = time.time(), _snapshot()
    ensure_quota(repo)

    # 1. commits：整個 repo 區間內的 commit 列一次，再依作者分。
    #    同時記下訊息裡提到的 PR 編號（#123），給「關掉但其實有進 default branch」的 PR 判斷用。
    commits_by_user = Counter()
    closed_by_commit = {}   # PR 編號 → 提到它的 commit（區間內、default branch）
    print("下載 commits ...", file=sys.stderr)
    t0, s0 = time.time(), _snapshot()
    for c in get_all(f"{API}/repos/{repo}/commits", {"since": f"{since}T00:00:00Z", "until": f"{until}T23:59:59Z"}):
        login = (c.get("author") or {}).get("login")
        if login in user_set:
            commits_by_user[login] += 1
        for n in re.findall(r"(?<![\w/])#(\d+)(?!\d)", (c.get("commit") or {}).get("message") or ""):
            closed_by_commit.setdefault(int(n), c)
    print(f"  {_elapsed(t0, s0)}", file=sys.stderr)

    # 2. 參與的 PR：三種關係（開的、留言的、review 過的）各分批搜，合併去重。
    #    不用 involves:，那會把只是被 @ 提到或被指派的 PR 也撈進來。
    print("搜尋參與的 PR ...", file=sys.stderr)
    t0, s0 = time.time(), _snapshot()
    prs, n_queries = search_prs(repo, since, users)
    print(f"  {n_queries} 次搜尋，{len(prs)} 個 PR，{_elapsed(t0, s0)}", file=sys.stderr)

    login = lambda r: (r.get("user") or {}).get("login")

    def is_sub(c, kind):
        """別人的留言算不算實質意見（判斷受評者是不是第一個留實質意見的人）。和受評者用同一套標準。"""
        return is_substantive(kind, c.get("body"))

    # 3. 每個 PR 抓一次留言，拆給每個有參與的人。回傳 {user: (mine, own_row, review_row, backport_kind)}
    def process_pr(pr):
        num, author = pr["number"], pr["author"]
        cache = PRCache(repo, pr)
        issue_c = cache.get("issue_comments", lambda: get_all(f"{API}/repos/{repo}/issues/{num}/comments")) \
            if pr["n_issue_comments"] else []
        reviews = cache.get("reviews", lambda: get_all(f"{API}/repos/{repo}/pulls/{num}/reviews"))
        # 行內留言一定隸屬於某個 review：有受評者 review 了別人的 PR，或受評者的 PR 被別人 review，才需要抓
        need_inline = any(login(r) in user_set and login(r) != author for r in reviews) or \
            (author in user_set and any(login(r) != author and not is_bot(login(r)) for r in reviews))
        review_c = cache.get("review_comments", lambda: get_all(f"{API}/repos/{repo}/pulls/{num}/comments")) \
            if need_inline else []

        # 誰在這個 PR 上有留言
        by_user = {}
        for kind, rows, tkey in (("issue_comment", issue_c, "created_at"),
                                 ("review_comment", review_c, "created_at"),
                                 ("review", reviews, "submitted_at")):
            for c in rows:
                u = login(c)
                if u not in user_set or not in_range(c.get(tkey), since, until):
                    continue
                by_user.setdefault(u, []).append({
                    "pr": num, "pr_title": pr["title"], "type": kind,
                    "review_state": c.get("state") if kind == "review" else None,
                    "date": c.get(tkey), "body": c.get("body") or "", "url": c.get("html_url"),
                    # 給 --dump 用：行內留言的檔案、行號、diff 片段
                    "path": c.get("path"), "line": c.get("line") or c.get("original_line"),
                    "hunk": c.get("diff_hunk") if kind == "review_comment" else None})

        out = {}
        backport = is_backport(pr["title"])

        # 有些專案（例如 YuniKorn）不是按 merge 按鈕，而是用工具把 commit 推上 default branch，
        # PR 被 commit 訊息裡的 "Closes #N" 自動關掉，GitHub 上狀態是 closed 而不是 merged。
        # 這種 PR 的 closed 事件會帶 commit_id，視同 merged。只對有人參與的 closed 未 merge PR 查一次。
        pr = dict(pr)
        if pr["state"] == "closed" and not pr["merged_at"] and \
                (author in user_set or any(u != author for u in by_user)):
            c = closed_by_commit.get(num)
            if c:
                # 區間內 default branch 上有 commit 提到這個 PR 編號
                pr["merged_at"] = ((c.get("commit") or {}).get("committer") or {}).get("date")
                pr["merged_by"] = (c.get("committer") or {}).get("login")
                pr["merged_via_commit"] = True
            else:
                # 沒有的話看 PR 的 closed 事件是否帶 commit_id（被 commit 訊息裡的 Closes #N 自動關掉）
                events = cache.get("events", lambda: get_all(f"{API}/repos/{repo}/issues/{num}/events"))
                closed = [e for e in events if e.get("event") == "closed" and e.get("commit_id")]
                if closed:
                    pr["merged_at"] = closed[-1].get("created_at")
                    pr["merged_by"] = (closed[-1].get("actor") or {}).get("login")
                    pr["merged_via_commit"] = True

        # 給 --dump 用：每則留言帶上所屬 PR 的作者、身分、狀態
        meta = {"pr_author": author, "pr_assoc": pr.get("author_association"), "pr_merged": pr["merged_at"],
                "pr_state": pr["state"], "pr_url": pr["url"]}
        for cs in by_user.values():
            for c in cs:
                c.update(meta)

        # 3a. review 別人的 PR
        for u, mine in by_user.items():
            if u == author:
                continue
            if backport:
                out[u] = (mine, None, None, "review"); continue
            # 實質意見：非 nit 的行內留言，或 40 字以上的結論。只有 nit 或沒文字的 request changes 不算，
            # 所以也不會觸發 first_reviewer / acted_on
            substantive = [c for c in mine if is_substantive(c["type"], c["body"])]
            acted = first_rv = False
            if substantive:
                first = min(c["date"] for c in substantive)
                pr_commits = cache.get("commits", lambda: get_all(f"{API}/repos/{repo}/pulls/{num}/commits"))
                acted =any((c["commit"]["committer"]["date"] or "") > first for c in pr_commits)
                others_first = [c.get("created_at") or c.get("submitted_at")
                                for kind, rows in (("review_comment", review_c), ("review", reviews))
                                for c in rows
                                if login(c) not in (u, author) and not is_bot(login(c)) and is_sub(c, kind)]
                first_rv = not others_first or first < min(others_first)
            out[u] = (mine, None, review_score(pr, mine, acted, first_rv), None)

        # 3b. 受評者自己開的 PR
        if author in user_set:
            mine = by_user.get(author, [])
            if backport:
                out[author] = (mine, None, None, "own")
            elif not in_range(pr["created_at"], since, until):
                out[author] = (mine, None, None, None)
            else:
                detail = cache.get("detail", lambda: get(f"{API}/repos/{repo}/pulls/{num}")[0])
                files = cache.get("files", lambda: get_all(f"{API}/repos/{repo}/pulls/{num}/files"))
                kinds = Counter(classify(f["filename"]) for f in files)
                others = lambda rows: [r for r in rows if login(r) != author and not is_bot(login(r))]
                reviewers = Counter(r["user"]["login"] for r in others(reviews))
                merged_at = detail.get("merged_at") or pr["merged_at"]
                row = {
                    "number": num, "title": pr["title"], "url": pr["url"],
                    "state": detail.get("state") or pr["state"],
                    "created_at": pr["created_at"], "merged_at": merged_at,
                    "merged_by": (detail.get("merged_by") or {}).get("login") or pr.get("merged_by"),
                    "merged_via_commit": pr.get("merged_via_commit", False),
                    "hours_to_merge": round(hours_between(pr["created_at"], merged_at), 1) if merged_at else None,
                    "additions": detail.get("additions"), "deletions": detail.get("deletions"),
                    "changed_files": detail.get("changed_files"),
                    "files_prod": kinds["prod"], "files_test": kinds["test"],
                    "files_docs": kinds["docs"], "files_ci": kinds["ci"], "files_config": kinds["config"],
                    "issue_comments": len(others(issue_c)), "review_comments": len(others(review_c)),
                    "reviews": len(others(reviews)), "reviewers": sorted(reviewers),
                    "max_review_rounds": max(reviewers.values(), default=0),
                }
                score(row)
                out[author] = (mine, row, None, None)
        cache.save()
        return out

    per_user = {u: {"comments": [], "scored": [], "reviewed": [], "backports": Counter()} for u in users}
    print(f"下載 {len(prs)} 個 PR 的留言（{WORKERS} 個 thread）...", file=sys.stderr)
    t0, s0 = time.time(), _snapshot()
    pool = ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futures = [pool.submit(process_pr, pr) for pr in prs]
        for n, fut in enumerate(as_completed(futures), 1):
            for u, (mine, row, rv, bp) in fut.result().items():
                d = per_user[u]
                d["comments"].extend(mine)
                if bp: d["backports"][bp] += 1
                if row: d["scored"].append(row)
                if rv: d["reviewed"].append(rv)
            if n % 100 == 0 or n == len(prs):
                print(f"  {n}/{len(prs)}", file=sys.stderr)
    except KeyboardInterrupt:
        pool.shutdown(wait=False, cancel_futures=True)
        sys.exit("\n已中斷。")
    pool.shutdown()
    print(f"  {_elapsed(t0, s0)}", file=sys.stderr)
    print(f"{repo} 合計 {_elapsed(t_repo, s_repo)}", file=sys.stderr)

    results = {}
    for u in users:
        d = per_user[u]
        scored, reviewed, comments = d["scored"], d["reviewed"], d["comments"]
        for r in scored: r["repo"] = repo
        for r in reviewed: r["repo"] = repo
        for c in comments: c["repo"] = repo
        scored.sort(key=lambda x: -x["score"])
        reviewed.sort(key=lambda x: -x["score"])
        by_type = Counter(c["type"] for c in comments)
        results[u] = {
            "user": u, "repo": repo, "since": since, "until": until,
            "commits": commits_by_user[u], "scored": scored, "reviewed": reviewed, "comments": comments,
            "comment_prs": len({c["pr"] for c in comments}),
            "issue_comments": by_type["issue_comment"], "review_comments": by_type["review_comment"],
            "reviews": by_type["review"], "comments_total": len(comments),
            "backport_own": d["backports"]["own"], "backport_review": d["backports"]["review"],
            "reviewed_newcomer": sum(1 for r in reviewed if r["author_association"] in NEWCOMER),
            "reviewed_acted": sum(1 for r in reviewed if r["acted_on"]),
            "reviewed_first": sum(1 for r in reviewed if r["first_reviewer"]),
            "reviewed_noncommitter": sum(1 for r in reviewed if r["author_association"] in NON_COMMITTER),
            "mentored": sum(1 for r in reviewed if r["mentored"]),
        }
    return results


def quality(rows, what, n, baseline):
    """回傳 (數值, 說明文字)。rows 已依分數由高到低排序。
    不足 n 個時缺的名額用 baseline 補；完全沒有樣本就是 0，不能比有做事的人高。"""
    if not rows:
        return 0.0, f"+0.00（沒有{what.strip()}）"
    top = [r["score"] for r in rows[:n]]
    if len(top) >= n:
        v = sum(top) / n
        return v, f"{v:+.2f}（{what}分數最高的 {n} 個平均）"
    v = (sum(top) + baseline * (n - len(top))) / n
    return v, f"{v:+.2f}（樣本 {len(top)}/{n}，其餘以基準 {baseline:.1f} 補）"


def median(xs, default):
    xs = sorted(xs)
    if not xs:
        return default
    m = len(xs) // 2
    return xs[m] if len(xs) % 2 else (xs[m - 1] + xs[m]) / 2


def metrics(scored, reviewed, stats, baselines=(BASELINE_PR_DEFAULT, BASELINE_REVIEW_DEFAULT)):
    """從 PR 清單和統計數字算出各分數，單 repo 和跨 repo 合併都用這個。"""
    pr_q, pr_q_text = quality(scored, "PR ", QUALITY_TOP_N_PR, baselines[0])
    rv_depth, rv_depth_text = quality(reviewed, "review ", QUALITY_TOP_N_REVIEW, baselines[1])
    n_sub = sum(1 for r in reviewed if r["substantive"])
    n_ment = sum(1 for r in reviewed if r["mentored"])
    breadth = (REVIEW_BREADTH["substantive_log"] * math.log2(1 + n_sub)
               + REVIEW_BREADTH["mentored_log"] * math.log2(1 + n_ment))
    rv_q = rv_depth + breadth
    rv_q_text = (f"{rv_q:+.2f}　深度 {rv_depth_text}；"
                 f"廣度 {breadth:+.2f}（實質 review {n_sub} 個、帶非 committer {n_ment} 個）")
    q_total, q_text = pr_q + rv_q, f"{pr_q + rv_q:+.2f}"
    imp_total = round(sum(r["counted"] for r in scored if r["important"]), 2)
    gen_total = round(sum(r["counted"] for r in scored if not r["important"]), 2)
    pr_total = round(imp_total + gen_total, 2)
    rv_total = round(sum(r["score"] for r in reviewed), 2)
    merged = sum(1 for r in scored if r["merged_at"])
    return {
        "pr_total": pr_total, "imp_total": imp_total, "gen_total": gen_total,
        "rv_total": rv_total,
        "total": round(rv_total + imp_total + gen_total, 2),
        "pr_q": pr_q, "pr_q_text": pr_q_text, "rv_q": rv_q, "rv_q_text": rv_q_text,
        "rv_depth": rv_depth, "rv_breadth": breadth,
        "q_total": q_total, "q_text": q_text,
        "n_pr": len(scored), "n_merged": merged, "n_reviewed": len(reviewed),
        "n_important": sum(1 for r in scored if r["important"] and r["merged_at"]),
        "n_open": sum(1 for r in scored if r["pr_state"] == "open"),
        "best_pr": scored[0] if scored else None,
        "best_review": reviewed[0] if reviewed else None,
        "deep_reviews": sum(1 for r in reviewed if r["review_comments"] >= DEEP_REVIEW_INLINE),
        "inline_per_pr": (stats["review_comments"] / len(reviewed)) if reviewed else 0.0,
    }


def summary_lines(scored, reviewed, stats, m, title, show_repo=False):
    lines = [
        f"{title}", "",
        f"總分：{m['total']:+.2f}",
        f"- Review 別人的 PR：{m['rv_total']:+.2f}（{m['n_reviewed']} 個加總）",
        f"- 重要 PR：{m['imp_total']:+.2f}（重要性 ≥ {IMPORTANT_PR_MIN:g} 的 PR，已 merge {m['n_important']} 個）",
        f"- 一般 PR：{m['gen_total']:+.2f}（乘 {PR_BUCKET_FACTOR['general']:g}；未 merge 再乘 {PR_STATE_FACTOR['open']:g}，關掉不計）", "",
        f"品質分：{m['q_text']}",
        f"- 自己開的 PR：{m['pr_q_text']}",
        f"- Review 別人的 PR：{m['rv_q_text']}", "",
        f"- Commits：{stats['commits']}",
        f"- 自己開的 PR：{m['n_pr']}（已 merge：{m['n_merged']}，其中重要 {m['n_important']}；還開著：{m['n_open']}）"
        + (f"，最佳 PR：{m['best_pr']['score']:+g}（#{m['best_pr']['number']} {m['best_pr']['title']}）" if m["best_pr"] else ""),
        f"- 有留言的 PR 數：{stats['comment_prs']}",
        f"- 一般留言（issue comment）：{stats['issue_comments']}",
        f"- Review 行內留言：{stats['review_comments']}",
        f"- Review 結論（approve / request changes / comment）：{stats['reviews']}",
        f"- 留言總數：{stats['comments_total']}",
        f"- Backport PR（不計分）：自己開的 {stats['backport_own']} 個、review 過的 {stats['backport_review']} 個",
        f"- Review 過的非 committer PR：{stats['reviewed_noncommitter']} 個（其中第一次貢獻者 {stats['reviewed_newcomer']} 個）",
        f"- 帶非 committer（留 {MENTOR_MIN_INLINE} 則以上非 nit 行內意見或 request changes、最後 merge）：{stats['mentored']} 個",
        f"- Review 後 PR 有更新（review 起作用）：{stats['reviewed_acted']} 個",
        f"- 第一個留實質意見的 PR：{stats['reviewed_first']} 個",
        f"- 深度 review（行內 {DEEP_REVIEW_INLINE} 則以上，每個 +{REVIEW_WEIGHTS['deep_review']:g}）：{m['deep_reviews']} 個"
        + (f"，最佳 review：{m['best_review']['score']:+.1f}（#{m['best_review']['number']} {m['best_review']['title']}）" if m["best_review"] else ""),
    ]
    tag = (lambda r: f"{r['repo']} " if show_repo else "")
    if scored:
        lines += ["", "自己開的 PR 評分（由高到低；[重要性 → 計入總分]）", ""]
        for r in scored:
            kind = "重要" if r["important"] else "一般"
            st = {"merged": "、commit 進 default branch" if r.get("merged_via_commit") else "",
                  "open": "、未 merge", "closed": "、已關閉"}[r["pr_state"]]
            lines.append(f"- [{r['score']:+g} → {r['counted']:+g} {kind}{st}] {tag(r)}#{r['number']} {r['title']}"
                         f"　（{', '.join(r['score_reasons']) or '無加減分'}）")
    if reviewed:
        lines += ["", "Review 過的 PR 評分（由高到低）", ""]
        for r in reviewed:
            detail = (f"行內 {r['review_comments']}（實質 {r.get('inline_substantive', 0)}）、一般 {r['issue_comments']}、結論 {r['reviews']}"
                      + (f"、{', '.join(r['score_reasons'])}" if r["score_reasons"] else ""))
            lines.append(f"- [{r['score']:+.2f}] {tag(r)}#{r['number']} {r['title']}　（{detail}）")
    return lines


STAT_KEYS = ("commits", "comment_prs", "issue_comments", "review_comments", "reviews",
             "comments_total", "backport_own", "backport_review", "reviewed_newcomer", "reviewed_acted",
             "reviewed_first", "reviewed_noncommitter", "mentored")


def combine(results):
    """把同一個人在多個 repo 的結果合併：數量分相加，品質分把所有 PR 放在一起取前 N。"""
    scored = sorted((r for res in results for r in res["scored"]), key=lambda x: -x["score"])
    reviewed = sorted((r for res in results for r in res["reviewed"]), key=lambda x: -x["score"])
    stats = {k: sum(res[k] for res in results) for k in STAT_KEYS}
    return scored, reviewed, stats


DUMP_HUNK_LINES = 8   # --dump 時每則行內留言附上 diff 片段的最後幾行（看留言針對的程式碼）


def dump_user(path, user, repos, results, scored, reviewed, stats, m, since, until):
    """把一個人的評分明細和區間內所有留言全文寫成一個 markdown 檔，給人或 LLM 看他在 PR 上的討論內容。
    留言依 PR 分段（PR 依第一則留言的時間排序）、段內依時間排序；每段標明 PR 作者與身分、merge 狀態、該 PR 的計分。"""
    comments = [c for res in results for c in res["comments"]]
    own = {(r["repo"], r["number"]): r for r in scored}
    rv = {(r["repo"], r["number"]): r for r in reviewed}
    lines = [f"# {user}", f"區間：{since} ~ {until}；repo：{', '.join(repos)}", ""]
    lines += summary_lines(scored, reviewed, stats, m, "## 評分", show_repo=len(results) > 1)

    by_pr = {}
    for c in comments:
        by_pr.setdefault((c["repo"], c["pr"]), []).append(c)
    n_short = sum(1 for c in comments if c["type"] != "review" and is_nit(c["body"]))
    lines += ["", "## 留言全文", "",
              f"{len(by_pr)} 個 PR、{len(comments)} 則（行內 {stats['review_comments']}、一般 {stats['issue_comments']}、"
              f"review 結論 {stats['reviews']}；短留言或只挑格式的 nit {n_short} 則，標 [nit]）。"
              f"行內留言附上 diff 片段的最後 {DUMP_HUNK_LINES} 行。", ""]
    for (repo, num), cs in sorted(by_pr.items(), key=lambda kv: min(c["date"] or "" for c in kv[1])):
        cs.sort(key=lambda c: c["date"] or "")
        c0 = cs[0]
        who = "本人開的 PR" if c0["pr_author"] == user else f"作者 {c0['pr_author']}（{c0['pr_assoc'] or 'NONE'}）"
        state = "已 merge" if c0["pr_merged"] else ("還開著" if c0["pr_state"] == "open" else "已關閉")
        if (repo, num) in rv:
            r = rv[(repo, num)]
            sc = f"review 分 {r['score']:+.2f}" + (f"（{', '.join(r['score_reasons'])}）" if r["score_reasons"] else "")
        elif (repo, num) in own:
            r = own[(repo, num)]
            sc = f"重要性 {r['score']:+g} → 計入 {r['counted']:+g}（{', '.join(r['score_reasons']) or '無加減分'}）"
        elif is_backport(c0["pr_title"]):
            sc = "backport，不計分"
        else:
            sc = "不計分"
        lines += [f"### {repo}#{num} {c0['pr_title']}", c0["pr_url"], f"{who}；{state}；{sc}", ""]
        for c in cs:
            when = (c["date"] or "")[:16].replace("T", " ")
            body = c["body"].replace("\r\n", "\n").strip()
            if c["type"] == "review":
                label = f"review {c['review_state'] or ''}".strip()
                if not body:
                    lines.append(f"- {when} [{label}]（無文字）")
                    continue
            elif c["type"] == "review_comment":
                label = f"行內 {c['path']}" + (f":{c['line']}" if c["line"] else "")
            else:
                label = "一般留言"
            if c["type"] != "review" and is_nit(body):
                label += "] [nit"
            lines.append(f"- {when} [{label}]")
            if c.get("hunk"):
                lines.append("  ```diff")
                lines += ["  " + l for l in c["hunk"].splitlines()[-DUMP_HUNK_LINES:]]
                lines.append("  ```")
            lines += ["  " + l for l in body.splitlines()]
            lines.append("")
        lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def fmt(v, digits=2):
    return f"{v:+.{digits}f}"


def best_pr(m):
    b = m["best_pr"]
    return f"{b['score']:+g}（#{b['number']}）" if b else "無"


def best_review(m):
    b = m["best_review"]
    return f"{b['score']:+.1f}（#{b['number']}，行內 {b['review_comments']}）" if b else "無"


def main():
    global _cache_enabled, CACHE_DIR
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", help="JSON 設定檔的路徑或 http(s) 網址，多個人 × 多個 repo")
    ap.add_argument("--repo", help="單人模式：owner/repo 或 https://github.com/owner/repo")
    ap.add_argument("--user", help="單人模式：GitHub 帳號")
    ap.add_argument("--since", help="YYYY-MM-DD，預設為 60 天前（--config 內的設定優先）")
    ap.add_argument("--until", help="YYYY-MM-DD，預設為今天")
    ap.add_argument("--detail", action="store_true", help="總表之後再印每個人的明細（預設只印總表）")
    ap.add_argument("--format", choices=["text", "markdown"], default="text",
                    help="輸出格式：text（預設，terminal / Jenkins console 好讀）或 markdown")
    ap.add_argument("--dump", metavar="DIR",
                    help="另外替每個人寫一個 DIR/<帳號>.md：評分明細加上區間內所有留言全文（含行內留言的 diff 片段）")
    ap.add_argument("--no-cache", action="store_true", help="不使用快取（ETag 和每個 PR 的原始資料都不用）")
    ap.add_argument("--cache-dir", help="快取目錄，預設 ~/.cache/gh_activity")
    a = ap.parse_args()
    _cache_enabled = not a.no_cache
    if a.cache_dir:
        CACHE_DIR = a.cache_dir
    if _cache_enabled:
        try:
            os.makedirs(CACHE_DIR, exist_ok=True)
            probe = os.path.join(CACHE_DIR, ".write_test")
            open(probe, "w").close(); os.remove(probe)
        except OSError as e:
            print(f"警告：快取目錄 {CACHE_DIR} 無法寫入（{e.strerror}），這次不使用快取", file=sys.stderr)
            _cache_enabled = False

    if not TOKEN:
        sys.exit("未設定 GITHUB_TOKEN，每小時只有 60 次額度，跑不完。請 export GITHUB_TOKEN=... 後再執行。")

    if a.config:
        cfg = load_config(a.config)
        since, until = cfg.get("since") or a.since, cfg.get("until") or a.until
        if "users" in cfg:
            # 新格式：每個人都查全部 repo
            repos = [normalize_repo(r) for r in cfg["repos"]]
            people = [{"user": u, "repos": repos} for u in cfg["users"]]
        else:
            # 舊格式：每個人各自列 repo
            people = [{"user": p["user"], "repos": [normalize_repo(r) for r in p["repos"]]} for p in cfg["people"]]
    elif a.repo and a.user:
        people = [{"user": a.user, "repos": [normalize_repo(a.repo)]}]
        since, until = a.since, a.until
    else:
        ap.error("請給 --config，或同時給 --repo 與 --user")
    today = datetime.now(timezone.utc).date()
    until = until or today.isoformat()
    since = since or (today - timedelta(days=DEFAULT_DAYS)).isoformat()

    # 反轉成 repo → 要查的人，一個 repo 只跑一次
    repo_users = {}
    for p in people:
        for repo in p["repos"]:
            repo_users.setdefault(repo, []).append(p["user"])

    first = next((r for r, us in repo_users.items() if len(us) >= 2), None)
    if first:
        verify_or_semantics(first, since, repo_users[first])

    # 逐 repo 抓。commit 清單一律要抓（判斷「關掉但有進 default branch」的 PR 要用）
    with_commits = True
    t_all, s_all = time.time(), _snapshot()
    results_by_user = {p["user"]: [] for p in people}
    skipped = {}
    for repo, users in repo_users.items():
        try:
            for u, res in analyze_repo(repo, users, since, until, with_commits).items():
                results_by_user[u].append(res)
        except RepoInaccessible as e:
            print(f"  跳過：{e}", file=sys.stderr)
            skipped[repo] = users
    for repo in skipped:
        repo_users.pop(repo)
    print(f"\n全部 {len(repo_users)} 個 repo 合計 {_elapsed(t_all, s_all)}\n", file=sys.stderr)

    fetched = []
    for p in people:
        results = results_by_user[p["user"]]
        p["skipped"] = [r for r, us in skipped.items() if p["user"] in us]
        if results:
            fetched.append((p, results))
        else:
            print(f"  {p['user']} 沒有任何可存取的 repo，略過", file=sys.stderr)

    # 品質分的基準值：多人模式用全體受評者的中位數，單人模式用固定值
    if len(fetched) > 1:
        all_pr = [r["score"] for _, results in fetched for res in results for r in res["scored"]]
        all_rv = [r["score"] for _, results in fetched for res in results for r in res["reviewed"]]
        baselines = (median(all_pr, BASELINE_PR_DEFAULT), median(all_rv, BASELINE_REVIEW_DEFAULT))
    else:
        baselines = (BASELINE_PR_DEFAULT, BASELINE_REVIEW_DEFAULT)

    rows = []   # (person, combined metrics, per-repo results, ...)
    for p, results in fetched:
        scored, reviewed, stats = combine(results)
        m = metrics(scored, reviewed, stats, baselines)
        per_repo = {r["repo"]: metrics(r["scored"], r["reviewed"], r, baselines)["total"] for r in results}
        m["main_repo"] = max(per_repo, key=per_repo.get)
        m["main_repo_total"] = per_repo[m["main_repo"]]
        if m["main_repo_total"] <= 0:
            m["main_repo"] = None   # 完全沒活動，不要顯示成第一個 repo
        rows.append((p, m, results, scored, reviewed, stats))

    if a.dump:
        os.makedirs(a.dump, exist_ok=True)
        for p, m, results, scored, reviewed, stats in rows:
            dump_user(os.path.join(a.dump, f"{p['user']}.md"), p["user"], p["repos"],
                      results, scored, reviewed, stats, m, since, until)
        print(f"已替 {len(rows)} 個人各寫一個留言全文檔到 {a.dump}/<帳號>.md", file=sys.stderr)

    # 單人單 repo：維持原本的輸出格式
    if len(rows) == 1 and len(rows[0][2]) == 1:
        p, m, results, scored, reviewed, stats = rows[0]
        lines = summary_lines(scored, reviewed, stats, m,
                              f"# {p['user']} @ {results[0]['repo']}\n區間：{since} ~ {until}")
        print(render(lines, a.format))
        return

    # 多人：總表 + 每人明細
    rows.sort(key=lambda x: -x[1]["total"])
    out = [f"# 評比結果", f"區間：{since} ~ {until}",
           f"總分 = review + 重要 PR + 一般 PR。重要 PR：重要性 ≥ {IMPORTANT_PR_MIN:g}；"
           f"一般 PR 乘 {PR_BUCKET_FACTOR['general']:g}；未 merge 的 PR 乘 {PR_STATE_FACTOR['open']:g}，關掉不計"
           f"（關掉但有 commit 進 default branch 的，例如 YuniKorn 的合併方式，視同 merge）",
           f"品質分基準值（樣本不足時補入）：PR {baselines[0]:.2f}、review {baselines[1]:.2f}（全體中位數）；沒有活動就是 0", "",
           "## 總表（依總分排序）", "",
           "| 人 | 總分 | review | 重要 PR | 一般 PR | 品質分 | PR merge/開 | 重要 | Review PR | 深度 | 帶非 committer | 主要 repo |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    if skipped:
        out.insert(3, "無法存取而跳過的 repo：" + "、".join(skipped))
    for p, m, results, scored, reviewed, stats in rows:
        main = f"{m['main_repo']}（{m['main_repo_total']:+.1f}）" if m["main_repo"] else "無"
        out.append(f"| {p['user']} | {m['total']:+.1f} | {m['rv_total']:+.1f} | {m['imp_total']:+.1f} "
                   f"| {m['gen_total']:+.1f} | {fmt(m['q_total'])} "
                   f"| {m['n_merged']}/{m['n_pr']} | {m['n_important']} "
                   f"| {m['n_reviewed']} | {m['deep_reviews']} | {stats['mentored']} "
                   f"| {main} |")
    out += ["", f"## 全體活動量（{len(rows)} 人加總，原始數字不計分）", "",
            "| repo | 開的 PR | 已 merge | 還開著 | Review 過的 PR | 留言總數 | Review 非 committer（新人） | 帶非 committer |",
            "|---|---|---|---|---|---|---|---|"]
    by_repo = {}
    for p, m, results, scored, reviewed, stats in rows:
        for res in results:
            g = by_repo.setdefault(res["repo"], {"n_pr": 0, "n_merged": 0, "n_open": 0, "n_reviewed": 0,
                                                 **{k: 0 for k in STAT_KEYS}})
            g["n_pr"] += len(res["scored"])
            g["n_merged"] += sum(1 for r in res["scored"] if r["merged_at"])
            g["n_open"] += sum(1 for r in res["scored"] if r["pr_state"] == "open")
            g["n_reviewed"] += len(res["reviewed"])
            for k in STAT_KEYS:
                g[k] += res[k]
    groups = list(by_repo.items())
    if len(groups) > 1:
        total = {k: sum(g[k] for g in by_repo.values()) for k in groups[0][1]}
        groups.append(("合計", total))
    for name, g in groups:
        out.append(f"| {name} | {g['n_pr']} | {g['n_merged']} | {g['n_open']} | {g['n_reviewed']} | {g['comments_total']} "
                   f"| {g['reviewed_noncommitter']}（{g['reviewed_newcomer']}） | {g['mentored']} |")
    if a.detail:
        for p, m, results, scored, reviewed, stats in rows:
            out += ["", "---", "", f"## {p['user']}", ""]
            if len(results) > 1:
                out += summary_lines(scored, reviewed, stats, m, f"### 合計（{', '.join(p['repos'])}）", show_repo=True)
                for res in results:
                    rm = metrics(res["scored"], res["reviewed"], res, baselines)
                    out += [""] + summary_lines(res["scored"], res["reviewed"], res, rm, f"### {res['repo']}")
            else:
                out += summary_lines(scored, reviewed, stats, m, f"### {results[0]['repo']}")
    print(render(out, a.format))


if __name__ == "__main__":
    main()