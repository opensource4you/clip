#!/usr/bin/env bash
# pr-review.sh
# Fetch a GitHub PR, merge it (no commit) onto the latest base branch, and write
# ONE markdown bundle for review:
#   PR metadata + description, CI check results for the PR head, full discussion
#   (comments, reviews, inline review threads with their resolved state), commit
#   list, stat, full diff, and full contents of modified files.
#   Only the diff context quoted under each inline thread is shortened (see
#   HUNK_LINES); everything else is complete.
#
# Usage:
#   pr-review.sh <pr-url> [base-branch]         # any GitHub repo, e.g.
#                                               #   pr-review.sh https://github.com/apache/kafka-site/pull/876
#   pr-review.sh <pr-number> [base-branch]      # apache/kafka (legacy form)
#
# base-branch defaults to the PR's own base branch (via `gh`), falling back to
# the repo's default branch (via `git ls-remote`).
#
# Env overrides:
#   REPO_DIR      local clone to operate on; default $HOME/project/<repo-name>,
#                 cloned automatically if missing
#   UPSTREAM      repo URL; derived from <pr-url>, default https://github.com/apache/kafka
#   OUT_DIR       default $HOME/pr-review
#   GITHUB_TOKEN  optional; falls back to `gh auth token`. Without a token the
#                 GitHub API allows 60 requests/hour, which is enough for a few PRs,
#                 but the resolved state of review threads is skipped (GraphQL
#                 needs a token).
#   HUNK_LINES    diff lines quoted under each inline thread; default 12, 0 = the
#                 whole hunk as GitHub returns it
#   GITHUB_API_URL  API root; default https://api.github.com
#
# Side effects (same as the manual workflow): the working tree is hard-reset,
# untracked files are removed, <base-branch> is forced to match upstream, and
# the PR changes are left UNSTAGED in the working tree when the script exits.
# On merge conflict the script aborts the merge and exits 1.

set -euo pipefail

ARG="${1:?usage: $(basename "$0") <pr-url|pr-number> [base-branch]}"
OUT_DIR="${OUT_DIR:-$HOME/pr-review}"

# ---- Resolve repo + PR number from the argument -----------------------------
if [[ "$ARG" =~ ^https?://github\.com/([^/]+)/([^/]+)/pull/([0-9]+) ]]; then
  UPSTREAM="https://github.com/${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
  PR_NUMBER="${BASH_REMATCH[3]}"
elif [[ "$ARG" =~ ^[0-9]+$ ]]; then
  UPSTREAM="${UPSTREAM:-https://github.com/apache/kafka}"
  PR_NUMBER="$ARG"
else
  echo "ERROR: expected a GitHub PR URL or a PR number, got: $ARG" >&2
  exit 2
fi

REPO_SLUG="$(printf '%s' "$UPSTREAM" | sed -E 's#^(https://github\.com/|git@github\.com:)##; s#\.git$##; s#/$##')"
REPO_NAME="${REPO_SLUG##*/}"
REPO_DIR="${REPO_DIR:-$HOME/project/$REPO_NAME}"
GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
export GITHUB_TOKEN

# ---- Resolve base branch: explicit arg > PR's base > repo default -----------
MAIN="${2:-${MAIN:-}}"
if [ -z "$MAIN" ] && command -v gh >/dev/null 2>&1; then
  MAIN="$(gh api "repos/$REPO_SLUG/pulls/$PR_NUMBER" --jq .base.ref 2>/dev/null || true)"
fi
if [ -z "$MAIN" ]; then
  MAIN="$(git ls-remote --symref "$UPSTREAM" HEAD 2>/dev/null \
          | sed -nE 's#^ref: refs/heads/([^[:space:]]+)[[:space:]]+HEAD$#\1#p')"
fi
if [ -z "$MAIN" ]; then
  echo "ERROR: could not determine base branch for $REPO_SLUG; pass it as the 2nd argument" >&2
  exit 1
fi

echo "==> Repo $REPO_SLUG, PR #$PR_NUMBER, base branch $MAIN"

if [ ! -d "$REPO_DIR/.git" ]; then
  echo "==> Cloning $UPSTREAM into $REPO_DIR"
  git clone -q "$UPSTREAM" "$REPO_DIR"
fi
cd "$REPO_DIR"

echo "==> Resetting working tree"
git reset -q --hard
git clean -fdq

echo "==> Syncing $MAIN with $UPSTREAM"
git fetch -q "$UPSTREAM" "$MAIN"
# -C creates the branch if the clone has never checked it out (e.g. a release branch).
git switch -q -C "$MAIN" FETCH_HEAD

echo "==> Fetching PR #$PR_NUMBER"
git branch -D "$PR_NUMBER" >/dev/null 2>&1 || true
git fetch -q "$UPSTREAM" "pull/$PR_NUMBER/head:$PR_NUMBER"

BASE_SHA="$(git rev-parse HEAD)"
PR_SHA="$(git rev-parse "$PR_NUMBER")"
MERGE_BASE="$(git merge-base HEAD "$PR_NUMBER")"
BEHIND="$(git rev-list --count "$PR_NUMBER..HEAD")"
COMMITS="$(git log --format='- %h %s (%an, %as)' "$MERGE_BASE..$PR_NUMBER")"

echo "==> Merging PR #$PR_NUMBER into $MAIN (no commit)"
if ! git merge -q --no-commit --no-ff "$PR_NUMBER" >/dev/null 2>&1; then
  echo "ERROR: PR #$PR_NUMBER conflicts with $MAIN @ $BASE_SHA in:" >&2
  git diff --name-only --diff-filter=U | sed 's/^/  /' >&2
  git merge --abort
  echo "Merge aborted. Branch '$PR_NUMBER' is kept for inspection; ask the author to rebase." >&2
  exit 1
fi

# ---- PR description + discussion from the GitHub API (needs python3) -------
pr_discussion() {
  if ! command -v python3 >/dev/null 2>&1; then
    echo "## PR description"; echo
    echo "_(python3 not found; PR description and discussion skipped)_"; echo
    return
  fi
  python3 - "$REPO_SLUG" "$PR_NUMBER" <<'PY'
import json, os, sys, urllib.request

slug, num = sys.argv[1], sys.argv[2]
token = os.environ.get("GITHUB_TOKEN") or ""
API = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
HUNK_LINES = int(os.environ.get("HUNK_LINES", "12"))

def headers():
    h = {"Accept": "application/vnd.github+json", "User-Agent": "pr-review.sh"}
    if token:
        h["Authorization"] = "Bearer " + token
    return h

def get(path, paginate=False, key=None):
    """GET one object, or all pages of a list. `key` names the list inside a
    wrapped response (check-runs returns {"check_runs": [...]})."""
    url = API + path
    if paginate:
        url += ("&" if "?" in url else "?") + "per_page=100"
    out = []
    while url:
        req = urllib.request.Request(url, headers=headers())
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
            link = r.headers.get("Link", "") or ""
        if not paginate:
            return data
        out.extend(data[key] if key else data)
        url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part[part.find("<") + 1:part.find(">")]
    return out

def text(body):
    body = (body or "").strip()
    return body if body else "_(no text)_"

def thread_states():
    """Root review-comment id -> True if the thread is resolved. The REST API
    does not expose this, so ask GraphQL, which requires a token."""
    if not token:
        return None, "no GitHub token"
    owner, name = slug.split("/", 1)
    query = """query($o:String!,$n:String!,$p:Int!,$c:String){
      repository(owner:$o,name:$n){pullRequest(number:$p){
        reviewThreads(first:100,after:$c){
          pageInfo{hasNextPage endCursor}
          nodes{isResolved comments(first:1){nodes{databaseId}}}}}}}"""
    states, cursor = {}, None
    try:
        while True:
            body = json.dumps({"query": query, "variables":
                               {"o": owner, "n": name, "p": int(num), "c": cursor}}).encode()
            req = urllib.request.Request(API + "/graphql", data=body, headers=headers())
            with urllib.request.urlopen(req, timeout=30) as r:
                data = json.load(r)
            if data.get("errors"):
                return None, data["errors"][0].get("message", "GraphQL error")
            page = data["data"]["repository"]["pullRequest"]["reviewThreads"]
            for node in page["nodes"]:
                first = node["comments"]["nodes"]
                if first:
                    states[first[0]["databaseId"]] = node["isResolved"]
            if not page["pageInfo"]["hasNextPage"]:
                return states, None
            cursor = page["pageInfo"]["endCursor"]
    except Exception as e:
        return None, str(e)

def ci_checks(sha):
    """Check runs for the PR head: counts, then details of everything that is
    not a plain pass, with failure annotations (these carry the failed tests)."""
    print(f"## CI checks (PR head `{sha[:10]}`)\n")
    try:
        runs = get(f"/repos/{slug}/commits/{sha}/check-runs", True, "check_runs")
    except Exception as e:
        print(f"_(check runs request failed: {e})_\n")
        return
    if not runs:
        print("_(no check runs reported for this commit)_\n")
        return
    def outcome(r):
        return r.get("conclusion") or r.get("status") or "unknown"
    counts = {}
    for r in runs:
        counts[outcome(r)] = counts.get(outcome(r), 0) + 1
    print(", ".join(f"{n} {k}" for k, n in sorted(counts.items())) + "\n")
    quiet = ("success", "skipped", "neutral")
    passed = sorted(r["name"] for r in runs if outcome(r) == "success")
    if passed:
        print("Passed: " + ", ".join(passed) + "\n")
    for r in sorted(runs, key=lambda r: r["name"]):
        if outcome(r) in quiet:
            continue
        print(f"### {r['name']}: {outcome(r)}\n")
        if r.get("html_url"):
            print(r["html_url"] + "\n")
        out = r.get("output") or {}
        if (out.get("title") or "").strip():
            print(f"**{out['title'].strip()}**\n")
        if (out.get("summary") or "").strip():
            print(out["summary"].strip() + "\n")
        if outcome(r) != "failure" or not out.get("annotations_count"):
            continue
        try:
            notes = get(f"/repos/{slug}/check-runs/{r['id']}/annotations", True)
        except Exception as e:
            print(f"_(annotations request failed: {e})_\n")
            continue
        for a in notes:
            if a.get("annotation_level") != "failure":
                continue
            where = a.get("path") or ""
            if where and a.get("start_line"):
                where += f":{a['start_line']}"
            head = " ".join(x for x in (a.get("title") or "", f"`{where}`" if where else "") if x)
            print(f"- {head}".rstrip())
            for line in (a.get("message") or "").strip().splitlines():
                print(f"  {line}")
        print()

print("## PR description\n")
try:
    pr = get(f"/repos/{slug}/pulls/{num}")
    comments = get(f"/repos/{slug}/issues/{num}/comments", True)
    reviews = get(f"/repos/{slug}/pulls/{num}/reviews", True)
    rcomments = get(f"/repos/{slug}/pulls/{num}/comments", True)
except Exception as e:
    print(f"_(GitHub API request failed: {e})_\n")
    sys.exit(0)

print(f"**Title:** {pr['title']}  ")
print(f"**Author:** {pr['user']['login']}  ")
print(f"**URL:** {pr['html_url']}  ")
print(f"**Base:** {pr['base']['ref']}  ")
state = pr["state"] + (" (draft)" if pr.get("draft") else "")
print(f"**State:** {state}  ")
print(f"**Created:** {pr['created_at']}  ")
labels = ", ".join(l["name"] for l in pr.get("labels", []))
if labels:
    print(f"**Labels:** {labels}  ")
print()
print(text(pr.get("body")))
print()

ci_checks(pr["head"]["sha"])

# Build one chronological timeline: conversation comments, review verdicts,
# and inline review threads (root comment + replies).
events = []
for c in comments:
    events.append((c["created_at"], "comment", c))
for r in reviews:
    st = r.get("state", "")
    if st == "PENDING":
        continue
    if st == "COMMENTED" and not (r.get("body") or "").strip():
        continue  # empty container for inline comments
    events.append((r.get("submitted_at") or "", "review", r))
threads, roots = {}, []
for rc in sorted(rcomments, key=lambda x: x["created_at"]):
    parent = rc.get("in_reply_to_id")
    if parent and parent in threads:
        threads[parent].append(rc)
    else:
        threads[rc["id"]] = [rc]
        roots.append(rc)
for rc in roots:
    events.append((rc["created_at"], "thread", threads[rc["id"]]))
events.sort(key=lambda e: e[0])

resolved, why = (thread_states() if roots else ({}, None))
print(f"## Discussion ({len(events)} items, chronological)\n")
if not events:
    print("_(no comments yet)_\n")
if resolved is None:
    print(f"_(resolved state of inline threads unavailable: {why})_\n")
elif roots:
    still_open = sum(1 for rc in roots if resolved.get(rc["id"]) is False)
    unknown = sum(1 for rc in roots if rc["id"] not in resolved)
    note = f", {unknown} with unknown state" if unknown else ""
    print(f"Inline threads: {len(roots)} total, {still_open} unresolved{note}.\n")
for ts, kind, obj in events:
    if kind == "comment":
        print(f"### {obj['user']['login']} commented ({ts})\n")
        print(text(obj["body"]))
        print()
    elif kind == "review":
        verdict = obj["state"].replace("_", " ").lower()
        print(f"### {obj['user']['login']} review: {verdict} ({ts})\n")
        print(text(obj.get("body")))
        print()
    else:
        root = obj[0]
        line = root.get("line") or root.get("original_line")
        outdated = " (outdated)" if root.get("line") is None else ""
        state = ""
        if resolved is not None:
            state = {True: " [resolved]", False: " [unresolved]"}.get(resolved.get(root["id"]), " [state unknown]")
        print(f"### Inline thread: `{root['path']}` line {line}{outdated}{state} ({ts})\n")
        hunk = (root.get("diff_hunk") or "").rstrip("\n").split("\n")
        if hunk != [""]:
            shown = hunk[-HUNK_LINES:] if 0 < HUNK_LINES < len(hunk) else hunk
            if len(shown) < len(hunk):
                print(f"_(last {len(shown)} of {len(hunk)} hunk lines; the comment is on the final line)_\n")
            print("````diff")
            print("\n".join(shown))
            print("````")
            print()
        for c in obj:
            print(f"**{c['user']['login']}** ({c['created_at']}):\n")
            print(text(c["body"]))
            print()
PY
}

# ---- Write bundle ----------------------------------------------------------
mkdir -p "$OUT_DIR"
BUNDLE="$OUT_DIR/$REPO_NAME-pr-$PR_NUMBER.md"

lang_of() {
  case "${1##*.}" in
    java) echo java ;;
    scala) echo scala ;;
    py) echo python ;;
    kt|kts) echo kotlin ;;
    gradle|groovy) echo groovy ;;
    sh|bash) echo bash ;;
    go) echo go ;;
    rs) echo rust ;;
    js|mjs) echo javascript ;;
    ts|tsx) echo typescript ;;
    css|scss) echo css ;;
    md) echo markdown ;;
    json) echo json ;;
    yml|yaml) echo yaml ;;
    xml) echo xml ;;
    html|htm) echo html ;;
    properties|conf|cfg) echo properties ;;
    *) echo text ;;
  esac
}

echo "==> Writing $BUNDLE"
{
  echo "# PR #$PR_NUMBER review bundle ($REPO_SLUG)"
  echo
  echo "- Generated: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Base branch: \`$MAIN\` @ \`$BASE_SHA\`"
  echo "- PR head: \`$PR_SHA\`"
  echo "- Merge base: \`$MERGE_BASE\` (PR is $BEHIND commit(s) behind \`$MAIN\`)"
  echo "- Merge status: clean"
  echo
  echo "The diff and file contents below are the PR merged onto the latest \`$MAIN\`, not the PR's own base commit."
  echo
  pr_discussion
  echo "## Commits in PR"
  echo
  echo "${COMMITS:-_(none)_}"
  echo
  echo "## Changed files"
  echo
  echo '```'
  # Wide enough that git never abbreviates long paths with "...".
  git diff HEAD -M --stat=10000
  echo '```'
  echo
  echo "## Diff"
  echo
  echo '````diff'
  git diff HEAD -M
  echo '````'
  echo
  echo "## Full contents of modified files (post-merge)"
  echo
  echo "_Added files are omitted here since the diff already contains them in full. Deleted and binary files are omitted. All other files are included in full._"
  echo
  git diff HEAD -M --name-status | while IFS=$'\t' read -r st p1 p2; do
    code="${st:0:1}"
    path="${p2:-$p1}"
    case "$code" in M|R|C) ;; *) continue ;; esac
    [ -f "$path" ] || continue
    grep -qI '' "$path" || continue   # skip binary
    total="$(wc -l < "$path" | tr -d ' ')"
    echo "### $path ($total lines)"
    echo
    echo "\`\`\`\`$(lang_of "$path")"
    cat "$path"
    # Close the fence on its own line even if the file has no trailing newline.
    [ -z "$(tail -c 1 "$path")" ] || echo
    echo '````'
    echo
  done
} > "$BUNDLE"

# ---- Leave the repo in the same state as the manual workflow ---------------
git reset -q >/dev/null 2>&1 || true
git branch -D "$PR_NUMBER" >/dev/null

echo
echo "==> Done"
echo "    bundle : $BUNDLE ($(du -h "$BUNDLE" | cut -f1))"
echo "    working tree: PR changes left unstaged on $MAIN"