#!/usr/bin/env bash
# pr-review.sh
# Fetch a GitHub PR, merge it (no commit) onto the latest base branch, and write
# ONE markdown bundle for review:
#   PR metadata + description, full discussion (comments, reviews, inline review
#   threads), commit list, stat, full diff, and full contents of modified files.
#   Nothing in the bundle is truncated.
#
# Usage:
#   pr-review.sh <pr-number> [base-branch]      # base-branch defaults to trunk
#
# Env overrides:
#   REPO_DIR      default $HOME/project/kafka (local clone to operate on)
#   UPSTREAM      default https://github.com/apache/kafka
#   OUT_DIR       default $HOME/pr-review
#   GITHUB_TOKEN  optional; falls back to `gh auth token`. Without a token the
#                 GitHub API allows 60 requests/hour, which is enough for a few PRs.
#
# Side effects (same as the manual workflow): the working tree is hard-reset,
# untracked files are removed, <base-branch> is forced to match upstream, and
# the PR changes are left UNSTAGED in the working tree when the script exits.
# On merge conflict the script aborts the merge and exits 1.

set -euo pipefail

PR_NUMBER="${1:?usage: $(basename "$0") <pr-number> [base-branch]}"
MAIN="${2:-${MAIN:-trunk}}"
UPSTREAM="${UPSTREAM:-https://github.com/apache/kafka}"
OUT_DIR="${OUT_DIR:-$HOME/pr-review}"

REPO_SLUG="$(printf '%s' "$UPSTREAM" | sed -E 's#^(https://github\.com/|git@github\.com:)##; s#\.git$##; s#/$##')"
GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token 2>/dev/null || true)}"
export GITHUB_TOKEN

cd "${REPO_DIR:-$HOME/project/kafka}"

echo "==> Resetting working tree"
git reset -q --hard
git clean -fdq

echo "==> Syncing $MAIN with $UPSTREAM"
git switch -q "$MAIN"
git fetch -q "$UPSTREAM" "$MAIN"
git reset -q --hard FETCH_HEAD

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
API = "https://api.github.com"

def get(path, paginate=False):
    url = API + path
    if paginate:
        url += ("&" if "?" in url else "?") + "per_page=100"
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "pr-review.sh"}
    if token:
        headers["Authorization"] = "Bearer " + token
    out = []
    while url:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            data = json.load(r)
            link = r.headers.get("Link", "") or ""
        if not paginate:
            return data
        out.extend(data)
        url = None
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part[part.find("<") + 1:part.find(">")]
    return out

def text(body):
    body = (body or "").strip()
    return body if body else "_(no text)_"

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

print(f"## Discussion ({len(events)} items, chronological)\n")
if not events:
    print("_(no comments yet)_\n")
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
        print(f"### Inline thread: `{root['path']}` line {line}{outdated} ({ts})\n")
        hunk = (root.get("diff_hunk") or "").rstrip("\n")
        if hunk:
            print("````diff")
            print(hunk)
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
BUNDLE="$OUT_DIR/pr-$PR_NUMBER.md"

lang_of() {
  case "${1##*.}" in
    java) echo java ;;
    scala) echo scala ;;
    py) echo python ;;
    kt|kts) echo kotlin ;;
    gradle|groovy) echo groovy ;;
    sh|bash) echo bash ;;
    md) echo markdown ;;
    json) echo json ;;
    yml|yaml) echo yaml ;;
    xml|html) echo xml ;;
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