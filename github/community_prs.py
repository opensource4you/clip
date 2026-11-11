#!/usr/bin/env python3
"""List community members' PRs across repos as one HTML page.

Reads logins from a people file (people_all.json by default, same format
gh_activity.py uses) and queries
GitHub via `gh api graphql`, batching several authors per request.

Usage:
  python3 community_prs.py --people-file people_all.json        # open PRs, all repos in the file
  python3 community_prs.py --people-file people_all.json --repo apache/kafka --repo apache/ozone
  python3 community_prs.py --people-file people_all.json --state merged --since 2026-10-01
  python3 community_prs.py --people-file people_all.json --max-age 60   # reuse results < 60 min old
  python3 community_prs.py --people brandboat,m1a2st --out prs.html
  python3 community_prs.py --people-file people_all.json --role reviewer --state any --since 2026-10-01

The generated page has a repo dropdown, so one fetch covers every project.
For open PRs it also shows how long the PR has gone without progress, worked out
from recent commits, reviews and comments.

--role reviewer lists the PRs each person reviewed (submitted a review on) instead
of the ones they wrote, with their own verdict and review count. Plain conversation
comments without a review don't count. --since then means "reviewed on or after".

people file: {"users": [...], "repos": [...]}; a plain list of logins also works.
Auth: uses whatever `gh auth login` set up; no separate token needed.
"""

import argparse
import datetime as dt
import hashlib
import html
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# Cache directory, same convention as gh_activity.py: default ~/.cache/community_prs,
# override with env COMMUNITY_PRS_CACHE or --cache-dir; --no-cache turns it off.
CACHE_DIR = os.environ.get("COMMUNITY_PRS_CACHE") or os.path.join(os.path.expanduser("~"), ".cache", "community_prs")

BATCH = 5           # authors per GraphQL request (bigger batches hit GitHub's 502 timeout)
PER_AUTHOR = 50     # PRs per page per author; more pages are fetched when needed
WORKERS = 8         # concurrent requests
RETRIES = 4
TIMELINE = 30       # recent commits/reviews/comments per open PR, used to work out who the PR is waiting on
WAIT_DAYS = 14      # no progress for this long counts as idle


def load_config(path, cli_people, cli_repos):
    """Return (people, repos). Config may be {"users": [...], "repos": [...]} or a plain list."""
    data = {}
    if not cli_people or not cli_repos:
        with open(path) as f:
            data = json.load(f)
    people = [p.strip() for p in cli_people.split(",") if p.strip()] if cli_people else load_people(data)
    repos = cli_repos or (data.get("repos") if isinstance(data, dict) else None) or ["apache/kafka"]
    return people, repos


def load_people(data):
    if isinstance(data, dict):
        data = data.get("users") or data.get("people") or list(data.keys())
    out = []
    for p in data:
        if isinstance(p, str):
            out.append(p)
        elif isinstance(p, dict):
            for k in ("github", "login", "id", "name"):
                if p.get(k):
                    out.append(p[k])
                    break
    seen, uniq = set(), []
    for p in out:
        if p.lower() not in seen:
            seen.add(p.lower())
            uniq.append(p)
    return uniq


def owner_scope(repos):
    """Search strings are capped at 256 chars, so 20 repo: qualifiers won't fit.
    Scoping by owner (user:apache user:kubernetes-sigs ...) keeps the search small
    enough and already drops forks and unrelated repos; exact repos are filtered
    client-side afterwards."""
    owners = []
    for r in repos:
        o = r.split("/")[0]
        if o not in owners:
            owners.append(o)
    scope = " ".join(f"user:{o}" for o in owners)
    return scope if len(scope) <= 180 else ""


def search_string(login, state, since, scope="", role="author"):
    who = f"reviewed-by:{login}" if role == "reviewer" else f"author:{login}"
    q = f"{scope} is:pr {who}".strip()
    if state != "any":
        q += f" is:{state}"
    if since:
        if role == "reviewer" or state in ("open", "any"):
            # submitting a review bumps updatedAt, so this keeps every PR reviewed since then;
            # the exact review time is checked client-side
            q += f" updated:>={since}"
        elif state == "merged":
            q += f" merged:>={since}"
        elif state == "closed":
            q += f" closed:>={since}"
    if len(q) > 256 and scope:   # search length cap; drop the owner scope, repos are filtered client-side anyway
        return search_string(login, state, since, "", role)
    return q


def build_query(authors, state, since, scope="", cursors=None, role="author", per=PER_AUTHOR):
    # only open PRs need the timeline; it makes the query noticeably heavier
    timeline = f'''
      timelineItems(last: {TIMELINE}, itemTypes: [PULL_REQUEST_COMMIT, PULL_REQUEST_REVIEW, ISSUE_COMMENT]) {{
        filteredCount
        nodes {{
          __typename
          ... on PullRequestCommit {{ commit {{ committedDate }} }}
          ... on PullRequestReview {{ submittedAt state author {{ __typename login }} }}
          ... on IssueComment {{ createdAt author {{ __typename login }} }}
        }}
      }}''' if state == "open" else ""
    parts = []
    for i, a in enumerate(authors):
        q = search_string(a, state, since, scope, role).replace('"', '\\"')
        after = f', after: "{cursors[a]}"' if cursors and cursors.get(a) else ""
        # reviewer mode: this person's own reviews on the PR (verdict, count, when)
        mine = (f'\n      mine: reviews(author: "{a}", last: 20) {{ totalCount nodes {{ state submittedAt }} }}'
                if role == "reviewer" else "")
        # mergeable and the CI rollup are the expensive fields; the reviews page doesn't need them
        checks = "" if role == "reviewer" else '''
      mergeable
      commits(last: 1) { nodes { commit { statusCheckRollup { state } } } }'''
        parts.append(f'''
  a{i}: search(type: ISSUE, query: "{q}", first: {per}{after}) {{
    issueCount
    pageInfo {{ hasNextPage endCursor }}
    nodes {{ ... on PullRequest {{
      number title url isDraft createdAt updatedAt mergedAt state reviewDecision
      author {{ login }}{checks}
      repository {{ nameWithOwner }}
      labels(first: 6) {{ nodes {{ name }} }}
      comments {{ totalCount }}
      reviews(first: 1) {{ totalCount }}{mine}{timeline}
    }} }}
  }}''')
    return "query {" + "".join(parts) + "\n}"


class Transient(Exception):
    """GitHub kept timing out (502/503/504) or cut the response short on this request."""


# stderr fragments that mean "GitHub gave up on this request, try again".
# A timed-out query sometimes comes back as an empty or cut-off body instead of a
# clean 502, which gh reports as a JSON parse error.
TRANSIENT = ("502", "503", "504", "timeout", "rate limit",
             "unexpected end of JSON input", "unexpected EOF", "invalid character",
             "connection reset", "stream error")


def gh_graphql(query, retries=RETRIES):
    for attempt in range(retries):
        r = subprocess.run(["gh", "api", "graphql", "-f", f"query={query}"],
                           capture_output=True, text=True)
        why = (r.stderr.strip().splitlines() or ["no error message"])[-1]
        transient = r.returncode != 0 and any(c in r.stderr for c in TRANSIENT)
        data = None
        if r.returncode == 0:
            try:
                data = json.loads(r.stdout)
            except ValueError:   # exit 0 but the body was cut off
                transient, why = True, "truncated response"
        if transient and attempt < retries - 1:
            wait = 3 * (attempt + 1)
            print(f"  transient error ({why}), retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if transient:
            raise Transient(r.stderr or why)
        if r.returncode != 0:
            sys.exit(f"gh api failed:\n{r.stderr}")
        if data.get("errors"):
            sys.exit(f"GraphQL errors:\n{json.dumps(data['errors'], indent=2)}")
        return data["data"]


MIN_PER = 10   # smallest page size tried before giving up on a request


def fetch_chunk(chunk, state, since, scope, role="author"):
    """Fetch every page for these logins. A request that keeps timing out is retried
    smaller: first fewer PRs per page, then fewer people per request."""
    out = {login: [] for login in chunk}
    cursors, pending = {}, list(chunk)
    per, width = PER_AUTHOR, len(chunk)
    while pending:
        batch = pending[:width]
        shrinkable = per > MIN_PER or width > 1
        try:
            data = gh_graphql(build_query(batch, state, since, scope, cursors, role, per),
                              retries=2 if shrinkable else RETRIES)
        except Transient as ex:
            if not shrinkable:
                sys.exit(f"gh api failed:\n{ex}")
            if per > MIN_PER:
                per = max(MIN_PER, per // 2)
            else:
                width = max(1, width // 2)
            print(f"  shrinking request to {width} people x {per} PRs", file=sys.stderr)
            continue
        nxt = []
        for j, login in enumerate(batch):
            block = data[f"a{j}"]
            out[login].extend(n for n in block["nodes"] if n)
            if block["pageInfo"]["hasNextPage"]:
                cursors[login] = block["pageInfo"]["endCursor"]
                nxt.append(login)
        pending = nxt + pending[width:]
    return out


def fetch(people, repos, state, since, all_repos=False, role="author"):
    wanted = {r.lower() for r in repos}
    scope = "" if all_repos else owner_scope(repos)
    chunks = [people[i:i + BATCH] for i in range(0, len(people), BATCH)]
    result, done, t0 = {}, 0, time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for out in ex.map(lambda c: fetch_chunk(c, state, since, scope, role), chunks):
            result.update(out)
            done += len(out)
            print(f"fetched {done}/{len(people)} ({time.time() - t0:.0f}s)", file=sys.stderr)
    if not all_repos:
        for login in result:
            result[login] = [p for p in result[login]
                             if p["repository"]["nameWithOwner"].lower() in wanted]
    if role == "reviewer":
        # drop their own PRs, and PRs whose reviews by them all predate --since
        for login in result:
            keep = []
            for p in result[login]:
                if ((p.get("author") or {}).get("login") or "").lower() == login.lower():
                    continue
                subs = submitted(p)
                if subs and (not since or subs[-1]["submittedAt"][:10] >= since):
                    keep.append(p)
            result[login] = keep
    return result


def submitted(pr):
    """The group owner's own submitted reviews on pr, oldest first (reviewer mode)."""
    nodes = (pr.get("mine") or {}).get("nodes") or []
    return sorted((r for r in nodes if r and r.get("submittedAt") and r.get("state") != "PENDING"),
                  key=lambda r: r["submittedAt"])


def verdict(subs):
    """A reviewer's standing verdict, the way GitHub shows it: the latest approve /
    request-changes / dismissed wins over later plain comments."""
    for r in reversed(subs):
        if r["state"] in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            return r["state"]
    return "COMMENTED" if subs else ""


def days_ago(iso):
    t = dt.datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return (dt.datetime.now(dt.timezone.utc) - t).days


def review_label(pr):
    if pr["state"] == "MERGED":
        return "merged", "ok"
    if pr["state"] == "CLOSED":
        return "closed", "muted"
    if pr["isDraft"]:
        return "draft", "muted"
    d = pr["reviewDecision"]
    if d == "APPROVED":
        return "approved", "ok"
    if d == "CHANGES_REQUESTED":
        return "changes requested", "warn"
    if pr["reviews"]["totalCount"] == 0:
        return "no review", "bad"
    return "in review", "info"


def ci_state(pr):
    """SUCCESS / FAILURE / PENDING / None (no checks or unknown)."""
    try:
        st = pr["commits"]["nodes"][0]["commit"]["statusCheckRollup"]["state"]
    except (KeyError, IndexError, TypeError):
        return None
    return {"SUCCESS": "SUCCESS", "FAILURE": "FAILURE", "ERROR": "FAILURE",
            "PENDING": "PENDING", "EXPECTED": "PENDING"}.get(st)


def ball(pr):
    """Who an open PR is waiting on: ("reviewer" | "merge" | "author", since_iso), or None.

    Walks the PR's recent commits, reviews and comments in time order:
      - a commit or comment by the PR author hands the ball to reviewers; if it was
        already theirs, the wait keeps counting from the first unanswered author action
      - any other person's review or comment hands it back to the author
      - an approval leaves the author nothing to do, so it becomes "merge" from then on
    Bots are ignored. Drafts, closed PRs and results without a timeline return None.
    """
    if pr["state"] != "OPEN" or pr["isDraft"] or not pr.get("timelineItems"):
        return None
    tl = pr["timelineItems"]
    me = ((pr.get("author") or {}).get("login") or "").lower()
    events = []
    for n in tl["nodes"]:
        if not n:
            continue
        t = n.get("__typename")
        if t == "PullRequestCommit":
            events.append((n["commit"]["committedDate"], "author"))
            continue
        when = n.get("submittedAt") if t == "PullRequestReview" else n.get("createdAt")
        a = n.get("author") or {}
        login = (a.get("login") or "").lower()
        if not when or not login or a.get("__typename") == "Bot" or login.endswith("[bot]"):
            continue
        if login == me:
            events.append((when, "author"))
        elif t == "PullRequestReview" and n.get("state") == "APPROVED":
            events.append((when, "merge"))
        else:
            events.append((when, "reviewer"))
    events.sort()
    # Before anything happens the PR waits for its first review. If the timeline was
    # cut off, start from the oldest event we have instead of the creation time.
    truncated = tl.get("filteredCount", 0) > len(tl["nodes"])
    whose, since = "reviewer", (events[0][0] if truncated and events else pr["createdAt"])
    for when, side in events:
        if side == "author":
            if whose == "author":
                whose, since = "reviewer", when
        elif side == "merge":
            whose, since = "merge", when
        else:
            whose, since = "author", when
    return whose, since


def dispatch_target(cli):
    """(owner/repo, workflow file) for the page's refresh link.
    --dispatch owner/repo:file.yml, or auto-detected inside GitHub Actions."""
    if cli:
        repo, _, wf = cli.partition(":")
        return (repo, wf) if repo and wf else None
    ref = os.environ.get("GITHUB_WORKFLOW_REF", "")   # owner/repo/.github/workflows/x.yml@refs/heads/main
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if ref and repo and "/.github/workflows/" in ref:
        return repo, ref.split("/.github/workflows/")[1].split("@")[0]
    return None


# verdict -> (shown text, css class, word for the filter box)
VERDICT = {"APPROVED": ("✓ approved", "mv-ok", "approved"),
           "CHANGES_REQUESTED": ("± changes requested", "mv-cr", "changes requested"),
           "COMMENTED": ("💬 commented", "mv-c", "commented"),
           "DISMISSED": ("✕ dismissed", "mv-c", "dismissed")}


def render(result, repos, state, since, dispatch=None, nav=None, out_name="", role="author"):
    e = html.escape
    navhtml = ""
    if nav:
        items = []
        for label, href in nav:
            cur = os.path.basename(href) == os.path.basename(out_name)
            items.append(f'<b>{e(label)}</b>' if cur else f'<a href="{e(href)}">{e(label)}</a>')
        navhtml = '<nav class="pages">' + " · ".join(items) + '</nav>'
    try:
        from zoneinfo import ZoneInfo
        now = dt.datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M (Taipei)")
    except Exception:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    refresh = ""
    if dispatch:
        repo, wf = dispatch
        refresh = (f' · <a href="https://github.com/{e(repo)}/actions/workflows/{e(wf)}" target="_blank" '
                   f'title="Run workflow there to refresh this page">&#8635; refresh via Actions</a>')
    revmode = role == "reviewer"
    title = (f"Community {'reviews' if revmode else 'PRs'} "
             f"({'all states' if state == 'any' else state}{', since ' + since if since else ''})")
    stale_key = "updatedAt"
    openmode = state == "open"
    mode = "review" if revmode else "open" if openmode else "done"

    def took(p):
        """days from opening to merge (or to last update for closed-unmerged)."""
        end = p.get("mergedAt") or p["updatedAt"]
        a = dt.datetime.fromisoformat(p["createdAt"].replace("Z", "+00:00"))
        b = dt.datetime.fromisoformat(end.replace("Z", "+00:00"))
        return max(0, (b - a).days)

    sumcols = ('<th data-k="ap" title="PRs where their standing review is an approval">approved</th><th data-k="rp" title="distinct repos">repos</th>' if revmode else
               f'<th data-k="nr">no review</th><th data-k="st" title="no progress for {WAIT_DAYS}+ days">idle ≥{WAIT_DAYS}d</th>' if openmode else
               '<th data-k="rp" title="distinct repos">repos</th><th data-k="ad" title="average days from open to merge">avg days</th>')
    # dropdown lists every configured repo; ones without PRs in this page are marked
    counts = {}
    for prs in result.values():
        for p in prs:
            counts[p["repository"]["nameWithOwner"]] = counts.get(p["repository"]["nameWithOwner"], 0) + 1
    all_repos = sorted(set(repos) | set(counts), key=str.lower)
    options = "".join(f'<option value="{e(r)}">{e(r)}{"" if counts.get(r) else " (0)"}</option>' for r in all_repos)

    body = []
    for login, prs in sorted(result.items(), key=lambda kv: (-len(kv[1]), kv[0].lower())):
        if not prs:
            continue
        balls = {p["url"]: ball(p) for p in prs}
        mine = {p["url"]: submitted(p) for p in prs} if revmode else {}
        if revmode:
            # most recently reviewed first
            prs = sorted(prs, key=lambda p: mine[p["url"]][-1]["submittedAt"] if mine[p["url"]] else "", reverse=True)
        elif openmode:
            # the PR that has gone longest without progress first; drafts last
            def order(p):
                b = balls[p["url"]]
                if b:
                    return (0, b[1], p["updatedAt"])
                return (1, "", p["updatedAt"])
            prs = sorted(prs, key=order)
        else:
            prs = sorted(prs, key=lambda p: p["updatedAt"], reverse=True)
        body.append(
            f'<tr class="grp" id="u-{e(login)}" data-login="{e(login)}"><td colspan="7">'
            f'<a href="https://github.com/{e(login)}" target="_blank">{e(login)}</a> <small></small> <span class="gstat"></span></td></tr>')
        for p in prs:
            lab, cls = review_label(p)
            repo = p["repository"]["nameWithOwner"]
            b = balls[p["url"]]
            whose, wait = (b[0], days_ago(b[1])) if b else ("", 0)
            if b:
                stale = wait >= WAIT_DAYS
            else:   # drafts, or results cached before the timeline was fetched
                stale = p["state"] == "OPEN" and not p["isDraft"] and days_ago(p[stale_key]) >= WAIT_DAYS
            labels = " ".join(f'<span class="lbl">{e(l["name"])}</span>' for l in p["labels"]["nodes"])
            flags, ftext = [], []
            sc = " stale" if stale else ""
            if whose:
                flags.append(f'<span class="fl wait-r{sc}" title="no progress for {wait} days">⏳ {wait}d</span>')
                if stale:
                    ftext.append("idle")
            if p["state"] == "OPEN":
                ci = ci_state(p)
                if ci == "FAILURE":
                    flags.append('<span class="fl ci-bad" title="latest commit: checks failed">✗ CI</span>'); ftext.append("ci failed")
                elif ci == "PENDING":
                    flags.append('<span class="fl ci-wait" title="latest commit: checks running">… CI</span>'); ftext.append("ci pending")
                elif ci == "SUCCESS":
                    flags.append('<span class="fl ci-ok" title="latest commit: checks passed">✓ CI</span>'); ftext.append("ci passed")
                if p.get("mergeable") == "CONFLICTING":
                    flags.append('<span class="fl conflict" title="has merge conflicts, needs rebase">⚠ conflict</span>'); ftext.append("conflict")
            conflict = int(p.get("mergeable") == "CONFLICTING" and p["state"] == "OPEN")
            days = took(p)
            byline, mv, ncol = "", "", f'<td class="n">{p["comments"]["totalCount"]}</td>'
            if revmode:
                subs = mine[p["url"]]
                mv = verdict(subs)
                if mv:
                    vtext, vcls, vword = VERDICT.get(mv, (mv.lower(), "mv-c", mv.lower()))
                    flags.insert(0, f'<span class="fl mv {vcls}" title="their standing review on this PR; click to show only these">{e(vtext)}</span>')
                    ftext.append(vword)
                pa = (p.get("author") or {}).get("login") or "ghost"
                byline = f'<span class="by">by <a href="https://github.com/{e(pa)}" target="_blank">{e(pa)}</a></span> '
                ftext.append(pa)
                ncol = f'<td class="n">{len(subs)}</td>'
                last = subs[-1]["submittedAt"] if subs else p["updatedAt"]
                tail = (f'<td class="n">{days_ago(last)}d</td>'
                        f'<td class="n">{days_ago(p["updatedAt"])}d</td>')
            elif openmode:
                tail = (f'<td class="n">{days_ago(p["createdAt"])}d</td>'
                        f'<td class="n">{days_ago(p["updatedAt"])}d</td>')
            else:
                end = p.get("mergedAt") or p["updatedAt"]
                tail = f'<td class="n">{days}d</td><td class="n">{days_ago(end)}d</td>'
            body.append(
                f'<tr class="pr" data-login="{e(login)}" data-repo="{e(repo)}" data-status="{e(lab)}" data-stale="{int(stale)}" data-conflict="{conflict}" data-days="{days}" data-mine="{mv}" '
                f'data-text="{e(login)} {e(repo)} {e(p["title"])} {e(lab)} {e(" ".join(ftext))}">'
                f'<td class="repo">{e(repo.split("/")[-1])}</td>'
                f'<td class="num"><a href="{e(p["url"])}" target="_blank">#{p["number"]}</a></td>'
                f'<td class="title"><span class="mrepo">{e(repo.split("/")[-1])}</span>{e(p["title"])}<div>{byline}{labels}</div></td>'
                f'<td><span class="st {cls}">{e(lab)}</span>{"".join(flags)}</td>'
                + ncol + tail + '</tr>')
    if revmode:
        heads = ('<th class="n" title="reviews they submitted on this PR">reviews</th>'
                 '<th class="n" title="their last review, days ago">reviewed</th><th class="n">updated</th>')
    elif openmode:
        heads = '<th title="comments">💬</th><th class="n">opened</th><th class="n">updated</th>'
    else:
        heads = ('<th title="comments">💬</th><th class="n" title="days from open to merge">took</th>'
                 '<th class="n" title="merged how long ago">merged</th>')
    main_table = ('<table class="prs"><colgroup><col class="c-repo"><col class="c-num"><col class="c-title">'
                  '<col class="c-st"><col class="c-n"><col class="c-n"><col class="c-n"></colgroup>'
                  '<thead><tr><th class="repo">repo</th><th>#</th><th>title</th><th>status</th>'
                  + heads
                  + '</tr></thead><tbody>'
                  + "".join(body) + '</tbody></table>')

    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{e(title)}</title>
<script>try{{var t=localStorage.getItem('cprs.theme');if(t)document.documentElement.setAttribute('data-theme',t);}}catch(e){{}}</script>
<style>
:root{{--bg:#fafafa;--fg:#222;--muted:#666;--faint:#888;--panel:#fff;--head:#f0f0f0;--head-hover:#e6e6e6;--grp:#f7f7f7;--line:#e5e5e5;--line2:#ddd;
  --link:#0645ad;--lbl-bg:#eef;--lbl-fg:#336;--ok-bg:#dcf5dc;--ok-fg:#166016;--warn-bg:#fff0cc;--warn-fg:#7a5200;--bad-bg:#fde0e0;--bad-fg:#8a1c1c;
  --info-bg:#e0ecfd;--info-fg:#1c4a8a;--muted-bg:#eee;--red:#b00;--input-bg:#fff;--input-bd:#ccc}}
:root:not([data-theme="light"]){{color-scheme:light}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{--bg:#111418;--fg:#e4e6ea;--muted:#9aa0a8;--faint:#7c838c;--panel:#1a1e24;--head:#232931;--head-hover:#2c333d;--grp:#20252c;--line:#2a3039;--line2:#3a414b;
  --link:#7ab3ff;--lbl-bg:#263048;--lbl-fg:#b7c6ef;--ok-bg:#1b3a22;--ok-fg:#8fdc9a;--warn-bg:#3d3112;--warn-fg:#f0c86a;--bad-bg:#45201f;--bad-fg:#ff9b94;
  --info-bg:#1d3150;--info-fg:#9cc3ff;--muted-bg:#2b3037;--red:#ff7b72;--input-bg:#1a1e24;--input-bd:#3a414b;color-scheme:dark}}}}
:root[data-theme="dark"]{{--bg:#111418;--fg:#e4e6ea;--muted:#9aa0a8;--faint:#7c838c;--panel:#1a1e24;--head:#232931;--head-hover:#2c333d;--grp:#20252c;--line:#2a3039;--line2:#3a414b;
  --link:#7ab3ff;--lbl-bg:#263048;--lbl-fg:#b7c6ef;--ok-bg:#1b3a22;--ok-fg:#8fdc9a;--warn-bg:#3d3112;--warn-fg:#f0c86a;--bad-bg:#45201f;--bad-fg:#ff9b94;
  --info-bg:#1d3150;--info-fg:#9cc3ff;--muted-bg:#2b3037;--red:#ff7b72;--input-bg:#1a1e24;--input-bd:#3a414b;color-scheme:dark}}
body{{font:14px/1.45 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:24px auto;padding:0 24px;max-width:1900px;color:var(--fg);background:var(--bg)}}
a{{color:var(--link)}}
body.onerepo .c-repo{{width:0}} body.onerepo td.repo,body.onerepo th.repo{{padding:0;font-size:0;overflow:hidden}}
body.onerepo .summary th[data-k="rp"],body.onerepo .summary td.rp{{display:none}}
h1{{font-size:20px;margin:0 0 4px}} .meta{{color:var(--muted);margin-bottom:16px}}
table{{border-collapse:collapse;width:100%;background:var(--panel)}} th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}}
th{{background:var(--head);font-weight:600}} th.n,td.n{{text-align:right;white-space:nowrap}} td.repo{{color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.prs{{table-layout:fixed}} .prs thead th{{position:sticky;top:0;z-index:2}}
.c-repo{{width:150px}} .c-num{{width:70px}} .c-st{{width:150px}} .c-n{{width:70px}}
.prs td.title{{overflow-wrap:anywhere}}
tr.grp td{{background:var(--grp);border:0;box-shadow:inset 4px 0 0 var(--link),inset 0 2px 0 var(--line2),inset 0 -1px 0 var(--line);padding:12px 10px 10px 14px;font-weight:600;font-size:17px}}
tr.grp{{position:sticky;top:var(--thead-h,37px);z-index:1}}
tr.grp.hit td{{animation:hit 1.6s ease-out}} @keyframes hit{{0%,40%{{background:var(--info-bg)}}100%{{background:var(--grp)}}}}
.summary tr.cur td{{background:var(--info-bg)}} .summary tr.cur a{{font-weight:600}} tr.grp small{{color:var(--faint);font-weight:normal}} .gstat{{font-weight:normal;font-size:12px;margin-left:10px}} .gstat b{{font-weight:600;margin-right:10px}} .gstat .nr{{color:var(--bad-fg)}} .gstat .sl{{color:var(--red)}}
.lbl{{display:inline-block;font-size:11px;background:var(--lbl-bg);color:var(--lbl-fg);border-radius:10px;padding:0 7px;margin:2px 4px 0 0}}
.st{{white-space:nowrap;font-size:12px;border-radius:4px;padding:1px 6px}}
.ok{{background:var(--ok-bg);color:var(--ok-fg)}} .warn{{background:var(--warn-bg);color:var(--warn-fg)}} .bad{{background:var(--bad-bg);color:var(--bad-fg)}}
.info{{background:var(--info-bg);color:var(--info-fg)}} .muted{{background:var(--muted-bg);color:var(--muted)}} .stale{{color:var(--red);font-weight:600}}
#q,#repo{{padding:6px 8px;font-size:14px;margin:0 8px 12px 0;background:var(--input-bg);color:var(--fg);border:1px solid var(--input-bd);border-radius:4px}} #q{{width:320px}}
.layout{{display:grid;grid-template-columns:minmax(300px,380px) minmax(0,1fr);gap:20px;align-items:start}}
.layout main{{padding-bottom:calc(100vh - 160px)}}
.side{{position:sticky;top:12px;max-height:calc(100vh - 24px);display:flex;flex-direction:column}}
.side .head{{flex:none}} .side .people{{flex:1;min-height:0;overflow-y:auto}}
.side .summary thead th{{position:sticky;top:0;z-index:1}}
.side #repo,.side #q{{display:block;width:100%;box-sizing:border-box;margin:0 0 8px}} .side #count{{margin:0 0 10px;font-size:12px}}
tr.grp{{scroll-margin-top:var(--thead-h,37px)}}
.summary{{width:100%}} .summary td:nth-child(n+2),.summary th:nth-child(n+2){{text-align:right}}
.summary th{{cursor:pointer;user-select:none;white-space:nowrap}} .summary th:hover{{background:var(--head-hover)}} .summary th.on::after{{content:" ↓";color:var(--muted)}} .summary th.on.asc::after{{content:" ↑"}}
#count{{color:var(--muted)}}
.prs .st,.prs .mv{{cursor:pointer}} .prs .st:hover,.prs .mv:hover{{text-decoration:underline}}
#fchip{{margin:0 0 10px;font-size:12px}} #fchip[hidden]{{display:none}}
#fchip button{{font:inherit;padding:1px 4px 1px 8px;cursor:pointer;background:var(--info-bg);color:var(--info-fg);border:0;border-radius:10px}}
.mrepo{{display:none}}
.fl{{display:block;font-size:11px;margin-top:3px;white-space:nowrap}} .ci-bad,.conflict{{color:var(--red);font-weight:600}} .ci-wait{{color:var(--warn-fg)}} .ci-ok{{color:var(--ok-fg)}}
.wait-r{{color:var(--info-fg)}} .fl.stale{{color:var(--red);font-weight:600}}
.gstat .cf{{color:var(--red)}} .gstat .ap,.mv-ok{{color:var(--ok-fg)}} .gstat .cr,.mv-cr{{color:var(--warn-fg)}} .mv-c{{color:var(--muted)}}
.by{{color:var(--muted);font-size:12px;margin-right:6px}}
nav.pages{{margin:0 0 10px;font-size:14px}} nav.pages b{{padding:2px 8px;background:var(--head-hover);border-radius:4px}} nav.pages a{{padding:2px 8px}}
#theme{{float:right;font-size:12px;padding:2px 8px;cursor:pointer;background:var(--input-bg);color:var(--muted);border:1px solid var(--input-bd);border-radius:4px}}
@media (max-width:1000px){{
  .layout main{{padding-bottom:0}}
  .layout{{display:block}} .side{{position:static;max-height:none;display:block;margin-bottom:20px}} .side .people{{overflow:visible}}
}}
@media (max-width:700px){{
  body{{margin:12px auto;padding:0 10px;font-size:13px}}
  h1{{font-size:17px}} .meta{{font-size:12px}}
  #repo,#q{{width:100%;box-sizing:border-box;margin:0 0 8px}} #count{{display:block;margin-bottom:8px;font-size:12px}}
  .summary{{width:100%;min-width:0}}
  th,td{{padding:5px 6px}}
  .c-repo,.c-n:nth-child(5),.c-n:nth-child(6){{width:0}}
  td.repo,th.repo,.prs td:nth-child(5),.prs th:nth-child(5),.prs td:nth-child(6),.prs th:nth-child(6){{padding:0;font-size:0;overflow:hidden}}
  .c-num{{width:56px}} .c-st{{width:88px}} .c-n:nth-child(7){{width:64px}} .prs th{{font-size:12px}}
  .st{{white-space:normal;font-size:11px}}
  .mrepo{{color:var(--faint);font-size:11px}}
  .lbl{{font-size:10px;margin-top:1px}} .prs td.title div{{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .mrepo{{display:block;margin:0}}
  tr.grp td{{font-size:15px}} tr.grp{{position:static}}
}}
</style></head><body>
<div class="layout">
<aside class="side">
<div class="head">
<button id="theme" title="theme: auto / light / dark">◐ auto</button>
<h1>{e(title)}</h1>
{navhtml}<div class="meta"><span title="{e(", ".join(repos))}">{len(repos)} repos</span> · generated {now}{refresh}</div>
<select id="repo"><option value="">all repos</option>{options}</select>
<input id="q" placeholder="filter: login / title / status">
<div id="count"></div>
<div id="fchip" hidden></div>
</div>
<div class="people"><table class="summary"><thead><tr><th data-k="login">login</th><th data-k="n">PRs</th>{sumcols}</tr></thead><tbody id="sum"></tbody></table></div>
</aside>
<main>{main_table}</main>
</div>
<script>
(function(){{
  var MODE='{mode}';
  var q=document.getElementById('q'), sel=document.getElementById('repo'), sum=document.getElementById('sum'), cnt=document.getElementById('count');
  var rows=Array.prototype.slice.call(document.querySelectorAll('tr.pr'));
  var ths=Array.prototype.slice.call(document.querySelectorAll('.summary th')), sortKey='n', sortAsc=false;
  ths.forEach(function(t){{t.addEventListener('click',function(){{
    if(sortKey===t.dataset.k) sortAsc=!sortAsc; else {{sortKey=t.dataset.k; sortAsc=(sortKey==='login');}}
    apply();
  }});}});
  function apply(){{
    var v=q.value.toLowerCase(), repo=sel.value, per={{}}, shown=0;
    rows.forEach(function(r){{
      var ok=(!repo||r.dataset.repo===repo)&&(!fst||r.dataset[fst.k]===fst.v)&&r.dataset.text.toLowerCase().includes(v);
      r.style.display=ok?'':'none';
      if(!ok) return;
      shown++;
      var p=per[r.dataset.login]||(per[r.dataset.login]={{n:0,nr:0,st:0,cf:0,ap:0,cr:0,days:0,repos:{{}}}});
      p.n++; if(r.dataset.status==='no review') p.nr++; if(r.dataset.stale==='1') p.st++; if(r.dataset.conflict==='1') p.cf++;
      if(r.dataset.mine==='APPROVED') p.ap++; if(r.dataset.mine==='CHANGES_REQUESTED') p.cr++;
      p.days+=(+r.dataset.days||0); p.repos[r.dataset.repo]=1;
    }});
    document.body.classList.toggle('onerepo',!!repo);
    document.querySelectorAll('tr.grp').forEach(function(g){{
      var p=per[g.dataset.login]||{{n:0,nr:0,st:0,cf:0,ap:0,cr:0,days:0}};
      g.style.display=p.n?'':'none'; g.querySelector('small').textContent=p.n;
      g.querySelector('.gstat').innerHTML=MODE==='review'
        ?(p.ap?'<b class="ap">'+p.ap+' approved</b>':'')+(p.cr?'<b class="cr">'+p.cr+' changes requested</b>':'')
        :MODE==='open'
        ?(p.nr?'<b class="nr">'+p.nr+' no review</b>':'')+(p.st?'<b class="sl">'+p.st+' idle ≥{WAIT_DAYS}d</b>':'')+(p.cf?'<b class="cf">'+p.cf+' conflict</b>':'')
        :(p.n?'<b>avg '+Math.round(p.days/p.n)+'d to merge</b>':'');
    }});
    Object.keys(per).forEach(function(l){{var p=per[l]; p.rp=Object.keys(p.repos).length; p.ad=p.n?Math.round(p.days/p.n):0;}});
    var logins=Object.keys(per).sort(function(a,b){{
      var d=sortKey==='login'?a.toLowerCase().localeCompare(b.toLowerCase()):per[a][sortKey]-per[b][sortKey];
      if(!sortAsc)d=-d;
      return d||a.toLowerCase().localeCompare(b.toLowerCase());
    }});
    ths.forEach(function(t){{t.classList.toggle('on',t.dataset.k===sortKey);t.classList.toggle('asc',sortAsc);}});
    sum.innerHTML=logins.map(function(l){{var p=per[l];
      var c=MODE==='review'?'<td>'+(p.ap||'')+'</td><td class="rp">'+p.rp+'</td>'
           :MODE==='open'?'<td>'+(p.nr||'')+'</td><td>'+(p.st||'')+'</td>':'<td class="rp">'+p.rp+'</td><td>'+p.ad+'d</td>';
      return '<tr data-login="'+l+'"><td><a href="#u-'+l+'">'+l+'</a></td><td>'+p.n+'</td>'+c+'</tr>';}}).join('');
    cnt.textContent=shown+(MODE==='review'?' PRs reviewed by ':' PRs from ')+logins.length+' people';
    try{{localStorage.setItem('cprs.repo',repo);}}catch(e){{}}
    curLogin=null; if(thead) spy();   // the list was rebuilt: re-mark who is on screen
  }}
  try{{var saved=localStorage.getItem('cprs.repo');if(saved&&sel.querySelector('option[value="'+saved+'"]'))sel.value=saved;}}catch(e){{}}
  // click a status badge (or a reviewer's verdict) to show only PRs with the same one; click again to clear
  var fst=null, chip=document.getElementById('fchip');
  function setStatus(f){{
    fst=f; chip.hidden=!f;
    chip.innerHTML=f?'<button title="clear">'+f.label.replace(/[&<>"]/g,'')+' &times;</button>':'';
    apply();
  }}
  chip.addEventListener('click',function(){{setStatus(null);}});
  document.querySelector('.prs').addEventListener('click',function(ev){{
    var b=ev.target.closest('.st,.mv'), r=b&&b.closest('tr.pr'); if(!r) return;
    var f=b.classList.contains('mv')?{{k:'mine',v:r.dataset.mine,label:b.textContent}}:{{k:'status',v:r.dataset.status,label:b.textContent}};
    setStatus(fst&&fst.k===f.k&&fst.v===f.v?null:f);
    r.scrollIntoView({{block:'nearest'}});
  }});
  document.addEventListener('keydown',function(ev){{if(ev.key==='Escape'&&fst&&document.activeElement!==q) setStatus(null);}});
  q.addEventListener('input',apply); sel.addEventListener('change',apply); apply();

  // keep the left list in step with the right: highlight the person on screen,
  // flash the group header that a click on the left jumps to
  // the head cells (not the thead) are sticky, and so are the group header rows
  var root=document.documentElement, thead=document.querySelector('.prs thead th');
  function setHead(){{root.style.setProperty('--thead-h',thead.getBoundingClientRect().height+'px');}}
  setHead(); window.addEventListener('resize',setHead);
  var curLogin='';
  function mark(l){{
    if(l===curLogin) return; curLogin=l;
    sum.querySelectorAll('tr').forEach(function(r){{
      var on=r.dataset.login===l; r.classList.toggle('cur',on);
      if(on){{var box=r.closest('.people'), rt=r.getBoundingClientRect(), bt=box.getBoundingClientRect();
        if(rt.top<bt.top+30||rt.bottom>bt.bottom) box.scrollTop+=rt.top-bt.top-box.clientHeight/3;}}
    }});
  }}
  function spy(){{
    var y=thead.getBoundingClientRect().bottom+4, l='';
    document.querySelectorAll('tr.grp').forEach(function(g){{
      if(g.style.display!=='none' && g.getBoundingClientRect().top<=y) l=g.dataset.login;
    }});
    mark(l);
  }}
  window.addEventListener('scroll',spy,{{passive:true}});
  // A group header that was scrolled past stays stuck at the top, so the browser treats it
  // as already visible and scrollIntoView does nothing. Work out where the header would sit
  // from its first PR row, which is never sticky.
  function jump(g){{
    var r=g.nextElementSibling;
    while(r&&(r.style.display==='none')) r=r.nextElementSibling;
    if(!r||r.classList.contains('grp')){{g.scrollIntoView({{block:'start'}}); return;}}
    var y=r.getBoundingClientRect().top+window.scrollY-g.offsetHeight-thead.getBoundingClientRect().height;
    window.scrollTo(0,Math.max(0,y));
  }}
  sum.addEventListener('click',function(ev){{
    var a=ev.target.closest('a'); if(!a) return;
    var g=document.getElementById(a.getAttribute('href').slice(1)); if(!g) return;
    ev.preventDefault(); history.replaceState(null,'',a.getAttribute('href'));
    jump(g);
    g.classList.remove('hit'); void g.offsetWidth; g.classList.add('hit');
    mark(g.dataset.login);
  }});
  spy();

  var tb=document.getElementById('theme'), modes=['auto','light','dark'], icons={{auto:'◐',light:'☀',dark:'☾'}};
  function cur(){{return document.documentElement.getAttribute('data-theme')||'auto';}}
  function show(){{tb.textContent=icons[cur()]+' '+cur();}}
  tb.addEventListener('click',function(){{
    var n=modes[(modes.indexOf(cur())+1)%3];
    if(n==='auto'){{document.documentElement.removeAttribute('data-theme');}} else {{document.documentElement.setAttribute('data-theme',n);}}
    try{{n==='auto'?localStorage.removeItem('cprs.theme'):localStorage.setItem('cprs.theme',n);}}catch(e){{}}
    show();
  }});
  show();

}})();
</script></body></html>'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--people-file", default="people_all.json", help='{"users": [...], "repos": [...]}')
    ap.add_argument("--people", help="comma-separated logins, overrides the file")
    ap.add_argument("--repo", action="append", help="repeatable; overrides repos in the file")
    ap.add_argument("--all-repos", action="store_true", help="don't filter by repo list")
    ap.add_argument("--state", choices=["open", "merged", "closed", "any"], default="open")
    ap.add_argument("--since", help="YYYY-MM-DD; merged/closed on or after (author), reviewed on or after (reviewer), "
                                    "updated on or after (open/any)")
    ap.add_argument("--role", choices=["author", "reviewer"], default="author",
                    help="author: PRs they opened; reviewer: PRs by others that they reviewed")
    ap.add_argument("--out", default="community_prs.html")
    ap.add_argument("--cache-dir", default=CACHE_DIR,
                    help=f"where raw results are kept (default {CACHE_DIR}, env COMMUNITY_PRS_CACHE)")
    ap.add_argument("--no-cache", action="store_true", help="neither read nor write the cache")
    ap.add_argument("--dispatch", metavar="OWNER/REPO:FILE.yml",
                    help="add a link to this workflow's Actions page for manual refresh; auto-detected inside GitHub Actions")
    ap.add_argument("--nav", action="append", metavar="LABEL=FILE",
                    help="repeatable; links between generated pages, e.g. --nav open=index.html --nav 'merged 30d=merged.html'")
    ap.add_argument("--max-age", type=int, default=0, metavar="MIN",
                    help="reuse cached results younger than MIN minutes (0 = always fetch)")
    args = ap.parse_args()

    people, repos = load_config(args.people_file, args.people, args.repo)
    if not people:
        sys.exit("no people found")

    # One cache file per query shape, so kafka-only and all-repos runs don't evict each other.
    key = {"people": sorted(p.lower() for p in people), "repos": sorted(repos),
           "state": args.state, "since": args.since, "all_repos": args.all_repos, "role": args.role,
           "v": 2}   # v2: open PRs carry timelineItems; bump when the query shape changes
    cache_path = os.path.join(args.cache_dir, hashlib.sha1(json.dumps(key, sort_keys=True).encode()).hexdigest() + ".json")
    result = None
    if args.max_age > 0 and not args.no_cache:
        try:
            with open(cache_path) as f:
                cached = json.load(f)
            age = (time.time() - cached["fetched_at"]) / 60
            if age <= args.max_age:
                result = cached["result"]
                print(f"using cache from {age:.0f} min ago ({cache_path})", file=sys.stderr)
        except (OSError, KeyError, ValueError):
            pass
    if result is None:
        result = fetch(people, repos, args.state, args.since, args.all_repos, args.role)
        if not args.no_cache:
            os.makedirs(args.cache_dir, exist_ok=True)
            tmp = cache_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"fetched_at": time.time(), "key": key, "result": result}, f)
            os.replace(tmp, cache_path)

    with open(args.out, "w") as f:
        nav = [tuple(n.split("=", 1)) for n in (args.nav or []) if "=" in n]
        f.write(render(result, repos, args.state, args.since, dispatch_target(args.dispatch), nav, args.out, args.role))
    total = sum(len(v) for v in result.values())
    print(f"{total} PRs {'reviewed by' if args.role == 'reviewer' else 'from'} {len(people)} people -> {args.out}")


if __name__ == "__main__":
    main()