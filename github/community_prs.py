#!/usr/bin/env python3
"""List community members' PRs across repos as one HTML page.

Reads logins from people.json (same file gh_activity.py uses) and queries
GitHub via `gh api graphql`, batching several authors per request.

Usage:
  python3 community_prs.py --people-file people_all.json        # open PRs, all repos in the file
  python3 community_prs.py --people-file people_all.json --repo apache/kafka --repo apache/ozone
  python3 community_prs.py --people-file people_all.json --state merged --since 2026-10-01
  python3 community_prs.py --people-file people_all.json --max-age 60   # reuse results < 60 min old
  python3 community_prs.py --people brandboat,m1a2st --out prs.html

The generated page has a repo dropdown, so one fetch covers every project.

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


def search_string(author, state, since, scope=""):
    q = f"{scope} is:pr author:{author}".strip()
    if state == "open":
        q += " is:open"
    elif state == "merged":
        q += " is:merged"
        if since:
            q += f" merged:>={since}"
    elif state == "closed":
        q += " is:closed"
        if since:
            q += f" closed:>={since}"
    return q


def build_query(authors, state, since, scope="", cursors=None):
    parts = []
    for i, a in enumerate(authors):
        q = search_string(a, state, since, scope).replace('"', '\\"')
        after = f', after: "{cursors[a]}"' if cursors and cursors.get(a) else ""
        parts.append(f'''
  a{i}: search(type: ISSUE, query: "{q}", first: {PER_AUTHOR}{after}) {{
    issueCount
    pageInfo {{ hasNextPage endCursor }}
    nodes {{ ... on PullRequest {{
      number title url isDraft createdAt updatedAt mergedAt state reviewDecision mergeable
      author {{ login }}
      commits(last: 1) {{ nodes {{ commit {{ statusCheckRollup {{ state }} }} }} }}
      repository {{ nameWithOwner }}
      labels(first: 6) {{ nodes {{ name }} }}
      comments {{ totalCount }}
      reviews(first: 1) {{ totalCount }}
    }} }}
  }}''')
    return "query {" + "".join(parts) + "\n}"


def gh_graphql(query):
    for attempt in range(RETRIES):
        r = subprocess.run(["gh", "api", "graphql", "-f", f"query={query}"],
                           capture_output=True, text=True)
        transient = r.returncode != 0 and any(c in r.stderr for c in ("502", "503", "504", "timeout", "rate limit"))
        if transient and attempt < RETRIES - 1:
            wait = 3 * (attempt + 1)
            print(f"  transient error ({r.stderr.strip().splitlines()[-1]}), retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if r.returncode != 0:
            sys.exit(f"gh api failed:\n{r.stderr}")
        data = json.loads(r.stdout)
        if data.get("errors"):
            sys.exit(f"GraphQL errors:\n{json.dumps(data['errors'], indent=2)}")
        return data["data"]


def fetch_chunk(chunk, state, since, scope):
    out = {login: [] for login in chunk}
    cursors, pending = {}, list(chunk)
    while pending:
        data = gh_graphql(build_query(pending, state, since, scope, cursors))
        nxt = []
        for j, login in enumerate(pending):
            block = data[f"a{j}"]
            out[login].extend(n for n in block["nodes"] if n)
            if block["pageInfo"]["hasNextPage"]:
                cursors[login] = block["pageInfo"]["endCursor"]
                nxt.append(login)
        pending = nxt
    return out


def fetch(people, repos, state, since, all_repos=False):
    wanted = {r.lower() for r in repos}
    scope = "" if all_repos else owner_scope(repos)
    chunks = [people[i:i + BATCH] for i in range(0, len(people), BATCH)]
    result, done, t0 = {}, 0, time.time()
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for out in ex.map(lambda c: fetch_chunk(c, state, since, scope), chunks):
            result.update(out)
            done += len(out)
            print(f"fetched {done}/{len(people)} ({time.time() - t0:.0f}s)", file=sys.stderr)
    if not all_repos:
        for login in result:
            result[login] = [p for p in result[login]
                             if p["repository"]["nameWithOwner"].lower() in wanted]
    return result


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


def render(result, repos, state, since, dispatch=None, nav=None, out_name=""):
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
    title = f"Community PRs ({state}{', since ' + since if since else ''})"
    total = sum(len(v) for v in result.values())
    stale_key = "updatedAt"

    seen_repos = sorted({p["repository"]["nameWithOwner"] for prs in result.values() for p in prs}, key=str.lower)
    options = "".join(f'<option value="{e(r)}">{e(r)}</option>' for r in seen_repos)

    body = []
    for login, prs in sorted(result.items(), key=lambda kv: (-len(kv[1]), kv[0].lower())):
        if not prs:
            continue
        prs = sorted(prs, key=lambda p: p["updatedAt"], reverse=(state != "open"))
        body.append(
            f'<tr class="grp" id="u-{e(login)}" data-login="{e(login)}"><td colspan="7">'
            f'<a href="https://github.com/{e(login)}" target="_blank">{e(login)}</a> <small></small> <span class="gstat"></span></td></tr>')
        for p in prs:
            lab, cls = review_label(p)
            repo = p["repository"]["nameWithOwner"]
            stale = p["state"] == "OPEN" and days_ago(p[stale_key]) >= 14
            labels = " ".join(f'<span class="lbl">{e(l["name"])}</span>' for l in p["labels"]["nodes"])
            flags, ftext = [], []
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
            body.append(
                f'<tr class="pr" data-login="{e(login)}" data-repo="{e(repo)}" data-status="{e(lab)}" data-stale="{int(stale)}" data-conflict="{conflict}" '
                f'data-text="{e(login)} {e(repo)} {e(p["title"])} {e(lab)} {" ".join(ftext)}">'
                f'<td class="repo">{e(repo.split("/")[-1])}</td>'
                f'<td class="num"><a href="{e(p["url"])}" target="_blank">#{p["number"]}</a></td>'
                f'<td class="title"><span class="mrepo">{e(repo.split("/")[-1])}</span>{e(p["title"])}<div>{labels}</div></td>'
                f'<td><span class="st {cls}">{e(lab)}</span>{"".join(flags)}</td>'
                f'<td class="n">{p["comments"]["totalCount"]}</td>'
                f'<td class="n">{days_ago(p["createdAt"])}d</td>'
                f'<td class="n{" stale" if stale else ""}">{days_ago(p["updatedAt"])}d</td>'
                f'</tr>')
    main_table = ('<table class="prs"><colgroup><col class="c-repo"><col class="c-num"><col class="c-title">'
                  '<col class="c-st"><col class="c-n"><col class="c-n"><col class="c-n"></colgroup>'
                  '<thead><tr><th class="repo">repo</th><th>#</th><th>title</th><th>status</th><th title="comments">💬</th>'
                  '<th class="n">opened</th><th class="n">updated</th></tr></thead><tbody>'
                  + "".join(body) + '</tbody></table>')

    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{e(title)}</title>
<style>
body{{font:14px/1.45 -apple-system,Segoe UI,Helvetica,Arial,sans-serif;margin:24px auto;padding:0 24px;max-width:1500px;color:#222;background:#fafafa}}
body.onerepo .c-repo{{width:0}} body.onerepo td.repo,body.onerepo th.repo{{padding:0;font-size:0;overflow:hidden}}
h1{{font-size:20px;margin:0 0 4px}} .meta{{color:#666;margin-bottom:16px}}
table{{border-collapse:collapse;width:100%;background:#fff}} th,td{{text-align:left;padding:6px 8px;border-bottom:1px solid #e5e5e5;vertical-align:top}}
th{{background:#f0f0f0;font-weight:600}} th.n,td.n{{text-align:right;white-space:nowrap}} td.repo{{color:#666;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
.prs{{table-layout:fixed;margin-top:20px}} .prs thead th{{position:sticky;top:0;z-index:1}}
.c-repo{{width:150px}} .c-num{{width:70px}} .c-st{{width:150px}} .c-n{{width:70px}}
.prs td.title{{overflow-wrap:anywhere}}
tr.grp td{{background:#f7f7f7;border-top:2px solid #ddd;padding-top:14px;font-weight:600;font-size:15px}} tr.grp small{{color:#888;font-weight:normal}} .gstat{{font-weight:normal;font-size:12px;margin-left:10px}} .gstat b{{font-weight:600;margin-right:10px}} .gstat .nr{{color:#8a1c1c}} .gstat .sl{{color:#b00}}
.lbl{{display:inline-block;font-size:11px;background:#eef;color:#336;border-radius:10px;padding:0 7px;margin:2px 4px 0 0}}
.st{{white-space:nowrap;font-size:12px;border-radius:4px;padding:1px 6px}}
.ok{{background:#dcf5dc;color:#166016}} .warn{{background:#fff0cc;color:#7a5200}} .bad{{background:#fde0e0;color:#8a1c1c}}
.info{{background:#e0ecfd;color:#1c4a8a}} .muted{{background:#eee;color:#666}} .stale{{color:#b00;font-weight:600}}
#q,#repo{{padding:6px 8px;font-size:14px;margin:0 8px 12px 0}} #q{{width:320px}}
.summary{{width:auto;min-width:420px}} .summary td:nth-child(n+2),.summary th:nth-child(n+2){{text-align:right}}
.summary th{{cursor:pointer;user-select:none;white-space:nowrap}} .summary th:hover{{background:#e6e6e6}} .summary th.on::after{{content:" ↓";color:#666}} .summary th.on.asc::after{{content:" ↑"}}
#count{{color:#666}}
.mrepo{{display:none}}
.fl{{display:block;font-size:11px;margin-top:3px;white-space:nowrap}} .ci-bad,.conflict{{color:#b00;font-weight:600}} .ci-wait{{color:#7a5200}} .ci-ok{{color:#166016}}
.gstat .cf{{color:#b00}}
nav.pages{{margin:0 0 10px;font-size:14px}} nav.pages b{{padding:2px 8px;background:#e6e6e6;border-radius:4px}} nav.pages a{{padding:2px 8px}}
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
  .mrepo{{color:#888;font-size:11px}}
  .lbl{{font-size:10px;margin-top:1px}} .prs td.title div{{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
  .mrepo{{display:block;margin:0}}
  tr.grp td{{font-size:14px}}
}}
</style></head><body>
<h1>{e(title)}</h1>
{navhtml}<div class="meta"><span title="{e(", ".join(repos))}">{len(repos)} repos</span> · {len(result)} people · {total} PRs · generated {now}{refresh}</div>
<select id="repo"><option value="">all repos</option>{options}</select>
<input id="q" placeholder="filter: login / title / status (e.g. no review)">
<span id="count"></span>
<table class="summary"><thead><tr><th data-k="login">login</th><th data-k="n">PRs</th><th data-k="nr">no review</th><th data-k="st">stale ≥14d</th></tr></thead><tbody id="sum"></tbody></table>
{main_table}
<script>
(function(){{
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
      var ok=(!repo||r.dataset.repo===repo)&&r.dataset.text.toLowerCase().includes(v);
      r.style.display=ok?'':'none';
      if(!ok) return;
      shown++;
      var p=per[r.dataset.login]||(per[r.dataset.login]={{n:0,nr:0,st:0,cf:0}});
      p.n++; if(r.dataset.status==='no review') p.nr++; if(r.dataset.stale==='1') p.st++; if(r.dataset.conflict==='1') p.cf++;
    }});
    document.body.classList.toggle('onerepo',!!repo);
    document.querySelectorAll('tr.grp').forEach(function(g){{
      var p=per[g.dataset.login]||{{n:0,nr:0,st:0,cf:0}};
      g.style.display=p.n?'':'none'; g.querySelector('small').textContent=p.n;
      g.querySelector('.gstat').innerHTML=(p.nr?'<b class="nr">'+p.nr+' no review</b>':'')+(p.st?'<b class="sl">'+p.st+' stale</b>':'')+(p.cf?'<b class="cf">'+p.cf+' conflict</b>':'');
    }});
    var logins=Object.keys(per).sort(function(a,b){{
      var d=sortKey==='login'?a.toLowerCase().localeCompare(b.toLowerCase()):per[a][sortKey]-per[b][sortKey];
      if(!sortAsc)d=-d;
      return d||a.toLowerCase().localeCompare(b.toLowerCase());
    }});
    ths.forEach(function(t){{t.classList.toggle('on',t.dataset.k===sortKey);t.classList.toggle('asc',sortAsc);}});
    sum.innerHTML=logins.map(function(l){{var p=per[l];return '<tr><td><a href="#u-'+l+'">'+l+'</a></td><td>'+p.n+'</td><td>'+(p.nr||'')+'</td><td>'+(p.st||'')+'</td></tr>';}}).join('');
    cnt.textContent=shown+' PRs · '+logins.length+' people';
    try{{localStorage.setItem('cprs.repo',repo);}}catch(e){{}}
  }}
  try{{var saved=localStorage.getItem('cprs.repo');if(saved&&sel.querySelector('option[value="'+saved+'"]'))sel.value=saved;}}catch(e){{}}
  q.addEventListener('input',apply); sel.addEventListener('change',apply); apply();

}})();
</script></body></html>'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--people-file", default="people.json", help='{"users": [...], "repos": [...]}')
    ap.add_argument("--people", help="comma-separated logins, overrides the file")
    ap.add_argument("--repo", action="append", help="repeatable; overrides repos in the file")
    ap.add_argument("--all-repos", action="store_true", help="don't filter by repo list")
    ap.add_argument("--state", choices=["open", "merged", "closed"], default="open")
    ap.add_argument("--since", help="YYYY-MM-DD, used with --state merged/closed")
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
           "state": args.state, "since": args.since, "all_repos": args.all_repos}
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
        result = fetch(people, repos, args.state, args.since, args.all_repos)
        if not args.no_cache:
            os.makedirs(args.cache_dir, exist_ok=True)
            tmp = cache_path + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"fetched_at": time.time(), "key": key, "result": result}, f)
            os.replace(tmp, cache_path)

    with open(args.out, "w") as f:
        nav = [tuple(n.split("=", 1)) for n in (args.nav or []) if "=" in n]
        f.write(render(result, repos, args.state, args.since, dispatch_target(args.dispatch), nav, args.out))
    total = sum(len(v) for v in result.values())
    print(f"{total} PRs from {len(people)} people -> {args.out}")


if __name__ == "__main__":
    main()