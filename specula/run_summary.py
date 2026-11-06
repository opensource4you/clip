#!/usr/bin/env python3
"""Summarize one Specula phase from Claude Code's stream-json log.

Usage: run_summary.py <claude-log.jsonl> <public-summary.md> <private-message.txt> [<report.md>]
       run_summary.py --status <claude-log.jsonl>

--status prints "status<TAB>turns<TAB>cost<TAB>minutes" for one phase and exits 0 only
when the phase succeeded. The workflow uses it to decide whether to run the next phase
and appends the line to CHAIN_FILE.

The public summary goes to $GITHUB_STEP_SUMMARY, which anyone logged in can read,
so it carries only status, turns, cost and duration. Claude's final report,
which may describe a bug, goes only into the private Slack message.
Env: TARGET, PHASE, MODEL, RUN_URL, ACTOR, and optionally
  CHAIN_FILE: one "phase<TAB>status<TAB>turns<TAB>cost<TAB>minutes" line per phase of a chained run
  STOP_FILE:  why the workflow stopped early (for example a phase that wrote no output)
"""
import json
import os
import re
import sys

LIMIT = 3000


def slack_mrkdwn(text):
    """Claude writes Markdown; Slack wants *bold*, has no headings and no tables."""
    out, table = [], []
    for line in text.split("\n") + [""]:
        if line.lstrip().startswith("|"):
            # drop the |---|---| separator row; keep the table as monospaced text
            if not re.fullmatch(r"\s*\|[\s:|-]+\|?\s*", line):
                table.append(line.strip())
            continue
        if table:
            out += ["```"] + table + ["```"]
            table = []
        line = re.sub(r"\*\*(.+?)\*\*", r"*\1*", line)
        line = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", line)
        out.append(line)
    return "\n".join(out[:-1])


def run_minutes(log, result):
    """Wall-clock minutes from the .time file the workflow writes; fall back to Claude's own number."""
    try:
        with open(os.path.splitext(log)[0] + ".time") as f:
            start, end = map(int, f.read().split())
        return f"{(end - start) / 60:.0f} min"
    except (OSError, ValueError):
        ms = result.get("duration_ms")
        return f"{ms / 60000:.0f} min" if isinstance(ms, (int, float)) else "?"


def read_results(path):
    results = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("{"):
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if obj.get("type") == "result":
                    results.append(obj)
    except FileNotFoundError:
        pass
    return results


def combine(results):
    """A run can end with more than one result: when a background command (e.g. TLC)
    finishes after Claude's report, Claude answers it in an extra short turn. The real
    report is the result with the most turns; later ones are appended as notes.
    Turns are per result, so they add up; the cost is cumulative, so take the largest."""
    if not results:
        return {}
    main = max(results, key=lambda r: r.get("num_turns") or 0)
    r = dict(main)
    r["num_turns"] = sum(x.get("num_turns") or 0 for x in results)
    costs = [x["total_cost_usd"] for x in results if isinstance(x.get("total_cost_usd"), (int, float))]
    if costs:
        r["total_cost_usd"] = max(costs)
    notes = [x.get("result", "").strip() for x in results if x is not main and x.get("result", "").strip()]
    if notes:
        r["result"] = (main.get("result") or "").strip() + "\n\n---\n" + "\n\n".join(
            "（後來的補充）" + n for n in notes)
    return r


def phase_stats(log):
    """(result, status, turns, cost in USD or None, minutes) of one phase."""
    r = combine(read_results(log))
    status = r.get("subtype", "no result (crashed or timed out)")
    if r.get("is_error") and status == "success":
        status = "error"
    cost = r.get("total_cost_usd")
    cost = cost if isinstance(cost, (int, float)) else None
    return r, status, r.get("num_turns", "?"), cost, run_minutes(log, r)


def fmt_cost(cost):
    return f"${cost:.2f}" if cost is not None else "?"


def read_chain():
    """Rows of CHAIN_FILE, or [] when this run had a single phase."""
    path = os.environ.get("CHAIN_FILE")
    rows = []
    try:
        with open(path) as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) == 5:
                    rows.append(parts)
    except (OSError, TypeError):
        pass
    return rows if len(rows) > 1 else []


def chain_lines(rows):
    """One line per phase plus the total. Status, turns, cost and time only, so it is safe in public."""
    lines = [f"- {p}: {s}, turns {t}, {fmt_cost(float(c)) if c else '?'}, {m}" for p, s, t, c, m in rows]
    total = sum(float(c) for _, _, _, c, _ in rows if c)
    lines.append(f"- total: ${total:.2f}")
    return lines


def main():
    if sys.argv[1:2] == ["--status"]:
        _, status, turns, cost, minutes = phase_stats(sys.argv[2])
        print(f"{status}\t{turns}\t{'' if cost is None else f'{cost:.4f}'}\t{minutes}")
        sys.exit(0 if status == "success" else 1)

    log, public_md, private_txt = sys.argv[1:4]
    report_md = sys.argv[4] if len(sys.argv) > 4 else None
    r, status, turns, cost, minutes = phase_stats(log)
    cost = fmt_cost(cost)
    env = os.environ.get
    chain = read_chain()
    stop = ""
    try:
        with open(env("STOP_FILE") or "") as f:
            stop = f.read().strip()
    except OSError:
        pass

    with open(public_md, "w", encoding="utf-8") as f:
        f.write(f"## specula: `{env('TARGET')}` / `{env('PHASE')}`\n")
        f.write(f"- model: `{env('MODEL')}`\n")
        f.write(f"- status: **{status}**, turns: {turns}, cost: {cost}, duration: {minutes}\n")
        if stop:
            f.write(f"- **stopped**: {stop}\n")
        if chain:
            f.write("- phases:\n" + "\n".join("  " + x for x in chain_lines(chain)) + "\n")
        f.write(f"- 結果已用 Slack 私訊給 @{env('ACTOR')}，這裡不顯示內容\n")

    report = (r.get("result") or "(Claude 沒有留下最後報告，詳見附件裡的 log)").strip()
    if report_md:
        with open(report_md, "w", encoding="utf-8") as f:
            f.write(report + "\n")
    short = slack_mrkdwn(report)
    if len(short) > LIMIT:
        short = short[:LIMIT].rsplit("\n", 1)[0] + "\n\n…（太長，完整報告見附件的 report）"
    with open(private_txt, "w", encoding="utf-8") as f:
        f.write(f"*Specula* `{env('TARGET')}` / `{env('PHASE')}`: *{status}*\n")
        f.write(f"turns {turns}, cost {cost}, {minutes}\n{env('RUN_URL')}\n\n")
        if stop:
            f.write(f"*停下來了*：{stop}\n\n")
        if chain:
            f.write("這次依序跑了：\n" + "\n".join(chain_lines(chain)) + "\n\n")
        f.write(short)

    # Fail the job when Claude did not finish the phase cleanly.
    sys.exit(0 if status == "success" and not stop else 1)


if __name__ == "__main__":
    main()
