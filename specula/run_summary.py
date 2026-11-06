#!/usr/bin/env python3
"""Summarize one Specula phase from Claude Code's stream-json log.

Usage: run_summary.py <claude-log.jsonl> <public-summary.md> <private-message.txt> [<report.md>]

The public summary goes to $GITHUB_STEP_SUMMARY, which anyone logged in can read,
so it carries only status, turns, cost and duration. Claude's final report,
which may describe a bug, goes only into the private Slack message.
Env: TARGET, PHASE, MODEL, RUN_URL, ACTOR
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


def last_result(path):
    result = {}
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
                    result = obj
    except FileNotFoundError:
        pass
    return result


def main():
    log, public_md, private_txt = sys.argv[1:4]
    report_md = sys.argv[4] if len(sys.argv) > 4 else None
    r = last_result(log)
    env = os.environ.get
    status = r.get("subtype", "no result (crashed or timed out)")
    if r.get("is_error") and status == "success":
        status = "error"
    turns = r.get("num_turns", "?")
    cost = r.get("total_cost_usd")
    cost = f"${cost:.2f}" if isinstance(cost, (int, float)) else "?"
    minutes = run_minutes(log, r)

    with open(public_md, "w", encoding="utf-8") as f:
        f.write(f"## specula: `{env('TARGET')}` / `{env('PHASE')}`\n")
        f.write(f"- model: `{env('MODEL')}`\n")
        f.write(f"- status: **{status}**, turns: {turns}, cost: {cost}, duration: {minutes}\n")
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
        f.write(short)

    # Fail the job when Claude did not finish the phase cleanly.
    sys.exit(0 if status == "success" else 1)


if __name__ == "__main__":
    main()
