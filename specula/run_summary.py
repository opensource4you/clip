#!/usr/bin/env python3
"""Summarize one Specula phase from Claude Code's stream-json log.

Usage: run_summary.py <claude-log.jsonl> <public-summary.md> <private-message.txt>

The public summary goes to $GITHUB_STEP_SUMMARY, which anyone logged in can read,
so it carries only status, turns, cost and duration. Claude's final report,
which may describe a bug, goes only into the private Slack message.
Env: TARGET, PHASE, MODEL, RUN_URL, ACTOR
"""
import json
import os
import sys


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
    r = last_result(log)
    env = os.environ.get
    status = r.get("subtype", "no result (crashed or timed out)")
    if r.get("is_error") and status == "success":
        status = "error"
    turns = r.get("num_turns", "?")
    cost = r.get("total_cost_usd")
    cost = f"${cost:.2f}" if isinstance(cost, (int, float)) else "?"
    minutes = r.get("duration_ms")
    minutes = f"{minutes / 60000:.0f} min" if isinstance(minutes, (int, float)) else "?"

    with open(public_md, "w", encoding="utf-8") as f:
        f.write(f"## specula: `{env('TARGET')}` / `{env('PHASE')}`\n")
        f.write(f"- model: `{env('MODEL')}`\n")
        f.write(f"- status: **{status}**, turns: {turns}, cost: {cost}, duration: {minutes}\n")
        f.write(f"- 結果已用 Slack 私訊給 @{env('ACTOR')}，這裡不顯示內容\n")

    report = (r.get("result") or "(Claude 沒有留下最後報告，詳見附件裡的 log)").strip()
    with open(private_txt, "w", encoding="utf-8") as f:
        f.write(f"*Specula* `{env('TARGET')}` / `{env('PHASE')}`: *{status}*\n")
        f.write(f"turns {turns}, cost {cost}, {minutes}\n{env('RUN_URL')}\n\n")
        f.write(report[:3000])

    # Fail the job when Claude did not finish the phase cleanly.
    sys.exit(0 if status == "success" else 1)


if __name__ == "__main__":
    main()
