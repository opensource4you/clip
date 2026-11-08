#!/usr/bin/env python3
"""Markdown summary of a ducktape run, for GITHUB_STEP_SUMMARY.

用法: ducktape_summary.py <kafka/results 目錄> [log 檔]

讀每個 session 的 report.json，列出統計和失敗/flaky 的測試（含錯誤訊息第一行）。
找不到任何 report 時（例如 ducker-ak 起不來），改貼 log 的最後幾十行，方便看是哪裡壞了。
"""
import json
import pathlib
import sys


def first_line(text, limit=200):
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            return line if len(line) <= limit else line[:limit] + "…"
    return ""


def sessions(results_dir):
    root = pathlib.Path(results_dir)
    if not root.is_dir():
        return []
    # "latest" is a symlink to the newest session; skip it so nothing is counted twice
    return sorted(p for p in root.iterdir() if p.is_dir() and not p.is_symlink() and (p / "report.json").is_file())


def main(argv):
    results_dir = argv[1] if len(argv) > 1 else "results"
    log = pathlib.Path(argv[2]) if len(argv) > 2 else None
    found = sessions(results_dir)
    if not found:
        print("沒有找到 ducktape 的 report，測試可能沒有跑起來。")
        if log and log.is_file():
            tail = log.read_text(errors="replace").splitlines()[-60:]
            print("\nlog 最後 60 行：\n```\n" + "\n".join(tail) + "\n```")
        return
    for s in found:
        report = json.loads((s / "report.json").read_text())
        results = report.get("results", [])
        status = lambda r: str(r.get("test_status", "")).lower()
        bad = [r for r in results if status(r) in ("fail", "flaky")]
        print(f"### {s.name}\n")
        print(f"- run time: {round(report.get('run_time_seconds', 0) / 60)} min")
        print(f"- passed {report.get('num_passed', 0)}, **failed {report.get('num_failed', 0)}**, "
              f"flaky {report.get('num_flaky', 0)}, ignored {report.get('num_ignored', 0)}\n")
        if not bad:
            continue
        print("| status | test | error |\n|---|---|---|")
        for r in sorted(bad, key=lambda r: (status(r) != "fail", r.get("test_id", ""))):
            err = first_line(r.get("summary")).replace("|", "\\|")
            print(f"| {status(r).upper()} | `{r.get('test_id', '')}` | {err} |")
        print()


if __name__ == "__main__":
    main(sys.argv)
