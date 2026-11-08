#!/usr/bin/env python3
"""讀 Gradle 的 JUnit XML，列出失敗或 flaky 的測試。

用法:
  junit_failures.py <kafka 目錄>              真的失敗的測試（重跑也沒過），每行 "<gradle task> <Class>.<method>"
  junit_failures.py <kafka 目錄> --flaky      失敗過但重跑通過的測試，格式同上
  junit_failures.py <kafka 目錄> --summary    Markdown 摘要（給 GITHUB_STEP_SUMMARY 用）

開了 -PmaxTestRetries 時，同一個測試的每次執行都會各有一個 <testcase>，
所以要把同名的合在一起看：有一次通過就算 flaky，全部失敗才算真的失敗。
"""
import collections
import pathlib
import re
import shlex
import sys
import xml.etree.ElementTree as ET


def collect(root: str):
    """回傳 {(task, classname, name): {"fail": bool, "pass": bool}} 和測試總數。"""
    root_path = pathlib.Path(root)
    results = collections.defaultdict(lambda: {"fail": False, "pass": False})
    for xml in sorted(root_path.glob("**/build/test-results/*/TEST-*.xml")):
        # metadata/build/test-results/test/TEST-x.xml -> :metadata:test
        rel = xml.relative_to(root_path).parts
        idx = rel.index("build")
        task = ":" + ":".join(rel[:idx] + (rel[idx + 2],))
        try:
            suite = ET.parse(xml).getroot()
        except ET.ParseError:
            continue
        for case in suite.iter("testcase"):
            r = results[(task, case.get("classname", ""), case.get("name", ""))]
            if case.find("skipped") is not None:
                continue
            failed = case.find("failure") is not None or case.find("error") is not None
            # mergeReruns 開啟時，重跑的結果會放在 flaky*/rerun* 子元素裡
            if case.find("flakyFailure") is not None or case.find("flakyError") is not None:
                r["fail"] = True
            if case.find("rerunFailure") is not None or case.find("rerunError") is not None:
                r["fail"] = True
            if failed:
                r["fail"] = True
            else:
                r["pass"] = True
    return results


def short(task, classname, name):
    # 參數化測試的名字像 testFoo(String)[1]，輸出時只留方法名，方便組 --tests。
    # Kafka 的 junit-platform.properties 把 display name 設成 "{displayName}.{argumentsWithNames}"，
    # properties 檔裡的引號是字面值，所以名字會變成 "testFoo(String).arg=x"，開頭多一個引號。
    m = re.match(r'\W*([A-Za-z_$][\w$]*)', name)
    return task, f"{classname}.{m.group(1) if m else name}"


def pick(results, flaky):
    return sorted({short(*k) for k, r in results.items()
                   if r["fail"] and (r["pass"] if flaky else not r["pass"])})


def rerun_command(tests):
    by_task = collections.defaultdict(list)
    for task, test in tests:
        by_task[task].append(test)
    return "./gradlew cleanTest " + " ".join(
        f"{t} " + " ".join(f"--tests {shlex.quote(x)}" for x in sorted(v)) for t, v in sorted(by_task.items()))


def summary(results):
    failed, flaky = pick(results, False), pick(results, True)
    ran = sum(1 for r in results.values() if r["fail"] or r["pass"])
    print(f"- 測試數：{ran}")
    print(f"- **失敗：{len(failed)}**（重跑也沒過）")
    print(f"- flaky：{len(flaky)}（失敗過但重跑通過）\n")
    for title, tests in (("失敗", failed), ("flaky", flaky)):
        if not tests:
            continue
        print(f"### {title}\n\n| task | test |\n|---|---|")
        for task, test in tests:
            print(f"| `{task}` | `{test}` |")
        print(f"\n重跑：\n```\n{rerun_command(tests)}\n```\n")
    if ran and not failed and not flaky:
        print("全部通過 :tada:")


def main(argv):
    root = argv[1] if len(argv) > 1 and not argv[1].startswith("--") else "."
    results = collect(root)
    if "--summary" in argv:
        summary(results)
        return
    for task, test in pick(results, "--flaky" in argv):
        print(f"{task} {test}")


if __name__ == "__main__":
    main(sys.argv)
