#!/usr/bin/env python3
"""列出 Gradle JUnit XML 裡失敗的測試，每行一筆：<gradle task> <Class>.<method>

用法: junit_failures.py <kafka 目錄>
例: :metadata:test org.apache.kafka.controller.QuorumControllerTest.testFoo
"""
import pathlib
import sys
import xml.etree.ElementTree as ET


def main(root: str) -> None:
    root_path = pathlib.Path(root)
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
            if case.find("failure") is not None or case.find("error") is not None:
                # 參數化測試的名字像 testFoo(String)[1]，只留方法名
                name = case.get("name", "").split("(")[0].split("[")[0]
                print(f"{task} {case.get('classname')}.{name}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
