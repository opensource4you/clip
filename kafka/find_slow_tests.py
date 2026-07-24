#!/usr/bin/env python3
import glob
import os
import sys
import xml.etree.ElementTree as ET
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(
        description="分析 Gradle/Maven 測試結果，找出執行最慢的個別測試方法"
    )
    parser.add_argument(
        "kafka_dir",
        nargs="?",
        default=".",
        help="Kafka 專案的根目錄路徑（預設為當前目錄）",
    )
    parser.add_argument(
        "-n",
        "--top",
        type=int,
        default=20,
        help="顯示前 N 個最慢的測試（預設 20 個）",
    )
    args = parser.parse_args()

    search_pattern = os.path.join(args.kafka_dir, "**", "TEST-*.xml")
    files = glob.glob(search_pattern, recursive=True)

    if not files:
        print(
            f"❌ 找不到任何測試結果 XML 檔（搜尋路徑：{args.kafka_dir}）\n請確認該路徑是否正確，以及是否已先執行過測試。"
        )
        sys.exit(1)

    # 使用 dict 來進行去重，Key 設定為 "完整類別名稱::方法名稱"
    unique_results = {}

    for xml_file in files:
        try:
            tree = ET.parse(xml_file)
            root = tree.getroot()
            for tc in root.findall(".//testcase"):
                time_sec = float(tc.attrib.get("time", 0.0))
                classname = tc.attrib.get("classname", "UnknownClass")
                method_name = tc.attrib.get("name", "UnknownTest")

                short_class = classname.split(".")[-1]
                unique_key = f"{classname}::{method_name}"

                # 如果這個測試還沒被記錄過，或者這次掃到的耗時比之前記錄的更長（例如 Flaky test retry），就更新它
                if unique_key not in unique_results or time_sec > unique_results[unique_key]["time"]:
                    unique_results[unique_key] = {
                        "time": time_sec,
                        "class": short_class,
                        "full_class": classname,
                        "method": method_name,
                    }
        except Exception:
            continue

    # 將 dict 轉回 list 並依照時間由大到小排序
    results = list(unique_results.values())
    results.sort(key=lambda x: x["time"], reverse=True)

    # 印出結果
    top_n = min(args.top, len(results))
    print(f"\n🐢 Kafka 專案前 {top_n} 名最慢的測試方法：\n")
    print(f"{'耗時 (s)':<10} | {'測試類別 (Class)':<35} | {'測試方法 (Method)'}")
    print("-" * 80)

    for res in results[:top_n]:
        time_str = f"{res['time']:.2f}s"
        print(f"{time_str:<10} | {res['class']:<35} | {res['method']}")


if __name__ == "__main__":
    main()
