#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import sys
from typing import Dict, Tuple

WORKLOADS = {"BM_InsertStream", "BM_FindHit", "BM_FindMixed50"}
KEY_LENGTHS = [2, 8, 16, 24, 64]
SUITES = [0, 1]  # 0 low-cardinality, 1 high-cardinality
BASELINE = "baseline"
CANDIDATE = "saha"


def _parse_entry(entry: dict):
    if "cpu_time" not in entry:
        return None

    aggregate_name = entry.get("aggregate_name")
    if aggregate_name not in (None, "mean"):
        return None

    name = entry.get("name", "")
    for suffix in ("_mean", "_median", "_stddev"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    parts = name.split("/")
    if len(parts) < 4:
        return None

    workload = parts[0]
    impl = parts[1]
    if workload not in WORKLOADS or impl not in (BASELINE, CANDIDATE):
        return None

    try:
        key_len = int(parts[2])
        suite = int(parts[3])
    except ValueError:
        return None

    if key_len not in KEY_LENGTHS or suite not in SUITES:
        return None

    return workload, impl, key_len, suite, float(entry["cpu_time"])


def load_results(path: str) -> Dict[Tuple[str, int, int, str], float]:
    with open(path, "r", encoding="utf-8") as fp:
        payload = json.load(fp)

    records: Dict[Tuple[str, int, int, str], float] = {}
    for entry in payload.get("benchmarks", []):
        parsed = _parse_entry(entry)
        if parsed is None:
            continue
        workload, impl, key_len, suite, cpu_time = parsed
        records[(workload, key_len, suite, impl)] = cpu_time
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="Gate SAHA benchmark results against baseline.")
    parser.add_argument("json_file", help="Google benchmark JSON output file")
    parser.add_argument("--slowdown-threshold", type=float, default=1.03,
                        help="Per-case max allowed candidate/baseline ratio. Default: 1.03")
    parser.add_argument("--short-key-gain-threshold", type=float, default=0.10,
                        help="Required gain ratio for at least one len<=24 high-card case. Default: 0.10")
    args = parser.parse_args()

    records = load_results(args.json_file)
    failures = []
    short_gain_achieved = False
    rows = []

    for workload in sorted(WORKLOADS):
        for key_len in KEY_LENGTHS:
            for suite in SUITES:
                base_key = (workload, key_len, suite, BASELINE)
                cand_key = (workload, key_len, suite, CANDIDATE)
                if base_key not in records or cand_key not in records:
                    failures.append(f"Missing benchmark pair for {workload}, len={key_len}, suite={suite}")
                    continue

                baseline = records[base_key]
                candidate = records[cand_key]
                ratio = candidate / baseline if baseline > 0 else float("inf")
                gain = (baseline - candidate) / baseline if baseline > 0 else 0.0
                rows.append((workload, key_len, suite, baseline, candidate, ratio, gain))

                if ratio > args.slowdown_threshold:
                    failures.append(
                        f"Regression: {workload}, len={key_len}, suite={suite}, ratio={ratio:.4f} > {args.slowdown_threshold:.4f}"
                    )

                if suite == 1 and key_len <= 24 and gain >= args.short_key_gain_threshold:
                    short_gain_achieved = True

    for workload, key_len, suite, baseline, candidate, ratio, gain in rows:
        suite_name = "high" if suite == 1 else "low"
        print(f"{workload:15s} len={key_len:2d} suite={suite_name:4s} "
              f"baseline={baseline:12.2f} candidate={candidate:12.2f} ratio={ratio:7.4f} gain={gain:7.4f}")

    if not short_gain_achieved:
        failures.append(
            f"No high-cardinality (len<=24) case reached gain >= {args.short_key_gain_threshold:.2%}"
        )

    if failures:
        print("\nGate FAILED:")
        for f in failures:
            print(f"- {f}")
        return 1

    print("\nGate PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
