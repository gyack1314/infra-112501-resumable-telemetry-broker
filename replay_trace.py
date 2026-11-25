"""把 data/*.jsonl 演练事件在本地 StateHub 中重放。

用法：
    python3 replay_trace.py data/delta-gap-and-critical-pressure.jsonl
    python3 replay_trace.py --all
    python3 replay_trace.py data --json

默认输出人类可读的关键结果；--json 输出每行对应的 JSON 结果。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from state_hub import StateHub
from trace_io import read_trace


def replay_rows(rows: Iterable[dict[str, Any]], hub: StateHub | None = None) -> list[dict[str, Any]]:
    hub = hub or StateHub()
    output: list[dict[str, Any]] = []
    for row in rows:
        result = hub.handle(row)
        output.append(
            {
                "timestamp": row.get("timestamp"),
                "type": row.get("type"),
                "data": row.get("data"),
                "result": result,
            }
        )
    return output


def replay_file(path: str | Path, hub: StateHub | None = None) -> tuple[StateHub, list[dict[str, Any]]]:
    path = Path(path)
    allowed = {
        "ack",
        "applied",
        "budget",
        "delta",
        "disconnect",
        "drain",
        "event",
        "inspect",
        "resume",
        "subscribe",
        "whole",
    }
    rows = list(read_trace(path, allowed))
    hub = hub or StateHub()
    return hub, replay_rows(rows, hub)


def _summarize(entry: dict[str, Any]) -> str:
    row_type = entry["type"]
    result = entry["result"]
    ts = entry["timestamp"]

    if row_type == "drain":
        parts = []
        for item in result.get("deliveries", []):
            if item["type"] == "event":
                parts.append(f"event:{item['bay']}/{item['event_id']}({item['kind']})")
            elif item["type"] == "recovery":
                parts.append(f"recovery:{item['bay']}:{item['reason']}")
            else:
                compact = "+compact" if item.get("compacted") else ""
                parts.append(f"{item['type']}:{item['bay']}/{item['session']}#{item['seq']}{compact}")
        allocation = ",".join(f"{bay}×{count}" for bay, count in sorted(result.get("allocation", {}).items()))
        return f"t={ts:<2} drain {result['count']}/{result['window']} [{allocation}] -> " + ("; ".join(parts) or "空")

    if row_type in {"whole", "delta", "event"}:
        flags = []
        if result.get("backpressure"):
            flags.append("BACKPRESSURE")
        if result.get("duplicate"):
            flags.append("duplicate")
        if result.get("buffered") is False:
            flags.append("not-frame-buffered")
        suffix = f" [{', '.join(flags)}]" if flags else ""
        return (
            f"t={ts:<2} publish {row_type} {result.get('bay')}/"
            f"{result.get('session')}#{result.get('seq')} "
            f"buffer={result.get('buffer_used')}/{result.get('buffer_limit')}{suffix}"
        )

    if row_type == "ack":
        return f"t={ts:<2} ack {result.get('bay')}/{result.get('session')}/{result.get('event_id')} already={result.get('already_acknowledged')}"
    if row_type == "applied":
        return f"t={ts:<2} applied {result['bay']}/{result['session']}#{result['state_seq']} (acks changed=0)"
    if row_type == "resume":
        return f"t={ts:<2} resume {result['terminal']} pending={result['unacknowledged_events']} redeliver={result['redelivery_required']}"
    if row_type == "disconnect":
        return f"t={ts:<2} disconnect {result['terminal']} retained_pending={result['unacknowledged_events']}"
    if row_type == "subscribe":
        return f"t={ts:<2} subscribe {result['terminal']} window={result['window']}"
    if row_type == "budget":
        return f"t={ts:<2} budget {result['bay']} limit={result['limit']} used={result['buffer_used']}"
    if row_type == "inspect":
        if "bay" in result and "current_session" in result:
            waiting = ",".join(w["event_id"] + "@" + w["terminal"] for w in result["waiting_ack"])
            return (
                f"t={ts:<2} inspect bay={result['bay']} used={result['buffer_used']}/{result['buffer_limit']} "
                f"samples={result['sample_frames']} events={result['critical_events_buffered']} "
                f"waiting=[{waiting}]"
            )
        if "terminal" in result:
            return f"t={ts:<2} inspect terminal={result['terminal']} pending={result['unacknowledged_event_count']}"
        return f"t={ts:<2} inspect hub bays={result['bay_count']} terminals={result['terminal_count']}"
    return f"t={ts:<2} {row_type}"


def main() -> int:
    parser = argparse.ArgumentParser(description="在本地状态流中枢中重放合成演练")
    parser.add_argument("paths", nargs="*", type=Path, help="JSONL 文件；--all 时可省略")
    parser.add_argument("--all", action="store_true", help="重放 data 目录中的两个资料场景")
    parser.add_argument("--json", action="store_true", help="输出完整 JSON 结果")
    args = parser.parse_args()

    raw_paths = list(args.paths)
    if args.all or not raw_paths:
        raw_paths = [Path(__file__).with_name("data")]

    paths: list[Path] = []
    for raw_path in raw_paths:
        if raw_path.is_dir():
            paths.extend(sorted(raw_path.glob("*.jsonl")))
        else:
            paths.append(raw_path)
    # 去重并保持顺序。
    unique_paths = list(dict.fromkeys(paths))

    for path in unique_paths:
        hub, entries = replay_file(path)
        if args.json:
            print(json.dumps({"file": str(path), "entries": entries}, ensure_ascii=False, indent=2, default=list))
        else:
            print(f"\n=== {path} ===")
            for entry in entries:
                print(_summarize(entry))
            print("--- 值班总览 ---")
            overview = hub.inspect(include_values=False)
            print(
                json.dumps(
                    {
                        "bays": {
                            name: {
                                "buffer_used": bay["buffer_used"],
                                "buffer_limit": bay["buffer_limit"],
                                "send_opportunities": [
                                    {
                                        "terminal": item["terminal"],
                                        "priority": item["priority"],
                                        "sent": item["sent"],
                                    }
                                    for item in bay["send_opportunities"]
                                ],
                                "waiting_ack": [
                                    f"{item['event_id']}@{item['terminal']}" for item in bay["waiting_ack"]
                                ],
                            }
                            for name, bay in overview["bays"].items()
                        }
                    },
                    ensure_ascii=False,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
