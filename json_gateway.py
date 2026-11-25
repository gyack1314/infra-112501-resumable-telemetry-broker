"""逐行 JSON 的本地网关。

每行输入既可以是资料中的事件信封：

    {"timestamp": 1, "type": "drain", "data": {"terminal": "ops"}}

也可以是直接方法调用：

    {"method": "inspect", "kwargs": {"bay": "east"}}

输出始终是一行 JSON：成功为 {"ok": true, "result": ...}，
失败为 {"ok": false, "error": "...", "error_type": "..."}。

命令行：
    python3 json_gateway.py
    cat data/combined-slow-restart-reconnect.jsonl | python3 json_gateway.py --event-envelope
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from state_hub import StateHub


class JsonGateway:
    def __init__(self, hub: StateHub | None = None) -> None:
        self.hub = hub or StateHub()

    def request(self, message: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(message, dict):
            raise ValueError("请求必须是 JSON 对象")

        # 资料事件信封。
        if "type" in message and ("data" in message or message["type"] in {"inspect", "drain"}):
            return self.hub.handle(message)

        method = message.get("method")
        if not isinstance(method, str) or not method:
            raise ValueError("请求必须包含 type/data 或 method")
        if method.startswith("_"):
            raise ValueError("不允许调用私有方法")

        target: Any = self.hub
        if message.get("role") == "duty":
            target = self.hub
        func = getattr(target, method, None)
        if not callable(func):
            raise ValueError(f"未知方法：{method}")
        args = message.get("args", [])
        kwargs = message.get("kwargs", {})
        if not isinstance(args, list) or not isinstance(kwargs, dict):
            raise ValueError("args 必须是数组，kwargs 必须是对象")
        return func(*args, **kwargs)

    def handle_line(self, line: str) -> str:
        try:
            message = json.loads(line)
            result = self.request(message)
            response = {"ok": True, "result": result}
        except Exception as error:  # 网关需要把明确错误返回给本地调用方。
            response = {
                "ok": False,
                "error": str(error),
                "error_type": type(error).__name__,
            }
        return json.dumps(response, ensure_ascii=False, default=list)


def main() -> int:
    parser = argparse.ArgumentParser(description="逐行 JSON 状态流网关")
    parser.add_argument("--event-envelope", action="store_true", help="输出中保留原始事件序号")
    args = parser.parse_args()
    gateway = JsonGateway()

    for line in sys.stdin:
        if not line.strip():
            continue
        response = gateway.handle_line(line)
        if args.event_envelope:
            try:
                request = json.loads(line)
                response_obj = json.loads(response)
                response_obj["timestamp"] = request.get("timestamp")
                response = json.dumps(response_obj, ensure_ascii=False, default=list)
            except json.JSONDecodeError:
                pass
        print(response, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
