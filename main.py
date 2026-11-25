"""统一本地入口：默认重放 data 下资料，也可转发到 replay_trace/json_gateway。"""

from __future__ import annotations

import sys

from replay_trace import main as replay_main


def main() -> int:
    if "--serve" in sys.argv:
        sys.argv.remove("--serve")
        from json_gateway import main as gateway_main

        return gateway_main()
    return replay_main()


if __name__ == "__main__":
    raise SystemExit(main())
