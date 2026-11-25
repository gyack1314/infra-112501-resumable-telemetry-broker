"""逐行 JSON 本地服务入口；实现见 json_gateway.py。"""

from json_gateway import JsonGateway, main

__all__ = ["JsonGateway", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
