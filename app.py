"""应用入口兼容模块；主要实现见 state_hub.py。"""

from json_gateway import JsonGateway
from state_hub import (
    Hub,
    LocalSimulation,
    StateHub,
)

hub = StateHub()
gateway = JsonGateway(hub)

__all__ = ["StateHub", "Hub", "LocalSimulation", "JsonGateway", "hub", "gateway"]


if __name__ == "__main__":
    raise SystemExit(__import__("json_gateway").main())
