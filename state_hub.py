"""多训练舱状态流中间件与本地仿真中枢。

该模块只依赖 Python 标准库。核心概念：

* 每个 bay（训练舱）有独立 session；session 内 seq 单调递增。
* whole 是完整状态，delta 只能作用于其 base_seq 指定的状态版本。
* 高频采样在服务端压缩成“最近可应用完整状态”；关键 event 不合并且逐条 ack。
* 每个终端维护实际 applied 状态、已 ack 事件和加权公平发送游标。
* bay 缓冲有限时只回收采样；未被所有相关终端 ack 的关键事件受保护，
  无法再容纳新关键事件时向生产端返回明确背压。
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Iterable


CRITICAL_KINDS = {"started", "contact_on", "contact_off", "completed"}


class HubError(ValueError):
    """非法请求或状态引用。"""


class BackpressureError(HubError):
    """关键事件缓冲已满；生产端应按 result['retry'] 原帧重发。"""

    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        super().__init__(result.get("reason", "critical buffer full"))


class UnknownTerminalError(HubError):
    """终端尚未 subscribe/resume 注册。"""


def event_uid(bay: str, session: str, event_id: str) -> str:
    return f"{bay}\x1f{session}\x1f{event_id}"


def frame_uid(bay: str, session: str, seq: int) -> str:
    return f"{bay}\x1f{session}\x1f{seq}"


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _require_name(value: Any, what: str) -> str:
    if not isinstance(value, str) or not value:
        raise HubError(f"{what} 必须是非空字符串")
    return value


def _require_positive_int(value: Any, what: str) -> int:
    if not _is_int(value) or value <= 0:
        raise HubError(f"{what} 必须是正整数")
    return value


@dataclass
class SessionState:
    """一个生产端会话的物化最新状态。"""

    session: str
    values: dict[str, Any] = field(default_factory=dict)
    state_seq: int | None = None
    has_state: bool = False
    gap_base_seq: int | None = None
    archived: bool = False

    def apply_whole(self, seq: int, values: dict[str, Any]) -> None:
        self.values = dict(values)
        self.state_seq = seq
        self.has_state = True
        self.gap_base_seq = None
        self.archived = False

    def apply_delta(self, seq: int, base_seq: int, changes: dict[str, Any]) -> bool:
        if self.gap_base_seq is None and self.has_state and base_seq == self.state_seq:
            self.values.update(changes)
            self.state_seq = seq
            return True

        # 保留最后一份仍有效的状态，同时明确新尾部缺基础版本。
        if self.gap_base_seq is None:
            self.gap_base_seq = base_seq
        return False

    def latest_state(self) -> dict[str, Any] | None:
        if not self.has_state or self.state_seq is None:
            return None
        return {
            "session": self.session,
            "state_seq": self.state_seq,
            "values": dict(self.values),
            "gap_base_seq": self.gap_base_seq,
            "state_current": self.gap_base_seq is None,
        }


@dataclass
class Bay:
    name: str
    limit: int | None = 256
    current_session: str | None = None
    sessions: dict[str, SessionState] = field(default_factory=dict)

    # uid -> "whole" | "delta" | "event"，严格按接纳顺序排列。
    buffer: OrderedDict[str, str] = field(default_factory=OrderedDict)
    # 仅保存当前仍在缓冲中的采样帧原文。
    frames: dict[str, dict[str, Any]] = field(default_factory=dict)
    # 已接纳且尚未被所有相关终端确认的关键事件。
    events: dict[str, dict[str, Any]] = field(default_factory=dict)

    # 幂等标识。有限 buffer 可以淘汰正文，但短周期重发仍可识别。
    frame_refs: dict[str, dict[str, Any]] = field(default_factory=dict)
    event_refs: dict[tuple[str, str], str] = field(default_factory=dict)

    received: int = 0
    accepted: int = 0
    duplicate: int = 0
    backpressure_rejected: int = 0
    sample_dropped: int = 0
    logical_time: int | None = None

    def session_state(self, session: str) -> SessionState:
        state = self.sessions.get(session)
        if state is None:
            state = SessionState(session=session)
            self.sessions[session] = state
        return state

    @property
    def sample_count(self) -> int:
        return sum(1 for kind in self.buffer.values() if kind in ("whole", "delta"))

    @property
    def event_count(self) -> int:
        return sum(1 for kind in self.buffer.values() if kind == "event")

    def has_capacity(self) -> bool:
        return self.limit is None or len(self.buffer) < self.limit

    def evict_oldest_sample(self) -> bool:
        for uid, kind in list(self.buffer.items()):
            if kind in ("whole", "delta"):
                del self.buffer[uid]
                self.frames.pop(uid, None)
                return True
        return False

    def free_one_slot_for_event(self) -> bool:
        if self.limit is None:
            return True
        while len(self.buffer) >= self.limit:
            if not self.evict_oldest_sample():
                return False
        return True

    def store_sample(self, uid: str, frame_type: str, payload: dict[str, Any]) -> bool:
        """存入采样帧；空间被受保护事件占满时允许只保留物化状态。"""
        if self.limit is not None:
            while len(self.buffer) >= self.limit:
                if not self.evict_oldest_sample():
                    self.sample_dropped += 1
                    return False
        self.buffer[uid] = frame_type
        self.frames[uid] = dict(payload)
        return True


@dataclass
class Subscription:
    bay: str
    priority: int = 1
    active: bool = True

    # 终端明确 applied 的状态：session -> state_seq。
    applied: dict[str, int] = field(default_factory=dict)
    # 已向该终端提供、但尚未被新 applied/resume 覆盖的状态版本。
    offered: dict[str, int] = field(default_factory=dict)
    recovery_signature: dict[str, str] = field(default_factory=dict)

    # 加权公平调度状态：tag 增长越慢，越优先获得下一次机会。
    fairness_tag: float = 0.0
    sent: int = 0
    state_sent: int = 0
    delta_sent: int = 0
    snapshot_sent: int = 0
    event_sent: int = 0
    recovery_sent: int = 0
    last_sent_drain: int | None = None


@dataclass
class Terminal:
    terminal_id: str
    window: int = 4
    online: bool = True
    subscriptions: dict[str, Subscription] = field(default_factory=dict)

    # uid -> 该终端的关键事件投递/确认状态。
    event_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    acked_uids: set[str] = field(default_factory=set)

    drain_count: int = 0
    connected_at: int | None = None
    disconnected_at: int | None = None
    resumed_at: int | None = None

    def sub(self, bay: str) -> Subscription:
        subscription = self.subscriptions.get(bay)
        if subscription is None:
            subscription = Subscription(bay=bay)
            self.subscriptions[bay] = subscription
        return subscription


class StateHub:
    """线程安全的多舱状态流中间件。"""

    def __init__(
        self,
        default_buffer_limit: int | None = 256,
        buffer_limit: int | None = None,
        priority_lower_is_higher: bool = False,
    ) -> None:
        if buffer_limit is not None:
            default_buffer_limit = buffer_limit
        self.priority_lower_is_higher = priority_lower_is_higher
        self.bays: dict[str, Bay] = {}
        self.terminals: dict[str, Terminal] = {}
        self.default_buffer_limit = default_buffer_limit
        self.logical_time: int = 0
        self.drain_sequence: int = 0
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 基础配置
    # ------------------------------------------------------------------
    def bay(self, name: str, create: bool = True) -> Bay | None:
        name = _require_name(name, "bay")
        bay = self.bays.get(name)
        if bay is None and create:
            bay = Bay(name=name, limit=self.default_buffer_limit)
            self.bays[name] = bay
        return bay

    def terminal(self, terminal_id: str, create: bool = False) -> Terminal | None:
        terminal_id = _require_name(terminal_id, "terminal")
        terminal = self.terminals.get(terminal_id)
        if terminal is None and create:
            terminal = Terminal(terminal_id=terminal_id)
            self.terminals[terminal_id] = terminal
        if terminal is None and not create:
            raise UnknownTerminalError(f"未知终端：{terminal_id}，请先 subscribe 或 resume")
        return terminal

    def set_budget(self, bay: str, limit: int, timestamp: int | None = None) -> dict[str, Any]:
        with self._lock:
            limit = _require_positive_int(limit, "limit")
            pod = self.bay(bay)
            assert pod is not None
            old_limit = pod.limit
            pod.limit = limit
            if timestamp is not None:
                pod.logical_time = timestamp
                self.logical_time = timestamp
            evicted = 0
            while pod.limit is not None and len(pod.buffer) > pod.limit:
                if pod.evict_oldest_sample():
                    evicted += 1
                else:
                    break
            return {
                "bay": bay,
                "limit": limit,
                "old_limit": old_limit,
                "buffer_used": len(pod.buffer),
                "evicted_samples": evicted,
                "over_limit": len(pod.buffer) > limit,
            }

    def configure_budget(self, bay: str, limit: int, timestamp: int | None = None):
        return self.set_budget(bay, limit, timestamp)

    def subscribe(
        self,
        terminal: str,
        bays: Iterable[str] | None = None,
        window: int | None = None,
        priority: int | dict[str, int] | None = None,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        """创建或更新终端订阅。

        再次调用 subscribe 即可在班中修改 window/priority。给出 bays 时，
        该列表是终端当前期望的完整订阅集合；省略 bays 则只改窗口/优先级。
        """
        with self._lock:
            terminal_id = _require_name(terminal, "terminal")
            term = self.terminals.get(terminal_id)
            is_new = term is None
            if term is None:
                term = Terminal(terminal_id=terminal_id)
                self.terminals[terminal_id] = term

            if window is not None:
                term.window = _require_positive_int(window, "window")
            elif is_new:
                term.window = 4

            term.online = True
            term.connected_at = timestamp if timestamp is not None else self.logical_time
            term.disconnected_at = None

            desired: list[str] | None = None
            if bays is not None:
                desired = [_require_name(item, "bay") for item in bays]
                if len(set(desired)) != len(desired):
                    raise HubError("bays 不能重复")
                for bay_name in desired:
                    self.bay(bay_name)  # ensure exists

            priorities = self._priority_map(priority)
            default_priority = 1
            if _is_int(priority):
                default_priority = _require_positive_int(priority, "priority")

            if desired is not None:
                desired_set = set(desired)
                for bay_name in desired:
                    sub = term.sub(bay_name)
                    sub.active = True
                    sub.priority = priorities.get(bay_name, default_priority)
                for bay_name, sub in term.subscriptions.items():
                    if bay_name not in desired_set:
                        sub.active = False
            else:
                for bay_name, prio in priorities.items():
                    sub = term.sub(bay_name)
                    sub.active = True
                    sub.priority = prio
                if _is_int(priority):
                    for sub in term.subscriptions.values():
                        sub.priority = default_priority

            if priority is not None:
                # 新优先级从配置生效点开始参与加权，避免旧的 fair tag 让
                # 值班员刚调高优先级的舱仍长期等待。累计 sent 不清零。
                if desired is not None:
                    reset_names = desired_set
                elif isinstance(priority, dict):
                    reset_names = set(priorities)
                else:
                    reset_names = set(term.subscriptions)
                for bay_name in reset_names:
                    if bay_name in term.subscriptions:
                        term.subscriptions[bay_name].fairness_tag = 0.0

            return {
                "terminal": terminal_id,
                "online": True,
                "window": term.window,
                "subscriptions": [
                    {"bay": name, "priority": sub.priority, "active": sub.active}
                    for name, sub in sorted(term.subscriptions.items())
                ],
                "created": is_new,
            }

    @staticmethod
    def _priority_map(priority: int | dict[str, int] | None) -> dict[str, int]:
        if priority is None:
            return {}
        if _is_int(priority):
            value = _require_positive_int(priority, "priority")
            return {}  # 统一默认值由调用方保留；整数可作为所有舱默认值。
        if not isinstance(priority, dict):
            if isinstance(priority, (list, tuple)):
                result: dict[str, int] = {}
                for item in priority:
                    if isinstance(item, dict):
                        bay = item.get("bay")
                        value = item.get("priority", item.get("value"))
                    else:
                        bay, value = item
                    result[_require_name(bay, "bay")] = _require_positive_int(value, "priority")
                return result
            raise HubError("priority 必须是正整数、bay->正整数 的对象或优先级条目列表")
        result = {}
        for bay, value in priority.items():
            result[_require_name(bay, "bay")] = _require_positive_int(value, f"priority[{bay}]")
        return result

    def update_config(self, terminal: str, **kwargs: Any) -> dict[str, Any]:
        """班中动态修改窗口/优先级的显式别名。"""
        return self.subscribe(terminal, **kwargs)

    def set_window(self, terminal: str, window: int) -> dict[str, Any]:
        return self.subscribe(terminal, window=window)

    def set_priority(self, terminal: str, priority: int | dict[str, int]) -> dict[str, Any]:
        return self.subscribe(terminal, priority=priority)

    def set_priority_convention(self, lower_number_is_higher: bool) -> dict[str, Any]:
        """切换优先级数字约定；默认数值越大优先级越高。"""
        with self._lock:
            self.priority_lower_is_higher = bool(lower_number_is_higher)
            for term in self.terminals.values():
                for sub in term.subscriptions.values():
                    sub.fairness_tag = 0.0
            return {"priority_lower_is_higher": self.priority_lower_is_higher}

    configure_terminal = update_config

    # ------------------------------------------------------------------
    # 生产端发布
    # ------------------------------------------------------------------
    def publish(self, frame_type: Any = None, payload: dict[str, Any] | None = None, **data: Any) -> dict[str, Any]:
        """发布帧。

        支持三种风格：
        * ``publish("whole", bay=..., ...)``
        * ``publish("whole", {...})``
        * ``publish({"type": "whole", "data": {...}})`` 或扁平 frame dict
        """
        if isinstance(frame_type, dict):
            message = frame_type
            if "type" in message and isinstance(message.get("data"), dict):
                return self.handle(message)
            payload = dict(message)
            frame_type = payload.pop("type", payload.pop("frame_type", None))
            data = payload
        elif payload is not None:
            data.update(payload)

        if frame_type == "whole":
            return self.publish_whole(**data)
        if frame_type == "delta":
            return self.publish_delta(**data)
        if frame_type == "event":
            return self.publish_event(**data)
        raise HubError(f"未知发布帧类型：{frame_type}")

    def publish_or_raise(self, frame_type: Any = None, payload: dict[str, Any] | None = None, **data: Any) -> dict[str, Any]:
        result = self.publish(frame_type, payload, **data)
        if result.get("backpressure"):
            raise BackpressureError(result)
        return result

    def publish_whole(
        self,
        bay: str,
        session: str,
        seq: int,
        values: dict[str, Any],
        timestamp: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        with self._lock:
            pod, sess, uid = self._prepare_frame(bay, session, seq, values=values)
            if isinstance(values, dict) is False:
                raise HubError("whole.values 必须是对象")

            if uid in pod.frame_refs:
                pod.duplicate += 1
                return self._duplicate_result(pod, uid, "whole")

            # 新全量或当前会话全量都使旧采样正文不再是恢复新画面的必要条件。
            # 关键事件在 pod.events 中，不受影响。
            is_new_session = pod.current_session != session
            if is_new_session:
                pod.current_session = session
            for old_uid, kind in list(pod.buffer.items()):
                if kind in ("whole", "delta"):
                    del pod.buffer[old_uid]
                    pod.frames.pop(old_uid, None)

            sess.apply_whole(seq, values)
            payload = {
                "bay": bay,
                "session": session,
                "seq": seq,
                "values": dict(values),
                "timestamp": timestamp,
            }
            buffered = pod.store_sample(uid, "whole", payload)
            pod.frame_refs[uid] = {"type": "whole", "seq": seq, "session": session}
            pod.accepted += 1
            if timestamp is not None:
                pod.logical_time = timestamp
            return {
                "accepted": True,
                "backpressure": False,
                "duplicate": False,
                "buffered": buffered,
                "compacted_state": True,
                "bay": bay,
                "session": session,
                "seq": seq,
                "buffer_used": len(pod.buffer),
                "buffer_limit": pod.limit,
            }

    def publish_delta(
        self,
        bay: str,
        session: str,
        seq: int,
        base_seq: int,
        changes: dict[str, Any],
        timestamp: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        with self._lock:
            if not _is_int(base_seq) or base_seq < 0:
                raise HubError("base_seq 必须是非负整数")
            if not isinstance(changes, dict):
                raise HubError("delta.changes 必须是对象")
            pod, sess, uid = self._prepare_frame(bay, session, seq, base_seq=base_seq)

            if uid in pod.frame_refs:
                pod.duplicate += 1
                return self._duplicate_result(pod, uid, "delta")

            applied_to_snapshot = sess.apply_delta(seq, base_seq, changes)
            payload = {
                "bay": bay,
                "session": session,
                "seq": seq,
                "base_seq": base_seq,
                "changes": dict(changes),
                "timestamp": timestamp,
            }
            buffered = pod.store_sample(uid, "delta", payload)
            pod.frame_refs[uid] = {"type": "delta", "seq": seq, "session": session}
            pod.accepted += 1
            if timestamp is not None:
                pod.logical_time = timestamp
            return {
                "accepted": True,
                "backpressure": False,
                "duplicate": False,
                "buffered": buffered,
                "applied_to_snapshot": applied_to_snapshot,
                "snapshot_ready": sess.has_state,
                "bay": bay,
                "session": session,
                "seq": seq,
                "base_seq": base_seq,
                "gap_base_seq": sess.gap_base_seq,
                "buffer_used": len(pod.buffer),
                "buffer_limit": pod.limit,
            }

    def publish_event(
        self,
        bay: str,
        session: str,
        seq: int,
        event_id: str,
        kind: str,
        timestamp: int | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        with self._lock:
            event_id = _require_name(event_id, "event_id")
            if not isinstance(kind, str) or not kind:
                raise HubError("event.kind 必须是非空字符串")
            if kind not in CRITICAL_KINDS:
                # 不因为新演练扩展 kind 而拒绝；但标准四类会在 inspect 中标识。
                pass
            pod, sess, uid = self._prepare_frame(bay, session, seq, event_id=event_id, kind=kind)
            event_key = (session, event_id)

            existing_uid = pod.event_refs.get(event_key)
            if existing_uid is not None or uid in pod.frame_refs:
                pod.duplicate += 1
                return {
                    "accepted": True,
                    "backpressure": False,
                    "duplicate": True,
                    "bay": bay,
                    "session": session,
                    "seq": seq,
                    "event_id": event_id,
                    "uid": existing_uid or uid,
                }

            if not pod.free_one_slot_for_event():
                pod.backpressure_rejected += 1
                retry = {
                    "type": "event",
                    "bay": bay,
                    "session": session,
                    "seq": seq,
                    "event_id": event_id,
                    "kind": kind,
                }
                if timestamp is not None:
                    retry["timestamp"] = timestamp
                return {
                    "accepted": False,
                    "backpressure": True,
                    "duplicate": False,
                    "reason": "critical_buffer_full",
                    "bay": bay,
                    "session": session,
                    "seq": seq,
                    "event_id": event_id,
                    "buffer_used": len(pod.buffer),
                    "buffer_limit": pod.limit,
                    "waiting_terminals": self._bay_waiting_terminal_ids(pod.name),
                    "retry": retry,
                }

            pending: set[str] = set()
            for term in self.terminals.values():
                sub = term.subscriptions.get(bay)
                if sub is None or not sub.active:
                    continue
                full_uid = event_uid(bay, session, event_id)
                if full_uid in term.acked_uids:
                    continue
                term.event_states[full_uid] = {
                    "uid": full_uid,
                    "bay": bay,
                    "session": session,
                    "seq": seq,
                    "event_id": event_id,
                    "kind": kind,
                    "status": "queued",
                    "delivery_count": 0,
                    "queued_time": timestamp if timestamp is not None else self.logical_time,
                    "last_sent_drain": None,
                    "acked_time": None,
                }
                pending.add(term.terminal_id)

            full_uid = event_uid(bay, session, event_id)
            record = {
                "uid": full_uid,
                "bay": bay,
                "session": session,
                "seq": seq,
                "event_id": event_id,
                "kind": kind,
                "timestamp": timestamp if timestamp is not None else self.logical_time,
                "pending_terminals": pending,
            }
            pod.events[full_uid] = record
            pod.event_refs[event_key] = full_uid
            pod.frame_refs[uid] = {"type": "event", "seq": seq, "session": session, "event_id": event_id}
            pod.buffer[uid] = "event"
            pod.accepted += 1
            if timestamp is not None:
                pod.logical_time = timestamp

            # 没有任何当前订阅者时，该事件的投递义务为空；幂等引用仍保留。
            if not pending:
                pod.buffer.pop(uid, None)
            return {
                "accepted": True,
                "backpressure": False,
                "duplicate": False,
                "buffered": uid in pod.buffer,
                "bay": bay,
                "session": session,
                "seq": seq,
                "event_id": event_id,
                "pending_terminals": sorted(pending),
                "buffer_used": len(pod.buffer),
                "buffer_limit": pod.limit,
            }

    def _prepare_frame(self, bay: str, session: str, seq: int, **_: Any) -> tuple[Bay, SessionState, str]:
        bay_name = _require_name(bay, "bay")
        session_id = _require_name(session, "session")
        if not _is_int(seq) or seq < 0:
            raise HubError("seq 必须是非负递增整数")
        pod = self.bay(bay_name)
        assert pod is not None
        if pod.current_session is None:
            pod.current_session = session_id
        pod.received += 1
        sess = pod.session_state(session_id)
        return pod, sess, frame_uid(bay_name, session_id, seq)

    @staticmethod
    def _duplicate_result(pod: Bay, uid: str, frame_type: str) -> dict[str, Any]:
        ref = pod.frame_refs.get(uid, {})
        return {
            "accepted": True,
            "backpressure": False,
            "duplicate": True,
            "buffered": uid in pod.buffer,
            "frame": frame_type,
            "bay": pod.name,
            "session": ref.get("session"),
            "seq": ref.get("seq"),
            "buffer_used": len(pod.buffer),
            "buffer_limit": pod.limit,
        }

    # ------------------------------------------------------------------
    # 终端状态确认
    # ------------------------------------------------------------------
    def mark_applied(
        self,
        terminal: str,
        bay: str,
        session: str,
        state_seq: int,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            bay = _require_name(bay, "bay")
            session = _require_name(session, "session")
            if not _is_int(state_seq) or state_seq < 0:
                raise HubError("state_seq 必须是非负整数")
            sub = term.sub(bay)
            old = sub.applied.get(session)
            stale = old is not None and state_seq < old
            if not stale:
                sub.applied[session] = state_seq
            # 终端实际应用了该版本后，已提供游标可以收敛。
            sub.offered[session] = max(state_seq, sub.offered.get(session, -1)) if not stale else sub.offered.get(session)
            if timestamp is not None:
                self.logical_time = timestamp

            # 关键：applied 状态绝不自动 ack 任何 seq <= state_seq 的事件。
            outstanding = sum(
                1
                for item in term.event_states.values()
                if item["bay"] == bay and item["session"] == session and item["status"] != "acked"
            )
            return {
                "applied": True,
                "stale": stale,
                "terminal": term.terminal_id,
                "bay": bay,
                "session": session,
                "state_seq": state_seq,
                "previous_state_seq": old,
                "outstanding_critical_events": outstanding,
                "critical_events_acked_by_state": 0,
            }

    # Event contract uses "applied"; provide same name for trace dispatch.
    applied = mark_applied

    def ack_event(
        self,
        terminal: str,
        event_id: str,
        bay: str | None = None,
        session: str | None = None,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            event_id = _require_name(event_id, "event_id")

            uid: str | None = None
            if bay is not None and session is not None:
                bay = _require_name(bay, "bay")
                session = _require_name(session, "session")
                uid = event_uid(bay, session, event_id)
            else:
                matches = [
                    candidate
                    for candidate, item in term.event_states.items()
                    if item["event_id"] == event_id
                ]
                if len(matches) == 1:
                    uid = matches[0]
                elif len(matches) > 1:
                    raise HubError(f"event_id={event_id} 跨舱/会话不唯一，ack 必须携带 bay 和 session")
                else:
                    # 也允许从全局事件索引中查找。
                    global_matches = []
                    for pod in self.bays.values():
                        for candidate, record in pod.events.items():
                            if record["event_id"] == event_id:
                                global_matches.append(candidate)
                    if len(global_matches) == 1:
                        uid = global_matches[0]
                    elif len(global_matches) > 1:
                        raise HubError("event_id 不唯一，ack 必须携带 bay 和 session")

            if uid is None:
                # 幂等：终端可能确认一个服务端已完成 GC 或 resume 中先声明的事件。
                if bay is None or session is None:
                    raise HubError("未知 event_id 的 ack 必须携带 bay 和 session")
                uid = event_uid(bay, session, event_id)
                already = uid in term.acked_uids
                term.acked_uids.add(uid)
                return {
                    "acknowledged": True,
                    "already_acknowledged": already,
                    "known_to_hub": False,
                    "terminal": term.terminal_id,
                    "bay": bay,
                    "session": session,
                    "event_id": event_id,
                }

            item = term.event_states.get(uid)
            already = uid in term.acked_uids or (item is not None and item["status"] == "acked")
            term.acked_uids.add(uid)
            if item is not None:
                item["status"] = "acked"
                item["acked_time"] = timestamp if timestamp is not None else self.logical_time

            resolved_bay = item["bay"] if item else bay
            resolved_session = item["session"] if item else session
            pod = self.bays.get(resolved_bay) if resolved_bay else None
            if pod is not None:
                record = pod.events.get(uid)
                if record is not None:
                    record["pending_terminals"].discard(term.terminal_id)
                    if not record["pending_terminals"]:
                        pod.events.pop(uid, None)
                        # 找到对应 buffer uid（seq 索引）并移除受保护事件槽。
                        buffer_uid = frame_uid(record["bay"], record["session"], record["seq"])
                        if pod.buffer.get(buffer_uid) == "event":
                            pod.buffer.pop(buffer_uid, None)

            if timestamp is not None:
                self.logical_time = timestamp
            return {
                "acknowledged": True,
                "already_acknowledged": already,
                "known_to_hub": item is not None,
                "terminal": term.terminal_id,
                "bay": resolved_bay,
                "session": resolved_session,
                "event_id": event_id,
                "freed_buffer_slot": pod is not None,
            }

    ack = ack_event

    # ------------------------------------------------------------------
    # 断线 / 恢复
    # ------------------------------------------------------------------
    def disconnect(self, terminal: str, timestamp: int | None = None) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            term.online = False
            term.disconnected_at = timestamp if timestamp is not None else self.logical_time
            pending = self._terminal_pending_acks(term)
            return {
                "terminal": term.terminal_id,
                "online": False,
                "retained_applied_states": self._applied_view(term),
                "unacknowledged_events": len(pending),
                "pending": pending,
            }

    def resume(
        self,
        terminal: str,
        seen_states: Iterable[dict[str, Any]] | None = None,
        confirmed_events: Iterable[Any] | None = None,
        window: int | None = None,
        priority: int | dict[str, int] | None = None,
        timestamp: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            terminal_id = _require_name(terminal, "terminal")
            term = self.terminals.get(terminal_id)
            created = term is None
            if term is None:
                term = Terminal(terminal_id=terminal_id)
                self.terminals[terminal_id] = term

            term.online = True
            term.resumed_at = timestamp if timestamp is not None else self.logical_time
            term.disconnected_at = None
            if window is not None:
                term.window = _require_positive_int(window, "window")
            if priority is not None:
                priorities = self._priority_map(priority)
                for bay_name, prio in priorities.items():
                    term.sub(bay_name).priority = prio

            # 终端带回的实际 applied 身份是恢复基准。
            for ref in seen_states or []:
                if not isinstance(ref, dict):
                    raise HubError("seen_states 每项必须是对象")
                bay_name = _require_name(ref.get("bay"), "seen_states.bay")
                session = _require_name(ref.get("session"), "seen_states.session")
                seq = ref.get("state_seq", ref.get("seq"))
                if not _is_int(seq) or seq < 0:
                    raise HubError("seen_states.state_seq 必须是非负整数")
                sub = term.sub(bay_name)
                old = sub.applied.get(session)
                if old is None or seq >= old:
                    sub.applied[session] = seq
                # 未出现在 seen 中的“已发送未 applied”状态一律要求重新提供。
                sub.offered[session] = sub.applied[session]
                sub.recovery_signature.pop(session, None)

            confirmed = 0
            for ref in confirmed_events or []:
                if isinstance(ref, str):
                    # 兼容 event_id 全局唯一的简化资料。
                    self.ack_event(terminal_id, ref, timestamp=timestamp)
                    confirmed += 1
                    continue
                if not isinstance(ref, dict):
                    raise HubError("confirmed_events 每项必须是字符串或对象")
                bay_name = ref.get("bay")
                session = ref.get("session")
                event_id = ref.get("event_id", ref.get("id"))
                event_ids = ref.get("event_ids")
                if event_ids is not None:
                    for one_id in event_ids:
                        self.ack_event(terminal_id, one_id, bay_name, session, timestamp=timestamp)
                        confirmed += 1
                elif event_id is not None:
                    self.ack_event(terminal_id, event_id, bay_name, session, timestamp=timestamp)
                    confirmed += 1
                elif _is_int(ref.get("seq")):
                    # “关键事件位置”也可能用 (bay,session,seq) 表示。
                    uid = frame_uid(bay_name, session, ref["seq"])
                    # frame ref 中可找到 event_id；找不到时用 seq 标识构造幂等键。
                    event_id = f"seq:{ref['seq']}"
                    for pod in self.bays.values():
                        frame_ref = pod.frame_refs.get(uid)
                        if frame_ref and "event_id" in frame_ref:
                            event_id = frame_ref["event_id"]
                            bay_name = pod.name
                            session = frame_ref["session"]
                            break
                    self.ack_event(terminal_id, event_id, bay_name, session, timestamp=timestamp)
                    confirmed += 1

            # 断线前已发送但未确认的通知恢复为 queued，drain 会按序重发。
            redeliver = 0
            for item in term.event_states.values():
                if item["status"] == "sent":
                    item["status"] = "queued"
                    item["redelivery_required"] = True
                    redeliver += 1

            pending = self._terminal_pending_acks(term)
            return {
                "resumed": True,
                "created": created,
                "terminal": terminal_id,
                "online": True,
                "confirmed_count": confirmed,
                "redelivery_required": redeliver,
                "unacknowledged_events": len(pending),
                "pending": pending,
                "applied_states": self._applied_view(term),
            }

    # ------------------------------------------------------------------
    # 拉取 / drain
    # ------------------------------------------------------------------
    def drain(
        self,
        terminal: str,
        max_items: int | None = None,
        limit: int | None = None,
        n: int | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            self.drain_sequence += 1
            term.drain_count += 1
            drain_id = self.drain_sequence

            if not term.online:
                return {
                    "terminal": term.terminal_id,
                    "drain_id": drain_id,
                    "online": False,
                    "count": 0,
                    "window": term.window,
                    "deliveries": [],
                    "items": [],
                    "messages": [],
                    "notifications": [],
                    "states": [],
                    "critical_events": [],
                    "recoveries": [],
                    "allocation": {},
                    "reason": "terminal_offline_call_resume",
                }

            requested = max_items if max_items is not None else limit
            requested = requested if requested is not None else n
            item_limit = term.window
            if requested is not None:
                item_limit = min(item_limit, _require_positive_int(requested, "max_items"))

            # 每个 bay 维护本次 drain 的候选状态和 queued event 列表。
            state_items: dict[str, dict[str, Any] | None] = {}
            event_lists: dict[str, list[str]] = {}
            for bay_name, sub in term.subscriptions.items():
                if not sub.active:
                    continue
                pod = self.bays.get(bay_name)
                if pod is None:
                    continue
                state_items[bay_name] = self.build_state_item(term, sub, pod)
                event_lists[bay_name] = [
                    uid
                    for uid, item in term.event_states.items()
                    if item["bay"] == bay_name and item["status"] == "queued"
                ]

            deliveries: list[dict[str, Any]] = []
            allocation: dict[str, int] = {}
            served_this_drain: set[str] = set()
            while len(deliveries) < item_limit:
                candidate_bay = self._choose_bay(term, state_items, event_lists, served_this_drain)
                if candidate_bay is None:
                    break
                served_this_drain.add(candidate_bay)
                sub = term.subscriptions[candidate_bay]
                state_item = state_items[candidate_bay]
                event_list = event_lists[candidate_bay]

                if state_item is not None:
                    delivery = state_item
                    state_items[candidate_bay] = None
                    if delivery["type"] == "recovery":
                        sub.recovery_sent += 1
                    elif delivery["type"] == "delta":
                        sub.state_sent += 1
                        sub.delta_sent += 1
                    else:
                        sub.state_sent += 1
                        if delivery.get("compacted"):
                            sub.snapshot_sent += 1
                else:
                    uid = event_list.pop(0)
                    item = term.event_states[uid]
                    item["status"] = "sent"
                    item["delivery_count"] = int(item.get("delivery_count", 0)) + 1
                    item["last_sent_drain"] = drain_id
                    delivery = {
                        "type": "event",
                        "frame_type": "event",
                        "delivery_kind": "critical_event",
                        "requires_ack": True,
                        "critical": True,
                        "applicable": True,
                        "id": item["event_id"],
                        **{k: v for k, v in item.items() if k not in ("status",)},
                        "delivery_status": "sent",
                    }
                    sub.event_sent += 1

                sub.sent += 1
                sub.last_sent_drain = drain_id
                if self.priority_lower_is_higher:
                    # 数字越小优先级越高：p=1 的 tag 增速是 p=2 的一半。
                    sub.fairness_tag += float(max(1, sub.priority))
                else:
                    sub.fairness_tag += 1.0 / max(1, sub.priority)
                allocation[candidate_bay] = allocation.get(candidate_bay, 0) + 1
                deliveries.append(delivery)

            states = [item for item in deliveries if item.get("type") in ("whole", "delta")]
            events = [item for item in deliveries if item.get("type") == "event"]
            recoveries = [item for item in deliveries if item.get("type") == "recovery"]
            return {
                "terminal": term.terminal_id,
                "drain_id": drain_id,
                "online": True,
                "count": len(deliveries),
                "window": term.window,
                "request_limit": item_limit,
                "allocation": allocation,
                "deliveries": deliveries,
                "items": deliveries,
                "messages": deliveries,
                "notifications": deliveries,
                "states": states,
                "critical_events": events,
                "recoveries": recoveries,
            }

    poll = drain
    receive = drain

    def drain_items(self, terminal: str, max_items: int | None = None) -> list[dict[str, Any]]:
        return self.drain(terminal, max_items=max_items)["deliveries"]

    def drain_events(self, terminal: str, max_items: int | None = None) -> list[dict[str, Any]]:
        return self.drain(terminal, max_items=max_items)["critical_events"]

    def _choose_bay(
        self,
        term: Terminal,
        state_items: dict[str, dict[str, Any] | None],
        event_lists: dict[str, list[str]],
        served_this_drain: set[str] | None = None,
    ) -> str | None:
        served_this_drain = served_this_drain or set()
        available: list[tuple[tuple[int, float, int, str], str]] = []
        for bay_name, state_item in state_items.items():
            has_event = bool(event_lists.get(bay_name))
            if state_item is None and not has_event:
                continue
            sub = term.subscriptions[bay_name]
            # 第一轮保证每个有内容的舱至少获得一个机会；之后用 WFQ 按
            # priority 分配更多份额。tag 小者优先，同 tag 时高优先级优先。
            priority_rank = sub.priority if self.priority_lower_is_higher else -max(1, sub.priority)
            key = (
                1 if bay_name in served_this_drain else 0,
                sub.fairness_tag,
                priority_rank,
                bay_name,
            )
            available.append((key, bay_name))
        if not available:
            return None
        available.sort(key=lambda pair: pair[0])
        return available[0][1]

    # ------------------------------------------------------------------
    # 可应用状态 / 恢复原因
    # ------------------------------------------------------------------
    def read_latest_state(self, bay: str, session: str | None = None) -> dict[str, Any]:
        with self._lock:
            pod = self.bay(bay, create=False)
            if pod is None:
                raise UnknownTerminalError(f"未知训练舱：{bay}")
            target_session = session or pod.current_session
            if target_session is None or target_session not in pod.sessions:
                return {
                    "bay": bay,
                    "session": target_session,
                    "state": None,
                    "ready": False,
                    "has_state": False,
                    "values": None,
                    "state_seq": None,
                }
            sess = pod.sessions[target_session]
            state = sess.latest_state()
            return {
                "bay": bay,
                "session": target_session,
                "state": state,
                "ready": sess.has_state and sess.gap_base_seq is None,
                "has_state": sess.has_state,
                "state_seq": sess.state_seq,
                "values": dict(sess.values) if sess.has_state else None,
                "gap_base_seq": sess.gap_base_seq,
            }

    def get_latest_state(self, bay: str, session: str | None = None) -> dict[str, Any]:
        return self.read_latest_state(bay, session)

    latest_state = get_latest_state

    def get_applicable_state(self, terminal: str, bay: str) -> dict[str, Any] | None:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            sub = term.sub(bay)
            pod = self.bay(bay, create=False)
            if pod is None:
                return None
            return self.build_state_item(term, sub, pod, commit=False)

    def state_status(self, terminal: str, bay: str) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            sub = term.sub(bay)
            pod = self.bay(bay, create=False)
            if pod is None:
                return {"bay": bay, "status": "unknown_bay"}
            return self.describe_state_view(term, sub, pod)

    def describe_state_view(self, term: Terminal, sub: Subscription, pod: Bay) -> dict[str, Any]:
        """告诉终端/值班台当前哪份状态可用、是否必须重新取全量。"""
        session = pod.current_session
        other_applied = {s: seq for s, seq in sub.applied.items() if s != session}
        if session is None:
            return {"bay": pod.name, "status": "no_stream", "other_applied_sessions": other_applied}

        sess = pod.sessions.get(session)
        applied_seq = sub.applied.get(session)
        offered_seq = sub.offered.get(session)
        if sess is None or not sess.has_state:
            reason = "missing_base" if sess is not None and sess.gap_base_seq is not None else "await_full"
            return {
                "bay": pod.name,
                "status": "waiting_full",
                "current_session": session,
                "applied_state_seq": applied_seq,
                "offered_state_seq": offered_seq,
                "full_state_available": False,
                "applicable_state_available": False,
                "must_request_full": True,
                "recovery_reason": reason,
                "missing_base_seq": None if sess is None else sess.gap_base_seq,
                "other_applied_sessions": other_applied,
            }

        latest_seq = sess.state_seq
        session_changed = applied_seq is None and bool(other_applied)
        if session_changed:
            status = "rebuild_after_session_change"
            reason = "session_changed"
        elif applied_seq is None:
            status = "full_state_required"
            reason = "initial_state"
        elif sess.gap_base_seq is not None:
            status = "last_valid_state_with_tail_gap"
            reason = "missing_tail_base"
        elif applied_seq >= latest_seq:  # type: ignore[operator]
            status = "current"
            reason = None
        elif offered_seq == latest_seq:
            status = "state_offered_waiting_application"
            reason = "await_applied_ack"
        else:
            status = "update_available"
            reason = "samples_coalesced"

        offered_current = offered_seq == latest_seq and (applied_seq is None or applied_seq < latest_seq)
        if offered_current:
            status = "state_offered_waiting_application"

        return {
            "bay": pod.name,
            "status": status,
            "current_session": session,
            "applied_state_seq": applied_seq,
            "offered_state_seq": offered_seq,
            "latest_state_seq": latest_seq,
            "full_state_available": True,
            "applicable_state_available": True,
            "full_state_already_offered": offered_current,
            "must_request_full": (session_changed or applied_seq is None) and not offered_current,
            "must_apply_offered_state": offered_current,
            "can_apply_direct_delta": (
                applied_seq is not None
                and sess.gap_base_seq is None
                and self._single_applicable_delta(pod, session, applied_seq, latest_seq) is not None
            ),
            "recovery_reason": reason,
            "tail_gap_base_seq": sess.gap_base_seq,
            "other_applied_sessions": other_applied,
        }

    def build_state_item(
        self,
        term: Terminal,
        sub: Subscription,
        pod: Bay,
        commit: bool = True,
    ) -> dict[str, Any] | None:
        session = pod.current_session
        if session is None:
            return None
        sess = pod.sessions.get(session)
        if sess is None:
            return None

        applied_seq = sub.applied.get(session)
        offered_seq = sub.offered.get(session)

        # 还没有任何完整状态：明确等待 whole，而不是发不可应用的 delta。
        if not sess.has_state:
            signature = f"no-full:{sess.gap_base_seq}"
            if sub.recovery_signature.get(session) == signature:
                return None
            if commit:
                sub.recovery_signature[session] = signature
            reason = "missing_base" if sess.gap_base_seq is not None else "await_full"
            return {
                "type": "recovery",
                "frame_type": "recovery",
                "delivery_kind": "recovery",
                "applicable": False,
                "bay": pod.name,
                "session": session,
                "reason": reason,
                "missing_base_seq": sess.gap_base_seq,
                "full_state_available": False,
                "waiting_for": "whole",
                "must_resync": True,
                "terminal_applied_seq": applied_seq,
            }

        latest_seq = sess.state_seq
        assert latest_seq is not None

        if applied_seq is not None and applied_seq >= latest_seq and sess.gap_base_seq is None:
            return None
        if offered_seq is not None and offered_seq >= latest_seq:
            return None

        session_changed = applied_seq is None and any(s != session for s in sub.applied)
        initial = applied_seq is None and not session_changed

        # 尝试寻找一个可直接应用的单帧 delta。
        delta_frame = self._single_applicable_delta(pod, session, applied_seq, latest_seq)
        if delta_frame is not None and sess.gap_base_seq is None:
            if commit:
                sub.offered[session] = delta_frame["seq"]
            return {
                "type": "delta",
                "frame_type": "delta",
                "delivery_kind": "state",
                "applicable": True,
                "bay": pod.name,
                "session": session,
                "seq": delta_frame["seq"],
                "state_seq": delta_frame["seq"],
                "base_seq": delta_frame["base_seq"],
                "changes": dict(delta_frame["changes"]),
                "coalesced": False,
                "recovery_reason": None,
            }

        # 有物化全量：始终可直接应用，可用于跳过采样/跨会话重建/缺 base 恢复。
        reason: str | None
        if session_changed:
            reason = "session_changed"
        elif initial:
            reason = "initial_state"
        elif sess.gap_base_seq is not None:
            reason = "missing_tail_base"
        elif applied_seq is None:
            reason = "full_state_required"
        else:
            reason = "samples_coalesced"

        if commit:
            sub.offered[session] = latest_seq
            sub.recovery_signature.pop(session, None)

        return {
            "type": "whole",
            "frame_type": "whole",
            "delivery_kind": "state",
            "full_state": True,
            "applicable": True,
            "bay": pod.name,
            "session": session,
            "seq": latest_seq,
            "state_seq": latest_seq,
            "values": dict(sess.values),
            "compacted": True,
            "coalesced": True,
            "materialized": True,
            "recovery_reason": reason,
            "requires_full_state": applied_seq is None,
            "must_resync": session_changed
            or reason in {"missing_tail_base", "base_unavailable", "samples_coalesced"},
            "tail_gap_base_seq": sess.gap_base_seq,
            "terminal_applied_seq": applied_seq,
        }

    def _single_applicable_delta(
        self,
        pod: Bay,
        session: str,
        applied_seq: int | None,
        latest_seq: int,
    ) -> dict[str, Any] | None:
        if applied_seq is None:
            return None
        candidates: list[dict[str, Any]] = []
        for uid, kind in pod.buffer.items():
            if kind != "delta":
                continue
            frame = pod.frames.get(uid)
            if frame is None or frame.get("session") != session:
                continue
            if frame["seq"] > applied_seq:
                candidates.append(frame)
        if len(candidates) != 1:
            return None
        frame = candidates[0]
        if frame["base_seq"] != applied_seq or frame["seq"] != latest_seq:
            return None
        return frame

    # ------------------------------------------------------------------
    # inspect
    # ------------------------------------------------------------------
    def inspect(
        self,
        bay: str | None = None,
        terminal: str | None = None,
        include_values: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            if terminal is not None:
                return self.inspect_terminal(terminal, include_pending=True)
            if bay is not None:
                return self.inspect_bay(bay, include_values=include_values)
            return {
                "hub": "state_hub",
                "logical_time": self.logical_time,
                "drain_sequence": self.drain_sequence,
                "priority_lower_is_higher": self.priority_lower_is_higher,
                "terminal_count": len(self.terminals),
                "bay_count": len(self.bays),
                "bays": {name: self.inspect_bay(name, include_values=False) for name in sorted(self.bays)},
                "terminals": {
                    term.terminal_id: self.inspect_terminal(term.terminal_id, include_pending=False)
                    for term in sorted(self.terminals.values(), key=lambda t: t.terminal_id)
                },
                "waiting_ack": self.all_waiting_acks(),
            }

    def inspect_bay(self, bay: str, include_values: bool = True) -> dict[str, Any]:
        with self._lock:
            pod = self.bay(bay, create=False)
            if pod is None:
                raise UnknownTerminalError(f"未知训练舱：{bay}")
            waiting = [
                item
                for item in self.all_waiting_acks()
                if item["bay"] == bay
            ]
            opportunities = []
            for term in self.terminals.values():
                sub = term.subscriptions.get(bay)
                if sub is None:
                    continue
                opportunities.append(
                    {
                        "terminal": term.terminal_id,
                        "online": term.online,
                        "active": sub.active,
                        "window": term.window,
                        "priority": sub.priority,
                        "fairness_tag": sub.fairness_tag,
                        "sent": sub.sent,
                        "state_sent": sub.state_sent,
                        "delta_sent": sub.delta_sent,
                        "snapshot_sent": sub.snapshot_sent,
                        "critical_event_sent": sub.event_sent,
                        "recovery_sent": sub.recovery_sent,
                        "last_sent_drain": sub.last_sent_drain,
                    }
                )

            current_state = None
            if pod.current_session is not None:
                sess = pod.sessions.get(pod.current_session)
                if sess is not None:
                    current_state = sess.latest_state()
                    if not include_values and current_state:
                        current_state.pop("values", None)

            opportunity_view = sorted(opportunities, key=lambda x: x["terminal"])
            waiting_terminal_ids = sorted({item["terminal"] for item in waiting})
            occupancy = {
                "used": len(pod.buffer),
                "limit": pod.limit,
                "available_slots": None if pod.limit is None else max(0, pod.limit - len(pod.buffer)),
                "sample_frames": pod.sample_count,
                "critical_events": pod.event_count,
                "over_limit": pod.limit is not None and len(pod.buffer) > pod.limit,
            }
            return {
                "bay": pod.name,
                "current_session": pod.current_session,
                "buffer_limit": pod.limit,
                "buffer_used": len(pod.buffer),
                "available_slots": occupancy["available_slots"],
                "over_limit": occupancy["over_limit"],
                "sample_frames": pod.sample_count,
                "critical_events_buffered": pod.event_count,
                "cache_occupancy": occupancy,
                "cache_entries": list(pod.buffer.values()),
                "current_materialized_state": current_state,
                "latest_valid_state": current_state,
                "publish": {
                    "received": pod.received,
                    "accepted": pod.accepted,
                    "duplicate": pod.duplicate,
                    "backpressure_rejected": pod.backpressure_rejected,
                    "sample_dropped_from_frame_buffer": pod.sample_dropped,
                },
                "send_opportunities": opportunity_view,
                "opportunities": opportunity_view,
                "waiting_ack": waiting,
                "pending_acks": waiting,
                "waiting_terminal_ids": waiting_terminal_ids,
                "ack_waiters": waiting_terminal_ids,
            }

    def inspect_terminal(self, terminal: str, include_pending: bool = True) -> dict[str, Any]:
        with self._lock:
            term = self.terminal(terminal)
            assert term is not None
            subscriptions = {}
            for bay_name, sub in sorted(term.subscriptions.items()):
                pod = self.bays.get(bay_name)
                subscriptions[bay_name] = {
                    "active": sub.active,
                    "priority": sub.priority,
                    "window": term.window,
                    "applied_states": dict(sub.applied),
                    "offered_states": dict(sub.offered),
                    "state_view": self.describe_state_view(term, sub, pod) if pod is not None else {"status": "unknown_bay"},
                    "fairness_tag": sub.fairness_tag,
                    "send_opportunities": {
                        "total": sub.sent,
                        "state": sub.state_sent,
                        "delta": sub.delta_sent,
                        "compacted_snapshot": sub.snapshot_sent,
                        "critical_event": sub.event_sent,
                        "recovery": sub.recovery_sent,
                    },
                    "last_sent_drain": sub.last_sent_drain,
                }
            pending = self._terminal_pending_acks(term)
            pending_view = pending if include_pending else len(pending)
            result = {
                "terminal": term.terminal_id,
                "online": term.online,
                "window": term.window,
                "drain_count": term.drain_count,
                "subscriptions": subscriptions,
                "applied_states": self._applied_view(term),
                "acked_event_count": len(term.acked_uids),
                "unacknowledged_event_count": len(pending),
                "waiting_ack": pending_view,
                "pending_acks": pending_view,
                "pending_critical_events": pending_view,
            }
            return result

    def all_waiting_acks(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for term in sorted(self.terminals.values(), key=lambda t: t.terminal_id):
            result.extend(self._terminal_pending_acks(term))
        result.sort(key=lambda item: (item["bay"], item["session"], item["seq"], item["terminal"]))
        return result

    def _terminal_pending_acks(self, term: Terminal) -> list[dict[str, Any]]:
        pending = []
        for item in term.event_states.values():
            if item["status"] == "acked":
                continue
            pending.append(
                {
                    "terminal": term.terminal_id,
                    "online": term.online,
                    "bay": item["bay"],
                    "session": item["session"],
                    "seq": item["seq"],
                    "event_id": item["event_id"],
                    "kind": item["kind"],
                    "delivery_status": item["status"],
                    "delivery_count": item.get("delivery_count", 0),
                    "last_sent_drain": item.get("last_sent_drain"),
                }
            )
        pending.sort(key=lambda x: (x["session"], x["seq"], x["event_id"]))
        return pending

    def _applied_view(self, term: Terminal) -> list[dict[str, Any]]:
        result = []
        for bay_name, sub in sorted(term.subscriptions.items()):
            for session, seq in sorted(sub.applied.items()):
                result.append({"bay": bay_name, "session": session, "state_seq": seq})
        return result

    def _bay_waiting_terminal_ids(self, bay: str) -> list[str]:
        return sorted(
            {
                item["terminal"]
                for item in self.all_waiting_acks()
                if item["bay"] == bay
            }
        )

    # ------------------------------------------------------------------
    # JSONL trace 分发
    # ------------------------------------------------------------------
    def handle(self, event: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(event, dict):
            raise HubError("事件必须是对象")
        event_type = event.get("type")
        data = event.get("data", {})
        timestamp = event.get("timestamp")
        if not isinstance(data, dict):
            raise HubError("data 必须是对象")
        if timestamp is not None and _is_int(timestamp):
            self.logical_time = timestamp

        payload = dict(data)
        payload.setdefault("timestamp", timestamp)

        if event_type == "subscribe":
            return self.subscribe(**payload)
        if event_type == "budget":
            return self.set_budget(**payload)
        if event_type in ("whole", "delta", "event"):
            return self.publish(event_type, **payload)
        if event_type == "drain":
            terminal = payload.pop("terminal")
            payload.pop("timestamp", None)
            return self.drain(terminal, **payload)
        if event_type == "applied":
            return self.mark_applied(**payload)
        if event_type == "ack":
            return self.ack_event(**payload)
        if event_type == "disconnect":
            return self.disconnect(**payload)
        if event_type == "resume":
            return self.resume(**payload)
        if event_type == "inspect":
            payload.pop("timestamp", None)
            return self.inspect(**payload)
        raise HubError(f"未知事件类型：{event_type}")


# 便于不同调用习惯的别名。
StreamHub = StateHub
Hub = StateHub
Middleware = StateHub
StateStream = StateHub
StreamMiddleware = StateHub
StateMiddleware = StateHub


class ProducerHandle:
    """某个舱的本地生产端句柄。

    关键事件遇到背压时，句柄保留 ``unadmitted_event``；值班台/终端释放
    缓冲后调用 :meth:`retry_unadmitted` 即可重发同一帧。
    """

    def __init__(self, hub: StateHub, bay: str) -> None:
        self.hub = hub
        self.bay = bay
        self.unadmitted_event: dict[str, Any] | None = None
        self.last_backpressure: dict[str, Any] | None = None

    def whole(self, session: str, seq: int, values: dict[str, Any]) -> dict[str, Any]:
        return self.hub.publish_whole(self.bay, session, seq, values)

    def delta(self, session: str, seq: int, base_seq: int, changes: dict[str, Any]) -> dict[str, Any]:
        return self.hub.publish_delta(self.bay, session, seq, base_seq, changes)

    def critical_event(self, session: str, seq: int, event_id: str, kind: str) -> dict[str, Any]:
        frame = {
            "bay": self.bay,
            "session": session,
            "seq": seq,
            "event_id": event_id,
            "kind": kind,
        }
        result = self.hub.publish_event(**frame)
        if result.get("backpressure"):
            self.unadmitted_event = frame
            self.last_backpressure = result
        elif (
            self.unadmitted_event is not None
            and self.unadmitted_event.get("session") == session
            and self.unadmitted_event.get("seq") == seq
            and self.unadmitted_event.get("event_id") == event_id
        ):
            self.unadmitted_event = None
            self.last_backpressure = None
        return result

    event = critical_event

    def retry_unadmitted(self) -> dict[str, Any]:
        if self.unadmitted_event is None:
            return {"accepted": True, "backpressure": False, "nothing_to_retry": True}
        frame = dict(self.unadmitted_event)
        frame.pop("bay", None)
        return self.critical_event(**frame)

    def critical_event_or_raise(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = self.critical_event(*args, **kwargs)
        if result.get("backpressure"):
            raise BackpressureError(result)
        return result


class ConsoleHandle:
    """某个操作台终端的本地句柄。"""

    def __init__(self, hub: StateHub, terminal: str) -> None:
        self.hub = hub
        self.terminal = terminal

    def subscribe(self, bays: Iterable[str], window: int = 4, priority: int | dict[str, int] | None = None):
        return self.hub.subscribe(self.terminal, bays, window, priority)

    def drain(self, max_items: int | None = None):
        return self.hub.drain(self.terminal, max_items)

    poll = drain
    receive = drain

    def applied(self, bay: str, session: str, state_seq: int):
        return self.hub.mark_applied(self.terminal, bay, session, state_seq)

    def ack(self, event_id: str, bay: str | None = None, session: str | None = None):
        return self.hub.ack_event(self.terminal, event_id, bay, session)

    def disconnect(self):
        return self.hub.disconnect(self.terminal)

    def resume(self, *args: Any, **kwargs: Any):
        return self.hub.resume(self.terminal, *args, **kwargs)

    def applicable_state(self, bay: str):
        return self.hub.get_applicable_state(self.terminal, bay)


class DutyConsole:
    def __init__(self, hub: StateHub) -> None:
        self.hub = hub

    def inspect(self, bay: str | None = None, terminal: str | None = None):
        return self.hub.inspect(bay=bay, terminal=terminal)

    def set_window(self, terminal: str, window: int):
        return self.hub.subscribe(terminal, window=window)

    def set_priority(self, terminal: str, priority: dict[str, int]):
        return self.hub.subscribe(terminal, priority=priority)

    def set_budget(self, bay: str, limit: int):
        return self.hub.set_budget(bay, limit)

    configure_budget = set_budget


class LocalSimulation:
    """生产端、操作台和值班台共用的进程内仿真接口。"""

    def __init__(self, default_buffer_limit: int | None = 256, buffer_limit: int | None = None) -> None:
        self.hub = StateHub(
            default_buffer_limit=default_buffer_limit,
            buffer_limit=buffer_limit,
        )
        self.duty = DutyConsole(self.hub)

    def producer(self, bay: str) -> ProducerHandle:
        self.hub.bay(bay)
        return ProducerHandle(self.hub, bay)

    def console(self, terminal: str) -> ConsoleHandle:
        return ConsoleHandle(self.hub, terminal)
