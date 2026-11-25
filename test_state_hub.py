from __future__ import annotations

import json
import unittest
from pathlib import Path

from state_hub import LocalSimulation, StateHub
from replay_trace import replay_file


ROOT = Path(__file__).parent
DATA = ROOT / "data"


def read_jsonl(name: str):
    with (DATA / name).open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def deliver_ids(result):
    return [item.get("event_id") for item in result["critical_events"]]


class SuppliedTraceTests(unittest.TestCase):
    def test_delta_gap_and_critical_pressure_trace(self):
        hub, entries = replay_file(DATA / "delta-gap-and-critical-pressure.jsonl")
        by_index = entries

        # 第一次 e 因 a/b/c/d 四个未确认关键事件占满 budget=4 而被明确背压。
        first_e = next(
            entry
            for entry in by_index
            if entry["type"] == "event" and entry["data"]["event_id"] == "e"
        )
        self.assertFalse(first_e["result"]["accepted"])
        self.assertTrue(first_e["result"]["backpressure"])
        self.assertEqual(first_e["result"]["reason"], "critical_buffer_full")

        # ack a 释放槽位后，生产端重送同一个 e 被接受，且不是重复事件。
        resent_e = [
            entry
            for entry in by_index
            if entry["type"] == "event" and entry["data"]["event_id"] == "e"
        ][-1]
        self.assertTrue(resent_e["result"]["accepted"])
        self.assertFalse(resent_e["result"]["backpressure"])
        self.assertFalse(resent_e["result"]["duplicate"])

        # 两次 contact 没有被合并；drain 中关键事件顺序保持 a,b,c,d。
        drained_ids = []
        for entry in by_index:
            if entry["type"] == "drain":
                drained_ids.extend(deliver_ids(entry["result"]))
        self.assertEqual(["a", "b", "c", "d"], drained_ids)

        bay = hub.inspect(bay="east")
        self.assertEqual(4, bay["buffer_used"])
        self.assertEqual(0, bay["sample_frames"])
        self.assertEqual(4, bay["critical_events_buffered"])
        waiting = {item["event_id"] for item in bay["waiting_ack"]}
        self.assertEqual({"b", "c", "d", "e"}, waiting)
        self.assertGreaterEqual(len(bay["send_opportunities"]), 1)

    def test_reconnect_after_source_restart_trace(self):
        hub, entries = replay_file(DATA / "reconnect-after-source-restart.jsonl")

        resume_result = next(entry["result"] for entry in entries if entry["type"] == "resume")
        self.assertEqual(1, resume_result["unacknowledged_events"])
        self.assertEqual(1, resume_result["redelivery_required"])

        post_resume_drain = next(
            entry["result"]
            for entry in entries
            if entry["timestamp"] == 10 and entry["type"] == "drain"
        )
        types = [(item["type"], item.get("session"), item.get("seq")) for item in post_resume_drain["deliveries"]]
        self.assertIn(("whole", "e2", 2), types)
        self.assertIn(("event", "e1", 2), types)

        new_state = next(item for item in post_resume_drain["states"] if item["session"] == "e2")
        self.assertTrue(new_state["applicable"])
        self.assertTrue(new_state["compacted"])
        self.assertEqual({"arm": 11}, new_state["values"])
        self.assertEqual("session_changed", new_state["recovery_reason"])

        terminal = hub.inspect(terminal="floor")
        applied = {(item["bay"], item["session"]): item["state_seq"] for item in terminal["applied_states"]}
        self.assertEqual({("east", "e1"): 1}, applied)
        self.assertEqual(0, terminal["unacknowledged_event_count"])


    def test_combined_example_replays_without_error(self):
        path = ROOT / "examples" / "combined-slow-restart-reconnect.jsonl"
        hub, entries = replay_file(path)
        self.assertTrue(entries)
        self.assertTrue(any(e["type"] == "resume" for e in entries))
        self.assertGreaterEqual(hub.inspect()["bay_count"], 2)


class StateReductionTests(unittest.TestCase):
    def test_slow_terminal_receives_one_coalesced_applicable_state(self):
        hub = StateHub()
        hub.subscribe("desk", ["arm"], window=10)
        hub.publish_whole("arm", "s1", 1, {"joint": 0, "grip": "open"})
        hub.publish_delta("arm", "s1", 2, 1, {"joint": 1})
        hub.publish_delta("arm", "s1", 3, 2, {"joint": 2})

        result = hub.drain("desk")
        self.assertEqual(1, result["count"])
        state = result["states"][0]
        self.assertEqual("whole", state["type"])
        self.assertEqual(3, state["state_seq"])
        self.assertEqual({"joint": 2, "grip": "open"}, state["values"])
        self.assertTrue(state["applicable"])
        self.assertTrue(state["compacted"])

        # 没有更新时不重复推送同一画面。
        self.assertEqual(0, hub.drain("desk")["count"])
        latest = hub.read_latest_state("arm")["state"]
        self.assertEqual(3, latest["state_seq"])
        self.assertEqual(2, latest["values"]["joint"])

    def test_caught_up_terminal_can_receive_direct_delta(self):
        hub = StateHub()
        hub.subscribe("desk", ["arm"], window=10)
        hub.publish_whole("arm", "s1", 1, {"a": 0})
        first = hub.drain("desk")
        self.assertEqual("whole", first["states"][0]["type"])
        hub.mark_applied("desk", "arm", "s1", 1)

        hub.publish_delta("arm", "s1", 2, 1, {"a": 1})
        result = hub.drain("desk")
        self.assertEqual(1, result["count"])
        delta = result["states"][0]
        self.assertEqual("delta", delta["type"])
        self.assertEqual(1, delta["base_seq"])
        self.assertEqual({"a": 1}, delta["changes"])

    def test_multiple_skipped_samples_jump_to_snapshot(self):
        hub = StateHub()
        hub.subscribe("desk", ["arm"], window=10)
        hub.publish_whole("arm", "s1", 1, {"a": 0})
        hub.mark_applied("desk", "arm", "s1", 1)
        hub.publish_delta("arm", "s1", 2, 1, {"a": 1})
        hub.publish_delta("arm", "s1", 3, 2, {"a": 2})

        result = hub.drain("desk")
        self.assertEqual(1, len(result["states"]))
        self.assertEqual("whole", result["states"][0]["type"])
        self.assertEqual(3, result["states"][0]["state_seq"])

    def test_missing_full_state_reports_wait_then_recovers(self):
        hub = StateHub()
        hub.subscribe("desk", ["arm"], window=10)
        hub.publish_delta("arm", "s1", 2, 1, {"a": 1})

        first = hub.drain("desk")
        self.assertEqual(1, len(first["recoveries"]))
        recovery = first["recoveries"][0]
        self.assertFalse(recovery["applicable"])
        self.assertEqual("missing_base", recovery["reason"])
        self.assertFalse(recovery["full_state_available"])

        # 同一等待原因不反复占用窗口。
        self.assertEqual(0, hub.drain("desk")["count"])

        hub.publish_whole("arm", "s1", 3, {"a": 1})
        recovered = hub.drain("desk")
        self.assertEqual("whole", recovered["states"][0]["type"])
        self.assertEqual(3, recovered["states"][0]["state_seq"])

    def test_latest_valid_state_survives_tail_gap(self):
        hub = StateHub()
        hub.publish_whole("arm", "s1", 1, {"a": 0})
        hub.publish_delta("arm", "s1", 2, 1, {"a": 1})
        hub.publish_delta("arm", "s1", 4, 3, {"a": 4})

        latest = hub.read_latest_state("arm")["state"]
        self.assertEqual(2, latest["state_seq"])
        self.assertEqual({"a": 1}, latest["values"])
        self.assertEqual(3, latest["gap_base_seq"])


class CriticalEventTests(unittest.TestCase):
    def test_events_are_ordered_individual_and_not_acked_by_applied_state(self):
        hub = StateHub()
        hub.subscribe("desk", ["east"], window=10)
        for seq, event_id, kind in [
            (4, "a", "contact_on"),
            (5, "b", "contact_off"),
        ]:
            hub.publish_event("east", "e1", seq, event_id, kind)

        first = hub.drain("desk")
        self.assertEqual(["a", "b"], deliver_ids(first))
        self.assertTrue(all(item["requires_ack"] for item in first["critical_events"]))

        # 即使完整状态 seq 高于事件 seq，也不能替代逐条 ack。
        hub.publish_whole("east", "e1", 6, {"arm": 1})
        hub.mark_applied("desk", "east", "e1", 6)
        terminal = hub.inspect_terminal("desk")
        self.assertEqual(2, terminal["unacknowledged_event_count"])

        ack = hub.ack_event("desk", "a", "east", "e1")
        self.assertFalse(ack["already_acknowledged"])
        terminal = hub.inspect_terminal("desk")
        self.assertEqual(1, terminal["unacknowledged_event_count"])

        # 重复 ack 幂等。
        again = hub.ack_event("desk", "a", "east", "e1")
        self.assertTrue(again["already_acknowledged"])

    def test_critical_buffer_backpressure_until_ack_frees_slot(self):
        hub = StateHub()
        hub.set_budget("east", 2)
        hub.subscribe("desk", ["east"], window=10)
        self.assertTrue(hub.publish_event("east", "e1", 1, "a", "started")["accepted"])
        self.assertTrue(hub.publish_event("east", "e1", 2, "b", "contact_on")["accepted"])

        rejected = hub.publish_event("east", "e1", 3, "c", "contact_off")
        self.assertFalse(rejected["accepted"])
        self.assertTrue(rejected["backpressure"])
        self.assertEqual("critical_buffer_full", rejected["reason"])
        self.assertIn("c", rejected["retry"]["event_id"])

        hub.ack_event("desk", "a", "east", "e1")
        retried = hub.publish_event("east", "e1", 3, "c", "contact_off")
        self.assertTrue(retried["accepted"])
        self.assertFalse(retried["duplicate"])

        # a 已在服务端被显式 ack，因此 drain 只投递尚未确认的 b、c。
        ids = deliver_ids(hub.drain("desk"))
        self.assertEqual(["b", "c"], ids)

    def test_samples_are_evicted_but_unacked_events_are_protected(self):
        hub = StateHub()
        hub.set_budget("east", 3)
        hub.subscribe("desk", ["east"], window=10)
        hub.publish_whole("east", "e1", 1, {"a": 0})
        hub.publish_event("east", "e1", 2, "on", "contact_on")
        hub.publish_event("east", "e1", 3, "off", "contact_off")

        bay = hub.inspect_bay("east")
        self.assertEqual(3, bay["buffer_used"])
        self.assertEqual(1, bay["sample_frames"])

        fourth = hub.publish_event("east", "e1", 4, "done", "completed")
        self.assertTrue(fourth["accepted"])
        bay = hub.inspect_bay("east")
        self.assertEqual(0, bay["sample_frames"])
        self.assertEqual(3, bay["critical_events_buffered"])

        rejected = hub.publish_event("east", "e1", 5, "again", "started")
        self.assertTrue(rejected["backpressure"])

    def test_event_must_be_acked_by_each_subscribed_terminal(self):
        hub = StateHub()
        hub.set_budget("east", 1)
        hub.subscribe("one", ["east"], window=10)
        hub.subscribe("two", ["east"], window=10)
        hub.publish_event("east", "e1", 1, "alarm", "contact_on")

        hub.ack_event("one", "alarm", "east", "e1")
        rejected = hub.publish_event("east", "e1", 2, "next", "started")
        self.assertTrue(rejected["backpressure"])

        hub.ack_event("two", "alarm", "east", "e1")
        accepted = hub.publish_event("east", "e1", 2, "next", "started")
        self.assertTrue(accepted["accepted"])


class SchedulingTests(unittest.TestCase):
    def test_priority_gets_more_but_low_priority_is_not_starved(self):
        hub = StateHub()
        hub.subscribe("desk", ["low", "high"], window=8, priority={"low": 1, "high": 3})
        for i in range(1, 25):
            hub.publish_event("low", "l", i, f"l{i}", "started")
            hub.publish_event("high", "h", i, f"h{i}", "started")

        result = hub.drain("desk")
        self.assertEqual(8, result["count"])
        self.assertGreater(result["allocation"]["high"], result["allocation"]["low"])
        self.assertGreaterEqual(result["allocation"]["low"], 1)

    def test_window_one_rotates_so_each_bay_gets_opportunity(self):
        hub = StateHub()
        hub.subscribe("desk", ["low", "high"], window=1, priority={"low": 1, "high": 100})
        for i in range(1, 100):
            hub.publish_event("low", "l", i, f"l{i}", "started")
            hub.publish_event("high", "h", i, f"h{i}", "started")

        first = hub.drain("desk")
        second = hub.drain("desk")
        bays = {first["deliveries"][0]["bay"], second["deliveries"][0]["bay"]}
        self.assertEqual({"low", "high"}, bays)

    def test_other_bay_latest_state_gets_through_event_flood(self):
        hub = StateHub()
        hub.subscribe("desk", ["flood", "quiet"], window=2, priority={"flood": 5, "quiet": 1})
        for i in range(1, 50):
            hub.publish_event("flood", "f", i, f"e{i}", "started")
        hub.publish_whole("quiet", "q", 1, {"ok": True})

        result = hub.drain("desk")
        self.assertEqual(2, result["count"])
        self.assertIn("quiet", result["allocation"])
        self.assertEqual("whole", result["states"][0]["type"])

    def test_backpressure_in_one_bay_does_not_block_other_bays(self):
        hub = StateHub()
        hub.set_budget("east", 1)
        hub.subscribe("desk", ["east", "west"], window=10)
        hub.publish_event("east", "e", 1, "east-alarm", "started")
        rejected = hub.publish_event("east", "e", 2, "east-full", "started")
        self.assertTrue(rejected["backpressure"])

        west = hub.publish_event("west", "w", 1, "west-ok", "completed")
        self.assertTrue(west["accepted"])
        result = hub.drain("desk")
        self.assertIn("west", result["allocation"])
        self.assertIn("east", result["allocation"])
        ids = {item["event_id"] for item in result["critical_events"]}
        self.assertIn("west-ok", ids)
        self.assertIn("east-alarm", ids)
        self.assertNotIn("east-full", ids)

    def test_priority_number_convention_can_be_inverted(self):
        hub = StateHub(priority_lower_is_higher=True)
        hub.subscribe("desk", ["one", "two"], window=8, priority={"one": 1, "two": 3})
        for i in range(1, 30):
            hub.publish_event("one", "s", i, f"one{i}", "started")
            hub.publish_event("two", "s", i, f"two{i}", "started")
        allocation = hub.drain("desk")["allocation"]
        self.assertGreater(allocation["one"], allocation["two"])
        self.assertGreaterEqual(allocation["two"], 1)

    def test_window_and_priority_can_change_during_shift(self):
        hub = StateHub()
        hub.subscribe("desk", ["a", "b"], window=1, priority={"a": 1, "b": 1})
        self.assertEqual(1, hub.inspect_terminal("desk")["window"])

        hub.subscribe("desk", window=6, priority={"a": 5, "b": 1})
        info = hub.inspect_terminal("desk")
        self.assertEqual(6, info["window"])
        self.assertEqual(5, info["subscriptions"]["a"]["priority"])

        for bay in ("a", "b"):
            for i in range(1, 20):
                hub.publish_event(bay, "s", i, f"{bay}{i}", "started")
        result = hub.drain("desk")
        self.assertEqual(6, result["count"])
        self.assertGreater(result["allocation"]["a"], result["allocation"]["b"])


class ReconnectAndSessionTests(unittest.TestCase):
    def test_combined_slow_restart_disconnect_resume(self):
        sim = LocalSimulation()
        hub = sim.hub
        east = sim.producer("east")
        console = sim.console("desk")
        console.subscribe(["east"], window=2, priority={"east": 1})

        east.whole("e1", 1, {"arm": 0})
        east.delta("e1", 2, 1, {"arm": 1})
        east.critical_event("e1", 3, "z", "started")

        first = console.drain()
        self.assertEqual(2, first["count"])
        self.assertEqual("whole", first["states"][0]["type"])
        self.assertEqual(["z"], deliver_ids(first))
        console.applied("east", "e1", 1)
        console.disconnect()

        # 断线期间终端没应用的 delta 可以被新会话全量彻底跳过。
        east.delta("e1", 4, 3, {"arm": 4})
        east.whole("e2", 1, {"arm": 10})
        east.delta("e2", 2, 1, {"arm": 11})

        resumed = console.resume(
            seen_states=[{"bay": "east", "session": "e1", "state_seq": 1}],
            confirmed_events=[],
        )
        self.assertEqual(1, resumed["redelivery_required"])

        # 值班员在恢复瞬间把窗口调大。
        hub.subscribe("desk", window=3)
        result = console.drain()
        self.assertEqual(2, result["count"])
        new_state = next(item for item in result["states"] if item["session"] == "e2")
        old_event = next(item for item in result["critical_events"] if item["session"] == "e1")
        self.assertEqual(2, new_state["state_seq"])
        self.assertEqual({"arm": 11}, new_state["values"])
        self.assertEqual("session_changed", new_state["recovery_reason"])
        self.assertEqual("z", old_event["event_id"])

        # 应用新会话状态不会确认旧会话义务。
        console.applied("east", "e2", 2)
        self.assertEqual(1, hub.inspect_terminal("desk")["unacknowledged_event_count"])
        ack = console.ack("z", bay="east", session="e1")
        self.assertTrue(ack["acknowledged"])
        self.assertEqual(0, hub.inspect_terminal("desk")["unacknowledged_event_count"])

    def test_old_increment_and_new_whole_are_never_joined(self):
        hub = StateHub()
        hub.subscribe("desk", ["east"], window=10)
        hub.publish_whole("east", "e1", 9, {"arm": 9})
        hub.publish_whole("east", "e2", 1, {"arm": 100})
        hub.publish_delta("east", "e2", 2, 1, {"arm": 101})

        state = hub.get_applicable_state("desk", "east")
        self.assertEqual("e2", state["session"])
        self.assertEqual(2, state["seq"])
        self.assertEqual({"arm": 101}, state["values"])
        self.assertNotIn("base_seq", state)

    def test_resume_confirmed_events_clear_obligation(self):
        hub = StateHub()
        hub.subscribe("desk", ["east"], window=10)
        hub.publish_event("east", "e1", 1, "z", "started")
        hub.disconnect("desk")
        result = hub.resume(
            "desk",
            seen_states=[],
            confirmed_events=[{"bay": "east", "session": "e1", "event_id": "z"}],
        )
        self.assertEqual(0, result["unacknowledged_events"])
        self.assertEqual(0, hub.drain("desk")["critical_events"].__len__())


class ExtensibilityAndInspectTests(unittest.TestCase):
    def test_new_bays_and_terminals_continue_to_be_accepted(self):
        hub = StateHub()
        hub.subscribe("one", ["east"], window=10)
        hub.publish_whole("east", "e", 1, {"x": 1})

        hub.subscribe("two", ["west"], window=10)
        hub.publish_whole("west", "w", 1, {"y": 2})
        hub.publish_event("west", "w", 2, "alarm", "completed")

        one = hub.drain("one")
        two = hub.drain("two")
        self.assertEqual(["east"], [d["bay"] for d in one["deliveries"]])
        self.assertEqual({"west"}, {d["bay"] for d in two["deliveries"]})

        overview = hub.inspect()
        self.assertEqual(2, overview["bay_count"])
        self.assertEqual(2, overview["terminal_count"])
        self.assertIn("send_opportunities", overview["bays"]["west"])
        self.assertIn("waiting_ack", overview["bays"]["west"])

    def test_local_simulation_handles(self):
        sim = LocalSimulation()
        producer = sim.producer("east")
        console = sim.console("desk")
        console.subscribe(["east"], window=2, priority={"east": 1})
        producer.whole("s", 1, {"a": 1})
        producer.critical_event("s", 2, "e", "started")
        result = console.drain()
        self.assertEqual(2, result["count"])
        duty_view = sim.duty.inspect(bay="east")
        self.assertEqual("east", duty_view["bay"])

    def test_producer_handle_keeps_backpressured_frame_for_retry(self):
        sim = LocalSimulation()
        sim.duty.set_budget("east", 1)
        producer = sim.producer("east")
        console = sim.console("desk")
        console.subscribe(["east"], window=10)
        producer.critical_event("s", 1, "a", "started")
        rejected = producer.critical_event("s", 2, "b", "completed")
        self.assertTrue(rejected["backpressure"])
        self.assertIsNotNone(producer.unadmitted_event)

        console.ack("a", bay="east", session="s")
        retried = producer.retry_unadmitted()
        self.assertTrue(retried["accepted"])
        self.assertIsNone(producer.unadmitted_event)


if __name__ == "__main__":
    unittest.main()
