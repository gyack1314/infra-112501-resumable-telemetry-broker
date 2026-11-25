# 多舱训练状态流的降载、补帧与恢复

多个训练舱向操作台提供合成状态。舱内 session 由生产端重启后更换，同会话的 seq 递增。帧为完整状态 whole、状态增量 delta 或关键状态变化 event。

## 事件格式

每个 JSONL 文件是一段独立演练。每行含整数逻辑秒点 timestamp、事件名称 type 和对象 data。同一秒的行按文件顺序排列。时间从零起算。文件里的业务标识只用于建立输入之间的引用。所有场景均可在本地用合成角色重新运行。

- **subscribe**　终端配置订阅 bays、发送窗口 window 和 priority。窗口为一次 drain 的最大通知数，priority 为正整数；priority 更高时无法同轮满足全部请求仍应分到更多机会。需在可交付通知持续存在的前提下避免任一舱一直饥饿。
- **whole**　发布 bay、session、seq 和 values，values 为各采样通道完整表示。
- **delta**　发布 bay、session、seq、base_seq 与 changes，未列通道不变。
- **event**　发布 bay、session、seq、event_id 和 kind，kind 为 started、contact_on、contact_off 或 completed。
- **drain**　终端请求获取不超过其窗口的推送。
- **applied**　终端确认应用 state_seq，只对该舱对应 session 生效。
- **ack**　终端确认 event_id，回执携带 bay 和 session。
- **disconnect**　终端离开，此前状态与确认信息独立保留。
- **resume**　终端以 seen_states 和 confirmed_events 回来。每项都明确舱和会话。
- **budget**　指定训练舱的可缓冲帧数 limit。背压后的生产者必须重送对应帧，才能以后推进其未接纳位置。
- **inspect**　值班端查看各舱或终端的当前状态。

## 资料文件

- delta-gap-and-critical-pressure.jsonl
- reconnect-after-source-restart.jsonl

## 本地资料检查

需 Python 3.11。资料工具使用标准库，无需联网。下列命令读取事件文件并检查封装格式。

```sh
python3 trace_io.py data
python3 -m unittest -v
```

资料工具输出读取的文件、事件数量及首尾秒点。业务状态由使用该事件契约的服务维护。

## 已实现的状态流中枢

业务中枢在 `state_hub.py`，只使用 Python 标准库：

- `StateHub` / `Hub` / `Middleware`：多舱、多终端、线程安全的中间件。
- `LocalSimulation`：本地仿真接口，包含 `producer(bay)`、`console(terminal)` 和 `duty`。
- `replay_trace.py`：按资料 JSONL 的原始顺序重放，并打印每次 drain、背压、恢复和检视结果。
- `json_gateway.py`：逐行 JSON 网关，可供非 Python 本地角色接入。
- `test_state_hub.py`：覆盖降载、补帧、关键事件、背压、调度公平性、换会话和断线恢复。

常用命令：

```sh
python3 -m unittest -v
python3 trace_io.py data
python3 replay_trace.py --all
python3 replay_trace.py examples/combined-slow-restart-reconnect.jsonl
cat data/delta-gap-and-critical-pressure.jsonl | python3 json_gateway.py
```

`examples/combined-slow-restart-reconnect.jsonl` 额外把“终端慢、生产端换会话、断线/resume、班中改窗口/优先级、关键事件背压后重发”放在同一段演练中。

### Python 本地接口示例

```python
from state_hub import LocalSimulation

sim = LocalSimulation()
east = sim.producer("east")
desk = sim.console("desk")

desk.subscribe(["east", "west"], window=4,
               priority={"east": 3, "west": 1})

east.whole("e1", 1, {"arm": 0})
east.delta("e1", 2, 1, {"arm": 1})
east.critical_event("e1", 3, "contact-on", "contact_on")

batch = desk.drain()
desk.applied("east", "e1", 2)
desk.ack("contact-on", bay="east", session="e1")

# 值班台检视缓存占用、发送机会和待确认人。
print(sim.duty.inspect(bay="east"))
```

班中直接再次调用 subscribe 即可修改配置：

```python
hub.subscribe("desk", window=8)
hub.subscribe("desk", priority={"east": 5, "west": 1})
```

### drain 会给出什么

`drain` 返回不超过终端 `window` 的通知，并额外分组：

- `states`：`whole` 或可直接应用的 `delta`。
- `critical_events`：必须按舱内顺序处理、逐条 ack 的关键事件。
- `recoveries`：当前没有可用完整状态时的等待/缺基版本原因。
- `deliveries`：本次按调度顺序返回的全部通知。
- `allocation`：本轮每个舱实际占用的窗口数。

状态策略：

1. 终端已应用 delta 的准确 `base_seq`，且只有这一个新采样时，可以收到该 delta。
2. 终端落后多个采样、曾经慢消费或基础帧可能已被回收时，中枢返回物化后的最新 `whole` 快照；该快照带有 `compacted: true` 和明确的 `state_seq`，可直接应用。
3. 新 session、缺少初始 whole 或 delta 基础版本无法恢复时：
   - 若已有物化全量，返回带 `recovery_reason: "session_changed" | "missing_tail_base" | ...` 的 whole；
   - 若确实没有全量，返回 `type: "recovery"`、`full_state_available: false` 和等待原因。
4. `applied` 只记录状态版本，绝不自动确认任何关键事件。

### 关键事件与缓冲

每个 bay 有独立 `budget.limit`：

- whole/delta 是可回收采样；中枢始终维护最近一份仍有效的物化状态。
- 关键事件按 bay/session/seq 排序并扇出给当时已订阅的终端。
- 已发送但未 ack 的通知保留在终端状态中；断线/resume 后会重新进入队列。
- 只要关键事件还有未确认终端，它占用的缓冲槽就不会被采样挤占或淘汰。
- buffer 没有空位容纳新关键事件时，发布结果为：

```json
{"accepted": false, "backpressure": true,
 "reason": "critical_buffer_full",
 "retry": {"type": "event", "bay": "east", "session": "e1",
           "seq": 9, "event_id": "e", "kind": "started"}}
```

对应生产端句柄会保留 `unadmitted_event`，在终端 ack 释放槽位后调用 `retry_unadmitted()` 重发。一个舱背压不会影响其他 bay 的发布、缓冲或调度。

### 调度与优先级

调度器使用带每轮公平保留的加权公平队列：

- 同一 drain 中，每个有可交付内容的订阅舱先获得至少一个机会，避免单舱占满整个窗口。
- 剩余机会按 `priority` 分配；默认正整数越大优先级越高，长期份额越多。若现场约定“数字越小越高”，可用 `StateHub(priority_lower_is_higher=True)` 或 `hub.set_priority_convention(True)` 切换。
- 跨 drain 保留 `fairness_tag`，窗口为 1 时也会轮转，高优先级舱不能让低优先级舱永久饥饿。
- 状态/恢复项在每个舱每轮最多合并为一个；相同等待原因不重复刷屏。
- `inspect(bay=...)` 的 `send_opportunities` 给出各终端的优先级、累计发送数、状态/事件/恢复分类计数和 fairness tag。

### 断线、resume 与换会话

- `disconnect` 只把终端标记为离线，不删除订阅、applied 身份、ack 记录或待确认事件。
- `resume(seen_states, confirmed_events)` 以终端实际应用的状态为恢复基准；未出现在 seen 中的已发送状态会重新提供。
- 旧 session 的未 ack 事件保持旧 `(bay, session, event_id)` 身份，必须用旧 session ack；应用新 session 状态不会覆盖该义务。
- 新 session 的 whole 建立新状态基线；旧 delta 不会与新 whole 拼接。旧采样可回收，不阻止新 session 快照恢复。

### 值班检视

```python
hub.inspect()                  # 所有 bay 和终端摘要
hub.inspect(bay="east")        # 缓存、背压、发送机会、等待 ack
hub.inspect(terminal="desk")   # 窗口、优先级、applied、待确认、state_view
hub.read_latest_state("east")  # 最近仍有效的物化状态
hub.state_status("desk", "east")
```

JSON 网关也支持直接方法调用：

```json
{"method": "inspect", "kwargs": {"bay": "east"}}
```
