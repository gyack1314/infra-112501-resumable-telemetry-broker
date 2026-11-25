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
