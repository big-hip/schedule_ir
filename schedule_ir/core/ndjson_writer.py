# -*- coding: utf-8 -*-
from typing import List, TextIO
from .events import Event

HEADER_LINES_ZH = [
    "// event_id：整型，全局唯一、稳定的事件编号（包含 Idle）。",
    "// stage：所属 stage。",
    "// lane：所属并发通道（计算/通信/自定义等）。",
    "// start：事件开始时间（单位与配置一致，示例为'帧'或 μs）。",
    "// dur：事件时长。",
    "// op：操作类型，比如 F/B/Send/Recv/AllReduce/Idle/自定义等。",
    "// mini：minibatch 序号（外层大批次）若 op=\"Idle\" 通常为 null。",
    "// micro：microbatch 序号（mini 的切分子批）若 op=\"Idle\" 通常为 null。",
    ""
]

def write_ndjson(f: TextIO, events: List[Event], include_comments: bool = True):
    if include_comments:
        for line in HEADER_LINES_ZH:
            f.write(line + "\n")
    for e in events:
        f.write(e.to_json() + "\n")

