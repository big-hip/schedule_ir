# -*- coding: utf-8 -*-
from typing import List, Dict
from .events import Event

def synthesize_idle(events: List[Event], stages: int) -> List[Event]:
    """
    在每个 stage 维度上，根据 compute-lane(或所有lane)已占用片段合成 Idle 段。
    这里按"所有 lane 的占用都算忙"来合并（更保守，空泡更真实）。
    如果只想以计算lane为准，可改 filter。
    """
    by_stage: Dict[int, List[Event]] = {s: [] for s in range(stages)}
    for e in events:
        by_stage[e.stage].append(e)

    out = list(events)
    for s in range(stages):
        arr = sorted(by_stage[s], key=lambda e: (e.start, e.dur))
        t = 0.0
        for e in arr:
            if e.start > t:
                # gap [t, e.start)
                idle = Event(stage=s, lane=0, start=t, dur=e.start - t, op="Idle", mini=None,micro=None)
                out.append(idle)
            t = max(t, e.start + e.dur)

        # 末尾是否需要补尾 Idle 由你们决定；通常画图不需要无穷延长的 Idle。
    return out

