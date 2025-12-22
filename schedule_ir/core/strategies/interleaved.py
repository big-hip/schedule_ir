# ppflow/strategies/interleaved.py   待定，需要再次确定
# -*- coding: utf-8 -*-
from typing import List
from ..events import Event
from ..params import ScheduleSpec

class InterleavedStrategy:
    """
    Interleaved Pipeline Parallelism:
    - 将每个 stage 拆成多个虚拟 chunk；
    - 每个 chunk 视为一个子 stage；
    - 前向按照 (stage, chunk) 顺序流水；
    - 反向则逆序执行；
    - chunk 内部使用对应 stage 的 forward/backward 耗时 / 虚拟划分数；
    """
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        N = spec.num_mini
        M = spec.num_micro
        lane_compute = spec.lanes.get("compute", 0)

        # 虚拟分区数（若未指定则默认为1）
        v = int(spec.meta.get("virtual_chunks", 2)) if hasattr(spec, "meta") else 2
        total_substages = S * v

        F = [f / v for f in spec.forward for _ in range(v)]
        B = [b / v for b in spec.backward for _ in range(v)]

        events: List[Event] = []
        stage_ready = [0.0] * total_substages
        f_finish = {}

        # Forward phase
        for n in range(N):
            for m in range(M):
                prev_finish = 0.0
                for i in range(total_substages):
                    s, chunk = divmod(i, v)
                    start = max(stage_ready[i], prev_finish)
                    dur = F[i]
                    ev = Event(stage=s, lane=lane_compute,
                               start=start, dur=dur,
                               op=f"F{chunk}", mini=n, micro=m)
                    events.append(ev)
                    finish = start + dur
                    stage_ready[i] = finish
                    f_finish[(n, m, i)] = finish
                    prev_finish = finish

            # Backward phase
            for m in reversed(range(M)):
                next_grad_ready = None
                for i in reversed(range(total_substages)):
                    s, chunk = divmod(i, v)
                    deps = [f_finish[(n, m, i)], stage_ready[i]]
                    if next_grad_ready is not None:
                        deps.append(next_grad_ready)
                    start = max(deps)
                    dur = B[i]
                    ev = Event(stage=s, lane=lane_compute,
                               start=start, dur=dur,
                               op=f"B{chunk}", mini=n,micro=m )
                    events.append(ev)
                    finish = start + dur
                    stage_ready[i] = finish
                    next_grad_ready = finish

        return events
