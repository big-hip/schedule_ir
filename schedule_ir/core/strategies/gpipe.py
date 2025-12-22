# -*- coding: utf-8 -*-
from typing import List
from ..events import Event
from ..params import ScheduleSpec

class GPipeStrategy:
    """
    经典 GPipe：先完成所有 F（含流水线 warmup/推进），再做所有 B（flush）。
    该策略简洁稳健，便于后续插入通信与算子。
    """
    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        M = spec.num_micro
        N = spec.num_mini
        F = spec.forward
        B = spec.backward
        lane_compute = spec.lanes.get("compute", 0)

        events: List[Event] = []
        # stage可用时间（贯穿多个mini，顺序推进）
        stage_ready = [0.0] * S

        # 记录每个 (mini, micro, stage) 的 F 结束时间，供 B 依赖
        f_finish = {}  # (n, m, s) -> time

        # 1) Forward 全部排完
        for n in range(N):
            for m in range(M):
                # 从 s=0 推到 s=S-1
                prev_finish = 0.0
                for s in range(S):
                    start = max(stage_ready[s], prev_finish)
                    dur = F[s]
                    ev = Event(stage=s, lane=lane_compute, start=start, dur=dur, op="F", mini=n, micro=m )
                    events.append(ev)
                    finish = start + dur
                    stage_ready[s] = finish
                    f_finish[(n, m, s)] = finish
                    prev_finish = finish

            # 2) Backward：GPipe flush（F 完成后开始 B，按 m 逆序，s 逆序）
            for m in reversed(range(M)):
                next_grad_ready = None  # 来自 s+1
                for s in reversed(range(S)):
                    # 依赖：本层该 micro 的 F 完成；以及上游梯度可用（来自 s+1 的 B 完成）
                    deps = [f_finish[(n, m, s)], stage_ready[s]]
                    if next_grad_ready is not None:
                        deps.append(next_grad_ready)
                    start = max(deps)
                    dur = B[s]
                    ev = Event(stage=s, lane=lane_compute, start=start, dur=dur, op="B",mini=n, micro=m )
                    events.append(ev)
                    finish = start + dur
                    stage_ready[s] = finish
                    next_grad_ready = finish

        return events
