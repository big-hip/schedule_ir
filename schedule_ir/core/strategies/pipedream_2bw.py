# -*- coding: utf-8 -*-
"""
PipeDream-2BW (Two-Bubble Warmup) pipeline schedule strategy.

Compared to 1F1B, 2BW allows two forward "bubbles" during warmup and flush,
reducing overall pipeline idle time. The key idea is to start the backward pass
two microbatches earlier than the final forward ends, resulting in higher utilization.

Author: ChatGPT (2025)
"""

from typing import List
from ..events import Event
from ..params import ScheduleSpec


class PipeDream2BWStrategy:
    """
    PipeDream-2BW scheduling (Two-Bubble Warmup).

    Stage behavior:
      - Warmup: fill S + 1 microbatches
      - Steady: each step executes 1 forward and 1 backward in overlap
      - Flush: drain the pipeline with S + 1 backward steps

    Differences from 1F1B:
      - Forward runs one additional round before starting the first backward.
      - Backward starts earlier (two bubbles lead and tail).
      - Maintains same throughput in steady phase, with reduced idle regions.

    """

    def build_fb_events(self, spec: ScheduleSpec) -> List[Event]:
        S = spec.num_stages
        M = spec.num_micro
        N = spec.num_mini
        Fdur = spec.forward
        Bdur = spec.backward
        lane_compute = spec.lanes.get("compute", 0)

        events: List[Event] = []
        stage_free = [0.0] * S  # cumulative stage availability

        for n in range(N):
            # -----------------
            # 1. Warmup: fill S+1 microbatches
            # -----------------
            for warm in range(S + 1):
                for s in range(min(warm + 1, S)):
                    m = warm - s
                    if m >= M:
                        continue
                    start = max(stage_free[s], stage_free[s - 1] if s > 0 else 0.0)
                    dur = Fdur[s]
                    events.append(Event(stage=s, lane=lane_compute,
                                        start=start, dur=dur,
                                        op="F", mini=n, micro=m))
                    stage_free[s] = start + dur

            # -----------------
            # 2. Steady phase: alternate F/B with two bubbles lead
            # -----------------
            for step in range(M - (S + 1)):
                for s in range(S):
                    # forward micro index
                    f_m = step + s + 1
                    if 0 <= f_m < M:
                        start_f = max(stage_free[s], stage_free[s - 1] if s > 0 else 0.0)
                        dur_f = Fdur[s]
                        events.append(Event(stage=s, lane=lane_compute,
                                            start=start_f, dur=dur_f,
                                            op="F", mini=n, micro=f_m))
                        stage_free[s] = start_f + dur_f

                    # backward micro index (2-bubble offset)
                    b_m = step + s - (S - 1)
                    if 0 <= b_m < M:
                        start_b = stage_free[s]
                        dur_b = Bdur[s]
                        events.append(Event(stage=s, lane=lane_compute,
                                            start=start_b, dur=dur_b,
                                            op="B", mini=n, micro=b_m))
                        stage_free[s] = start_b + dur_b

            # -----------------
            # 3. Flush phase: drain remaining backward
            # -----------------
            for flush in range(S + 1):
                for s in reversed(range(S)):
                    m = M - S - 1 + flush + (S - 1 - s)
                    if m < 0 or m >= M:
                        continue
                    start = stage_free[s]
                    dur = Bdur[s]
                    events.append(Event(stage=s, lane=lane_compute,
                                        start=start, dur=dur,
                                        op="B", mini=n, micro=m,))
                    stage_free[s] = start + dur

        return events
